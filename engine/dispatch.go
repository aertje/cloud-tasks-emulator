package engine

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func updateStateForReschedule(task *Task) {
	task.stateMutex.Lock()
	defer task.stateMutex.Unlock()

	retryConfig := task.queue.state.RetryConfig

	doubling := min(task.state.DispatchCount-1, retryConfig.MaxDoublings)
	backoff := min(retryConfig.MinBackoff*time.Duration(1<<uint32(doubling)), retryConfig.MaxBackoff)

	task.state.ScheduleTime = task.state.ScheduleTime.Add(backoff)
}

func updateStateForDispatch(task *Task) TaskState {
	task.stateMutex.Lock()
	defer task.stateMutex.Unlock()

	dispatchTime := time.Now()

	task.state.LastAttempt = &Attempt{
		ScheduleTime: task.state.ScheduleTime,
		DispatchTime: dispatchTime,
	}

	task.state.DispatchCount++

	if task.state.FirstAttempt == nil {
		task.state.FirstAttempt = &Attempt{
			DispatchTime: dispatchTime,
		}
	}

	return task.state
}

func updateStateAfterDispatch(task *Task, statusCode int) {
	task.stateMutex.Lock()
	defer task.stateMutex.Unlock()

	rpcCode := toRPCStatusCode(statusCode)
	rpcCodeName := toCodeName(rpcCode)

	// Copy-on-write: publish a fresh Attempt rather than mutating the one already
	// handed out through State() snapshots, whose *Attempt is shared shallowly
	// and read (unlocked) by the gRPC edge via taskToProto.
	attempt := *task.state.LastAttempt
	attempt.ResponseTime = time.Now()
	attempt.ResponseStatus = &AttemptStatus{
		Code:    rpcCode,
		Message: fmt.Sprintf("%s(%d): HTTP status code %d", rpcCodeName, rpcCode, statusCode),
	}
	task.state.LastAttempt = &attempt

	task.state.ResponseCount++
}

func (task *Task) reschedule(retry bool, statusCode int) {
	if statusCode >= 200 && statusCode <= 299 {
		log.Println("Task done")
		task.markDone()
		return
	}

	log.Println("Task exec error with status " + strconv.Itoa(statusCode))
	if !retry {
		return
	}

	task.stateMutex.Lock()
	dispatchCount := task.state.DispatchCount
	maxAttempts := task.queue.state.RetryConfig.MaxAttempts
	task.stateMutex.Unlock()

	if dispatchCount >= maxAttempts {
		log.Println("Ran out of attempts")
		task.markDone()
		return
	}

	updateStateForReschedule(task)
	task.Schedule()
}

// Dispatcher performs a single delivery attempt for a task-state snapshot and
// returns the resulting HTTP-equivalent status code (or -1 on transport
// failure). ctx bounds the delivery to the queue's lifetime (see Queue.ctx):
// it is not the gRPC request that created the task, which is long gone by
// dispatch time. The default implementation (HTTPDispatcher) delivers over
// HTTP; tests inject a fake to exercise queue/task lifecycle and retry
// behaviour without real network I/O.
type Dispatcher interface {
	Dispatch(ctx context.Context, state TaskState, oidc *OIDCConfig) int
}

// HTTPDispatcher is the production Dispatcher; it delivers tasks over HTTP.
type HTTPDispatcher struct{}

// Dispatch delivers the task over HTTP.
func (HTTPDispatcher) Dispatch(ctx context.Context, state TaskState, oidc *OIDCConfig) int {
	return dispatch(ctx, state, oidc)
}

// dispatch performs a single HTTP delivery for the supplied task-state snapshot
// and returns the target's HTTP status code (or -1 if the request could not be
// built or sent). It never mutates the snapshot: injected Cloud Tasks headers
// are merged into a fresh request header map, because the task's live header map
// is read concurrently by gRPC handlers. ctx bounds the request's lifetime (see
// Queue.ctx); DispatchDeadline is still enforced via the http.Client timeout.
func dispatch(ctx context.Context, state TaskState, oidc *OIDCConfig) int {
	client := &http.Client{Timeout: state.DispatchDeadline}

	nameParts, ok := parseTaskName(state.Name)
	if !ok {
		fmt.Fprintf(os.Stderr, "dispatch: invalid task name %q\n", state.Name)
		return -1
	}

	headerQueueName := nameParts.queueId
	headerTaskName := nameParts.taskId
	headerTaskRetryCount := fmt.Sprintf("%v", state.DispatchCount-1)
	headerTaskExecutionCount := fmt.Sprintf("%v", state.ResponseCount)
	headerTaskETA := fmt.Sprintf("%f", float64(state.ScheduleTime.UnixNano())/1e9)

	var (
		method     string
		url        string
		body       []byte
		srcHeaders map[string]string
		injected   map[string]string
	)

	switch {
	case state.HTTPRequest != nil:
		method = state.HTTPRequest.Method
		url = state.HTTPRequest.URL
		body = state.HTTPRequest.Body
		srcHeaders = state.HTTPRequest.Headers

		// Headers as per https://cloud.google.com/tasks/docs/creating-http-target-tasks#handler
		// TODO: optional headers
		injected = map[string]string{
			"X-CloudTasks-QueueName":          headerQueueName,
			"X-CloudTasks-TaskName":           headerTaskName,
			"X-CloudTasks-TaskExecutionCount": headerTaskExecutionCount,
			"X-CloudTasks-TaskRetryCount":     headerTaskRetryCount,
			"X-CloudTasks-TaskETA":            headerTaskETA,
		}

		if auth := state.HTTPRequest.OIDCToken; auth != nil {
			tokenStr := oidc.CreateToken(auth.ServiceAccountEmail, url, auth.Audience)
			injected["Authorization"] = "Bearer " + tokenStr
		}
	case state.AppEngineHTTPRequest != nil:
		ae := state.AppEngineHTTPRequest

		method = ae.Method
		url = ae.AppEngineRouting.Host + ae.RelativeURI
		body = ae.Body
		srcHeaders = ae.Headers

		// These headers are only set on dispatch, see https://cloud.google.com/tasks/docs/reference/rpc/google.cloud.tasks.v2#google.cloud.tasks.v2.AppEngineHttpRequest
		// TODO: optional headers
		injected = map[string]string{
			"X-AppEngine-QueueName":          headerQueueName,
			"X-AppEngine-TaskName":           headerTaskName,
			"X-AppEngine-TaskRetryCount":     headerTaskRetryCount,
			"X-AppEngine-TaskExecutionCount": headerTaskExecutionCount,
			"X-AppEngine-TaskETA":            headerTaskETA,
		}
	default:
		fmt.Fprintf(os.Stderr, "dispatch: task %q has neither HTTPRequest nor AppEngineHTTPRequest\n", state.Name)
		return -1
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dispatch: build request for %q: %v\n", state.Name, err)
		return -1
	}

	// Merge the task's own headers and the injected Cloud Tasks headers into the
	// request's fresh header map. Injected headers win on collision.
	// Uses a direct set to maintain capitalization.
	// TODO: figure out a way to test these, as the Go net/http client lib overrides the incoming header capitalization
	for k, v := range srcHeaders {
		req.Header[k] = []string{v}
	}
	for k, v := range injected {
		req.Header[k] = []string{v}
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return -1
	}
	defer resp.Body.Close()

	return resp.StatusCode
}

func (task *Task) doDispatch(retry bool, state TaskState) {
	respCode := task.queue.dispatcher.Dispatch(task.queue.ctx, state, task.queue.oidc)

	updateStateAfterDispatch(task, respCode)
	task.reschedule(retry, respCode)
}
