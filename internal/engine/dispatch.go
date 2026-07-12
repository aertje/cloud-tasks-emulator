package engine

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/oidc"
)

// httpRetryReason and appEngineRetryReason are the X-*-TaskRetryReason values
// real Cloud Tasks sends on a retry after a 5XX response, captured from
// production (see conformance/golden/dispatch.json). The HTTP family sends an
// empty reason; the App Engine family sends "App Error".
const (
	httpRetryReason      = ""
	appEngineRetryReason = "App Error"
)

// addOptionalRetryHeaders adds the two retry-only dispatch headers -
// X-<family>-TaskPreviousResponse and X-<family>-TaskRetryReason - to injected,
// but only when this dispatch retries an attempt that received an HTTP response
// (previousResponseCode > 0). Both are absent on the first attempt, and when the
// previous attempt got no response (transport failure), matching production.
func addOptionalRetryHeaders(injected map[string]string, prefix string, previousResponseCode int, retryReason string) {
	if previousResponseCode <= 0 {
		return
	}
	injected[prefix+"TaskPreviousResponse"] = strconv.Itoa(previousResponseCode)
	injected[prefix+"TaskRetryReason"] = retryReason
}

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

	// Capture the previous attempt's response code before its Attempt is
	// overwritten below, so this dispatch can report it via
	// X-*-TaskPreviousResponse (retries only).
	previousResponseCode := 0
	if prev := task.state.LastAttempt; prev != nil {
		previousResponseCode = prev.ResponseCode
	}

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

	// PreviousResponseCode rides on the returned snapshot only, not the retained
	// state, which would otherwise carry a stale value into the next dispatch.
	frozen := task.state
	frozen.PreviousResponseCode = previousResponseCode
	return frozen
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
	attempt.ResponseCode = statusCode
	attempt.ResponseStatus = &AttemptStatus{
		Code:    rpcCode,
		Message: fmt.Sprintf("%s(%d): HTTP status code %d", rpcCodeName, rpcCode, statusCode),
	}
	task.state.LastAttempt = &attempt

	task.state.ResponseCount++
	// A received non-5XX response counts toward the HTTP target's execution
	// count, which excludes 5XX failures (see TaskState.ExecutionCount). A
	// transport failure (statusCode -1) received no response and never counts.
	if statusCode >= 100 && (statusCode < 500 || statusCode > 599) {
		task.state.ExecutionCount++
	}
}

func (task *Task) reschedule(retry bool, statusCode int) {
	logger := task.queue.logger

	if statusCode >= 200 && statusCode <= 299 {
		logger.Info("task done", "task", task.state.Name, "status", statusCode)
		task.markDone()
		return
	}

	logger.Warn("task execution error", "task", task.state.Name, "status", statusCode)
	if !retry {
		return
	}

	task.stateMutex.Lock()
	dispatchCount := task.state.DispatchCount
	maxAttempts := task.queue.state.RetryConfig.MaxAttempts
	task.stateMutex.Unlock()

	if dispatchCount >= maxAttempts {
		logger.Warn("task exhausted retries", "task", task.state.Name, "attempts", dispatchCount)
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
	Dispatch(ctx context.Context, state TaskState, oidcCfg oidc.Config) int
}

// HTTPDispatcher is the production Dispatcher; it delivers tasks over HTTP.
type HTTPDispatcher struct {
	// logger receives dispatch diagnostics. The engine injects its resolved
	// logger when it constructs the default dispatcher, so it is never nil in
	// production; the zero value falls back to slog.Default() defensively.
	logger *slog.Logger

	// transport, when non-nil, overrides the transport used for deliveries. The
	// engine sets it to an InsecureSkipVerify transport when insecure mode is
	// enabled; nil uses http.DefaultTransport.
	transport http.RoundTripper
}

// Dispatch delivers the task over HTTP.
func (h HTTPDispatcher) Dispatch(ctx context.Context, state TaskState, oidcCfg oidc.Config) int {
	logger := h.logger
	if logger == nil {
		logger = slog.Default()
	}
	return dispatch(ctx, state, oidcCfg, logger, h.transport)
}

// insecureTransport clones the default transport and disables TLS certificate
// verification. Used only when the operator explicitly opts into insecure mode
// (development against self-signed targets); see Options.InsecureSkipTLSVerify.
func insecureTransport() *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{}
	}
	tr.TLSClientConfig.InsecureSkipVerify = true
	return tr
}

// dispatch performs a single HTTP delivery for the supplied task-state snapshot
// and returns the target's HTTP status code (or -1 if the request could not be
// built or sent). It never mutates the snapshot: injected Cloud Tasks headers
// are merged into a fresh request header map, because the task's live header map
// is read concurrently by gRPC handlers. ctx bounds the request's lifetime (see
// Queue.ctx); DispatchDeadline is still enforced via the http.Client timeout.
func dispatch(ctx context.Context, state TaskState, oidcCfg oidc.Config, logger *slog.Logger, transport http.RoundTripper) int {
	client := &http.Client{Timeout: state.DispatchDeadline, Transport: transport}

	nameParts, ok := parseTaskName(state.Name)
	if !ok {
		logger.Error("dispatch: invalid task name", "task", state.Name)
		return -1
	}

	headerQueueName := nameParts.queueId
	headerTaskName := nameParts.taskId
	headerTaskRetryCount := fmt.Sprintf("%v", state.DispatchCount-1)
	// The two families count executions differently: the HTTP header excludes
	// 5XX failures (state.ExecutionCount), the App Engine header counts every
	// response (state.ResponseCount). See TaskState.ExecutionCount.
	headerHTTPExecutionCount := fmt.Sprintf("%v", state.ExecutionCount)
	headerAppEngineExecutionCount := fmt.Sprintf("%v", state.ResponseCount)
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
		injected = map[string]string{
			"User-Agent":                      "Google-Cloud-Tasks",
			"X-CloudTasks-QueueName":          headerQueueName,
			"X-CloudTasks-TaskName":           headerTaskName,
			"X-CloudTasks-TaskExecutionCount": headerHTTPExecutionCount,
			"X-CloudTasks-TaskRetryCount":     headerTaskRetryCount,
			"X-CloudTasks-TaskETA":            headerTaskETA,
		}
		addOptionalRetryHeaders(injected, "X-CloudTasks-", state.PreviousResponseCode, httpRetryReason)

		if auth := state.HTTPRequest.OIDCToken; auth != nil {
			tokenStr, err := oidcCfg.CreateToken(auth.ServiceAccountEmail, url, auth.Audience)
			if err != nil {
				logger.Error("dispatch: create OIDC token", "task", state.Name, "err", err)
				return -1
			}
			injected["Authorization"] = "Bearer " + tokenStr
		}
	case state.AppEngineHTTPRequest != nil:
		ae := state.AppEngineHTTPRequest

		method = ae.Method
		url = ae.AppEngineRouting.Host + ae.RelativeURI
		body = ae.Body
		srcHeaders = ae.Headers

		// These headers are only set on dispatch, see https://cloud.google.com/tasks/docs/reference/rpc/google.cloud.tasks.v2#google.cloud.tasks.v2.AppEngineHttpRequest
		injected = map[string]string{
			"X-AppEngine-QueueName":          headerQueueName,
			"X-AppEngine-TaskName":           headerTaskName,
			"X-AppEngine-TaskRetryCount":     headerTaskRetryCount,
			"X-AppEngine-TaskExecutionCount": headerAppEngineExecutionCount,
			"X-AppEngine-TaskETA":            headerTaskETA,
		}
		addOptionalRetryHeaders(injected, "X-AppEngine-", state.PreviousResponseCode, appEngineRetryReason)
	default:
		logger.Error("dispatch: task has neither HTTPRequest nor AppEngineHTTPRequest", "task", state.Name)
		return -1
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		// A client-supplied URL that passes create-time validation (starts with
		// http(s)://) but is not fully parseable by net/url reaches here: Cloud
		// Tasks accepts such URLs at create, so this is a failed delivery of a
		// valid task, not an emulator error.
		logger.Warn("dispatch: build request", "task", state.Name, "err", err)
		return -1
	}

	// Merge the task's own headers with the injected Cloud Tasks headers.
	// Injected headers win on any case-insensitive collision: HTTP field names
	// are case-insensitive and Cloud Tasks does not emit repeated headers, so a
	// task header differing only in casing from an injected one is dropped
	// rather than sent alongside it. Task headers otherwise keep their original
	// casing (a direct set, not Header.Set), matching what Cloud Tasks stores.
	injectedFold := make(map[string]struct{}, len(injected))
	for k := range injected {
		injectedFold[strings.ToLower(k)] = struct{}{}
	}
	for k, v := range srcHeaders {
		if _, clash := injectedFold[strings.ToLower(k)]; clash {
			continue
		}
		req.Header[k] = []string{v}
	}
	for k, v := range injected {
		req.Header[k] = []string{v}
	}

	resp, err := client.Do(req)
	if err != nil {
		// A target being unreachable (connection refused, timeout, DNS) is a
		// normal, expected condition that retries exist to handle; it is not an
		// emulator error. reschedule logs the failed attempt at Warn, so this
		// line only carries the underlying transport cause for debugging.
		logger.Debug("dispatch: deliver request", "task", state.Name, "err", err)
		return -1
	}
	defer func() { _ = resp.Body.Close() }()

	return resp.StatusCode
}

func (task *Task) doDispatch(retry bool, state TaskState) {
	task.queue.logger.Debug("dispatching task attempt",
		"task", state.Name, "attempt", state.DispatchCount)

	respCode := task.queue.dispatcher.Dispatch(task.queue.ctx, state, task.queue.oidcCfg)

	task.queue.logger.Debug("task attempt completed",
		"task", state.Name, "attempt", state.DispatchCount, "code", respCode)

	updateStateAfterDispatch(task, respCode)
	task.reschedule(retry, respCode)
}
