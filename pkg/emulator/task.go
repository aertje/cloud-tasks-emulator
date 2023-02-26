package emulator

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Task holds all internals for a task
type Task struct {
	queue *Queue

	state *taskspb.Task

	cancel chan bool

	stateMutex sync.Mutex
}

// NewTask creates a new task for the specified queue. It updates the supplied queue
// state to its initial values.
func NewTask(queue *Queue, taskState *taskspb.Task) *Task {
	task := &Task{
		queue:  queue,
		state:  taskState,
		cancel: make(chan bool),
	}

	task.setInitialTaskState(queue.state.GetName())

	return task
}

func (t *Task) setInitialTaskState(queueName string) {
	t.stateMutex.Lock()
	defer t.stateMutex.Unlock()

	if t.state.GetName() == "" {
		taskID := strconv.FormatUint(uint64(rand.Uint64()), 10)
		t.state.Name = queueName + "/tasks/" + taskID
	}

	t.state.CreateTime = timestamppb.Now()
	// For some reason the cloud does not set nanos
	t.state.CreateTime.Nanos = 0

	if t.state.GetScheduleTime() == nil {
		t.state.ScheduleTime = timestamppb.Now()
	}
	if t.state.GetDispatchDeadline() == nil {
		t.state.DispatchDeadline = &durationpb.Duration{Seconds: 600}
	}

	// This should probably be set somewhere else?
	t.state.View = taskspb.Task_BASIC

	httpRequest := t.state.GetHttpRequest()

	if httpRequest != nil {
		if httpRequest.GetHttpMethod() == taskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
			httpRequest.HttpMethod = taskspb.HttpMethod_POST
		}
		if httpRequest.GetHeaders() == nil {
			httpRequest.Headers = make(map[string]string)
		}
		// Override
		httpRequest.Headers["User-Agent"] = "Google-Cloud-Tasks"
	}

	appEngineHTTPRequest := t.state.GetAppEngineHttpRequest()

	if appEngineHTTPRequest != nil {
		if appEngineHTTPRequest.GetHttpMethod() == taskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
			appEngineHTTPRequest.HttpMethod = taskspb.HttpMethod_POST
		}
		if appEngineHTTPRequest.GetHeaders() == nil {
			appEngineHTTPRequest.Headers = make(map[string]string)
		}

		appEngineHTTPRequest.Headers["User-Agent"] = "AppEngine-Google; (+http://code.google.com/appengine)"

		if appEngineHTTPRequest.GetBody() != nil {
			if _, ok := appEngineHTTPRequest.GetHeaders()["Content-Type"]; !ok {
				appEngineHTTPRequest.Headers["Content-Type"] = "application/octet-stream"
			}
		}

		if appEngineHTTPRequest.GetAppEngineRouting() == nil {
			appEngineHTTPRequest.AppEngineRouting = &taskspb.AppEngineRouting{}
		}

		if appEngineHTTPRequest.GetAppEngineRouting().Host == "" {
			var host, domainSeparator string

			emulatorHost := os.Getenv("APP_ENGINE_EMULATOR_HOST")

			if emulatorHost == "" {
				// TODO: the new route format for appengine is <PROJECT_ID>.<REGION_ID>.r.appspot.com
				// TODO: support custom domains
				// https://cloud.google.com/appengine/docs/standard/python/how-requests-are-routed
				host = "https://" + parseTaskName(t.state.GetName()).projectID + ".appspot.com"
				domainSeparator = "-dot-"
			} else {
				host = emulatorHost
				domainSeparator = "."
			}

			hostURL, err := url.Parse(host)
			if err != nil {
				panic(err)
			}

			if appEngineHTTPRequest.GetAppEngineRouting().GetService() != "" {
				hostURL.Host = appEngineHTTPRequest.GetAppEngineRouting().GetService() + domainSeparator + hostURL.Host
			}
			if appEngineHTTPRequest.GetAppEngineRouting().GetVersion() != "" {
				hostURL.Host = appEngineHTTPRequest.GetAppEngineRouting().GetVersion() + domainSeparator + hostURL.Host
			}
			if appEngineHTTPRequest.GetAppEngineRouting().GetInstance() != "" {
				hostURL.Host = appEngineHTTPRequest.GetAppEngineRouting().GetInstance() + domainSeparator + hostURL.Host
			}

			appEngineHTTPRequest.GetAppEngineRouting().Host = hostURL.String()
		}

		if appEngineHTTPRequest.GetRelativeUri() == "" {
			appEngineHTTPRequest.RelativeUri = "/"
		}
	}
}

func (t *Task) updateStateForReschedule() {
	t.stateMutex.Lock()
	defer t.stateMutex.Unlock()

	retryConfig := t.queue.state.GetRetryConfig()

	minBackoff := retryConfig.GetMinBackoff().AsDuration()
	maxBackoff := retryConfig.GetMaxBackoff().AsDuration()

	doubling := t.state.GetDispatchCount() - 1
	if doubling > retryConfig.MaxDoublings {
		doubling = retryConfig.MaxDoublings
	}
	backoff := minBackoff * time.Duration(1<<uint32(doubling))
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	protoBackoff := durationpb.New(backoff)
	prevScheduleTime := t.state.GetScheduleTime()

	// Avoid int32 nanos overflow
	scheduleNanos := int64(prevScheduleTime.GetNanos()) + int64(protoBackoff.GetNanos())
	scheduleSeconds := prevScheduleTime.GetSeconds() + protoBackoff.GetSeconds()
	if scheduleNanos >= 1e9 {
		scheduleSeconds++
		scheduleNanos -= 1e9
	}

	t.state.ScheduleTime = &timestamppb.Timestamp{
		Nanos:   int32(scheduleNanos),
		Seconds: scheduleSeconds,
	}
}

func (t *Task) updateStateForDispatch() *taskspb.Task {
	t.stateMutex.Lock()
	defer t.stateMutex.Unlock()

	taskState := t.state

	dispatchTime := timestamppb.Now()

	taskState.LastAttempt = &taskspb.Attempt{
		ScheduleTime: &timestamppb.Timestamp{
			Nanos:   taskState.GetScheduleTime().GetNanos(),
			Seconds: taskState.GetScheduleTime().GetSeconds(),
		},
		DispatchTime: dispatchTime,
	}

	taskState.DispatchCount++

	if taskState.GetFirstAttempt() == nil {
		taskState.FirstAttempt = &taskspb.Attempt{
			DispatchTime: dispatchTime,
		}
	}

	frozenTaskState := proto.Clone(taskState).(*taskspb.Task)

	return frozenTaskState
}

func (t *Task) updateStateAfterDispatch(statusCode int) *taskspb.Task {
	t.stateMutex.Lock()
	t.stateMutex.Unlock()

	rpcCode := toRPCStatusCode(statusCode)
	rpcCodeName := toCodeName(rpcCode)

	lastAttempt := t.state.GetLastAttempt()

	lastAttempt.ResponseTime = timestamppb.Now()
	lastAttempt.ResponseStatus = &rpcstatus.Status{
		Code:    int32(rpcCode),
		Message: fmt.Sprintf("%s(%d): HTTP status code %d", rpcCodeName, rpcCode, statusCode),
	}

	t.state.ResponseCount++

	frozenTaskState := proto.Clone(t.state).(*taskspb.Task)

	return frozenTaskState
}

func (t *Task) reschedule(retry bool, statusCode int) {
	if statusCode >= 200 && statusCode <= 299 {
		log.Printf("Task %v done with status code %d", t.state.GetName(), statusCode)
		t.queue.taskDone(t)
	} else {
		log.Printf("Task %v error with status code %d", t.state.GetName(), statusCode)
		if retry {
			retryConfig := t.queue.state.GetRetryConfig()

			if t.state.DispatchCount >= retryConfig.GetMaxAttempts() {
				// TODO: Check if prod cloud tasks removes the task or not
				log.Printf("Task %v ran out of attempts (%d)", t.state.GetName(), retryConfig.GetMaxAttempts())
			} else {
				t.updateStateForReschedule()
				t.Schedule()
			}
		}
	}
}

func (t *Task) dispatch(retry bool) (int, error) {
	client := &http.Client{}
	client.Timeout = t.state.GetDispatchDeadline().AsDuration()

	var req *http.Request
	var headers map[string]string

	httpRequest := t.state.GetHttpRequest()
	appEngineHTTPRequest := t.state.GetAppEngineHttpRequest()

	scheduled := t.state.GetScheduleTime().AsTime()
	nameParts := parseTaskName(t.state.GetName())

	headerQueueName := nameParts.queueID
	headerTaskName := nameParts.taskID
	headerTaskRetryCount := fmt.Sprintf("%v", t.state.GetDispatchCount()-1)
	headerTaskExecutionCount := fmt.Sprintf("%v", t.state.GetResponseCount())
	headerTaskETA := fmt.Sprintf("%f", float64(scheduled.UnixNano())/1e9)

	if httpRequest != nil {
		method := toHTTPMethod(httpRequest.GetHttpMethod())

		req, _ = http.NewRequest(method, httpRequest.GetUrl(), bytes.NewBuffer(httpRequest.GetBody()))

		headers = httpRequest.GetHeaders()

		if auth := httpRequest.GetOidcToken(); auth != nil {
			// tokenStr := oidc.CreateOIDCToken(auth.ServiceAccountEmail, httpRequest.GetUrl(), auth.Audience)
			email := auth.GetServiceAccountEmail()
			audience := auth.GetAudience()
			if audience == "" {
				audience = httpRequest.GetUrl()
			}
			token, err := t.queue.server.oidcTokenCreator.CreateOIDCToken(email, email, audience)
			if err != nil {
				return 0, fmt.Errorf("unable to generate OIDC token: %w", err)
			}
			headers["Authorization"] = "Bearer " + token
		}

		// Headers as per https://cloud.google.com/tasks/docs/creating-http-target-tasks#handler
		// TODO: optional headers
		headers["X-CloudTasks-QueueName"] = headerQueueName
		headers["X-CloudTasks-TaskName"] = headerTaskName
		headers["X-CloudTasks-TaskExecutionCount"] = headerTaskExecutionCount
		headers["X-CloudTasks-TaskRetryCount"] = headerTaskRetryCount
		headers["X-CloudTasks-TaskETA"] = headerTaskETA
	} else if appEngineHTTPRequest != nil {
		method := toHTTPMethod(appEngineHTTPRequest.GetHttpMethod())

		host := appEngineHTTPRequest.GetAppEngineRouting().GetHost()

		url := host + appEngineHTTPRequest.GetRelativeUri()

		req, _ = http.NewRequest(method, url, bytes.NewBuffer(appEngineHTTPRequest.GetBody()))

		headers = appEngineHTTPRequest.GetHeaders()

		// These headers are only set on dispatch, see https://cloud.google.com/tasks/docs/reference/rpc/google.cloud.tasks.v2#google.cloud.tasks.v2.AppEngineHttpRequest
		// TODO: optional headers
		headers["X-AppEngine-QueueName"] = headerQueueName
		headers["X-AppEngine-TaskName"] = headerTaskName
		headers["X-AppEngine-TaskRetryCount"] = headerTaskRetryCount
		headers["X-AppEngine-TaskExecutionCount"] = headerTaskExecutionCount
		headers["X-AppEngine-TaskETA"] = headerTaskETA
	}

	for k, v := range headers {
		// Uses a direct set to maintain capitalization
		// TODO: figure out a way to test these, as the Go net/http client lib overrides the incoming header capitalization
		req.Header[k] = []string{v}
	}

	resp, err := t.queue.server.httpDoer.Do(req)
	if err != nil {
		return 0, fmt.Errorf("unable to perform HTTP request: %w", err)
	}
	defer resp.Body.Close() // ignore error

	return resp.StatusCode, nil
}

func (t *Task) doDispatch(retry bool) error {
	respCode, err := t.dispatch(retry)
	if err != nil {
		return fmt.Errorf("error dispatching task: %w", err)
	}

	t.updateStateAfterDispatch(respCode)
	t.reschedule(retry, respCode)

	return nil
}

// Attempt tries to execute a task
func (t *Task) Attempt() error {
	t.updateStateForDispatch()

	return t.doDispatch(true)
}

// Run runs the task outside of the normal queueing mechanism.
// This method is called directly by request.
func (t *Task) Run() *taskspb.Task {
	taskState := t.updateStateForDispatch()

	go t.doDispatch(false)

	return taskState
}

// Delete cancels the task if it is queued for execution.
// This method is called directly by request.
func (t *Task) Delete() {
	close(t.cancel)
}

// Schedule schedules the task for execution.
// It is initially called by the queue, later by the task reschedule.
func (t *Task) Schedule() {
	scheduled := t.state.GetScheduleTime().AsTime()

	fromNow := time.Until(scheduled)

	go func() {
		select {
		case <-time.After(fromNow):
			t.queue.fire <- t
			return
		case <-t.cancel:
			t.queue.taskDone(t)
			return
		}
	}()
}
