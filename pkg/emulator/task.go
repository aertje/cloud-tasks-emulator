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
	"github.com/aertje/cloud-tasks-emulator/pkg/oidc"
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

	onDone func(*Task)

	stateMutex sync.Mutex

	cancelOnce sync.Once
}

// NewTask creates a new task for the specified queue. It updates the supplied queue
// state to its initial values.
func NewTask(queue *Queue, taskState *taskspb.Task, onDone func(task *Task)) *Task {
	task := &Task{
		queue:  queue,
		state:  taskState,
		onDone: onDone,
		cancel: make(chan bool, 1), // Buffered in case cancel comes when task is not scheduled
	}

	task.setInitialTaskState(queue.name)

	return task
}

func (t *Task) setInitialTaskState(queueName string) {
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
	// The lock is to ensure a consistent state when updating
	t.stateMutex.Lock()

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

	t.stateMutex.Unlock()
}

func updateStateForDispatch(task *Task) *taskspb.Task {
	task.stateMutex.Lock()
	taskState := task.state

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
	task.stateMutex.Unlock()

	return frozenTaskState
}

func updateStateAfterDispatch(task *Task, statusCode int) *taskspb.Task {
	task.stateMutex.Lock()

	taskState := task.state

	rpcCode := toRPCStatusCode(statusCode)
	rpcCodeName := toCodeName(rpcCode)

	lastAttempt := taskState.GetLastAttempt()

	lastAttempt.ResponseTime = timestamppb.Now()
	lastAttempt.ResponseStatus = &rpcstatus.Status{
		Code:    int32(rpcCode),
		Message: fmt.Sprintf("%s(%d): HTTP status code %d", rpcCodeName, rpcCode, statusCode),
	}

	taskState.ResponseCount++

	frozenTaskState := proto.Clone(taskState).(*taskspb.Task)
	task.stateMutex.Unlock()

	return frozenTaskState
}

func (task *Task) reschedule(retry bool, statusCode int) {
	if statusCode >= 200 && statusCode <= 299 {
		log.Println("Task done")
		task.onDone(task)
	} else {
		log.Println("Task exec error with status " + strconv.Itoa(statusCode))
		if retry {
			retryConfig := task.queue.state.GetRetryConfig()

			if task.state.DispatchCount >= retryConfig.GetMaxAttempts() {
				log.Println("Ran out of attempts")
			} else {
				task.updateStateForReschedule()
				task.Schedule()
			}
		}
	}
}

func dispatch(retry bool, taskState *taskspb.Task) int {
	client := &http.Client{}
	client.Timeout = taskState.GetDispatchDeadline().AsDuration()

	var req *http.Request
	var headers map[string]string

	httpRequest := taskState.GetHttpRequest()
	appEngineHTTPRequest := taskState.GetAppEngineHttpRequest()

	scheduled := taskState.GetScheduleTime().AsTime()
	nameParts := parseTaskName(taskState.GetName())

	headerQueueName := nameParts.queueID
	headerTaskName := nameParts.taskID
	headerTaskRetryCount := fmt.Sprintf("%v", taskState.GetDispatchCount()-1)
	headerTaskExecutionCount := fmt.Sprintf("%v", taskState.GetResponseCount())
	headerTaskETA := fmt.Sprintf("%f", float64(scheduled.UnixNano())/1e9)

	if httpRequest != nil {
		method := toHTTPMethod(httpRequest.GetHttpMethod())

		req, _ = http.NewRequest(method, httpRequest.GetUrl(), bytes.NewBuffer(httpRequest.GetBody()))

		headers = httpRequest.GetHeaders()

		if auth := httpRequest.GetOidcToken(); auth != nil {
			tokenStr := oidc.CreateOIDCToken(auth.ServiceAccountEmail, httpRequest.GetUrl(), auth.Audience)
			headers["Authorization"] = "Bearer " + tokenStr
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

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return -1
	}
	defer resp.Body.Close()

	return resp.StatusCode
}

func (task *Task) doDispatch(retry bool) {
	respCode := dispatch(retry, task.state)

	updateStateAfterDispatch(task, respCode)
	task.reschedule(retry, respCode)
}

// Attempt tries to execute a task
func (task *Task) Attempt() {
	updateStateForDispatch(task)

	task.doDispatch(true)
}

// Run runs the task outside of the normal queueing mechanism.
// This method is called directly by request.
func (task *Task) Run() *taskspb.Task {
	taskState := updateStateForDispatch(task)

	go task.doDispatch(false)

	return taskState
}

// Delete cancels the task if it is queued for execution.
// This method is called directly by request.
func (task *Task) Delete(doCallback bool) {
	task.cancelOnce.Do(func() {
		task.cancel <- doCallback
	})
}

// Schedule schedules the task for execution.
// It is initially called by the queue, later by the task reschedule.
func (task *Task) Schedule() {
	scheduled := task.state.GetScheduleTime().AsTime()

	fromNow := time.Until(scheduled)

	go func() {
		select {
		case <-time.After(fromNow):
			task.queue.fire <- task
			return
		case doCallback := <-task.cancel:
			if doCallback {
				task.onDone(task)
			}
			return
		}
	}()
}
