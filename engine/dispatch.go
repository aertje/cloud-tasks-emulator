package engine

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	ptimestamp "github.com/golang/protobuf/ptypes/timestamp"
	tasks "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
)

func updateStateForReschedule(task *Task) *tasks.Task {
	// The lock is to ensure a consistent state when updating
	task.stateMutex.Lock()
	taskState := task.state
	queueState := task.queue.state

	retryConfig := queueState.GetRetryConfig()

	minBackoff, _ := ptypes.Duration(retryConfig.GetMinBackoff())
	maxBackoff, _ := ptypes.Duration(retryConfig.GetMaxBackoff())

	doubling := taskState.GetDispatchCount() - 1
	if doubling > retryConfig.MaxDoublings {
		doubling = retryConfig.MaxDoublings
	}
	backoff := minBackoff * time.Duration(1<<uint32(doubling))
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	protoBackoff := ptypes.DurationProto(backoff)
	prevScheduleTime := taskState.GetScheduleTime()

	// Avoid int32 nanos overflow
	scheduleNanos := int64(prevScheduleTime.GetNanos()) + int64(protoBackoff.GetNanos())
	scheduleSeconds := prevScheduleTime.GetSeconds() + protoBackoff.GetSeconds()
	if scheduleNanos >= 1e9 {
		scheduleSeconds++
		scheduleNanos -= 1e9
	}

	taskState.ScheduleTime = &ptimestamp.Timestamp{
		Nanos:   int32(scheduleNanos),
		Seconds: scheduleSeconds,
	}

	frozenTaskState := proto.Clone(taskState).(*tasks.Task)
	task.stateMutex.Unlock()

	return frozenTaskState
}

func updateStateForDispatch(task *Task) *tasks.Task {
	task.stateMutex.Lock()
	taskState := task.state

	dispatchTime := ptypes.TimestampNow()

	taskState.LastAttempt = &tasks.Attempt{
		ScheduleTime: &ptimestamp.Timestamp{
			Nanos:   taskState.GetScheduleTime().GetNanos(),
			Seconds: taskState.GetScheduleTime().GetSeconds(),
		},
		DispatchTime: dispatchTime,
	}

	taskState.DispatchCount++

	if taskState.GetFirstAttempt() == nil {
		taskState.FirstAttempt = &tasks.Attempt{
			DispatchTime: dispatchTime,
		}
	}

	frozenTaskState := proto.Clone(taskState).(*tasks.Task)
	task.stateMutex.Unlock()

	return frozenTaskState
}

func updateStateAfterDispatch(task *Task, statusCode int) *tasks.Task {
	task.stateMutex.Lock()

	taskState := task.state

	rpcCode := toRPCStatusCode(statusCode)
	rpcCodeName := toCodeName(rpcCode)

	lastAttempt := taskState.GetLastAttempt()

	lastAttempt.ResponseTime = ptypes.TimestampNow()
	lastAttempt.ResponseStatus = &rpcstatus.Status{
		Code:    rpcCode,
		Message: fmt.Sprintf("%s(%d): HTTP status code %d", rpcCodeName, rpcCode, statusCode),
	}

	taskState.ResponseCount++

	frozenTaskState := proto.Clone(taskState).(*tasks.Task)
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
				updateStateForReschedule(task)
				task.Schedule()
			}
		}
	}
}

func dispatch(retry bool, taskState *tasks.Task) int {
	client := &http.Client{}
	client.Timeout, _ = ptypes.Duration(taskState.GetDispatchDeadline())

	var req *http.Request
	var headers map[string]string

	httpRequest := taskState.GetHttpRequest()
	appEngineHTTPRequest := taskState.GetAppEngineHttpRequest()

	scheduled, _ := ptypes.Timestamp(taskState.GetScheduleTime())
	nameParts := parseTaskName(taskState)

	headerQueueName := nameParts.queueId
	headerTaskName := nameParts.taskId
	headerTaskRetryCount := fmt.Sprintf("%v", taskState.GetDispatchCount()-1)
	headerTaskExecutionCount := fmt.Sprintf("%v", taskState.GetResponseCount())
	headerTaskETA := fmt.Sprintf("%f", float64(scheduled.UnixNano())/1e9)

	if httpRequest != nil {
		method := toHTTPMethod(httpRequest.GetHttpMethod())

		req, _ = http.NewRequest(method, httpRequest.GetUrl(), bytes.NewBuffer(httpRequest.GetBody()))

		headers = httpRequest.GetHeaders()

		if auth := httpRequest.GetOidcToken(); auth != nil {
			tokenStr := CreateOIDCToken(auth.ServiceAccountEmail, httpRequest.GetUrl(), auth.Audience)
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
