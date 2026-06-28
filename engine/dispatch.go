package engine

import (
	"bytes"
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

	doubling := task.state.DispatchCount - 1
	if doubling > retryConfig.MaxDoublings {
		doubling = retryConfig.MaxDoublings
	}
	backoff := retryConfig.MinBackoff * time.Duration(1<<uint32(doubling))
	if backoff > retryConfig.MaxBackoff {
		backoff = retryConfig.MaxBackoff
	}

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

	task.state.LastAttempt.ResponseTime = time.Now()
	task.state.LastAttempt.ResponseStatus = &AttemptStatus{
		Code:    rpcCode,
		Message: fmt.Sprintf("%s(%d): HTTP status code %d", rpcCodeName, rpcCode, statusCode),
	}

	task.state.ResponseCount++
}

func (task *Task) reschedule(retry bool, statusCode int) {
	if statusCode >= 200 && statusCode <= 299 {
		log.Println("Task done")
		task.onDone(task)
	} else {
		log.Println("Task exec error with status " + strconv.Itoa(statusCode))
		if retry {
			if task.state.DispatchCount >= task.queue.state.RetryConfig.MaxAttempts {
				log.Println("Ran out of attempts")
			} else {
				updateStateForReschedule(task)
				task.Schedule()
			}
		}
	}
}

func dispatch(retry bool, state *TaskState) int {
	client := &http.Client{Timeout: state.DispatchDeadline}

	var req *http.Request
	var headers map[string]string

	nameParts := parseTaskName(state.Name)

	headerQueueName := nameParts.queueId
	headerTaskName := nameParts.taskId
	headerTaskRetryCount := fmt.Sprintf("%v", state.DispatchCount-1)
	headerTaskExecutionCount := fmt.Sprintf("%v", state.ResponseCount)
	headerTaskETA := fmt.Sprintf("%f", float64(state.ScheduleTime.UnixNano())/1e9)

	if state.HTTPRequest != nil {
		req, _ = http.NewRequest(state.HTTPRequest.Method, state.HTTPRequest.URL, bytes.NewBuffer(state.HTTPRequest.Body))

		headers = state.HTTPRequest.Headers

		if auth := state.HTTPRequest.OIDCToken; auth != nil {
			tokenStr := CreateOIDCToken(auth.ServiceAccountEmail, state.HTTPRequest.URL, auth.Audience)
			headers["Authorization"] = "Bearer " + tokenStr
		}

		// Headers as per https://cloud.google.com/tasks/docs/creating-http-target-tasks#handler
		// TODO: optional headers
		headers["X-CloudTasks-QueueName"] = headerQueueName
		headers["X-CloudTasks-TaskName"] = headerTaskName
		headers["X-CloudTasks-TaskExecutionCount"] = headerTaskExecutionCount
		headers["X-CloudTasks-TaskRetryCount"] = headerTaskRetryCount
		headers["X-CloudTasks-TaskETA"] = headerTaskETA
	} else if state.AppEngineHTTPRequest != nil {
		ae := state.AppEngineHTTPRequest

		url := ae.AppEngineRouting.Host + ae.RelativeURI

		req, _ = http.NewRequest(ae.Method, url, bytes.NewBuffer(ae.Body))

		headers = ae.Headers

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
	respCode := dispatch(retry, &task.state)

	updateStateAfterDispatch(task, respCode)
	task.reschedule(retry, respCode)
}
