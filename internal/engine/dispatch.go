package engine

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/maybe"
	"github.com/aertje/cloud-tasks-emulator/v2/internal/oidc"
)

// retryHeaderPolicy holds the family-specific values for the two retry-only
// dispatch headers (X-<family>-TaskPreviousResponse / -TaskRetryReason). All
// values are captured from production (see conformance/golden/dispatch.json):
// the reason depends only on the family and on whether the previous attempt
// returned an HTTP response or failed with none (e.g. a dispatch-deadline
// timeout), not on the specific status code.
type retryHeaderPolicy struct {
	prefix          string // "X-CloudTasks-" or "X-AppEngine-"
	responseReason  string // TaskRetryReason after an HTTP error response
	timeoutPrevious string // TaskPreviousResponse after a no-response failure
	timeoutReason   string // TaskRetryReason after a no-response failure
}

var (
	// HTTP: an empty reason for every failure; a no-response failure is reported
	// as a synthesized 504.
	httpRetryPolicy = retryHeaderPolicy{
		prefix:          "X-CloudTasks-",
		responseReason:  "",
		timeoutPrevious: "504",
		timeoutReason:   "",
	}
	// App Engine: "App Error" for an HTTP error response, "Instance Unavailable"
	// for a no-response failure (reported with previous response 0).
	appEngineRetryPolicy = retryHeaderPolicy{
		prefix:          "X-AppEngine-",
		responseReason:  "App Error",
		timeoutPrevious: "0",
		timeoutReason:   "Instance Unavailable",
	}
)

// addOptionalRetryHeaders adds the two retry-only dispatch headers to injected,
// keyed off how the previous attempt failed. previous is the prior attempt's
// raw HTTP status: a positive value means it returned that status; a negative
// value means it got no response (a transport failure or dispatch-deadline
// timeout); an absent value means there was no previous attempt (the first
// dispatch), so nothing is added. Only the no-response case was captured as a
// timeout; other no-response modes (e.g. a refused connection) are unobserved
// and treated the same.
func addOptionalRetryHeaders(injected map[string]string, p retryHeaderPolicy, previous maybe.M[int]) {
	code, ok := previous.Get()
	if !ok {
		return
	}
	switch {
	case code > 0:
		injected[p.prefix+"TaskPreviousResponse"] = strconv.Itoa(code)
		injected[p.prefix+"TaskRetryReason"] = p.responseReason
	case code < 0:
		injected[p.prefix+"TaskPreviousResponse"] = p.timeoutPrevious
		injected[p.prefix+"TaskRetryReason"] = p.timeoutReason
	}
}

func updateStateForReschedule(task *Task) {
	task.stateMutex.Lock()
	defer task.stateMutex.Unlock()

	retryConfig := task.queue.state.RetryConfig

	doubling := min(task.state.DispatchCount-1, retryConfig.MaxDoublings.OrZero())
	maxBackoff := retryConfig.MaxBackoff.OrZero()
	// The exponential term is computed in float64 so a large doubling count
	// saturates (towards +Inf) and clamps to maxBackoff, instead of overflowing
	// time.Duration into a negative - i.e. immediate - backoff.
	backoff := maxBackoff
	if scaled := float64(retryConfig.MinBackoff.OrZero()) * math.Pow(2, float64(doubling)); scaled < float64(maxBackoff) {
		backoff = time.Duration(scaled)
	}

	task.state.ScheduleTime = maybe.Some(task.state.ScheduleTime.OrZero().Add(backoff))
}

func updateStateForDispatch(task *Task) TaskState {
	task.stateMutex.Lock()
	defer task.stateMutex.Unlock()

	dispatchTime := time.Now()

	// Capture the previous attempt's response code before its Attempt is
	// overwritten below, so this dispatch can report it via
	// X-*-TaskPreviousResponse (retries only). Absent when there was no previous
	// attempt (the first dispatch).
	previousResponseCode := maybe.None[int]()
	if prev, ok := task.state.LastAttempt.Get(); ok {
		previousResponseCode = prev.ResponseCode
	}

	task.state.LastAttempt = maybe.Some(Attempt{
		ScheduleTime: task.state.ScheduleTime,
		DispatchTime: maybe.Some(dispatchTime),
	})

	task.state.DispatchCount++

	if !task.state.FirstAttempt.IsPresent() {
		task.state.FirstAttempt = maybe.Some(Attempt{
			DispatchTime: maybe.Some(dispatchTime),
		})
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

	// Complete the in-flight LastAttempt with its response fields. LastAttempt is
	// a value-typed maybe.M, so State() snapshots already hold their own copy of the
	// Attempt; storing the completed value here cannot mutate a snapshot the gRPC
	// edge is reading (unlocked) via taskToProto.
	attempt := task.state.LastAttempt.OrZero()
	attempt.ResponseTime = maybe.Some(time.Now())
	attempt.ResponseCode = maybe.Some(statusCode)
	attempt.ResponseStatus = maybe.Some(AttemptStatus{
		Code:    rpcCode,
		Message: fmt.Sprintf("%s(%d): HTTP status code %d", rpcCodeName, rpcCode, statusCode),
	})
	task.state.LastAttempt = maybe.Some(attempt)

	// Only an attempt that actually received an HTTP response counts: a transport
	// failure or dispatch-deadline timeout (statusCode < 0) received none. This
	// matches what real Cloud Tasks reports as X-AppEngine-TaskExecutionCount
	// (ResponseCount, which counts every response) and, excluding 5XX, the HTTP
	// target's X-CloudTasks-TaskExecutionCount (see TaskState.ExecutionCount).
	if statusCode >= 100 {
		task.state.ResponseCount++
		if statusCode < 500 || statusCode > 599 {
			task.state.ExecutionCount++
		}
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
	maxAttempts := task.queue.state.RetryConfig.MaxAttempts.OrZero()
	task.stateMutex.Unlock()

	// -1 is the documented "unlimited attempts" marker (see validateQueueConfig).
	if maxAttempts != -1 && dispatchCount >= maxAttempts {
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
	client := &http.Client{Timeout: state.DispatchDeadline.OrZero(), Transport: transport}

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
	headerTaskETA := fmt.Sprintf("%f", float64(state.ScheduleTime.OrZero().UnixNano())/1e9)

	var (
		method     string
		url        string
		body       []byte
		srcHeaders map[string]string
		injected   map[string]string
	)

	switch {
	case state.HTTPRequest.IsPresent():
		hr, _ := state.HTTPRequest.Get()
		method = hr.Method.OrZero()
		url = hr.URL.OrZero()
		body = hr.Body.OrZero()
		srcHeaders = hr.Headers.OrZero()

		// Headers as per https://cloud.google.com/tasks/docs/creating-http-target-tasks#handler
		injected = map[string]string{
			"User-Agent":                      "Google-Cloud-Tasks",
			"X-CloudTasks-QueueName":          headerQueueName,
			"X-CloudTasks-TaskName":           headerTaskName,
			"X-CloudTasks-TaskExecutionCount": headerHTTPExecutionCount,
			"X-CloudTasks-TaskRetryCount":     headerTaskRetryCount,
			"X-CloudTasks-TaskETA":            headerTaskETA,
		}
		addOptionalRetryHeaders(injected, httpRetryPolicy, state.PreviousResponseCode)

		if auth, ok := hr.OIDCToken.Get(); ok {
			tokenStr, err := oidcCfg.CreateToken(auth.ServiceAccountEmail, url, auth.Audience.OrZero())
			if err != nil {
				logger.Error("dispatch: create OIDC token", "task", state.Name, "err", err)
				return -1
			}
			injected["Authorization"] = "Bearer " + tokenStr
		}
	case state.AppEngineHTTPRequest.IsPresent():
		ae, _ := state.AppEngineHTTPRequest.Get()

		method = ae.Method.OrZero()
		url = ae.AppEngineRouting.OrZero().Host.OrZero() + ae.RelativeURI.OrZero()
		body = ae.Body.OrZero()
		srcHeaders = ae.Headers.OrZero()

		// These headers are only set on dispatch, see https://cloud.google.com/tasks/docs/reference/rpc/google.cloud.tasks.v2#google.cloud.tasks.v2.AppEngineHttpRequest
		injected = map[string]string{
			"X-AppEngine-QueueName":          headerQueueName,
			"X-AppEngine-TaskName":           headerTaskName,
			"X-AppEngine-TaskRetryCount":     headerTaskRetryCount,
			"X-AppEngine-TaskExecutionCount": headerAppEngineExecutionCount,
			"X-AppEngine-TaskETA":            headerTaskETA,
		}
		addOptionalRetryHeaders(injected, appEngineRetryPolicy, state.PreviousResponseCode)
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
