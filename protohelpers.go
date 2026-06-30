package main

import (
	"net/http"
	"strings"
	"time"

	"github.com/aertje/cloud-tasks-emulator/engine"

	"github.com/golang/protobuf/ptypes"
	ptimestamp "github.com/golang/protobuf/ptypes/timestamp"
	tasks "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	errdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// queueFromProto maps the subset of *tasks.Queue fields that the engine cares
// about into a QueueState. Fields outside this set (AppEngineRoutingOverride,
// StackdriverLoggingConfig, PurgeTime) are not preserved through round-trip.
func queueFromProto(q *tasks.Queue) engine.QueueState {
	s := engine.QueueState{
		Name:  q.GetName(),
		State: queueRunStateFromProto(q.GetState()),
	}
	if rl := q.GetRateLimits(); rl != nil {
		s.RateLimits = engine.RateLimits{
			MaxDispatchesPerSecond:  rl.GetMaxDispatchesPerSecond(),
			MaxBurstSize:            rl.GetMaxBurstSize(),
			MaxConcurrentDispatches: rl.GetMaxConcurrentDispatches(),
		}
	}
	if rc := q.GetRetryConfig(); rc != nil {
		s.RetryConfig = engine.RetryConfig{
			MaxAttempts:  rc.GetMaxAttempts(),
			MaxDoublings: rc.GetMaxDoublings(),
		}
		if rc.GetMinBackoff() != nil {
			s.RetryConfig.MinBackoff, _ = ptypes.Duration(rc.GetMinBackoff())
		}
		if rc.GetMaxBackoff() != nil {
			s.RetryConfig.MaxBackoff, _ = ptypes.Duration(rc.GetMaxBackoff())
		}
	}
	return s
}

func queueToProto(s engine.QueueState) *tasks.Queue {
	return &tasks.Queue{
		Name:  s.Name,
		State: queueRunStateToProto(s.State),
		RateLimits: &tasks.RateLimits{
			MaxDispatchesPerSecond:  s.RateLimits.MaxDispatchesPerSecond,
			MaxBurstSize:            s.RateLimits.MaxBurstSize,
			MaxConcurrentDispatches: s.RateLimits.MaxConcurrentDispatches,
		},
		RetryConfig: &tasks.RetryConfig{
			MaxAttempts:  s.RetryConfig.MaxAttempts,
			MaxDoublings: s.RetryConfig.MaxDoublings,
			MinBackoff:   ptypes.DurationProto(s.RetryConfig.MinBackoff),
			MaxBackoff:   ptypes.DurationProto(s.RetryConfig.MaxBackoff),
		},
	}
}

func queueRunStateFromProto(s tasks.Queue_State) engine.QueueRunState {
	switch s {
	case tasks.Queue_RUNNING:
		return engine.QueueRunStateRunning
	case tasks.Queue_PAUSED:
		return engine.QueueRunStatePaused
	case tasks.Queue_DISABLED:
		return engine.QueueRunStateDisabled
	default:
		return engine.QueueRunStateUnspecified
	}
}

func queueRunStateToProto(s engine.QueueRunState) tasks.Queue_State {
	switch s {
	case engine.QueueRunStateRunning:
		return tasks.Queue_RUNNING
	case engine.QueueRunStatePaused:
		return tasks.Queue_PAUSED
	case engine.QueueRunStateDisabled:
		return tasks.Queue_DISABLED
	default:
		return tasks.Queue_STATE_UNSPECIFIED
	}
}

// taskFromProto maps a *tasks.Task into an engine.TaskState. The View field is
// not represented in the domain - the engine always treats tasks as BASIC view.
func taskFromProto(t *tasks.Task) engine.TaskState {
	s := engine.TaskState{
		Name:          t.GetName(),
		DispatchCount: t.GetDispatchCount(),
		ResponseCount: t.GetResponseCount(),
	}
	if ct := t.GetCreateTime(); ct != nil {
		s.CreateTime, _ = ptypes.Timestamp(ct)
	}
	if st := t.GetScheduleTime(); st != nil {
		s.ScheduleTime, _ = ptypes.Timestamp(st)
	}
	if dd := t.GetDispatchDeadline(); dd != nil {
		s.DispatchDeadline, _ = ptypes.Duration(dd)
	}
	if fa := t.GetFirstAttempt(); fa != nil {
		s.FirstAttempt = attemptFromProto(fa)
	}
	if la := t.GetLastAttempt(); la != nil {
		s.LastAttempt = attemptFromProto(la)
	}
	if hr := t.GetHttpRequest(); hr != nil {
		s.HTTPRequest = &engine.HTTPRequest{
			URL:     hr.GetUrl(),
			Method:  httpMethodFromProto(hr.GetHttpMethod()),
			Headers: copyHeaders(hr.GetHeaders()),
			Body:    hr.GetBody(),
		}
		if ot := hr.GetOidcToken(); ot != nil {
			s.HTTPRequest.OIDCToken = &engine.OIDCToken{
				ServiceAccountEmail: ot.GetServiceAccountEmail(),
				Audience:            ot.GetAudience(),
			}
		}
	}
	if ae := t.GetAppEngineHttpRequest(); ae != nil {
		s.AppEngineHTTPRequest = &engine.AppEngineHTTPRequest{
			Method:      httpMethodFromProto(ae.GetHttpMethod()),
			RelativeURI: ae.GetRelativeUri(),
			Headers:     copyHeaders(ae.GetHeaders()),
			Body:        ae.GetBody(),
		}
		if r := ae.GetAppEngineRouting(); r != nil {
			s.AppEngineHTTPRequest.AppEngineRouting = &engine.AppEngineRouting{
				Service:  r.GetService(),
				Version:  r.GetVersion(),
				Instance: r.GetInstance(),
				Host:     r.GetHost(),
			}
		}
	}
	return s
}

func taskToProto(s engine.TaskState) *tasks.Task {
	t := &tasks.Task{
		Name:          s.Name,
		DispatchCount: s.DispatchCount,
		ResponseCount: s.ResponseCount,
		// The original emulator set this on every task.
		View: tasks.Task_BASIC,
	}
	if !s.CreateTime.IsZero() {
		t.CreateTime = timestampToProto(s.CreateTime)
	}
	if !s.ScheduleTime.IsZero() {
		t.ScheduleTime = timestampToProto(s.ScheduleTime)
	}
	if s.DispatchDeadline != 0 {
		t.DispatchDeadline = ptypes.DurationProto(s.DispatchDeadline)
	}
	if s.FirstAttempt != nil {
		t.FirstAttempt = attemptToProto(s.FirstAttempt)
	}
	if s.LastAttempt != nil {
		t.LastAttempt = attemptToProto(s.LastAttempt)
	}
	if s.HTTPRequest != nil {
		hr := &tasks.HttpRequest{
			Url:        s.HTTPRequest.URL,
			HttpMethod: httpMethodToProto(s.HTTPRequest.Method),
			Headers:    copyHeaders(s.HTTPRequest.Headers),
			Body:       s.HTTPRequest.Body,
		}
		if s.HTTPRequest.OIDCToken != nil {
			hr.AuthorizationHeader = &tasks.HttpRequest_OidcToken{
				OidcToken: &tasks.OidcToken{
					ServiceAccountEmail: s.HTTPRequest.OIDCToken.ServiceAccountEmail,
					Audience:            s.HTTPRequest.OIDCToken.Audience,
				},
			}
		}
		t.MessageType = &tasks.Task_HttpRequest{HttpRequest: hr}
	} else if s.AppEngineHTTPRequest != nil {
		ae := &tasks.AppEngineHttpRequest{
			HttpMethod:  httpMethodToProto(s.AppEngineHTTPRequest.Method),
			RelativeUri: s.AppEngineHTTPRequest.RelativeURI,
			Headers:     copyHeaders(s.AppEngineHTTPRequest.Headers),
			Body:        s.AppEngineHTTPRequest.Body,
		}
		if r := s.AppEngineHTTPRequest.AppEngineRouting; r != nil {
			ae.AppEngineRouting = &tasks.AppEngineRouting{
				Service:  r.Service,
				Version:  r.Version,
				Instance: r.Instance,
				Host:     r.Host,
			}
		}
		t.MessageType = &tasks.Task_AppEngineHttpRequest{AppEngineHttpRequest: ae}
	}
	return t
}

func attemptFromProto(a *tasks.Attempt) *engine.Attempt {
	out := &engine.Attempt{}
	if a.GetScheduleTime() != nil {
		out.ScheduleTime, _ = ptypes.Timestamp(a.GetScheduleTime())
	}
	if a.GetDispatchTime() != nil {
		out.DispatchTime, _ = ptypes.Timestamp(a.GetDispatchTime())
	}
	if a.GetResponseTime() != nil {
		out.ResponseTime, _ = ptypes.Timestamp(a.GetResponseTime())
	}
	if rs := a.GetResponseStatus(); rs != nil {
		out.ResponseStatus = &engine.AttemptStatus{Code: rs.GetCode(), Message: rs.GetMessage()}
	}
	return out
}

func attemptToProto(a *engine.Attempt) *tasks.Attempt {
	out := &tasks.Attempt{}
	if !a.ScheduleTime.IsZero() {
		out.ScheduleTime = timestampToProto(a.ScheduleTime)
	}
	if !a.DispatchTime.IsZero() {
		out.DispatchTime = timestampToProto(a.DispatchTime)
	}
	if !a.ResponseTime.IsZero() {
		out.ResponseTime = timestampToProto(a.ResponseTime)
	}
	if a.ResponseStatus != nil {
		out.ResponseStatus = &rpcstatus.Status{
			Code:    a.ResponseStatus.Code,
			Message: a.ResponseStatus.Message,
		}
	}
	return out
}

func timestampToProto(t time.Time) *ptimestamp.Timestamp {
	p, _ := ptypes.TimestampProto(t)
	return p
}

func copyHeaders(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func httpMethodFromProto(m tasks.HttpMethod) string {
	switch m {
	case tasks.HttpMethod_GET:
		return http.MethodGet
	case tasks.HttpMethod_POST:
		return http.MethodPost
	case tasks.HttpMethod_DELETE:
		return http.MethodDelete
	case tasks.HttpMethod_HEAD:
		return http.MethodHead
	case tasks.HttpMethod_OPTIONS:
		return http.MethodOptions
	case tasks.HttpMethod_PATCH:
		return http.MethodPatch
	case tasks.HttpMethod_PUT:
		return http.MethodPut
	default:
		return ""
	}
}

func httpMethodToProto(m string) tasks.HttpMethod {
	switch m {
	case http.MethodGet:
		return tasks.HttpMethod_GET
	case http.MethodPost:
		return tasks.HttpMethod_POST
	case http.MethodDelete:
		return tasks.HttpMethod_DELETE
	case http.MethodHead:
		return tasks.HttpMethod_HEAD
	case http.MethodOptions:
		return tasks.HttpMethod_OPTIONS
	case http.MethodPatch:
		return tasks.HttpMethod_PATCH
	case http.MethodPut:
		return tasks.HttpMethod_PUT
	default:
		return tasks.HttpMethod_HTTP_METHOD_UNSPECIFIED
	}
}

// mapErr converts an engine sentinel to a gRPC status, preserving the codes and
// messages that emulator.go produced before the refactor. Per-handler variants
// below override specific cases where the same sentinel maps to a different
// status in different RPCs - the real-cloud error-mapping work will collapse
// those once the actual upstream behaviour is known.
func mapErr(err error) error {
	switch err {
	case engine.ErrQueueNotFound, engine.ErrQueueRecentlyDeleted:
		// Cloud responds with the same error message whether the queue was recently deleted or never existed.
		return status.Errorf(codes.NotFound, "Queue does not exist. If you just created the queue, wait at least a minute for the queue to initialize.")
	case engine.ErrQueueAlreadyExists:
		return status.Errorf(codes.AlreadyExists, "Queue already exists")
	case engine.ErrInvalidQueueName:
		// Real Cloud Tasks attaches a Help detail pointing at the queue-name
		// field definition; conformance validates it (see conformance/golden).
		return statusWithHelp(codes.InvalidArgument,
			`Queue name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>".`,
			"Definition of queue name",
			"https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues#Queue.FIELDS.name",
		)
	case engine.ErrInvalidParent:
		return status.Errorf(codes.InvalidArgument, "Invalid resource field value in the request.")
	case engine.ErrTaskNotFound:
		// GetTask/DeleteTask/RunTask all report a missing task with this generic message.
		return status.Errorf(codes.NotFound, "Requested entity was not found.")
	case engine.ErrTaskRecentlyDeleted:
		return status.Errorf(codes.NotFound, "The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.")
	case engine.ErrTaskAlreadyExists:
		return status.Errorf(codes.AlreadyExists, "Requested entity already exists")
	case engine.ErrInvalidTaskName:
		return status.Errorf(codes.InvalidArgument, `Task name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>/tasks/<TASK_ID>"`)
	case engine.ErrUnimplemented:
		return status.Errorf(codes.Unimplemented, "Not yet implemented")
	default:
		return status.Errorf(codes.Internal, "unmapped engine error: %v", err)
	}
}

// statusWithHelp builds an error with a single Help detail (a description plus a
// documentation link), matching the detail real Cloud Tasks attaches to some
// InvalidArgument responses. If attaching the detail fails it degrades to a
// plain status so callers always get a usable error.
func statusWithHelp(code codes.Code, msg, linkDesc, linkURL string) error {
	st := status.New(code, msg)
	withDetails, err := st.WithDetails(&errdetails.Help{
		Links: []*errdetails.Help_Link{{Description: linkDesc, Url: linkURL}},
	})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

// DeleteQueue collapses missing/recently-deleted into a single short message.
func mapErrForDeleteQueue(err error) error {
	switch err {
	case engine.ErrQueueNotFound, engine.ErrQueueRecentlyDeleted:
		return status.Errorf(codes.NotFound, "Requested entity was not found.")
	}
	return mapErr(err)
}

// CreateQueue is the one RPC that distinguishes a recently-deleted queue from a
// missing one: re-creating a name still under its post-deletion cooldown fails
// with FailedPrecondition rather than the generic not-found. Every other queue
// RPC (and CreateTask) collapses recently-deleted into plain not-found.
func mapErrForCreateQueue(err error) error {
	if err == engine.ErrQueueRecentlyDeleted {
		return status.Errorf(codes.FailedPrecondition, "The queue cannot be created because a queue with this name existed too recently.")
	}
	return mapErr(err)
}

// CreateTask treats a missing or recently-deleted queue identically to GetQueue
// (the generic queue-not-found message, via mapErr). It only needs a bespoke
// case for the task-name/parent mismatch, whose message names the request
// parent and the queue embedded in the task name.
func mapErrForCreateTask(err error, in *tasks.CreateTaskRequest) error {
	if err == engine.ErrTaskQueueMismatch {
		return status.Errorf(codes.InvalidArgument,
			"The queue name from request ('%s') must be the same as the queue name in the named task ('%s').",
			in.GetParent(),
			queueNameFromTaskName(in.GetTask().GetName()),
		)
	}
	return mapErr(err)
}

// queueNameFromTaskName strips the "/tasks/<id>" suffix off a task resource
// name, yielding the queue it belongs to. Returns the input unchanged if it has
// no task segment.
func queueNameFromTaskName(taskName string) string {
	if i := strings.LastIndex(taskName, "/tasks/"); i >= 0 {
		return taskName[:i]
	}
	return taskName
}
