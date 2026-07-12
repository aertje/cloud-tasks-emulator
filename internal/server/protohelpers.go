package server

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/engine"
	"github.com/aertje/cloud-tasks-emulator/v2/internal/maybe"

	tasks "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	errdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			MaxDispatchesPerSecond:  maybe.OfNonZero(rl.GetMaxDispatchesPerSecond()),
			MaxBurstSize:            maybe.OfNonZero(rl.GetMaxBurstSize()),
			MaxConcurrentDispatches: maybe.OfNonZero(rl.GetMaxConcurrentDispatches()),
		}
	}
	if rc := q.GetRetryConfig(); rc != nil {
		s.RetryConfig = engine.RetryConfig{
			MaxAttempts:  maybe.OfNonZero(rc.GetMaxAttempts()),
			MaxDoublings: maybe.OfNonZero(rc.GetMaxDoublings()),
			// GetMinBackoff/GetMaxBackoff and AsDuration are nil-safe, yielding a
			// zero duration when the field is absent; a zero backoff is treated as
			// "unset, apply the server default" (see setInitialQueueState).
			MinBackoff: maybe.OfNonZero(rc.GetMinBackoff().AsDuration()),
			MaxBackoff: maybe.OfNonZero(rc.GetMaxBackoff().AsDuration()),
		}
	}
	return s
}

func queueToProto(s engine.QueueState) *tasks.Queue {
	return &tasks.Queue{
		Name:  s.Name,
		State: queueRunStateToProto(s.State),
		RateLimits: &tasks.RateLimits{
			MaxDispatchesPerSecond:  s.RateLimits.MaxDispatchesPerSecond.OrZero(),
			MaxBurstSize:            s.RateLimits.MaxBurstSize.OrZero(),
			MaxConcurrentDispatches: s.RateLimits.MaxConcurrentDispatches.OrZero(),
		},
		RetryConfig: &tasks.RetryConfig{
			MaxAttempts:  s.RetryConfig.MaxAttempts.OrZero(),
			MaxDoublings: s.RetryConfig.MaxDoublings.OrZero(),
			MinBackoff:   durationpb.New(s.RetryConfig.MinBackoff.OrZero()),
			MaxBackoff:   durationpb.New(s.RetryConfig.MaxBackoff.OrZero()),
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
// not represented in the domain - the engine stores the full task and
// taskToProto applies the requested response view on the way out.
func taskFromProto(t *tasks.Task) engine.TaskState {
	s := engine.TaskState{
		Name:          t.GetName(),
		DispatchCount: t.GetDispatchCount(),
		ResponseCount: t.GetResponseCount(),
	}
	if ct := t.GetCreateTime(); ct != nil {
		s.CreateTime = maybe.Some(ct.AsTime())
	}
	if st := t.GetScheduleTime(); st != nil {
		s.ScheduleTime = maybe.Some(st.AsTime())
	}
	// GetDispatchDeadline and AsDuration are nil-safe, yielding a zero duration
	// when absent; a zero deadline is treated as "unset, apply the server
	// default" (see setInitialTaskState) rather than a zero (no-timeout) client.
	s.DispatchDeadline = maybe.OfNonZero(t.GetDispatchDeadline().AsDuration())
	if fa := t.GetFirstAttempt(); fa != nil {
		s.FirstAttempt = maybe.Some(attemptFromProto(fa))
	}
	if la := t.GetLastAttempt(); la != nil {
		s.LastAttempt = maybe.Some(attemptFromProto(la))
	}
	if hr := t.GetHttpRequest(); hr != nil {
		req := engine.HTTPRequest{
			URL:     maybe.OfNonZero(hr.GetUrl()),
			Method:  maybe.OfNonZero(httpMethodFromProto(hr.GetHttpMethod())),
			Headers: someIfNonNil(copyHeaders(hr.GetHeaders())),
			Body:    someIfNonNil(hr.GetBody()),
		}
		if ot := hr.GetOidcToken(); ot != nil {
			req.OIDCToken = maybe.Some(engine.OIDCToken{
				ServiceAccountEmail: ot.GetServiceAccountEmail(),
				Audience:            maybe.OfNonZero(ot.GetAudience()),
			})
		}
		s.HTTPRequest = maybe.Some(req)
	}
	if ae := t.GetAppEngineHttpRequest(); ae != nil {
		req := engine.AppEngineHTTPRequest{
			Method:      maybe.OfNonZero(httpMethodFromProto(ae.GetHttpMethod())),
			RelativeURI: maybe.OfNonZero(ae.GetRelativeUri()),
			Headers:     someIfNonNil(copyHeaders(ae.GetHeaders())),
			Body:        someIfNonNil(ae.GetBody()),
		}
		if r := ae.GetAppEngineRouting(); r != nil {
			req.AppEngineRouting = maybe.Some(engine.AppEngineRouting{
				Service:  maybe.OfNonZero(r.GetService()),
				Version:  maybe.OfNonZero(r.GetVersion()),
				Instance: maybe.OfNonZero(r.GetInstance()),
				Host:     maybe.OfNonZero(r.GetHost()),
			})
		}
		s.AppEngineHTTPRequest = maybe.Some(req)
	}
	return s
}

// resolveView maps a request's ResponseView onto the effective view. An
// unspecified view defaults to BASIC, matching Cloud Tasks.
func resolveView(v tasks.Task_View) tasks.Task_View {
	if v == tasks.Task_FULL {
		return tasks.Task_FULL
	}
	return tasks.Task_BASIC
}

// taskToProto maps an engine.TaskState into a *tasks.Task rendered for the given
// response view. The BASIC view (the default) omits the request body, which
// Cloud Tasks withholds because it can be large or sensitive; FULL returns it.
// Headers are returned under both views (see conformance/golden/headers.json).
func taskToProto(s engine.TaskState, view tasks.Task_View) *tasks.Task {
	view = resolveView(view)
	t := &tasks.Task{
		Name:          s.Name,
		DispatchCount: s.DispatchCount,
		ResponseCount: s.ResponseCount,
		View:          view,
	}
	if ct, ok := s.CreateTime.Get(); ok {
		t.CreateTime = timestampToProto(ct)
	}
	if st, ok := s.ScheduleTime.Get(); ok {
		t.ScheduleTime = timestampToProto(st)
	}
	if dd, ok := s.DispatchDeadline.Get(); ok {
		t.DispatchDeadline = durationpb.New(dd)
	}
	if fa, ok := s.FirstAttempt.Get(); ok {
		t.FirstAttempt = attemptToProto(fa)
	}
	if la, ok := s.LastAttempt.Get(); ok {
		t.LastAttempt = attemptToProto(la)
	}
	if hr, ok := s.HTTPRequest.Get(); ok {
		req := &tasks.HttpRequest{
			Url:        hr.URL.OrZero(),
			HttpMethod: httpMethodToProto(hr.Method.OrZero()),
			Headers:    copyHeaders(hr.Headers.OrZero()),
			Body:       hr.Body.OrZero(),
		}
		if auth, ok := hr.OIDCToken.Get(); ok {
			req.AuthorizationHeader = &tasks.HttpRequest_OidcToken{
				OidcToken: &tasks.OidcToken{
					ServiceAccountEmail: auth.ServiceAccountEmail,
					Audience:            auth.Audience.OrZero(),
				},
			}
		}
		if view != tasks.Task_FULL {
			req.Body = nil
		}
		t.MessageType = &tasks.Task_HttpRequest{HttpRequest: req}
	} else if ae, ok := s.AppEngineHTTPRequest.Get(); ok {
		req := &tasks.AppEngineHttpRequest{
			HttpMethod:  httpMethodToProto(ae.Method.OrZero()),
			RelativeUri: ae.RelativeURI.OrZero(),
			Headers:     copyHeaders(ae.Headers.OrZero()),
			Body:        ae.Body.OrZero(),
		}
		if view != tasks.Task_FULL {
			req.Body = nil
		}
		if r, ok := ae.AppEngineRouting.Get(); ok {
			req.AppEngineRouting = &tasks.AppEngineRouting{
				Service:  r.Service.OrZero(),
				Version:  r.Version.OrZero(),
				Instance: r.Instance.OrZero(),
				Host:     r.Host.OrZero(),
			}
		}
		t.MessageType = &tasks.Task_AppEngineHttpRequest{AppEngineHttpRequest: req}
	}
	return t
}

func attemptFromProto(a *tasks.Attempt) engine.Attempt {
	out := engine.Attempt{}
	if a.GetScheduleTime() != nil {
		out.ScheduleTime = maybe.Some(a.GetScheduleTime().AsTime())
	}
	if a.GetDispatchTime() != nil {
		out.DispatchTime = maybe.Some(a.GetDispatchTime().AsTime())
	}
	if a.GetResponseTime() != nil {
		out.ResponseTime = maybe.Some(a.GetResponseTime().AsTime())
	}
	if rs := a.GetResponseStatus(); rs != nil {
		out.ResponseStatus = maybe.Some(engine.AttemptStatus{Code: rs.GetCode(), Message: rs.GetMessage()})
	}
	return out
}

func attemptToProto(a engine.Attempt) *tasks.Attempt {
	out := &tasks.Attempt{}
	if st, ok := a.ScheduleTime.Get(); ok {
		out.ScheduleTime = timestampToProto(st)
	}
	if dt, ok := a.DispatchTime.Get(); ok {
		out.DispatchTime = timestampToProto(dt)
	}
	if rt, ok := a.ResponseTime.Get(); ok {
		out.ResponseTime = timestampToProto(rt)
	}
	if rs, ok := a.ResponseStatus.Get(); ok {
		out.ResponseStatus = &rpcstatus.Status{
			Code:    rs.Code,
			Message: rs.Message,
		}
	}
	return out
}

func timestampToProto(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

// someIfNonNil wraps a nilable reference field pulled from a proto: a nil
// map/slice becomes None, anything else Some. Proto3 cannot distinguish an
// absent repeated/bytes field from an explicitly empty one (both decode to a
// nil getter result), so an empty value maps to None and round-trips back to an
// absent proto field.
func someIfNonNil[T []byte | map[string]string](v T) maybe.Maybe[T] {
	if v == nil {
		return maybe.None[T]()
	}
	return maybe.Some(v)
}

func copyHeaders(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	maps.Copy(out, in)
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

// mapErr converts an engine sentinel to a gRPC status. Per-handler variants
// below override specific cases where the same sentinel maps to a different
// status in different RPCs.
//
// It also maps the context errors that an Engine method returns when the
// caller's ctx is cancelled or its deadline expires (e.g. PurgeQueue in
// hard-reset mode waiting on task completion), so gRPC callers see the
// standard Canceled/DeadlineExceeded status instead of an opaque Internal.
func mapErr(err error) error {
	switch err {
	case context.Canceled:
		return status.Errorf(codes.Canceled, "context canceled")
	case context.DeadlineExceeded:
		return status.Errorf(codes.DeadlineExceeded, "context deadline exceeded")
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
	case engine.ErrHTTPRequestURLRequired:
		return status.Errorf(codes.InvalidArgument, "HttpRequest.url is required.")
	case engine.ErrHTTPRequestURLScheme:
		return status.Errorf(codes.InvalidArgument, "HttpTarget.url must start with 'http://' or 'https://'.")
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
// (the generic queue-not-found message, via mapErr). It needs bespoke cases for
// the errors whose messages interpolate request values: the task-name/parent
// mismatch (naming the request parent and the queue in the task name) and an
// invalid task ID (naming the offending ID, plus a Help detail).
func mapErrForCreateTask(err error, in *tasks.CreateTaskRequest) error {
	switch err {
	case engine.ErrTaskQueueMismatch:
		return status.Errorf(codes.InvalidArgument,
			"The queue name from request ('%s') must be the same as the queue name in the named task ('%s').",
			in.GetParent(),
			queueNameFromTaskName(in.GetTask().GetName()),
		)
	case engine.ErrInvalidTaskID:
		// The message names the offending task ID; real Cloud Tasks also
		// attaches a Help detail pointing at the task-name field definition.
		msg := fmt.Sprintf(
			`Task ID "%s" can contain only letters ([A-Za-z]), numbers ([0-9]), hyphens (-), or underscores (_). Task ID must between 1 and 500 characters.`,
			taskIDFromTaskName(in.GetTask().GetName()),
		)
		return statusWithHelp(codes.InvalidArgument, msg,
			"Definition of task ID",
			"https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks#Task.FIELDS.name",
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

// taskIDFromTaskName returns the task-ID segment of a task resource name (the
// part after "/tasks/"), or the input unchanged if it has no task segment.
func taskIDFromTaskName(taskName string) string {
	if i := strings.LastIndex(taskName, "/tasks/"); i >= 0 {
		return taskName[i+len("/tasks/"):]
	}
	return taskName
}
