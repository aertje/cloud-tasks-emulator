package engine

import (
	"time"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/maybe"
)

// Optionality convention for the state types below: a maybe.Maybe[T] marks a
// value that may be genuinely absent - an unset input still awaiting a
// server-assigned default, or state a task has not reached yet (e.g. the
// response of an attempt still in flight). Values that always carry a meaning
// within a present parent stay plain: counters, names, the queue-run enum, and
// the request scalars whose empty value already reads as "unset" and are
// defaulted at creation (Method, URL, RelativeURI, Headers, Body).

// QueueRunState mirrors tasks.Queue_State but lives in the engine layer.
type QueueRunState int

const (
	QueueRunStateUnspecified QueueRunState = iota
	QueueRunStateRunning
	QueueRunStatePaused
	QueueRunStateDisabled
)

// RateLimits holds the per-queue dispatch rate configuration. Each field is a
// Maybe: absent means "apply the server default" (see setInitialQueueState).
type RateLimits struct {
	MaxDispatchesPerSecond  maybe.Maybe[float64]
	MaxBurstSize            maybe.Maybe[int32]
	MaxConcurrentDispatches maybe.Maybe[int32]
}

// RetryConfig holds the per-queue retry/backoff configuration. Each field is a
// Maybe: absent means "apply the server default" (see setInitialQueueState).
type RetryConfig struct {
	MaxAttempts  maybe.Maybe[int32]
	MaxDoublings maybe.Maybe[int32]
	MinBackoff   maybe.Maybe[time.Duration]
	MaxBackoff   maybe.Maybe[time.Duration]
}

// QueueState is the engine's view of a queue. Proto<->QueueState mapping
// happens at the handler edge (see root protohelpers.go).
type QueueState struct {
	Name        string
	State       QueueRunState
	RateLimits  RateLimits
	RetryConfig RetryConfig
}

// TaskState is the engine's view of a task. Exactly one of HTTPRequest /
// AppEngineHTTPRequest is present for any live task.
type TaskState struct {
	Name string
	// CreateTime, ScheduleTime and DispatchDeadline are absent on a task-creation
	// input and filled with server-assigned values by setInitialTaskState, after
	// which they are always present on a live task.
	CreateTime       maybe.Maybe[time.Time]
	ScheduleTime     maybe.Maybe[time.Time]
	DispatchDeadline maybe.Maybe[time.Duration]

	DispatchCount int32
	// ResponseCount counts attempts that received an HTTP response - a transport
	// failure or dispatch-deadline timeout received none and is not counted. It
	// backs the App Engine target's X-AppEngine-TaskExecutionCount.
	ResponseCount int32
	// ExecutionCount counts attempts that received a non-5XX response. It backs
	// the HTTP target's X-CloudTasks-TaskExecutionCount, which - unlike the App
	// Engine target's X-AppEngine-TaskExecutionCount (which uses ResponseCount) -
	// excludes failures due to 5XX status codes.
	ExecutionCount int32

	// PreviousResponseCode is the raw HTTP status of the previous attempt. It is
	// populated only on the snapshot returned by updateStateForDispatch (absent on
	// the first attempt, or when the previous attempt received no HTTP response)
	// and feeds the retry-only X-*-TaskPreviousResponse dispatch header.
	PreviousResponseCode maybe.Maybe[int]

	FirstAttempt maybe.Maybe[Attempt]
	LastAttempt  maybe.Maybe[Attempt]

	HTTPRequest          maybe.Maybe[HTTPRequest]
	AppEngineHTTPRequest maybe.Maybe[AppEngineHTTPRequest]
}

// Attempt records a single dispatch attempt against a task target. The response
// fields (ResponseTime, ResponseStatus, ResponseCode) are absent until the
// attempt has completed.
type Attempt struct {
	ScheduleTime   maybe.Maybe[time.Time]
	DispatchTime   maybe.Maybe[time.Time]
	ResponseTime   maybe.Maybe[time.Time]
	ResponseStatus maybe.Maybe[AttemptStatus]
	// ResponseCode is the raw HTTP status the target returned for this attempt
	// (e.g. 503), or a negative marker when no HTTP response was received
	// (transport failure). It is absent until the attempt completes. It is kept
	// alongside the RPC-coded ResponseStatus so the next dispatch can report it
	// via X-*-TaskPreviousResponse.
	ResponseCode maybe.Maybe[int]
}

// AttemptStatus is the gRPC-style status of a dispatch attempt.
// Code matches the values in google.rpc.Code.
type AttemptStatus struct {
	Code    int32
	Message string
}

// HTTPRequest is the engine view of a Cloud Tasks HTTP target. Method is the
// uppercase HTTP verb (e.g. "POST"); absent means unspecified and is defaulted
// to POST at creation.
type HTTPRequest struct {
	URL       string
	Method    maybe.Maybe[string]
	Headers   map[string]string
	Body      []byte
	OIDCToken maybe.Maybe[OIDCToken]
}

// OIDCToken describes the OIDC credentials to mint for the HTTP target.
type OIDCToken struct {
	ServiceAccountEmail string
	Audience            string
}

// AppEngineHTTPRequest is the engine view of an App Engine target. Method
// (absent defaults to POST) and RelativeURI (absent defaults to "/") are filled
// in at creation.
type AppEngineHTTPRequest struct {
	Method           maybe.Maybe[string]
	AppEngineRouting maybe.Maybe[AppEngineRouting]
	RelativeURI      maybe.Maybe[string]
	Headers          map[string]string
	Body             []byte
}

// AppEngineRouting describes the App Engine service routing for a request.
type AppEngineRouting struct {
	Service  string
	Version  string
	Instance string
	Host     string
}
