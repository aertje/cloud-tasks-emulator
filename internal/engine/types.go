package engine

import (
	"time"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/maybe"
)

// Optionality convention for the state types below: a field that may be
// genuinely absent - an unset input still awaiting a server default, or state a
// task has not reached yet - is a maybe.M[T]. Everything else is always
// present and stays plain: counters, resource names, the queue-run enum, an
// attempt's status code/message, and a token's service-account email.

// QueueRunState mirrors tasks.Queue_State but lives in the engine layer.
type QueueRunState int

const (
	QueueRunStateUnspecified QueueRunState = iota
	QueueRunStateRunning
	QueueRunStatePaused
	QueueRunStateDisabled
)

// RateLimits holds the per-queue dispatch rate configuration. Each field is a
// maybe.M: absent means "apply the server default" (see setInitialQueueState).
type RateLimits struct {
	MaxDispatchesPerSecond  maybe.M[float64]
	MaxBurstSize            maybe.M[int32]
	MaxConcurrentDispatches maybe.M[int32]
}

// RetryConfig holds the per-queue retry/backoff configuration. Each field is a
// maybe.M: absent means "apply the server default" (see setInitialQueueState).
type RetryConfig struct {
	MaxAttempts  maybe.M[int32]
	MaxDoublings maybe.M[int32]
	MinBackoff   maybe.M[time.Duration]
	MaxBackoff   maybe.M[time.Duration]
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
	CreateTime       maybe.M[time.Time]
	ScheduleTime     maybe.M[time.Time]
	DispatchDeadline maybe.M[time.Duration]

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
	PreviousResponseCode maybe.M[int]

	FirstAttempt maybe.M[Attempt]
	LastAttempt  maybe.M[Attempt]

	HTTPRequest          maybe.M[HTTPRequest]
	AppEngineHTTPRequest maybe.M[AppEngineHTTPRequest]
}

// Attempt records a single dispatch attempt against a task target. The response
// fields (ResponseTime, ResponseStatus, ResponseCode) are absent until the
// attempt has completed.
type Attempt struct {
	ScheduleTime   maybe.M[time.Time]
	DispatchTime   maybe.M[time.Time]
	ResponseTime   maybe.M[time.Time]
	ResponseStatus maybe.M[AttemptStatus]
	// ResponseCode is the raw HTTP status the target returned for this attempt
	// (e.g. 503), or a negative marker when no HTTP response was received
	// (transport failure). It is absent until the attempt completes. It is kept
	// alongside the RPC-coded ResponseStatus so the next dispatch can report it
	// via X-*-TaskPreviousResponse.
	ResponseCode maybe.M[int]
}

// AttemptStatus is the gRPC-style status of a dispatch attempt.
// Code matches the values in google.rpc.Code.
type AttemptStatus struct {
	Code    int32
	Message string
}

// HTTPRequest is the engine view of a Cloud Tasks HTTP target. Method is the
// uppercase HTTP verb (e.g. "POST"); absent means unspecified and is defaulted
// to POST at creation. URL is absent only on an unvalidated creation input; a
// live task always carries one.
type HTTPRequest struct {
	URL       maybe.M[string]
	Method    maybe.M[string]
	Headers   maybe.M[map[string]string]
	Body      maybe.M[[]byte]
	OIDCToken maybe.M[OIDCToken]
}

// OIDCToken describes the OIDC credentials to mint for the HTTP target. Audience
// is absent when the caller left it to default to the target URL.
type OIDCToken struct {
	ServiceAccountEmail string
	Audience            maybe.M[string]
}

// AppEngineHTTPRequest is the engine view of an App Engine target. Method
// (absent defaults to POST) and RelativeURI (absent defaults to "/") are filled
// in at creation.
type AppEngineHTTPRequest struct {
	Method           maybe.M[string]
	AppEngineRouting maybe.M[AppEngineRouting]
	RelativeURI      maybe.M[string]
	Headers          maybe.M[map[string]string]
	Body             maybe.M[[]byte]
}

// AppEngineRouting describes the App Engine service routing for a request. Each
// field is absent when the caller left it to the App Engine default; Host is
// absent on input and filled in at creation.
type AppEngineRouting struct {
	Service  maybe.M[string]
	Version  maybe.M[string]
	Instance maybe.M[string]
	Host     maybe.M[string]
}
