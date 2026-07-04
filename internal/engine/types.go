package engine

import "time"

// QueueRunState mirrors tasks.Queue_State but lives in the engine layer.
type QueueRunState int

const (
	QueueRunStateUnspecified QueueRunState = iota
	QueueRunStateRunning
	QueueRunStatePaused
	QueueRunStateDisabled
)

// RateLimits holds the per-queue dispatch rate configuration.
type RateLimits struct {
	MaxDispatchesPerSecond  float64
	MaxBurstSize            int32
	MaxConcurrentDispatches int32
}

// RetryConfig holds the per-queue retry/backoff configuration.
type RetryConfig struct {
	MaxAttempts  int32
	MaxDoublings int32
	MinBackoff   time.Duration
	MaxBackoff   time.Duration
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
// AppEngineHTTPRequest is non-nil for any live task.
type TaskState struct {
	Name             string
	CreateTime       time.Time
	ScheduleTime     time.Time
	DispatchDeadline time.Duration

	DispatchCount int32
	ResponseCount int32

	FirstAttempt *Attempt
	LastAttempt  *Attempt

	HTTPRequest          *HTTPRequest
	AppEngineHTTPRequest *AppEngineHTTPRequest
}

// Attempt records a single dispatch attempt against a task target.
type Attempt struct {
	ScheduleTime   time.Time
	DispatchTime   time.Time
	ResponseTime   time.Time
	ResponseStatus *AttemptStatus
}

// AttemptStatus is the gRPC-style status of a dispatch attempt.
// Code matches the values in google.rpc.Code.
type AttemptStatus struct {
	Code    int32
	Message string
}

// HTTPRequest is the engine view of a Cloud Tasks HTTP target.
// Method is the uppercase HTTP verb (e.g. "POST"); empty means unspecified.
type HTTPRequest struct {
	URL       string
	Method    string
	Headers   map[string]string
	Body      []byte
	OIDCToken *OIDCToken
}

// OIDCToken describes the OIDC credentials to mint for the HTTP target.
type OIDCToken struct {
	ServiceAccountEmail string
	Audience            string
}

// AppEngineHTTPRequest is the engine view of an App Engine target.
type AppEngineHTTPRequest struct {
	Method           string
	AppEngineRouting *AppEngineRouting
	RelativeURI      string
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
