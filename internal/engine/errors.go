package engine

import "errors"

var (
	ErrQueueNotFound        = errors.New("queue not found")
	ErrQueueRecentlyDeleted = errors.New("queue recently deleted")
	ErrQueueAlreadyExists   = errors.New("queue already exists")
	ErrInvalidQueueName     = errors.New("invalid queue name")
	ErrInvalidParent        = errors.New("invalid parent")
	ErrQueueParentMismatch  = errors.New("queue name does not begin with parent")

	// Queue-configuration violations, reported by CreateQueue (see
	// validateQueueConfig). Real Cloud Tasks rejects these with InvalidArgument
	// and distinguishes a negative value from one above the allowed maximum
	// (see the queue-invalid-config cases in conformance/golden/errors.json),
	// so each direction gets its own sentinel.
	ErrMaxDispatchesPerSecondNegative  = errors.New("max dispatches per second negative")
	ErrMaxDispatchesPerSecondTooHigh   = errors.New("max dispatches per second too high")
	ErrMaxBurstSizeRange               = errors.New("max burst size out of range")
	ErrMaxConcurrentDispatchesNegative = errors.New("max concurrent dispatches negative")
	ErrMaxConcurrentDispatchesTooHigh  = errors.New("max concurrent dispatches too high")
	ErrMaxAttemptsRange                = errors.New("max attempts out of range")
	ErrMaxDoublingsNegative            = errors.New("max doublings negative")
	ErrMinBackoffNegative              = errors.New("min backoff negative")
	ErrMaxBackoffNegative              = errors.New("max backoff negative")
	ErrBackoffOrder                    = errors.New("min backoff greater than max backoff")

	ErrTaskNotFound        = errors.New("task not found")
	ErrTaskRecentlyDeleted = errors.New("task recently deleted")
	ErrTaskAlreadyExists   = errors.New("task already exists")
	ErrInvalidTaskName     = errors.New("invalid task name")
	ErrInvalidTaskID       = errors.New("invalid task id")
	ErrTaskQueueMismatch   = errors.New("task name does not belong to queue")

	// Task-configuration violations, reported by CreateTask (see
	// validateTaskConfig). Real Cloud Tasks rejects these with InvalidArgument.
	// The dispatch-deadline interval depends on the target family, so each
	// family gets its own sentinel.
	ErrDispatchDeadlineHTTPRange      = errors.New("http dispatch deadline out of range")
	ErrDispatchDeadlineAppEngineRange = errors.New("app engine dispatch deadline out of range")
	ErrScheduleTimeTooFarInFuture     = errors.New("schedule time too far in the future")

	// ErrTaskTooLarge reports a task whose canonicalized stored form exceeds
	// the size limit real Cloud Tasks enforces at CreateTask (see tasksize.go
	// for the measured law). Reported as InvalidArgument.
	ErrTaskTooLarge = errors.New("task size too large")

	// ErrHTTPRequestURLRequired and ErrHTTPRequestURLScheme report the two
	// create-time URL validations Cloud Tasks performs on an HTTP-target task:
	// the URL must be present and must start with http:// or https://. Cloud
	// Tasks does not fully parse the URL at create time (e.g. an invalid
	// percent-escape is accepted and only fails at dispatch), so neither does
	// the emulator - see conformance case task/create/invalid-url-bad-escape.
	ErrHTTPRequestURLRequired = errors.New("http request url is required")
	ErrHTTPRequestURLScheme   = errors.New("http request url must be http or https")

	ErrUnimplemented = errors.New("not yet implemented")
)
