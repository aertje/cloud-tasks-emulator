package engine

import "errors"

var (
	ErrQueueNotFound        = errors.New("queue not found")
	ErrQueueRecentlyDeleted = errors.New("queue recently deleted")
	ErrQueueAlreadyExists   = errors.New("queue already exists")
	ErrInvalidQueueName     = errors.New("invalid queue name")
	ErrInvalidParent        = errors.New("invalid parent")

	ErrTaskNotFound        = errors.New("task not found")
	ErrTaskRecentlyDeleted = errors.New("task recently deleted")
	ErrTaskAlreadyExists   = errors.New("task already exists")
	ErrInvalidTaskName     = errors.New("invalid task name")
	ErrInvalidTaskID       = errors.New("invalid task id")
	ErrTaskQueueMismatch   = errors.New("task name does not belong to queue")

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
