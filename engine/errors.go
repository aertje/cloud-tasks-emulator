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

	ErrUnimplemented = errors.New("not yet implemented")
)
