package main

import (
	"github.com/aertje/cloud-tasks-emulator/engine"

	tasks "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

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
		return status.Errorf(codes.InvalidArgument, `Queue name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>"`)
	case engine.ErrInvalidParent:
		return status.Errorf(codes.InvalidArgument, "Invalid resource field value in the request.")
	case engine.ErrTaskNotFound:
		return status.Errorf(codes.NotFound, "Task does not exist.")
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

// DeleteQueue collapses missing/recently-deleted into a single short message.
func mapErrForDeleteQueue(err error) error {
	switch err {
	case engine.ErrQueueNotFound, engine.ErrQueueRecentlyDeleted:
		return status.Errorf(codes.NotFound, "Requested entity was not found.")
	}
	return mapErr(err)
}

// CreateTask uses a shorter queue-not-found message and treats the
// recently-deleted case as FailedPrecondition. It also formats the mismatch
// error with the offending names from the request.
func mapErrForCreateTask(err error, in *tasks.CreateTaskRequest) error {
	switch err {
	case engine.ErrQueueNotFound:
		return status.Errorf(codes.NotFound, "Queue does not exist.")
	case engine.ErrQueueRecentlyDeleted:
		return status.Errorf(codes.FailedPrecondition, "The queue no longer exists, though a queue with this name existed recently.")
	case engine.ErrTaskQueueMismatch:
		return status.Errorf(codes.InvalidArgument,
			"The queue name from request ('%s') must be the same as the queue name in the named task ('%s').",
			in.GetTask().GetName(),
			in.GetParent(),
		)
	}
	return mapErr(err)
}

// GetTask reports the recently-deleted case as FailedPrecondition, unlike
// DeleteTask/RunTask which use NotFound (the default).
func mapErrForGetTask(err error) error {
	if err == engine.ErrTaskRecentlyDeleted {
		return status.Errorf(codes.FailedPrecondition, "The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.")
	}
	return mapErr(err)
}
