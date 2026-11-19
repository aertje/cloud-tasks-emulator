package engine

import (
	"context"
	"fmt"
	"time"
)

// MaxBatchCreateTasks is the most tasks a single BatchCreateTasks call may
// create, matching the limit documented for real Cloud Tasks.
const MaxBatchCreateTasks = 100

// BatchCreateTask is one task of a BatchCreateTasks call: the queue its request
// named, and the task to create on it.
type BatchCreateTask struct {
	Parent string
	Task   TaskState
}

// BatchCreateTaskResult is the outcome of one task of a batch. On success Task
// is a snapshot of the created task and Err is nil; otherwise Err is the error
// CreateTask would have returned for the same task.
type BatchCreateTaskResult struct {
	Task TaskState
	Err  error
}

// BatchCreateTasksResult reports a completed batch: one result per task, in
// request order, and the engine-clock times the batch started and finished.
type BatchCreateTasksResult struct {
	Tasks     []BatchCreateTaskResult
	StartTime time.Time
	EndTime   time.Time
}

// BatchTaskError reports the task that made BatchCreateTasks reject a whole
// batch: its index in the batch, and the error it failed with.
type BatchTaskError struct {
	Index int
	Err   error
}

func (e *BatchTaskError) Error() string {
	return fmt.Sprintf("batch task %d: %v", e.Index, e.Err)
}

func (e *BatchTaskError) Unwrap() error {
	return e.Err
}

// BatchCreateTasks creates every task of batch on the queue identified by
// parent, in order, matching how real Cloud Tasks splits a batch's failures
// (see the task/batch-create/* cases and the batch battery in
// test/conformance):
//
//   - The batch is rejected, creating nothing, when it is empty or holds more
//     than MaxBatchCreateTasks tasks, or when a task's request names another
//     queue (an empty one included), names the task invalidly or for another
//     queue, or repeats a name earlier in the batch. A task-specific rejection
//     is a *BatchTaskError naming the first offending task. These are the
//     checks of ValidateBatch.
//   - A batch for a missing queue is accepted, and every task fails with the
//     queue's not-found or recently-deleted error.
//   - Otherwise each task is created as CreateTask would, and a task that fails
//     does not stop the rest: its error is reported in its result. When every
//     task fails, though, the batch is rejected with the first task's error, as
//     a *BatchTaskError.
//
// The queue is looked up once for the whole batch. As with CreateTask, a queue
// deleted while the batch runs is not re-checked before each task is admitted.
func (e *Engine) BatchCreateTasks(ctx context.Context, parent string, batch []BatchCreateTask) (BatchCreateTasksResult, error) {
	if err := ctx.Err(); err != nil {
		return BatchCreateTasksResult{}, err
	}
	if err := ValidateBatch(parent, batch); err != nil {
		return BatchCreateTasksResult{}, err
	}

	result := BatchCreateTasksResult{
		Tasks:     make([]BatchCreateTaskResult, len(batch)),
		StartTime: e.now(),
	}
	queue, err := e.fetchQueueForTasks(parent)
	if err != nil {
		for i := range result.Tasks {
			result.Tasks[i] = BatchCreateTaskResult{Err: err}
		}
		result.EndTime = e.now()
		return result, nil
	}

	created := 0
	for i, task := range batch {
		if err := ctx.Err(); err != nil {
			result.Tasks[i] = BatchCreateTaskResult{Err: err}
			continue
		}
		_, frozen, err := e.createTaskInQueue(queue, parent, task.Task)
		result.Tasks[i] = BatchCreateTaskResult{Task: frozen, Err: err}
		if err == nil {
			created++
		}
	}
	if created == 0 {
		return BatchCreateTasksResult{}, &BatchTaskError{Index: 0, Err: result.Tasks[0].Err}
	}
	result.EndTime = e.now()
	return result, nil
}

// ValidateBatch reports why a batch must be rejected as a whole before any of
// its tasks are created, or nil if it may run. BatchCreateTasks runs it first;
// callers run it on its own to act between validating a batch and creating its
// tasks.
func ValidateBatch(parent string, batch []BatchCreateTask) error {
	if len(batch) == 0 {
		return ErrBatchEmpty
	}
	if len(batch) > MaxBatchCreateTasks {
		return ErrBatchTooLarge
	}
	for i, task := range batch {
		if task.Parent != parent {
			return &BatchTaskError{Index: i, Err: ErrBatchQueueMismatch}
		}
	}
	names := make(map[string]bool, len(batch))
	for i, task := range batch {
		if err := validateTaskName(parent, task.Task.Name); err != nil {
			return &BatchTaskError{Index: i, Err: err}
		}
		if task.Task.Name == "" {
			continue
		}
		if names[task.Task.Name] {
			return &BatchTaskError{Index: i, Err: ErrBatchDuplicateTaskName}
		}
		names[task.Task.Name] = true
	}
	return nil
}
