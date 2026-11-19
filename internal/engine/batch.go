package engine

import (
	"context"
	"time"
)

// MaxBatchCreateTasks is the most tasks a single BatchCreateTasks call may
// create, matching the limit documented for real Cloud Tasks.
const MaxBatchCreateTasks = 100

// BatchCreateTask is one task of a BatchCreateTasks call: the queue its request
// named, and the task to create on it. An empty Parent means the batch's queue.
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

// BatchCreateTasks creates every task of batch on the queue identified by
// parent, in order. The batch itself is rejected, creating nothing, when it is
// empty, holds more than MaxBatchCreateTasks tasks, names another queue in any
// task, or targets a missing queue. Otherwise, each task is validated and
// created exactly as CreateTask would.
func (e *Engine) BatchCreateTasks(ctx context.Context, parent string, batch []BatchCreateTask) (BatchCreateTasksResult, error) {
	if err := ctx.Err(); err != nil {
		return BatchCreateTasksResult{}, err
	}
	if len(batch) == 0 {
		return BatchCreateTasksResult{}, ErrBatchEmpty
	}
	if len(batch) > MaxBatchCreateTasks {
		return BatchCreateTasksResult{}, ErrBatchTooLarge
	}
	for _, task := range batch {
		if task.Parent != "" && task.Parent != parent {
			return BatchCreateTasksResult{}, ErrBatchQueueMismatch
		}
	}
	queue, err := e.fetchQueueForTasks(parent)
	if err != nil {
		return BatchCreateTasksResult{}, err
	}

	result := BatchCreateTasksResult{
		Tasks:     make([]BatchCreateTaskResult, len(batch)),
		StartTime: e.now(),
	}
	for i, task := range batch {
		if err := ctx.Err(); err != nil {
			result.Tasks[i] = BatchCreateTaskResult{Err: err}
			continue
		}
		_, frozen, err := e.createTaskInQueue(queue, parent, task.Task)
		result.Tasks[i] = BatchCreateTaskResult{Task: frozen, Err: err}
	}
	result.EndTime = e.now()
	return result, nil
}
