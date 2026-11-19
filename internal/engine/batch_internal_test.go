package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/maybe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchCreateTasksValidation(t *testing.T) {
	future := time.Now().Add(time.Hour)

	tooMany := make([]BatchCreateTask, MaxBatchCreateTasks+1)
	for i := range tooMany {
		tooMany[i] = BatchCreateTask{Parent: testParent, Task: httpTaskState("", future)}
	}

	tests := []struct {
		name      string
		batch     []BatchCreateTask
		wantErr   error
		wantIndex int // index of the offending task, or -1 for a batch-wide error
	}{
		{name: "empty batch", batch: nil, wantErr: ErrBatchEmpty, wantIndex: -1},
		{name: "too many tasks", batch: tooMany, wantErr: ErrBatchTooLarge, wantIndex: -1},
		{
			name: "request for another queue",
			batch: []BatchCreateTask{
				{Parent: testParent, Task: httpTaskState("", future)},
				{Parent: "projects/p/locations/l/queues/other", Task: httpTaskState("", future)},
			},
			wantErr:   ErrBatchQueueMismatch,
			wantIndex: 1,
		},
		{
			name:      "request with no queue",
			batch:     []BatchCreateTask{{Task: httpTaskState("", future)}},
			wantErr:   ErrBatchQueueMismatch,
			wantIndex: 0,
		},
		{
			name: "malformed task name",
			batch: batchOn(testParent,
				httpTaskState("", future),
				httpTaskState("is-this-a-name", future),
			),
			wantErr:   ErrInvalidTaskName,
			wantIndex: 1,
		},
		{
			name: "illegal task id",
			batch: batchOn(testParent,
				httpTaskState("", future),
				httpTaskState(testParent+"/tasks/not a valid id", future),
			),
			wantErr:   ErrInvalidTaskID,
			wantIndex: 1,
		},
		{
			name: "task named for another queue",
			batch: batchOn(testParent,
				httpTaskState("", future),
				httpTaskState("projects/p/locations/l/queues/other/tasks/valid", future),
			),
			wantErr:   ErrTaskQueueMismatch,
			wantIndex: 1,
		},
		{
			name: "duplicate task name",
			batch: batchOn(testParent,
				httpTaskState(testParent+"/tasks/dupe", future),
				httpTaskState("", future),
				httpTaskState(testParent+"/tasks/dupe", future),
			),
			wantErr:   ErrBatchDuplicateTaskName,
			wantIndex: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEngine(t, newFakeDispatcher(200))
			createRunningQueue(t, e)

			_, err := e.BatchCreateTasks(t.Context(), testParent, tc.batch)
			assert.ErrorIs(t, err, tc.wantErr)

			var taskErr *BatchTaskError
			if tc.wantIndex < 0 {
				assert.False(t, errors.As(err, &taskErr), "a batch-wide error names no task")
			} else if assert.True(t, errors.As(err, &taskErr)) {
				assert.Equal(t, tc.wantIndex, taskErr.Index)
			}

			tasks, err := e.ListTasks(t.Context(), testParent)
			require.NoError(t, err)
			assert.Empty(t, tasks, "a rejected batch must create nothing")
		})
	}
}

func TestBatchCreateTasksReportsMissingQueueForEveryTask(t *testing.T) {
	future := time.Now().Add(time.Hour)

	t.Run("never existed", func(t *testing.T) {
		e := newTestEngine(t, newFakeDispatcher(200))
		absent := "projects/p/locations/l/queues/absent"

		result, err := e.BatchCreateTasks(t.Context(), absent, batchOn(absent,
			httpTaskState("", future),
			httpTaskState("", future),
		))
		require.NoError(t, err)
		require.Len(t, result.Tasks, 2)
		for _, task := range result.Tasks {
			assert.ErrorIs(t, task.Err, ErrQueueNotFound)
		}
	})

	t.Run("recently deleted", func(t *testing.T) {
		e := newTestEngine(t, newFakeDispatcher(200))
		createRunningQueue(t, e)
		require.NoError(t, e.DeleteQueue(t.Context(), testParent))

		result, err := e.BatchCreateTasks(t.Context(), testParent, batchOn(testParent, httpTaskState("", future)))
		require.NoError(t, err)
		require.Len(t, result.Tasks, 1)
		assert.ErrorIs(t, result.Tasks[0].Err, ErrQueueRecentlyDeleted)
	})
}

func TestBatchCreateTasksReportsEachTask(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))
	createRunningQueue(t, e)
	ctx := t.Context()
	future := time.Now().Add(time.Hour)

	existing := testParent + "/tasks/existing"
	_, _, err := e.CreateTask(ctx, testParent, httpTaskState(existing, future))
	require.NoError(t, err)

	result, err := e.BatchCreateTasks(ctx, testParent, batchOn(testParent,
		httpTaskState(testParent+"/tasks/first", future),
		httpTaskState(existing, future),
		httpTaskState("", future),
		withoutURL(httpTaskState("", future)),
	))
	require.NoError(t, err)
	require.Len(t, result.Tasks, 4)

	assert.NoError(t, result.Tasks[0].Err)
	assert.Equal(t, testParent+"/tasks/first", result.Tasks[0].Task.Name)
	assert.ErrorIs(t, result.Tasks[1].Err, ErrTaskAlreadyExists)
	assert.NoError(t, result.Tasks[2].Err)
	assert.Contains(t, result.Tasks[2].Task.Name, testParent+"/tasks/")
	assert.ErrorIs(t, result.Tasks[3].Err, ErrHTTPRequestURLRequired)

	for _, i := range []int{0, 2} {
		_, err := e.GetTask(ctx, result.Tasks[i].Task.Name)
		assert.NoError(t, err, "task %d should exist", i)
	}
}

func TestBatchCreateTasksRejectsBatchWhenEveryTaskFails(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))
	createRunningQueue(t, e)
	ctx := t.Context()
	future := time.Now().Add(time.Hour)

	existing := testParent + "/tasks/existing"
	_, _, err := e.CreateTask(ctx, testParent, httpTaskState(existing, future))
	require.NoError(t, err)

	_, err = e.BatchCreateTasks(ctx, testParent, batchOn(testParent,
		httpTaskState(existing, future),
		withoutURL(httpTaskState("", future)),
	))

	var taskErr *BatchTaskError
	require.True(t, errors.As(err, &taskErr))
	assert.Equal(t, 0, taskErr.Index)
	assert.ErrorIs(t, err, ErrTaskAlreadyExists)
}

func TestBatchCreateTasksTimesComeFromEngineClock(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	e := newClockedTestEngine(t, clock, time.Minute)
	createRunningQueue(t, e)

	result, err := e.BatchCreateTasks(t.Context(), testParent, batchOn(testParent,
		httpTaskState("", clock.Now().Add(time.Hour)),
	))
	require.NoError(t, err)

	assert.Equal(t, clock.Now(), result.StartTime)
	assert.Equal(t, clock.Now(), result.EndTime)
}

// batchOn builds a batch whose requests all name the queue parent.
func batchOn(parent string, states ...TaskState) []BatchCreateTask {
	batch := make([]BatchCreateTask, len(states))
	for i, state := range states {
		batch[i] = BatchCreateTask{Parent: parent, Task: state}
	}
	return batch
}

// withoutURL removes an HTTP task's URL, which CreateTask rejects per task.
func withoutURL(state TaskState) TaskState {
	state.HTTPRequest = maybe.Some(HTTPRequest{})
	return state
}
