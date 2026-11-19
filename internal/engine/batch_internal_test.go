package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchCreateTasksValidation(t *testing.T) {
	future := time.Now().Add(time.Hour)

	tooMany := make([]BatchCreateTask, MaxBatchCreateTasks+1)
	for i := range tooMany {
		tooMany[i] = BatchCreateTask{Task: httpTaskState("", future)}
	}

	tests := []struct {
		name    string
		parent  string
		batch   []BatchCreateTask
		wantErr error
	}{
		{name: "empty batch", parent: testParent, batch: nil, wantErr: ErrBatchEmpty},
		{name: "too many tasks", parent: testParent, batch: tooMany, wantErr: ErrBatchTooLarge},
		{
			name:   "task for another queue",
			parent: testParent,
			batch: []BatchCreateTask{
				{Parent: testParent, Task: httpTaskState("", future)},
				{Parent: "projects/p/locations/l/queues/other", Task: httpTaskState("", future)},
			},
			wantErr: ErrBatchQueueMismatch,
		},
		{
			name:    "nonexistent queue",
			parent:  "projects/p/locations/l/queues/absent",
			batch:   []BatchCreateTask{{Task: httpTaskState("", future)}},
			wantErr: ErrQueueNotFound,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEngine(t, newFakeDispatcher(200))
			createRunningQueue(t, e)

			_, err := e.BatchCreateTasks(t.Context(), tc.parent, tc.batch)
			assert.ErrorIs(t, err, tc.wantErr)

			tasks, err := e.ListTasks(t.Context(), testParent)
			require.NoError(t, err)
			assert.Empty(t, tasks, "a rejected batch must create nothing")
		})
	}
}

func TestBatchCreateTasksRejectsRecentlyDeletedQueue(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))
	createRunningQueue(t, e)
	require.NoError(t, e.DeleteQueue(t.Context(), testParent))

	_, err := e.BatchCreateTasks(t.Context(), testParent, []BatchCreateTask{
		{Task: httpTaskState("", time.Now().Add(time.Hour))},
	})

	assert.ErrorIs(t, err, ErrQueueRecentlyDeleted)
}

func TestBatchCreateTasksReportsEachTask(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))
	createRunningQueue(t, e)
	ctx := t.Context()
	future := time.Now().Add(time.Hour)

	existing := testParent + "/tasks/existing"
	_, _, err := e.CreateTask(ctx, testParent, httpTaskState(existing, future))
	require.NoError(t, err)

	result, err := e.BatchCreateTasks(ctx, testParent, []BatchCreateTask{
		{Parent: testParent, Task: httpTaskState(testParent+"/tasks/first", future)},
		{Task: httpTaskState(existing, future)},
		{Task: httpTaskState(testParent+"/tasks/not a valid id", future)},
		{Task: httpTaskState("", future)},
	})
	require.NoError(t, err)
	require.Len(t, result.Tasks, 4)

	assert.NoError(t, result.Tasks[0].Err)
	assert.Equal(t, testParent+"/tasks/first", result.Tasks[0].Task.Name)
	assert.ErrorIs(t, result.Tasks[1].Err, ErrTaskAlreadyExists)
	assert.ErrorIs(t, result.Tasks[2].Err, ErrInvalidTaskID)
	assert.NoError(t, result.Tasks[3].Err)
	assert.Contains(t, result.Tasks[3].Task.Name, testParent+"/tasks/")

	for _, i := range []int{0, 3} {
		_, err := e.GetTask(ctx, result.Tasks[i].Task.Name)
		assert.NoError(t, err, "task %d should exist", i)
	}
}

func TestBatchCreateTasksTimesComeFromEngineClock(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	e := newClockedTestEngine(t, clock, time.Minute)
	createRunningQueue(t, e)

	result, err := e.BatchCreateTasks(t.Context(), testParent, []BatchCreateTask{
		{Task: httpTaskState("", clock.Now().Add(time.Hour))},
	})
	require.NoError(t, err)

	assert.Equal(t, clock.Now(), result.StartTime)
	assert.Equal(t, clock.Now(), result.EndTime)
}
