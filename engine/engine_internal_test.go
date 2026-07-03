package engine

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testParent = "projects/p/locations/l/queues/q"
)

// fakeDispatcher records every dispatch and returns a scripted status code,
// letting the engine tests drive lifecycle and retry behaviour without real
// network I/O. statusFor, if set, chooses the status by (zero-based) attempt
// index; otherwise status is returned for every attempt.
type fakeDispatcher struct {
	mu        sync.Mutex
	calls     []TaskState
	status    int
	statusFor func(attempt int) int

	// dispatched is signalled (non-blocking) after each dispatch so tests can
	// await a given number of attempts.
	dispatched chan struct{}
}

func newFakeDispatcher(status int) *fakeDispatcher {
	return &fakeDispatcher{
		status:     status,
		dispatched: make(chan struct{}, 1024),
	}
}

func (f *fakeDispatcher) Dispatch(state TaskState, _ *OIDCConfig) int {
	f.mu.Lock()
	attempt := len(f.calls)
	f.calls = append(f.calls, state)
	f.mu.Unlock()

	code := f.status
	if f.statusFor != nil {
		code = f.statusFor(attempt)
	}

	select {
	case f.dispatched <- struct{}{}:
	default:
	}
	return code
}

func (f *fakeDispatcher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// awaitDispatches blocks until at least n dispatches have happened or the
// timeout elapses.
func (f *fakeDispatcher) awaitDispatches(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for f.count() < n {
		select {
		case <-f.dispatched:
		case <-deadline:
			require.GreaterOrEqualf(t, f.count(), n, "expected at least %d dispatches", n)
			return
		}
	}
}

// newTestEngine returns an engine wired to the supplied dispatcher, with its
// queues cancelled on test cleanup.
func newTestEngine(t *testing.T, d Dispatcher) *Engine {
	t.Helper()
	e := New(&Options{Dispatcher: d})
	t.Cleanup(e.Stop)
	return e
}

// fakeClock is a manually advanced clock, letting the tombstone-expiry tests
// jump past the cooldown without real sleeps. Its Now method is safe to call
// concurrently with Advance (e.g. from the background sweep goroutine).
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{t: t}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newClockedTestEngine returns an engine driven by the supplied clock and
// tombstone TTL, with the queues and sweep goroutine cancelled on cleanup.
func newClockedTestEngine(t *testing.T, clock *fakeClock, ttl time.Duration) *Engine {
	t.Helper()
	e := New(&Options{
		Dispatcher:   newFakeDispatcher(200),
		TombstoneTTL: ttl,
		clock:        clock.Now,
	})
	t.Cleanup(e.Stop)
	return e
}

// createRunningQueue creates the standard test queue on the engine.
func createRunningQueue(t *testing.T, e *Engine) *Queue {
	t.Helper()
	q, err := e.CreateQueue("projects/p/locations/l", QueueState{Name: testParent})
	require.NoError(t, err)
	return q
}

// httpTaskState builds a valid HTTP task state, optionally with a fixed name and
// schedule time (zero schedule dispatches immediately).
func httpTaskState(name string, schedule time.Time) TaskState {
	return TaskState{
		Name:         name,
		ScheduleTime: schedule,
		HTTPRequest:  &HTTPRequest{URL: "http://example.test/"},
	}
}

func TestParseTaskName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantOK  bool
		wantIDs TaskNameParts
	}{
		{
			name:   "valid",
			input:  "projects/proj/locations/loc/queues/que/tasks/tsk",
			wantOK: true,
			wantIDs: TaskNameParts{
				project:  "proj",
				location: "loc",
				queueId:  "que",
				taskId:   "tsk",
			},
		},
		{name: "empty", input: "", wantOK: false},
		{name: "missing tasks segment", input: "projects/p/locations/l/queues/q", wantOK: false},
		{name: "not a resource name", input: "just-a-string", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseTaskName(tc.input)
			assert.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				assert.Equal(t, tc.wantIDs, got)
			}
		})
	}
}

func TestSplitAndValidateTaskID(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		wantStructured bool
		wantID         string
		wantValidID    bool
	}{
		{name: "valid", input: testParent + "/tasks/abc-123_ID", wantStructured: true, wantID: "abc-123_ID", wantValidID: true},
		{name: "illegal id chars", input: testParent + "/tasks/not a valid id", wantStructured: true, wantID: "not a valid id", wantValidID: false},
		{name: "malformed", input: "is-this-a-name", wantStructured: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, structured := splitTaskName(tc.input)
			assert.Equal(t, tc.wantStructured, structured)
			if tc.wantStructured {
				assert.Equal(t, tc.wantID, id)
				assert.Equal(t, tc.wantValidID, isValidTaskID(id))
			}
		})
	}
}

func TestBackoffReschedule(t *testing.T) {
	baseSchedule := time.Unix(1_700_000_000, 0)
	tests := []struct {
		name          string
		retry         RetryConfig
		dispatchCount int32
		wantBackoff   time.Duration
	}{
		{
			name:          "first retry uses min backoff",
			retry:         RetryConfig{MinBackoff: 100 * time.Millisecond, MaxBackoff: time.Hour, MaxDoublings: 16},
			dispatchCount: 1,
			wantBackoff:   100 * time.Millisecond,
		},
		{
			name:          "doubles each attempt",
			retry:         RetryConfig{MinBackoff: 100 * time.Millisecond, MaxBackoff: time.Hour, MaxDoublings: 16},
			dispatchCount: 3, // doubling = 2 => x4
			wantBackoff:   400 * time.Millisecond,
		},
		{
			name:          "capped by max doublings",
			retry:         RetryConfig{MinBackoff: 1 * time.Second, MaxBackoff: time.Hour, MaxDoublings: 1},
			dispatchCount: 5, // doubling capped at 1 => x2
			wantBackoff:   2 * time.Second,
		},
		{
			name:          "capped by max backoff",
			retry:         RetryConfig{MinBackoff: 1 * time.Second, MaxBackoff: 3 * time.Second, MaxDoublings: 16},
			dispatchCount: 10, // would be huge, capped at 3s
			wantBackoff:   3 * time.Second,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := &Task{
				queue: &Queue{state: QueueState{RetryConfig: tc.retry}},
				state: TaskState{ScheduleTime: baseSchedule, DispatchCount: tc.dispatchCount},
			}
			updateStateForReschedule(task)
			assert.Equal(t, baseSchedule.Add(tc.wantBackoff), task.state.ScheduleTime)
		})
	}
}

func TestQueueLifecycle(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))

	// Invalid name / parent.
	_, err := e.CreateQueue("projects/p/locations/l", QueueState{Name: "not a queue"})
	assert.ErrorIs(t, err, ErrInvalidQueueName)
	_, err = e.CreateQueue("bad-parent", QueueState{Name: testParent})
	assert.ErrorIs(t, err, ErrInvalidParent)

	// Create then duplicate.
	_, err = e.CreateQueue("projects/p/locations/l", QueueState{Name: testParent})
	require.NoError(t, err)
	_, err = e.CreateQueue("projects/p/locations/l", QueueState{Name: testParent})
	assert.ErrorIs(t, err, ErrQueueAlreadyExists)

	// Get.
	q, err := e.GetQueue(testParent)
	require.NoError(t, err)
	assert.Equal(t, testParent, q.State().Name)

	// Delete, then get and recreate observe the tombstone.
	require.NoError(t, e.DeleteQueue(testParent))
	_, err = e.GetQueue(testParent)
	assert.ErrorIs(t, err, ErrQueueNotFound)
	_, err = e.CreateQueue("projects/p/locations/l", QueueState{Name: testParent})
	assert.ErrorIs(t, err, ErrQueueRecentlyDeleted)

	// Deleting a missing queue.
	assert.ErrorIs(t, e.DeleteQueue("projects/p/locations/l/queues/absent"), ErrQueueNotFound)
}

func TestCreateTaskValidation(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))
	createRunningQueue(t, e)

	future := time.Now().Add(time.Hour)

	tests := []struct {
		name    string
		parent  string
		state   TaskState
		wantErr error
	}{
		{name: "nonexistent queue", parent: "projects/p/locations/l/queues/absent", state: httpTaskState("", future), wantErr: ErrQueueNotFound},
		{name: "malformed name", parent: testParent, state: httpTaskState("is-this-a-name", future), wantErr: ErrInvalidTaskName},
		{name: "illegal task id", parent: testParent, state: httpTaskState(testParent+"/tasks/not a valid id", future), wantErr: ErrInvalidTaskID},
		{name: "wrong queue", parent: testParent, state: httpTaskState("projects/p/locations/l/queues/other/tasks/valid", future), wantErr: ErrTaskQueueMismatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := e.CreateTask(tc.parent, tc.state)
			assert.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestCreateTaskRejectsDuplicateName(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))
	createRunningQueue(t, e)

	name := testParent + "/tasks/dupe"
	future := time.Now().Add(time.Hour)

	_, _, err := e.CreateTask(testParent, httpTaskState(name, future))
	require.NoError(t, err)

	_, _, err = e.CreateTask(testParent, httpTaskState(name, future))
	assert.ErrorIs(t, err, ErrTaskAlreadyExists)
}

func TestDeleteTaskTombstonesName(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))
	createRunningQueue(t, e)

	name := testParent + "/tasks/to-delete"
	future := time.Now().Add(time.Hour)

	_, _, err := e.CreateTask(testParent, httpTaskState(name, future))
	require.NoError(t, err)

	require.NoError(t, e.DeleteTask(name))

	// A deleted task reads back as recently-deleted, and the name stays reserved.
	_, err = e.GetTask(name)
	assert.ErrorIs(t, err, ErrTaskRecentlyDeleted)
	_, _, err = e.CreateTask(testParent, httpTaskState(name, future))
	assert.ErrorIs(t, err, ErrTaskAlreadyExists)

	// Deleting again reports recently-deleted, not a hard not-found.
	assert.ErrorIs(t, e.DeleteTask(name), ErrTaskRecentlyDeleted)
}

func TestSoftPurgeKeepsNamesReserved(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))
	q := createRunningQueue(t, e)

	name := testParent + "/tasks/purged"
	future := time.Now().Add(time.Hour)
	_, _, err := e.CreateTask(testParent, httpTaskState(name, future))
	require.NoError(t, err)

	_, err = e.PurgeQueue(testParent)
	require.NoError(t, err)

	// Soft purge is asynchronous; wait until the task is tombstoned.
	require.Eventually(t, func() bool {
		tasks, err := e.ListTasks(testParent)
		return err == nil && len(tasks) == 0
	}, 2*time.Second, 5*time.Millisecond)

	// The name is still reserved.
	_, _, err = e.CreateTask(testParent, httpTaskState(name, future))
	assert.ErrorIs(t, err, ErrTaskAlreadyExists)
	assert.Equal(t, testParent, q.State().Name)
}

func TestHardResetReleasesNames(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))
	e.opts.HardResetOnPurgeQueue = true
	createRunningQueue(t, e)

	name := testParent + "/tasks/reset"
	future := time.Now().Add(time.Hour)
	_, _, err := e.CreateTask(testParent, httpTaskState(name, future))
	require.NoError(t, err)

	// Hard reset is synchronous: on return the task and its name handle are gone.
	_, err = e.PurgeQueue(testParent)
	require.NoError(t, err)

	tasks, err := e.ListTasks(testParent)
	require.NoError(t, err)
	assert.Empty(t, tasks)

	_, err = e.GetTask(name)
	assert.ErrorIs(t, err, ErrTaskNotFound)

	// The name is reusable after a hard reset.
	_, _, err = e.CreateTask(testParent, httpTaskState(name, future))
	assert.NoError(t, err)
}

func TestTaskRetriesUntilSuccess(t *testing.T) {
	// Fail the first two attempts, succeed on the third.
	d := newFakeDispatcher(0)
	d.statusFor = func(attempt int) int {
		if attempt < 2 {
			return 404
		}
		return 200
	}
	e := newTestEngine(t, d)

	// Short backoff so the retries land quickly.
	_, err := e.CreateQueue("projects/p/locations/l", QueueState{
		Name:        testParent,
		RetryConfig: RetryConfig{MinBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, MaxAttempts: 100, MaxDoublings: 1},
	})
	require.NoError(t, err)

	name := testParent + "/tasks/retry"
	_, _, err = e.CreateTask(testParent, httpTaskState(name, time.Time{}))
	require.NoError(t, err)

	d.awaitDispatches(t, 3, 2*time.Second)

	// After the successful third attempt the task is removed.
	require.Eventually(t, func() bool {
		_, err := e.GetTask(name)
		return errors.Is(err, ErrTaskRecentlyDeleted)
	}, 2*time.Second, 5*time.Millisecond)

	// The retry-count header value is derived from DispatchCount-1, so the third
	// (successful) attempt carried retry count 2.
	require.Equal(t, 3, d.count())
	assert.Equal(t, int32(3), d.calls[2].DispatchCount)
}

func TestTaskStopsAfterMaxAttempts(t *testing.T) {
	d := newFakeDispatcher(404) // always fails
	e := newTestEngine(t, d)

	_, err := e.CreateQueue("projects/p/locations/l", QueueState{
		Name:        testParent,
		RetryConfig: RetryConfig{MinBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, MaxAttempts: 3, MaxDoublings: 1},
	})
	require.NoError(t, err)

	name := testParent + "/tasks/doomed"
	_, _, err = e.CreateTask(testParent, httpTaskState(name, time.Time{}))
	require.NoError(t, err)

	d.awaitDispatches(t, 3, 2*time.Second)

	// It must not dispatch a fourth time, and the exhausted task is removed.
	require.Eventually(t, func() bool {
		_, err := e.GetTask(name)
		return errors.Is(err, ErrTaskRecentlyDeleted)
	}, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, 3, d.count(), "must not exceed MaxAttempts dispatches")
}

func TestPauseAndResume(t *testing.T) {
	d := newFakeDispatcher(200)
	e := newTestEngine(t, d)
	q := createRunningQueue(t, e)

	_, err := e.PauseQueue(testParent)
	require.NoError(t, err)
	assert.Equal(t, QueueRunStatePaused, q.State().State)

	_, err = e.ResumeQueue(testParent)
	require.NoError(t, err)
	assert.Equal(t, QueueRunStateRunning, q.State().State)

	// A task created after resume must still dispatch.
	_, _, err = e.CreateTask(testParent, httpTaskState(testParent+"/tasks/after-resume", time.Time{}))
	require.NoError(t, err)

	d.awaitDispatches(t, 1, 2*time.Second)
	assert.GreaterOrEqual(t, d.count(), 1)
}

// countQueueTombstones / countTaskTombstones expose the tombstone map sizes for
// the sweep test.
func (e *Engine) countQueueTombstones() int {
	e.qsMux.Lock()
	defer e.qsMux.Unlock()
	return len(e.qTombstones)
}

func (e *Engine) countTaskTombstones() int {
	e.tsMux.Lock()
	defer e.tsMux.Unlock()
	return len(e.tTombstones)
}

func TestTombstoneExpiry(t *testing.T) {
	const ttl = time.Minute
	base := time.Unix(1_700_000_000, 0)

	tests := []struct {
		name string
		run  func(t *testing.T, e *Engine, clock *fakeClock)
	}{
		{
			name: "queue name reusable after cooldown",
			run: func(t *testing.T, e *Engine, clock *fakeClock) {
				_, err := e.CreateQueue("projects/p/locations/l", QueueState{Name: testParent})
				require.NoError(t, err)
				require.NoError(t, e.DeleteQueue(testParent))

				// During the cooldown the name is reserved. GetQueue reports the same
				// not-found as a name that never existed; recreate is rejected as
				// recently-deleted.
				_, err = e.GetQueue(testParent)
				assert.ErrorIs(t, err, ErrQueueNotFound)
				_, err = e.CreateQueue("projects/p/locations/l", QueueState{Name: testParent})
				assert.ErrorIs(t, err, ErrQueueRecentlyDeleted)

				// Creating a task under the recently-deleted parent surfaces the
				// cooldown too.
				_, _, err = e.CreateTask(testParent, httpTaskState("", time.Now().Add(time.Hour)))
				assert.ErrorIs(t, err, ErrQueueRecentlyDeleted)

				// Still reserved right up to the cooldown boundary.
				clock.Advance(ttl - time.Nanosecond)
				_, err = e.CreateQueue("projects/p/locations/l", QueueState{Name: testParent})
				assert.ErrorIs(t, err, ErrQueueRecentlyDeleted)

				// Once the cooldown elapses the name is reusable again.
				clock.Advance(time.Nanosecond)
				_, err = e.CreateQueue("projects/p/locations/l", QueueState{Name: testParent})
				assert.NoError(t, err)
			},
		},
		{
			name: "task name reusable after cooldown",
			run: func(t *testing.T, e *Engine, clock *fakeClock) {
				createRunningQueue(t, e)
				name := testParent + "/tasks/tomb"
				future := time.Now().Add(time.Hour)

				_, _, err := e.CreateTask(testParent, httpTaskState(name, future))
				require.NoError(t, err)
				require.NoError(t, e.DeleteTask(name))

				// During the cooldown the task reads as recently-deleted and the name
				// stays reserved against recreate.
				_, err = e.GetTask(name)
				assert.ErrorIs(t, err, ErrTaskRecentlyDeleted)
				_, _, err = e.CreateTask(testParent, httpTaskState(name, future))
				assert.ErrorIs(t, err, ErrTaskAlreadyExists)

				// Still reserved right up to the cooldown boundary.
				clock.Advance(ttl - time.Nanosecond)
				_, err = e.GetTask(name)
				assert.ErrorIs(t, err, ErrTaskRecentlyDeleted)

				// Past the cooldown the task is unknown and the name is reusable.
				clock.Advance(time.Nanosecond)
				_, err = e.GetTask(name)
				assert.ErrorIs(t, err, ErrTaskNotFound)
				_, _, err = e.CreateTask(testParent, httpTaskState(name, future))
				assert.NoError(t, err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock(base)
			e := newClockedTestEngine(t, clock, ttl)
			tc.run(t, e, clock)
		})
	}
}

func TestSweepRemovesExpiredTombstones(t *testing.T) {
	const ttl = time.Minute
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	e := newClockedTestEngine(t, clock, ttl)

	// Tombstone one task and one queue.
	_, err := e.CreateQueue("projects/p/locations/l", QueueState{Name: testParent})
	require.NoError(t, err)
	taskName := testParent + "/tasks/swept"
	_, _, err = e.CreateTask(testParent, httpTaskState(taskName, time.Now().Add(time.Hour)))
	require.NoError(t, err)
	require.NoError(t, e.DeleteTask(taskName))
	require.NoError(t, e.DeleteQueue(testParent))

	require.Equal(t, 1, e.countQueueTombstones())
	require.Equal(t, 1, e.countTaskTombstones())

	// Before the cooldown elapses the sweep keeps both tombstones.
	clock.Advance(ttl - time.Nanosecond)
	e.sweepTombstones()
	assert.Equal(t, 1, e.countQueueTombstones())
	assert.Equal(t, 1, e.countTaskTombstones())

	// Once the cooldown elapses the sweep prunes them.
	clock.Advance(time.Nanosecond)
	e.sweepTombstones()
	assert.Zero(t, e.countQueueTombstones())
	assert.Zero(t, e.countTaskTombstones())
}
