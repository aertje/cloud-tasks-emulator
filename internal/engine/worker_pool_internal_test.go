package engine

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/maybe"
	"github.com/aertje/cloud-tasks-emulator/v2/internal/oidc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockingDispatcher blocks every dispatch until released, tracking how many
// attempts are in flight simultaneously. It lets the worker-pool tests assert
// the concurrency bound without real sleeps: tests poll on active/maxSeen.
type blockingDispatcher struct {
	status  int
	release chan struct{} // closed once to release all current and future attempts
	active  atomic.Int32  // attempts currently blocked inside Dispatch
	maxSeen atomic.Int32  // high-water mark of active

	mu    sync.Mutex
	calls int
}

func newBlockingDispatcher(status int) *blockingDispatcher {
	return &blockingDispatcher{status: status, release: make(chan struct{})}
}

func (b *blockingDispatcher) Dispatch(_ context.Context, _ TaskState, _ oidc.Config, _ time.Time) int {
	n := b.active.Add(1)
	for {
		m := b.maxSeen.Load()
		if n <= m || b.maxSeen.CompareAndSwap(m, n) {
			break
		}
	}

	<-b.release

	b.active.Add(-1)
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	return b.status
}

func (b *blockingDispatcher) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *blockingDispatcher) releaseAll() { close(b.release) }

// TestDispatchConcurrencyIsBounded verifies that no more than
// MaxConcurrentDispatches attempts run concurrently even when the backlog of
// ready tasks far exceeds the concurrency limit.
func TestDispatchConcurrencyIsBounded(t *testing.T) {
	const maxConcurrent = 3

	d := newBlockingDispatcher(200)
	e := newTestEngine(t, d)

	_, err := e.CreateQueue(t.Context(), "projects/p/locations/l", QueueState{
		Name: testParent,
		// A generous rate limit (the allowed maximum) so the token bucket, a
		// separate axis, does not throttle the backlog and mask the concurrency
		// bound under test.
		RateLimits: RateLimits{MaxConcurrentDispatches: maybe.Some[int32](maxConcurrent), MaxBurstSize: maybe.Some[int32](100), MaxDispatchesPerSecond: maybe.Some[float64](500)},
	})
	require.NoError(t, err)

	// Queue far more ready tasks than there are concurrency slots.
	const tasks = 4 * maxConcurrent
	for range tasks {
		_, _, err := e.CreateTask(t.Context(), testParent, httpTaskState("", time.Time{}))
		require.NoError(t, err)
	}

	// The pool saturates at exactly maxConcurrent in-flight attempts...
	require.Eventually(t, func() bool {
		return d.active.Load() == maxConcurrent
	}, 2*time.Second, 5*time.Millisecond)

	// ...and never exceeds it while every slot stays held.
	require.Never(t, func() bool {
		return d.active.Load() > maxConcurrent
	}, 200*time.Millisecond, 5*time.Millisecond)

	// Release the backlog and let it drain; the bound must hold throughout.
	d.releaseAll()
	require.Eventually(t, func() bool {
		return d.count() == tasks && d.active.Load() == 0
	}, 2*time.Second, 5*time.Millisecond)

	assert.Equal(t, int32(maxConcurrent), d.maxSeen.Load(), "concurrency high-water mark must equal the bound")
}

// TestIdleQueuesDoNotLeakGoroutines verifies that creating idle queues does not
// spawn a worker goroutine per concurrency slot: an idle queue must start no
// worker goroutines until there is work, even though each queue defaults to
// MaxConcurrentDispatches = 1000.
func TestIdleQueuesDoNotLeakGoroutines(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))

	before := runtime.NumGoroutine()

	const queues = 5
	for i := range queues {
		_, err := e.CreateQueue(t.Context(), "projects/p/locations/l",
			QueueState{Name: fmt.Sprintf("%s-%d", testParent, i)})
		require.NoError(t, err)
	}

	// Let any transient startup goroutines settle, then assert the increase is a
	// small constant per queue (token generator + dispatcher), nowhere near the
	// queues * 1000 a worker-per-slot pool would spawn.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine()-before < 50
	}, 2*time.Second, 10*time.Millisecond)
}

// TestPausedTaskDispatchesAfterResume verifies that a task queued while a queue
// is paused still dispatches once the queue is resumed. Note that pause is not
// instantaneous - a dispatcher mid-loop may
// deliver one task right at the pause boundary - so this asserts eventual
// dispatch rather than the absence of dispatch during the pause.
func TestPausedTaskDispatchesAfterResume(t *testing.T) {
	d := newFakeDispatcher(200)
	e := newTestEngine(t, d)
	createRunningQueue(t, e)

	_, err := e.PauseQueue(t.Context(), testParent)
	require.NoError(t, err)

	// Queue a ready task while paused, then resume; it must dispatch.
	_, _, err = e.CreateTask(t.Context(), testParent, httpTaskState("", time.Time{}))
	require.NoError(t, err)

	_, err = e.ResumeQueue(t.Context(), testParent)
	require.NoError(t, err)
	d.awaitDispatches(t, 1, 2*time.Second)
	assert.GreaterOrEqual(t, d.count(), 1)
}

// TestConcurrentCreateQueueSameName verifies that when many goroutines race to
// create a queue with the same name, exactly one wins and the losers are
// rejected as already-existing. Crucially, the losers' queues must never start
// their goroutines: the pre-fix check-then-insert let multiple creates win and
// leak the losers' token-generator and dispatcher goroutines forever (they have
// no Delete path).
func TestConcurrentCreateQueueSameName(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))

	before := runtime.NumGoroutine()

	const goroutines = 20
	var (
		start     = make(chan struct{})
		wg        sync.WaitGroup
		successes atomic.Int32
	)
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Go(func() {
			<-start
			_, err := e.CreateQueue(context.Background(), "projects/p/locations/l", QueueState{Name: testParent})
			if err == nil {
				successes.Add(1)
			}
			errs[i] = err
		})
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), successes.Load(), "exactly one create must win the name")
	for _, err := range errs {
		if err != nil {
			assert.ErrorIs(t, err, ErrQueueAlreadyExists)
		}
	}

	// Only the winning queue may run its goroutines; the losers must have been
	// discarded. A per-loser leak would add roughly goroutines' worth on top of
	// the single winner's small constant.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine()-before < goroutines
	}, 2*time.Second, 10*time.Millisecond)
}

// TestConcurrentCreateTaskSameName verifies that racing creates of a task with
// the same explicit name yield exactly one winner and that the task is
// scheduled (and dispatched) only once: the pre-fix check-then-insert let
// multiple creates win and each schedule its own copy, double-dispatching.
func TestConcurrentCreateTaskSameName(t *testing.T) {
	d := newFakeDispatcher(200)
	e := newTestEngine(t, d)
	createRunningQueue(t, e)

	name := testParent + "/tasks/racy"

	const goroutines = 20
	var (
		start     = make(chan struct{})
		wg        sync.WaitGroup
		successes atomic.Int32
	)
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Go(func() {
			<-start
			_, _, err := e.CreateTask(context.Background(), testParent, httpTaskState(name, time.Time{}))
			if err == nil {
				successes.Add(1)
			}
			errs[i] = err
		})
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), successes.Load(), "exactly one create must win the name")
	for _, err := range errs {
		if err != nil {
			assert.ErrorIs(t, err, ErrTaskAlreadyExists)
		}
	}

	// The single winning task dispatches exactly once. A second scheduled copy
	// (the pre-fix double-dispatch) would push the count past one.
	d.awaitDispatches(t, 1, 2*time.Second)
	require.Never(t, func() bool { return d.count() > 1 }, 200*time.Millisecond, 10*time.Millisecond)
}

// TestDeleteAfterPauseDoesNotHang verifies that deleting an already-paused queue
// does not deadlock the caller.
func TestDeleteAfterPauseDoesNotHang(t *testing.T) {
	e := newTestEngine(t, newFakeDispatcher(200))
	createRunningQueue(t, e)

	_, err := e.PauseQueue(t.Context(), testParent)
	require.NoError(t, err)

	errc := make(chan error, 1)
	go func() { errc <- e.DeleteQueue(context.Background(), testParent) }()

	select {
	case err := <-errc:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteQueue after PauseQueue hung")
	}
}
