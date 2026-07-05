package engine

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func (b *blockingDispatcher) Dispatch(_ context.Context, _ TaskState, _ *oidc.Config, _ *slog.Logger) int {
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
		// A generous rate limit so the token bucket, a separate axis, does not
		// throttle the backlog and mask the concurrency bound under test.
		RateLimits: RateLimits{MaxConcurrentDispatches: maxConcurrent, MaxBurstSize: 100, MaxDispatchesPerSecond: 1000},
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
// spawn a worker goroutine per concurrency slot. Each queue defaults to
// MaxConcurrentDispatches = 1000; the old eager pool started that many worker
// goroutines per queue, the lazy pool starts none until there is work.
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
	// queues * 1000 the eager pool would have spawned.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine()-before < 50
	}, 2*time.Second, 10*time.Millisecond)
}

// TestPausedTaskDispatchesAfterResume is a regression test for the cancellation
// rewrite: a task queued while a queue is paused must still dispatch once the
// queue is resumed (the pre-rewrite scheme left a resumed queue unable to
// dispatch). Note that pause is not instantaneous - a dispatcher mid-loop may
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

// TestDeleteAfterPauseDoesNotHang is a regression test: deleting a queue that is
// already paused must not deadlock (the pre-rewrite cancellation scheme blocked
// the caller here forever).
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
