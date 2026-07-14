package engine

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/oidc"
)

// Queue holds all internals for a task queue
type Queue struct {
	name string

	// stateMutex guards the queue lifecycle fields below (state, paused,
	// cancelled and stopDispatch), all of which are reachable concurrently from
	// gRPC handlers. tsMux (further down) separately guards the task map.
	stateMutex sync.Mutex

	state QueueState

	cancelled bool

	paused bool

	// stopAll is closed once, by Delete, to stop the token generator and (via
	// stopDispatch) the dispatcher. stopDispatch is closed to stop the dispatcher
	// on Pause or Delete, and replaced with a fresh channel by Resume so a new
	// generation of the dispatcher can be started. In-flight attempts launched by
	// a stopped generation are allowed to run to completion.
	stopAll      chan struct{}
	stopDispatch chan struct{}

	fire chan *Task

	// sem bounds the number of concurrent in-flight task.Attempt() calls to
	// MaxConcurrentDispatches. The dispatcher acquires a slot before launching a
	// per-task worker goroutine and the worker releases it on return, so an idle
	// queue holds no worker goroutines at all. It is a queue-level (not
	// per-generation) channel so attempts still in flight from a paused
	// generation keep holding their slots: a resumed generation must wait for
	// them before the bound can be exceeded.
	sem chan struct{}

	ts map[string]*Task

	tsMux sync.Mutex

	tokenBucket chan bool

	maxDispatchesPerSecond float64

	onTaskDone func(task *Task)

	// oidcCfg is the token-signing configuration used by tasks on this queue when
	// dispatching with an OIDC token. A value snapshot threaded down from the engine.
	oidcCfg oidc.Config

	// dispatcher delivers tasks on this queue. Threaded down from the engine so
	// tests can substitute a fake. Never nil for a live queue.
	dispatcher Dispatcher

	// logger receives this queue's lifecycle and dispatch diagnostics. Threaded
	// down from the engine (resolved to slog.Default() when the caller left it
	// unset). Never nil for a live queue.
	logger *slog.Logger

	// appEngineEmulatorHost is the base URL App Engine target tasks on this queue route
	// to instead of appspot.com. Threaded down from the engine; empty keeps the
	// production appspot.com routing.
	appEngineEmulatorHost string

	// appEngineRegionID is the App Engine region ID used in the default
	// appspot.com routing for tasks on this queue. Threaded down from the
	// engine; empty keeps the legacy <project>.appspot.com format.
	appEngineRegionID string

	// ctx bounds the lifetime of in-flight dispatches on this queue; cancel is
	// called exactly once, by Delete, to abort any HTTP requests still in
	// flight. It is deliberately not derived from a gRPC request context: the
	// request that created a task is long gone by the time it dispatches.
	ctx    context.Context
	cancel context.CancelFunc
}

// newQueue creates a new task queue
func newQueue(state QueueState, oidcCfg oidc.Config, dispatcher Dispatcher, logger *slog.Logger, appEngineEmulatorHost string, appEngineRegionID string, onTaskDone func(task *Task)) *Queue {
	setInitialQueueState(&state)

	ctx, cancel := context.WithCancel(context.Background())

	queue := &Queue{
		name:                   state.Name,
		state:                  state,
		fire:                   make(chan *Task),
		sem:                    make(chan struct{}, int(state.RateLimits.MaxConcurrentDispatches.OrZero())),
		ts:                     make(map[string]*Task),
		onTaskDone:             onTaskDone,
		oidcCfg:                oidcCfg,
		dispatcher:             dispatcher,
		logger:                 logger,
		appEngineEmulatorHost:  appEngineEmulatorHost,
		appEngineRegionID:      appEngineRegionID,
		tokenBucket:            make(chan bool, state.RateLimits.MaxBurstSize.OrZero()),
		maxDispatchesPerSecond: state.RateLimits.MaxDispatchesPerSecond.OrZero(),
		stopAll:                make(chan struct{}),
		stopDispatch:           make(chan struct{}),
		ctx:                    ctx,
		cancel:                 cancel,
	}
	// Fill the token bucket
	for i := 0; i < int(state.RateLimits.MaxBurstSize.OrZero()); i++ {
		queue.tokenBucket <- true
	}

	return queue
}

// State returns a snapshot of the queue state.
func (q *Queue) State() QueueState {
	q.stateMutex.Lock()
	defer q.stateMutex.Unlock()
	return q.state
}

func (queue *Queue) setTask(taskName string, task *Task) {
	queue.tsMux.Lock()
	defer queue.tsMux.Unlock()
	queue.ts[taskName] = task
}

func (queue *Queue) removeTask(taskName string) {
	queue.setTask(taskName, nil)
}

// retireTask tombstones a task's slot in the queue map (a nil entry) when it
// terminates, but only while the task still owns that slot. A task deleted long
// ago may fire its terminal callback late; the ownership check stops it from
// clobbering a same-named task created after the name became reusable.
func (queue *Queue) retireTask(task *Task) {
	queue.tsMux.Lock()
	defer queue.tsMux.Unlock()
	if cur, ok := queue.ts[task.state.Name]; ok && cur == task {
		queue.ts[task.state.Name] = nil
	}
}

// Server defaults applied to absent queue-configuration fields, matching the
// documented Cloud Tasks defaults.
const (
	defaultMaxDispatchesPerSecond        = 500.0
	defaultMaxBurstSize            int32 = 100
	defaultMaxConcurrentDispatches int32 = 1000
	defaultMaxAttempts             int32 = 100
	defaultMaxDoublings            int32 = 16
	defaultMinBackoff                    = 100 * time.Millisecond
	defaultMaxBackoff                    = 3600 * time.Second
)

// Upper bounds on client-supplied queue configuration, from the documented
// Cloud Tasks limits (see the field docs on tasks.RateLimits). The burst-size
// bound is queue.yaml's bucket_size limit: the v2 API treats max_burst_size as
// output-only, but the emulator accepts it as input, so it bounds it too.
const (
	maxAllowedDispatchesPerSecond        = 500.0
	maxAllowedBurstSize            int32 = 500
	maxAllowedConcurrentDispatches int32 = 5000
)

// validateQueueConfig rejects out-of-range RateLimits/RetryConfig values on a
// queue-creation input, as real Cloud Tasks does (verified against the
// queue-invalid-config cases in conformance/golden/errors.json). Beyond
// fidelity, this guards the emulator's own internals: newQueue sizes channels
// from MaxBurstSize and MaxConcurrentDispatches (a negative capacity panics
// make), and runTokenGenerator derives its refill period from
// MaxDispatchesPerSecond (a non-positive rate breaks that arithmetic). The
// MaxBurstSize check is engine-only: on the wire the field is output-only and
// dropped at the proto edge, mirroring real v2, so it can only be set by
// embedded callers. Absent fields are valid and get server defaults
// (setInitialQueueState); the backoff-order check therefore compares the
// effective values, defaults included.
func validateQueueConfig(s QueueState) error {
	if v, ok := s.RateLimits.MaxDispatchesPerSecond.Get(); ok {
		if v > maxAllowedDispatchesPerSecond {
			return ErrMaxDispatchesPerSecondTooHigh
		}
		// Written !(v > 0) rather than v < 0 so zero (unrepresentable on the
		// wire, where proto3 zero means unset, but possible for embedded
		// callers) and NaN (which fails every ordered comparison) are rejected
		// too - the token generator cannot run with either.
		if !(v > 0) {
			return ErrMaxDispatchesPerSecondNegative
		}
	}
	if v, ok := s.RateLimits.MaxBurstSize.Get(); ok && (v < 1 || v > maxAllowedBurstSize) {
		return ErrMaxBurstSizeRange
	}
	if v, ok := s.RateLimits.MaxConcurrentDispatches.Get(); ok {
		if v > maxAllowedConcurrentDispatches {
			return ErrMaxConcurrentDispatchesTooHigh
		}
		// Zero (embedded callers only, see above) would mean a semaphore no
		// dispatch could ever acquire a slot on.
		if v < 1 {
			return ErrMaxConcurrentDispatchesNegative
		}
	}
	// -1 is the documented "unlimited attempts" marker.
	if v, ok := s.RetryConfig.MaxAttempts.Get(); ok && v < -1 {
		return ErrMaxAttemptsRange
	}
	if v, ok := s.RetryConfig.MaxDoublings.Get(); ok && v < 0 {
		return ErrMaxDoublingsNegative
	}
	if v, ok := s.RetryConfig.MinBackoff.Get(); ok && v < 0 {
		return ErrMinBackoffNegative
	}
	if v, ok := s.RetryConfig.MaxBackoff.Get(); ok && v < 0 {
		return ErrMaxBackoffNegative
	}
	if s.RetryConfig.MinBackoff.OrElse(defaultMinBackoff) > s.RetryConfig.MaxBackoff.OrElse(defaultMaxBackoff) {
		return ErrBackoffOrder
	}
	return nil
}

func setInitialQueueState(s *QueueState) {
	s.RateLimits.MaxDispatchesPerSecond = s.RateLimits.MaxDispatchesPerSecond.Or(defaultMaxDispatchesPerSecond)
	s.RateLimits.MaxBurstSize = s.RateLimits.MaxBurstSize.Or(defaultMaxBurstSize)
	s.RateLimits.MaxConcurrentDispatches = s.RateLimits.MaxConcurrentDispatches.Or(defaultMaxConcurrentDispatches)

	s.RetryConfig.MaxAttempts = s.RetryConfig.MaxAttempts.Or(defaultMaxAttempts)
	s.RetryConfig.MaxDoublings = s.RetryConfig.MaxDoublings.Or(defaultMaxDoublings)
	s.RetryConfig.MinBackoff = s.RetryConfig.MinBackoff.Or(defaultMinBackoff)
	s.RetryConfig.MaxBackoff = s.RetryConfig.MaxBackoff.Or(defaultMaxBackoff)

	s.State = QueueRunStateRunning
}

// startDispatch launches the dispatcher listening on the supplied stop channel.
// Closing that channel stops this generation of the dispatcher; Resume starts a
// fresh generation with a new channel. The dispatcher spawns worker goroutines
// lazily - at most one per task and never more than MaxConcurrentDispatches at
// once - so an idle queue holds no worker goroutines beyond the dispatcher and
// token generator.
func (queue *Queue) startDispatch(stop <-chan struct{}) {
	go queue.runDispatcher(stop)
}

func (queue *Queue) runTokenGenerator(stop <-chan struct{}) {
	// The refill period is computed in float64: integer Duration division would
	// truncate a fractional rate (e.g. 0.5/s, legal in Cloud Tasks) to a zero
	// divisor. A rate so small the period overflows time.Duration saturates at
	// the maximum instead.
	period := time.Duration(math.MaxInt64)
	if p := float64(time.Second) / queue.maxDispatchesPerSecond; p < math.MaxInt64 {
		period = time.Duration(p)
	}
	// Use Timer with Reset() in place of time.Ticker as the latter was causing high CPU usage in Docker
	t := time.NewTimer(period)

	for {
		select {
		case <-t.C:
			select {
			case queue.tokenBucket <- true:
				// Added token
				t.Reset(period)
			case <-stop:
				return
			}
		case <-stop:
			if !t.Stop() {
				<-t.C
			}
			return
		}
	}
}

func (queue *Queue) runDispatcher(stop <-chan struct{}) {
	for {
		select {
		// Consume a token
		case <-queue.tokenBucket:
			select {
			// Wait for task
			case task := <-queue.fire:
				// Acquire a concurrency slot before dispatching, unless we are
				// stopping (guard the acquire so Pause/Delete can't wedge here
				// while every slot is held). The worker releases the slot on
				// return; slots are shared across generations, so at most
				// MaxConcurrentDispatches attempts run at once even across a
				// pause/resume.
				select {
				case queue.sem <- struct{}{}:
					go func() {
						defer func() { <-queue.sem }()
						task.Attempt()
					}()
				case <-stop:
					// We already committed this task by receiving it from fire,
					// so we must not drop it: re-arm it so the next dispatcher
					// generation (after Resume) picks it up, or so Delete's Purge
					// can cancel it. Dropping it here loses the task entirely -
					// its Schedule goroutine has already returned and nothing
					// else re-schedules it.
					task.Schedule()
					return
				}
			case <-stop:
				return
			}
		case <-stop:
			return
		}
	}
}

// Run starts the queue (token generator and dispatcher)
func (queue *Queue) Run() {
	queue.stateMutex.Lock()
	stopAll := queue.stopAll
	stopDispatch := queue.stopDispatch
	queue.stateMutex.Unlock()

	go queue.runTokenGenerator(stopAll)
	queue.startDispatch(stopDispatch)
}

// NewTask creates a new task on the queue. Returns the live *Task and a snapshot
// of its state immediately after creation.
func (queue *Queue) NewTask(taskState TaskState) (*Task, TaskState) {
	task := newTask(queue, taskState, func(task *Task) {
		queue.retireTask(task)
		queue.onTaskDone(task)
	})

	frozen := task.state

	queue.setTask(frozen.Name, task)

	task.Schedule()

	return task, frozen
}

// closeDispatchLocked closes the current stopDispatch channel unless it is
// already closed. The caller must hold stateMutex.
func (queue *Queue) closeDispatchLocked() {
	select {
	case <-queue.stopDispatch:
		// Already closed (queue is paused or being deleted).
	default:
		close(queue.stopDispatch)
	}
}

// Delete stops, purges and removes the queue
func (queue *Queue) Delete() {
	queue.stateMutex.Lock()
	if queue.cancelled {
		queue.stateMutex.Unlock()
		return
	}
	queue.cancelled = true
	queue.logger.Info("stopping queue", "queue", queue.name)
	// Close-to-broadcast: stops the token generator (stopAll) and the dispatcher
	// (stopDispatch, idempotent if the queue is paused). In-flight attempts run
	// to completion.
	close(queue.stopAll)
	queue.closeDispatchLocked()
	// Abort any HTTP requests currently in flight on this queue.
	queue.cancel()
	queue.stateMutex.Unlock()

	queue.Purge()
}

// Purge purges all tasks from the queue
// - Normally this is a fire-and-forget operation, but it returns a WaitGroup to allow HardReset to wait for completion
func (queue *Queue) Purge() *sync.WaitGroup {
	waitGroup := &sync.WaitGroup{}

	waitGroup.Go(func() {
		queue.tsMux.Lock()
		defer queue.tsMux.Unlock()

		for _, task := range queue.ts {
			// Avoid task firing
			if task != nil {
				task.Delete()
			}
		}
	})

	return waitGroup
}

// Pause pauses the queue
func (queue *Queue) Pause() {
	queue.stateMutex.Lock()
	defer queue.stateMutex.Unlock()
	if queue.cancelled || queue.paused {
		return
	}
	queue.paused = true
	queue.state.State = QueueRunStatePaused

	// Stop the dispatcher; the token generator keeps filling the bucket so a
	// resumed queue can dispatch immediately.
	queue.closeDispatchLocked()
}

// Resume resumes a paused queue
func (queue *Queue) Resume() {
	queue.stateMutex.Lock()
	defer queue.stateMutex.Unlock()
	if queue.cancelled || !queue.paused {
		return
	}
	queue.paused = false
	queue.state.State = QueueRunStateRunning

	// A fresh stop channel for the new generation of the dispatcher; the
	// previous one stays closed.
	queue.stopDispatch = make(chan struct{})
	queue.startDispatch(queue.stopDispatch)
}
