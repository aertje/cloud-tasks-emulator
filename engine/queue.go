package engine

import (
	"log"
	"sync"
	"time"
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
	// stopDispatch) the dispatcher and workers. stopDispatch is closed to stop
	// the dispatcher and workers on Pause or Delete, and replaced with a fresh
	// channel by Resume so a new generation of goroutines can be started.
	stopAll      chan struct{}
	stopDispatch chan struct{}

	fire chan *Task

	work chan *Task

	ts map[string]*Task

	tsMux sync.Mutex

	tokenBucket chan bool

	maxDispatchesPerSecond float64

	onTaskDone func(task *Task)

	// oidc is the token-signing configuration used by tasks on this queue when
	// dispatching with an OIDC token. Threaded down from the engine.
	oidc *OIDCConfig

	// dispatcher delivers tasks on this queue. Threaded down from the engine so
	// tests can substitute a fake. Never nil for a live queue.
	dispatcher Dispatcher
}

// newQueue creates a new task queue
func newQueue(state QueueState, oidc *OIDCConfig, dispatcher Dispatcher, onTaskDone func(task *Task)) *Queue {
	setInitialQueueState(&state)

	queue := &Queue{
		name:                   state.Name,
		state:                  state,
		fire:                   make(chan *Task),
		work:                   make(chan *Task),
		ts:                     make(map[string]*Task),
		onTaskDone:             onTaskDone,
		oidc:                   oidc,
		dispatcher:             dispatcher,
		tokenBucket:            make(chan bool, state.RateLimits.MaxBurstSize),
		maxDispatchesPerSecond: state.RateLimits.MaxDispatchesPerSecond,
		stopAll:                make(chan struct{}),
		stopDispatch:           make(chan struct{}),
	}
	// Fill the token bucket
	for i := 0; i < int(state.RateLimits.MaxBurstSize); i++ {
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

func setInitialQueueState(s *QueueState) {
	if s.RateLimits.MaxDispatchesPerSecond == 0 {
		s.RateLimits.MaxDispatchesPerSecond = 500.0
	}
	if s.RateLimits.MaxBurstSize == 0 {
		s.RateLimits.MaxBurstSize = 100
	}
	if s.RateLimits.MaxConcurrentDispatches == 0 {
		s.RateLimits.MaxConcurrentDispatches = 1000
	}

	if s.RetryConfig.MaxAttempts == 0 {
		s.RetryConfig.MaxAttempts = 100
	}
	if s.RetryConfig.MaxDoublings == 0 {
		s.RetryConfig.MaxDoublings = 16
	}
	if s.RetryConfig.MinBackoff == 0 {
		s.RetryConfig.MinBackoff = 100 * time.Millisecond
	}
	if s.RetryConfig.MaxBackoff == 0 {
		s.RetryConfig.MaxBackoff = 3600 * time.Second
	}

	s.State = QueueRunStateRunning
}

// startDispatch launches the dispatcher and its worker pool, all listening on
// the supplied stop channel. Closing that channel stops this generation of
// goroutines; Resume starts a fresh generation with a new channel.
func (queue *Queue) startDispatch(stop <-chan struct{}) {
	for i := 0; i < int(queue.state.RateLimits.MaxConcurrentDispatches); i++ {
		go queue.runWorker(stop)
	}
	go queue.runDispatcher(stop)
}

func (queue *Queue) runWorker(stop <-chan struct{}) {
	for {
		select {
		case task := <-queue.work:
			task.Attempt()
		case <-stop:
			return
		}
	}
}

func (queue *Queue) runTokenGenerator(stop <-chan struct{}) {
	period := time.Second / time.Duration(queue.maxDispatchesPerSecond)
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
				// Pass on to workers, unless we are stopping (in which case the
				// workers may already have exited, so guard the send).
				select {
				case queue.work <- task:
				case <-stop:
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

// Run starts the queue (workers, token generator and dispatcher)
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
		queue.removeTask(task.state.Name)
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
	log.Println("Stopping queue")
	// Close-to-broadcast: stops the token generator (stopAll) and the dispatcher
	// plus every worker (stopDispatch, idempotent if the queue is paused).
	close(queue.stopAll)
	queue.closeDispatchLocked()
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

	// Stop the dispatcher and workers; the token generator keeps filling the
	// bucket so a resumed queue can dispatch immediately.
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

	// A fresh stop channel for the new generation of dispatcher/workers; the
	// previous one stays closed.
	queue.stopDispatch = make(chan struct{})
	queue.startDispatch(queue.stopDispatch)
}
