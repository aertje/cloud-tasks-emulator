package engine

import (
	"log"
	"sync"
	"time"
)

// Queue holds all internals for a task queue
type Queue struct {
	name string

	state QueueState

	fire chan *Task

	work chan *Task

	ts map[string]*Task

	tsMux sync.Mutex

	tokenBucket chan bool

	maxDispatchesPerSecond float64

	cancelTokenGenerator chan bool

	cancelDispatcher chan bool

	cancelWorkers chan bool

	cancelled bool

	paused bool

	onTaskDone func(task *Task)

	// oidc is the token-signing configuration used by tasks on this queue when
	// dispatching with an OIDC token. Threaded down from the engine.
	oidc *OIDCConfig
}

// newQueue creates a new task queue
func newQueue(state QueueState, oidc *OIDCConfig, onTaskDone func(task *Task)) *Queue {
	setInitialQueueState(&state)

	queue := &Queue{
		name:                   state.Name,
		state:                  state,
		fire:                   make(chan *Task),
		work:                   make(chan *Task),
		ts:                     make(map[string]*Task),
		onTaskDone:             onTaskDone,
		oidc:                   oidc,
		tokenBucket:            make(chan bool, state.RateLimits.MaxBurstSize),
		maxDispatchesPerSecond: state.RateLimits.MaxDispatchesPerSecond,
		cancelTokenGenerator:   make(chan bool, 1),
		cancelDispatcher:       make(chan bool, 1),
		cancelWorkers:          make(chan bool, 1),
	}
	// Fill the token bucket
	for i := 0; i < int(state.RateLimits.MaxBurstSize); i++ {
		queue.tokenBucket <- true
	}

	return queue
}

// State returns a snapshot of the queue state.
func (q *Queue) State() QueueState {
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

func (queue *Queue) runWorkers() {
	for i := 0; i < int(queue.state.RateLimits.MaxConcurrentDispatches); i++ {
		go queue.runWorker()
	}
}

func (queue *Queue) runWorker() {
	for {
		select {
		case task := <-queue.work:
			task.Attempt()
		case <-queue.cancelWorkers:
			// Forward for next worker
			queue.cancelWorkers <- true
			return
		}
	}
}

func (queue *Queue) runTokenGenerator() {
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
			case <-queue.cancelTokenGenerator:
				return
			}
		case <-queue.cancelTokenGenerator:
			if !t.Stop() {
				<-t.C
			}
			return
		}
	}
}

func (queue *Queue) runDispatcher() {
	for {
		select {
		// Consume a token
		case <-queue.tokenBucket:
			select {
			// Wait for task
			case task := <-queue.fire:
				// Pass on to workers
				queue.work <- task
			case <-queue.cancelDispatcher:
				return
			}
		case <-queue.cancelDispatcher:
			return
		}
	}
}

// Run starts the queue (workers, token generator and dispatcher)
func (queue *Queue) Run() {
	go queue.runWorkers()
	go queue.runTokenGenerator()
	go queue.runDispatcher()
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

// Delete stops, purges and removes the queue
func (queue *Queue) Delete() {
	if !queue.cancelled {
		queue.cancelled = true
		log.Println("Stopping queue")
		queue.cancelTokenGenerator <- true
		queue.cancelDispatcher <- true
		queue.cancelWorkers <- true

		queue.Purge()
	}
}

// Purge purges all tasks from the queue
// - Normally this is a fire-and-forget operation, but it returns a WaitGroup to allow HardReset to wait for completion
func (queue *Queue) Purge() *sync.WaitGroup {
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)

	go func() {
		defer waitGroup.Done()

		queue.tsMux.Lock()
		defer queue.tsMux.Unlock()

		for _, task := range queue.ts {
			// Avoid task firing
			if task != nil {
				task.Delete()
			}
		}
	}()

	return &waitGroup
}

// Pause pauses the queue
func (queue *Queue) Pause() {
	if !queue.paused {
		queue.paused = true
		queue.state.State = QueueRunStatePaused

		queue.cancelDispatcher <- true
		queue.cancelWorkers <- true
	}
}

// Resume resumes a paused queue
func (queue *Queue) Resume() {
	if queue.paused {
		queue.paused = false
		queue.state.State = QueueRunStateRunning

		go queue.runDispatcher()
		go queue.runWorkers()
	}
}
