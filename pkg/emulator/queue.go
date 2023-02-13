package emulator

import (
	"log"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	pduration "github.com/golang/protobuf/ptypes/duration"

	tasks "google.golang.org/genproto/googleapis/cloud/tasks/v2"
)

// Queue holds all internals for a task queue
type Queue struct {
	name string

	state *tasks.Queue

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
}

// NewQueue creates a new task queue
func NewQueue(name string, state *tasks.Queue, onTaskDone func(task *Task)) (*Queue, *tasks.Queue) {
	setInitialQueueState(state)

	queue := &Queue{
		name:                   name,
		state:                  state,
		fire:                   make(chan *Task),
		work:                   make(chan *Task),
		ts:                     make(map[string]*Task),
		onTaskDone:             onTaskDone,
		tokenBucket:            make(chan bool, state.GetRateLimits().GetMaxBurstSize()),
		maxDispatchesPerSecond: state.GetRateLimits().GetMaxDispatchesPerSecond(),
		cancelTokenGenerator:   make(chan bool, 1),
		cancelDispatcher:       make(chan bool, 1),
		cancelWorkers:          make(chan bool, 1),
	}
	// Fill the token bucket
	for i := 0; i < int(state.GetRateLimits().GetMaxBurstSize()); i++ {
		queue.tokenBucket <- true
	}

	return queue, state
}

func (queue *Queue) setTask(taskName string, task *Task) {
	queue.tsMux.Lock()
	defer queue.tsMux.Unlock()
	queue.ts[taskName] = task
}

func (queue *Queue) removeTask(taskName string) {
	queue.setTask(taskName, nil)
}

func setInitialQueueState(queueState *tasks.Queue) {
	if queueState.GetRateLimits() == nil {
		queueState.RateLimits = &tasks.RateLimits{}
	}
	if queueState.GetRateLimits().GetMaxDispatchesPerSecond() == 0 {
		queueState.RateLimits.MaxDispatchesPerSecond = 500.0
	}
	if queueState.GetRateLimits().GetMaxBurstSize() == 0 {
		queueState.RateLimits.MaxBurstSize = 100
	}
	if queueState.GetRateLimits().GetMaxConcurrentDispatches() == 0 {
		queueState.RateLimits.MaxConcurrentDispatches = 1000
	}

	if queueState.GetRetryConfig() == nil {
		queueState.RetryConfig = &tasks.RetryConfig{}
	}
	if queueState.GetRetryConfig().GetMaxAttempts() == 0 {
		queueState.RetryConfig.MaxAttempts = 100
	}
	if queueState.GetRetryConfig().GetMaxDoublings() == 0 {
		queueState.RetryConfig.MaxDoublings = 16
	}
	if queueState.GetRetryConfig().GetMinBackoff() == nil {
		queueState.RetryConfig.MinBackoff = &pduration.Duration{
			Nanos: 100000000,
		}
	}
	if queueState.GetRetryConfig().GetMaxBackoff() == nil {
		queueState.RetryConfig.MaxBackoff = &pduration.Duration{
			Seconds: 3600,
		}
	}

	queueState.State = tasks.Queue_RUNNING
}

func (queue *Queue) runWorkers() {
	for i := 0; i < int(queue.state.GetRateLimits().GetMaxConcurrentDispatches()); i++ {
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

// NewTask creates a new task on the queue
func (queue *Queue) NewTask(newTaskState *tasks.Task) (*Task, *tasks.Task) {
	task := NewTask(queue, newTaskState, func(task *Task) {
		queue.removeTask(task.state.GetName())
		queue.onTaskDone(task)
	})

	taskState := proto.Clone(task.state).(*tasks.Task)

	queue.setTask(taskState.GetName(), task)

	task.Schedule()

	return task, taskState
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

// Goes beyond `Purge` behaviour to synchronously delete all tasks and their name handles
func (queue *Queue) HardReset(s *Server) {
	waitGroup := queue.Purge()
	waitGroup.Wait()

	// This is still a bit awkward - we can't *guarantee* the task is fully deleted even after the WaitGroup because:
	// - Purge() calls task.Delete()
	// - task.Delete() writes to a buffered `cancel` channel
	// - task.Schedule() reads from that buffered channel in a separate goroutine
	// - When that goroutine sees the task is cancelled, it sets the task value to nil in the tasks map
	//
	// We need to be certain that we only remove the task from map *after* that completes, otherwise the task name will
	// be reinserted with the nil value. At the moment the only easy way I can think of is to sleep for a very short
	// period to allow the tasks' internal goroutines to fire first.
	time.Sleep(10 * time.Millisecond)

	queue.tsMux.Lock()
	defer queue.tsMux.Unlock()
	for taskName, task := range queue.ts {
		if task != nil {
			// The naive "sleep till it deletes" approach described above is too naive...
			panic("Expected task to be deleted by now!")
		}

		delete(queue.ts, taskName)
		s.hardDeleteTask(taskName)
	}
}

// Pause pauses the queue
func (queue *Queue) Pause() {
	if !queue.paused {
		queue.paused = true
		queue.state.State = tasks.Queue_PAUSED

		queue.cancelDispatcher <- true
		queue.cancelWorkers <- true
	}
}

// Resume resumes a paused queue
func (queue *Queue) Resume() {
	if queue.paused {
		queue.paused = false
		queue.state.State = tasks.Queue_RUNNING

		go queue.runDispatcher()
		go queue.runWorkers()
	}
}
