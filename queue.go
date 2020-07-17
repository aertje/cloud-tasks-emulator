package main

import (
	"log"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	pduration "github.com/golang/protobuf/ptypes/duration"

	tasks "google.golang.org/genproto/googleapis/cloud/tasks/v2beta3"
)

// Queue holds all internals for a task queue
type Queue struct {
	name string

	state *tasks.Queue

	fire chan *Task

	work chan *Task

	ts sync.Map

	tokenBucket chan bool

	tokenGenerator *time.Ticker

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
		name:                 name,
		state:                state,
		fire:                 make(chan *Task),
		work:                 make(chan *Task),
		ts:                   sync.Map{},
		onTaskDone:           onTaskDone,
		tokenBucket:          make(chan bool, state.GetRateLimits().GetMaxBurstSize()),
		tokenGenerator:       time.NewTicker(time.Second / time.Duration(state.GetRateLimits().GetMaxDispatchesPerSecond())),
		cancelTokenGenerator: make(chan bool, 1),
		cancelDispatcher:     make(chan bool, 1),
		cancelWorkers:        make(chan bool, 1),
	}
	// Fill the token bucket
	for i := 0; i < int(state.GetRateLimits().GetMaxBurstSize()); i++ {
		queue.tokenBucket <- true
	}

	return queue, state
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
	defer queue.tokenGenerator.Stop()

	for {
		select {
		case <-queue.tokenGenerator.C:
			select {
			case queue.tokenBucket <- true:
				// Added token
			default:
				// Bucket is full (fall through)
			}
		case <-queue.cancelTokenGenerator:
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
		queue.ts.Store(task.state.GetName(), nil)
		queue.onTaskDone(task)
	})

	taskState := proto.Clone(task.state).(*tasks.Task)

	queue.ts.Store(taskState.GetName(), task)

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
func (queue *Queue) Purge() {
	go func() {
		queue.ts.Range(func(_, v interface{}) bool {
			// Avoid task firing
			if v != nil {
				task := v.(*Task)
				task.Delete()
			}
			return true
		})
	}()
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
