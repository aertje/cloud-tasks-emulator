package emulator

import (
	"log"
	"sync"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Queue holds all internals for a task queue
type Queue struct {
	name  string
	state *taskspb.Queue

	ts    map[string]*Task
	tsMux sync.Mutex

	fire chan *Task
	work chan *Task

	tokenBucket chan bool

	maxDispatchesPerSecond float64

	cancelTokenGenerator chan bool
	cancelDispatcher     chan bool
	cancelWorkers        chan bool

	cancelled bool
	paused    bool
}

// NewQueue creates a new task queue
func NewQueue(name string, state *taskspb.Queue) *Queue {
	queue := &Queue{
		name:                   name,
		state:                  state,
		fire:                   make(chan *Task),
		work:                   make(chan *Task),
		ts:                     make(map[string]*Task),
		tokenBucket:            make(chan bool, state.GetRateLimits().GetMaxBurstSize()),
		maxDispatchesPerSecond: state.GetRateLimits().GetMaxDispatchesPerSecond(),
		cancelTokenGenerator:   make(chan bool, 1),
		cancelDispatcher:       make(chan bool, 1),
		cancelWorkers:          make(chan bool, 1),
	}

	queue.setInitialQueueState()

	// Fill the token bucket
	for i := 0; i < int(state.GetRateLimits().GetMaxBurstSize()); i++ {
		queue.tokenBucket <- true
	}

	return queue
}

func (q *Queue) stateCopy() *taskspb.Queue {
	return proto.Clone(q.state).(*taskspb.Queue)
}

func (q *Queue) fetchTask(taskName string) (*Task, bool) {
	q.tsMux.Lock()
	defer q.tsMux.Unlock()
	task, ok := q.ts[taskName]
	return task, ok
}

func (q *Queue) setTask(taskName string, task *Task) {
	q.tsMux.Lock()
	defer q.tsMux.Unlock()
	q.ts[taskName] = task
}

func (q *Queue) removeTask(taskName string) {
	q.setTask(taskName, nil)
}

func (q *Queue) setInitialQueueState() {
	if q.state.GetRateLimits() == nil {
		q.state.RateLimits = &taskspb.RateLimits{}
	}
	if q.state.GetRateLimits().GetMaxDispatchesPerSecond() == 0 {
		q.state.RateLimits.MaxDispatchesPerSecond = 500.0
	}
	if q.state.GetRateLimits().GetMaxBurstSize() == 0 {
		q.state.RateLimits.MaxBurstSize = 100
	}
	if q.state.GetRateLimits().GetMaxConcurrentDispatches() == 0 {
		q.state.RateLimits.MaxConcurrentDispatches = 1000
	}
	if q.state.GetRetryConfig() == nil {
		q.state.RetryConfig = &taskspb.RetryConfig{}
	}
	if q.state.GetRetryConfig().GetMaxAttempts() == 0 {
		q.state.RetryConfig.MaxAttempts = 100
	}
	if q.state.GetRetryConfig().GetMaxDoublings() == 0 {
		q.state.RetryConfig.MaxDoublings = 16
	}
	if q.state.GetRetryConfig().GetMinBackoff() == nil {
		q.state.RetryConfig.MinBackoff = &durationpb.Duration{
			Nanos: 100000000,
		}
	}
	if q.state.GetRetryConfig().GetMaxBackoff() == nil {
		q.state.RetryConfig.MaxBackoff = &durationpb.Duration{
			Seconds: 3600,
		}
	}

	q.state.State = taskspb.Queue_STATE_UNSPECIFIED
}

func (q *Queue) runWorkers() {
	for i := 0; i < int(q.state.GetRateLimits().GetMaxConcurrentDispatches()); i++ {
		go q.runWorker()
	}
}

func (q *Queue) runWorker() {
	for {
		select {
		case task := <-q.work:
			task.Attempt()
		case <-q.cancelWorkers:
			// Forward for next worker
			q.cancelWorkers <- true
			return
		}
	}
}

func (q *Queue) runTokenGenerator() {
	period := time.Second / time.Duration(q.maxDispatchesPerSecond)
	// Use Timer with Reset() in place of time.Ticker as the latter was causing high CPU usage in Docker
	t := time.NewTimer(period)

	for {
		select {
		case <-t.C:
			select {
			case q.tokenBucket <- true:
				// Added token
				t.Reset(period)
			case <-q.cancelTokenGenerator:
				return
			}
		case <-q.cancelTokenGenerator:
			if !t.Stop() {
				<-t.C
			}
			return
		}
	}
}

func (q *Queue) runDispatcher() {
	for {
		select {
		// Consume a token
		case <-q.tokenBucket:
			select {
			// Wait for task
			case task := <-q.fire:
				// Pass on to workers
				q.work <- task
			case <-q.cancelDispatcher:
				return
			}
		case <-q.cancelDispatcher:
			return
		}
	}
}

// Run starts the queue (workers, token generator and dispatcher)
func (q *Queue) Run() *taskspb.Queue {
	q.state.State = taskspb.Queue_RUNNING

	queueState := q.stateCopy()

	go q.runWorkers()
	go q.runTokenGenerator()
	go q.runDispatcher()

	return queueState
}

// NewTask creates a new task on the queue
func (q *Queue) NewTask(newTaskState *taskspb.Task) (*Task, *taskspb.Task) {
	task := NewTask(q, newTaskState, func(task *Task) {
		q.removeTask(task.state.GetName())
	})

	taskState := proto.Clone(task.state).(*taskspb.Task)

	q.setTask(taskState.GetName(), task)

	task.Schedule()

	return task, taskState
}

// Delete stops, purges and removes the queue
func (q *Queue) Delete() {
	if !q.cancelled {
		q.cancelled = true
		log.Println("Stopping queue")
		q.cancelTokenGenerator <- true
		q.cancelDispatcher <- true
		q.cancelWorkers <- true

		q.Purge()
	}
}

// Purge purges all tasks from the queue
func (q *Queue) Purge() {
	go func() {
		q.tsMux.Lock()
		defer q.tsMux.Unlock()

		for _, task := range q.ts {
			if task != nil {
				// Avoid task firing but still allow onTaskDone callbacks
				task.Delete(true)
			}
		}
	}()
}

// Goes beyond `Purge` behaviour to synchronously delete all tasks and their name handles
func (q *Queue) HardReset(s *Server) {
	q.tsMux.Lock()
	defer q.tsMux.Unlock()

	for taskName, task := range q.ts {
		// Avoid task firing
		if task != nil {
			// Avoid callback, we do map cleanup locally and synchronously
			task.Delete(false)
		}

		delete(q.ts, taskName)
	}
}

// Pause pauses the queue
func (q *Queue) Pause() {
	if !q.paused {
		q.paused = true
		q.state.State = taskspb.Queue_PAUSED

		q.cancelDispatcher <- true
		q.cancelWorkers <- true
	}
}

// Resume resumes a paused queue
func (q *Queue) Resume() {
	if q.paused {
		q.paused = false
		q.state.State = taskspb.Queue_RUNNING

		go q.runDispatcher()
		go q.runWorkers()
	}
}
