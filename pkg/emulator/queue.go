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
	server *Server

	state      *taskspb.Queue
	stateMutex sync.Mutex

	tasks      map[string]*Task
	tasksMutex sync.Mutex

	fire chan *Task
	work chan *Task

	tokenBucket chan bool

	maxDispatchesPerSecond float64

	pause  chan bool
	resume chan bool
	cancel chan bool

	cancelWait sync.WaitGroup
}

// NewQueue creates a new task queue
func NewQueue(server *Server, state *taskspb.Queue) *Queue {
	queue := &Queue{
		server: server,
		state:  state,
		fire:   make(chan *Task),
		work:   make(chan *Task),
		tasks:  make(map[string]*Task),
		pause:  make(chan bool, 1),
		resume: make(chan bool, 1),
		cancel: make(chan bool),
	}

	queue.setInitialQueueState()

	// Need to update these here after the defaults are set
	queue.maxDispatchesPerSecond = state.GetRateLimits().GetMaxDispatchesPerSecond()
	queue.tokenBucket = make(chan bool, queue.state.GetRateLimits().GetMaxBurstSize())

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
	q.tasksMutex.Lock()
	defer q.tasksMutex.Unlock()
	task, ok := q.tasks[taskName]
	return task, ok
}

func (q *Queue) setTask(taskName string, task *Task) {
	q.tasksMutex.Lock()
	defer q.tasksMutex.Unlock()
	q.tasks[taskName] = task
}

func (q *Queue) removeTask(taskName string) {
	q.tasksMutex.Lock()
	defer q.tasksMutex.Unlock()
	// Only set the task to nil if it's present. It might have already
	// been removed by a `HardReset`.
	if _, ok := q.tasks[taskName]; ok {
		q.tasks[taskName] = nil
	}
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
	defer q.cancelWait.Done()

	workerCount := int(q.state.GetRateLimits().GetMaxConcurrentDispatches())
	log.Printf("Starting %d workers for queue %v...", workerCount, q.state.GetName())

	q.cancelWait.Add(workerCount)

	for i := 0; i < int(workerCount); i++ {
		go q.runWorker(i)
	}
}

func (q *Queue) runWorker(workerID int) {
	defer q.cancelWait.Done()

	for {
		select {
		case task := <-q.work:
			err := task.Attempt()
			if err != nil {
				log.Printf("Error in worker %d: %v", workerID, err)
			}
		case <-q.cancel:
			return
		}
	}
}

func (q *Queue) runTokenGenerator() {
	defer q.cancelWait.Done()

	period := time.Second / time.Duration(q.maxDispatchesPerSecond)
	// Use Timer with Reset() in place of time.Ticker as the latter was causing high CPU usage in Docker
	timer := time.NewTimer(period)

	for {
		select {
		case <-timer.C:
			select {
			case q.tokenBucket <- true:
				// Added token
				timer.Reset(period)
			case <-q.cancel:
				timer.Stop()
				return
			}
		case <-q.cancel:
			timer.Stop()
			return
		}
	}
}

func (q *Queue) runDispatcher() {
	defer q.cancelWait.Done()

	for {
		select {
		// Consume a token
		case <-q.tokenBucket:
			select {
			case paused := <-q.pause:
				if paused {
					select {
					case <-q.cancel:
						return
					case <-q.resume:
					}
				}
			// Wait for task
			case task := <-q.fire:
				// Pass on to workers
				q.work <- task
			case <-q.cancel:
				return
			}
		case <-q.cancel:
			return
		}
	}
}

// Run starts the queue (workers, token generator and dispatcher)
func (q *Queue) Run() *taskspb.Queue {
	q.state.State = taskspb.Queue_RUNNING

	queueState := q.stateCopy()

	q.cancelWait.Add(3)
	go q.runWorkers()
	go q.runTokenGenerator()
	go q.runDispatcher()

	return queueState
}

// NewTask creates a new task on the queue
func (q *Queue) NewTask(newTaskState *taskspb.Task) (*Task, *taskspb.Task) {
	task := NewTask(q, newTaskState)

	taskState := proto.Clone(task.state).(*taskspb.Task)

	q.setTask(taskState.GetName(), task)

	task.Schedule()

	return task, taskState
}

// Delete stops and purges the queue
func (q *Queue) Delete() {
	close(q.cancel)

	q.Purge()

	// Wait for all workers to be finished
	q.cancelWait.Wait()
}

// Purge purges all tasks from the queue
func (q *Queue) Purge() {
	go func() {
		q.tasksMutex.Lock()
		defer q.tasksMutex.Unlock()

		for _, task := range q.tasks {
			if task != nil {
				task.Delete()
			}
		}
	}()
}

// Goes beyond `Purge` behaviour to synchronously delete all tasks and their name handles
func (q *Queue) HardReset() {
	q.tasksMutex.Lock()
	defer q.tasksMutex.Unlock()

	for taskName, task := range q.tasks {
		if task != nil {
			task.Delete()
		}

		delete(q.tasks, taskName)
	}
}

// Pause pauses the queue, if not already paused
func (q *Queue) Pause() {
	q.stateMutex.Lock()
	defer q.stateMutex.Unlock()

	if q.state.GetState() != taskspb.Queue_PAUSED {
		q.state.State = taskspb.Queue_PAUSED
		q.pause <- true
	}
}

// Resume resumes a paused queue, if not already running
func (q *Queue) Resume() {
	q.stateMutex.Lock()
	defer q.stateMutex.Unlock()

	if q.state.GetState() != taskspb.Queue_RUNNING {
		q.state.State = taskspb.Queue_RUNNING
		q.resume <- true
	}
}

func (q *Queue) taskDone(task *Task) {
	q.removeTask(task.state.GetName())
}
