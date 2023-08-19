package emulator

import (
	"sync"
	"time"
)

const (
	DefaultMaxDispatchesPerSecond  = 500.0
	DefaultMaxBurstSize            = 100
	DefaultMaxConcurrentDispatches = 1000
	DefaultMaxAttempts             = 100
	DefaultMaxDoublings            = 16
	DefaultMinBackOff              = 100 * time.Millisecond
	DefaultMaxBackOff              = time.Hour
)

type RateLimits struct {
	MaxDispatchesPerSecond  float64
	MaxBurstSize            int
	MaxConcurrentDispatches int
}

type RetryConfig struct {
	MaxAttempts  int
	MaxDoublings int
	MinBackoff   time.Duration
	MaxBackoff   time.Duration
}

type QueueConfig struct {
	RateLimits  RateLimits
	RetryConfig RetryConfig
}

func NewDefaultQueueConfig() QueueConfig {
	rateLimits := RateLimits{
		MaxDispatchesPerSecond:  DefaultMaxDispatchesPerSecond,
		MaxBurstSize:            DefaultMaxBurstSize,
		MaxConcurrentDispatches: DefaultMaxConcurrentDispatches,
	}
	retryConfig := RetryConfig{
		MaxAttempts:  DefaultMaxAttempts,
		MaxDoublings: DefaultMaxDoublings,
		MinBackoff:   DefaultMinBackOff,
		MaxBackoff:   DefaultMaxBackOff,
	}
	return QueueConfig{
		RateLimits:  rateLimits,
		RetryConfig: retryConfig,
	}
}

type QueueRunState int

const (
	QueueRunStateUnspecified QueueRunState = iota
	QueueRunStateRunning
	QueueRunStatePaused
	QueueRunStateDisabled
)

type QueueState struct {
	RunState QueueRunState
}

type Queue struct {
	Name   string
	Config QueueConfig
	State  QueueState
}

func NewQueue(name string, config QueueConfig) Queue {
	return Queue{
		Name:   name,
		Config: config,
	}
}

// queueRunner holds all internals for a task queue
type queueRunner struct {
	emulator *Emulator

	queue Queue

	taskRunners map[string]*taskRunner
	lock        sync.RWMutex

	tokenBucket chan bool

	// fire chan *Task
	// work chan *Task

	// pause  chan bool
	// resume chan bool
	// cancel chan bool

	// cancelWait sync.WaitGroup
}

func newQueueRunner(emulator *Emulator, queue Queue) *queueRunner {
	qr := queueRunner{
		queue:       queue,
		taskRunners: make(map[string]*taskRunner),
		tokenBucket: make(chan bool, queue.Config.RateLimits.MaxBurstSize),
		// fire:        make(chan *Task),
		// work:        make(chan *Task),
		// pause:       make(chan bool, 1),
		// resume:      make(chan bool, 1),
		// cancel:      make(chan bool),
	}

	// Fill the token bucket
	for i := 0; i < int(queue.Config.RateLimits.MaxBurstSize); i++ {
		qr.tokenBucket <- true
	}

	return &qr
}

func (qr *queueRunner) getTasks() []Task {
	qr.lock.RLock()
	defer qr.lock.RUnlock()

	var tasks []Task
	for _, tr := range qr.taskRunners {
		if tr != nil {
			tasks = append(tasks, tr.task)
		}
	}

	return tasks
}

// func (q *Queue) stateCopy() *taskspb.Queue {
// 	return proto.Clone(q.state).(*taskspb.Queue)
// }

// func (q *Queue) fetchTask(taskName string) (*Task, bool) {
// 	q.tasksMutex.RLock()
// 	defer q.tasksMutex.RUnlock()
// 	task, ok := q.tasks[taskName]
// 	return task, ok
// }

// func (q *Queue) setTask(taskName string, task *Task) {
// 	q.tasksMutex.Lock()
// 	defer q.tasksMutex.Unlock()
// 	q.tasks[taskName] = task
// }

// // removeTask sets the task handle to nil and effectively removes it.
// // It is protected by the tasks lock as
// func (q *Queue) removeTask(taskName string) {
// 	q.tasksMutex.Lock()
// 	defer q.tasksMutex.Unlock()
// 	// Only set the task to nil if it's present. It might have already
// 	// been removed by a `HardReset`.
// 	if _, ok := q.tasks[taskName]; ok {
// 		q.tasks[taskName] = nil
// 	}
// }

// func (q *Queue) runWorkers() {
// 	defer q.cancelWait.Done()

// 	workerCount := int(q.state.GetRateLimits().GetMaxConcurrentDispatches())
// 	log.Printf("Starting %d workers for queue %v...", workerCount, q.state.GetName())

// 	q.cancelWait.Add(workerCount)

// 	for i := 0; i < int(workerCount); i++ {
// 		go q.runWorker(i)
// 	}
// }

// func (q *Queue) runWorker(workerID int) {
// 	defer q.cancelWait.Done()

// 	for {
// 		select {
// 		case task := <-q.work:
// 			err := task.Attempt()
// 			if err != nil {
// 				log.Printf("Error in worker %d: %v", workerID, err)
// 			}
// 		case <-q.cancel:
// 			return
// 		}
// 	}
// }

// func (q *Queue) runTokenGenerator() {
// 	defer q.cancelWait.Done()

// 	period := time.Second / time.Duration(q.maxDispatchesPerSecond)
// 	// Use Timer with Reset() in place of time.Ticker as the latter was causing high CPU usage in Docker
// 	timer := time.NewTimer(period)

// 	for {
// 		select {
// 		case <-timer.C:
// 			select {
// 			case q.tokenBucket <- true:
// 				// Added token
// 				timer.Reset(period)
// 			case <-q.cancel:
// 				timer.Stop()
// 				return
// 			}
// 		case <-q.cancel:
// 			timer.Stop()
// 			return
// 		}
// 	}
// }

// func (q *Queue) runDispatcher() {
// 	defer q.cancelWait.Done()

// 	for {
// 		select {
// 		// Consume a token
// 		case <-q.tokenBucket:
// 			select {
// 			case paused := <-q.pause:
// 				if paused {
// 					select {
// 					case <-q.cancel:
// 						return
// 					case <-q.resume:
// 					}
// 				}
// 			// Wait for task
// 			case task := <-q.fire:
// 				// Pass on to workers
// 				q.work <- task
// 			case <-q.cancel:
// 				return
// 			}
// 		case <-q.cancel:
// 			return
// 		}
// 	}
// }

// // Run starts the queue (workers, token generator and dispatcher)
// func (q *Queue) Run() *taskspb.Queue {
// 	q.state.State = taskspb.Queue_RUNNING

// 	queueState := q.stateCopy()

// 	q.cancelWait.Add(3)
// 	go q.runWorkers()
// 	go q.runTokenGenerator()
// 	go q.runDispatcher()

// 	return queueState
// }

// // NewTask creates a new task on the queue
// func (q *Queue) NewTask(newTaskState *taskspb.Task) (*Task, *taskspb.Task) {
// 	task := NewTask(q, newTaskState)

// 	taskState := proto.Clone(task.state).(*taskspb.Task)

// 	q.setTask(taskState.GetName(), task)

// 	task.Schedule()

// 	return task, taskState
// }

// // Delete stops and purges the queue
// func (q *Queue) Delete() {
// 	close(q.cancel)

// 	q.Purge()

// 	// Wait for all workers to be finished
// 	q.cancelWait.Wait()
// }

// // Purge purges all tasks from the queue asynchronously.
// // The cleanup of the task handles in the queue is handled by the `taskCanceled`
// // callback.
// func (q *Queue) Purge() {
// 	for _, task := range q.tasks {
// 		if task != nil {
// 			go task.Cancel()
// 		}
// 	}
// }

// // Goes beyond `Purge` behaviour to synchronously delete all tasks.
// // The cleanup of the task handles happens synchronously, and the
// func (q *Queue) HardReset() {
// 	for taskName, task := range q.tasks {
// 		if task != nil {
// 			task.Cancel()
// 		}

// 		delete(q.tasks, taskName)
// 	}
// }

// // Pause pauses the queue, if not already paused
// func (q *Queue) Pause() {
// 	q.stateMutex.Lock()
// 	defer q.stateMutex.Unlock()

// 	if q.state.GetState() != taskspb.Queue_PAUSED {
// 		q.state.State = taskspb.Queue_PAUSED
// 		q.pause <- true
// 	}
// }

// // Resume resumes a paused queue, if not already running
// func (q *Queue) Resume() {
// 	q.stateMutex.Lock()
// 	defer q.stateMutex.Unlock()

// 	if q.state.GetState() != taskspb.Queue_RUNNING {
// 		q.state.State = taskspb.Queue_RUNNING
// 		q.resume <- true
// 	}
// }

// func (q *Queue) taskCanceled(task *Task) {
// 	q.removeTask(task.state.GetName())
// }
