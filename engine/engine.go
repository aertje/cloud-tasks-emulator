package engine

import (
	"regexp"
	"strings"
	"sync"
)

// Options tunes runtime behaviour of the engine.
type Options struct {
	// HardResetOnPurgeQueue makes PurgeQueue synchronously delete tasks and
	// release their name handles. This mirrors a development-environment
	// behaviour rather than production Cloud Tasks.
	HardResetOnPurgeQueue bool

	// OIDC holds the token-signing configuration used when dispatching tasks
	// with an OIDC token, and published via the issuer's HTTP endpoints. New
	// defaults it to DefaultOIDCConfig when nil.
	OIDC *OIDCConfig

	// Dispatcher delivers tasks. New defaults it to HTTPDispatcher when nil;
	// tests supply a fake to drive lifecycle/retry logic without network I/O.
	Dispatcher Dispatcher
}

// Engine owns all queue/task state and the runtime that drives task dispatch.
// It is the core layer; gRPC handlers should wrap an Engine and translate
// proto requests/responses + sentinel errors at the edge.
type Engine struct {
	qs map[string]*Queue
	ts map[string]*Task

	qsMux sync.Mutex
	tsMux sync.Mutex

	// opts is held via pointer so callers retain ownership of the value and
	// mutations made after construction (e.g. test setup) are observed.
	opts *Options
}

// New creates a new engine with empty queue/task bookkeeping. opts may be nil
// to accept defaults.
func New(opts *Options) *Engine {
	if opts == nil {
		opts = &Options{}
	}
	if opts.OIDC == nil {
		opts.OIDC = DefaultOIDCConfig()
	}
	return &Engine{
		qs:   make(map[string]*Queue),
		ts:   make(map[string]*Task),
		opts: opts,
	}
}

// Stop cancels every queue - and thereby every queue's token generator,
// dispatcher, workers and pending tasks - so no engine goroutine outlives the
// engine. It is idempotent and safe to call from shutdown paths and test
// teardown. After Stop, the engine's bookkeeping still reflects the (now
// cancelled) queues; callers that want a fresh engine should create a new one.
func (e *Engine) Stop() {
	e.qsMux.Lock()
	queues := make([]*Queue, 0, len(e.qs))
	for _, queue := range e.qs {
		if queue != nil {
			queues = append(queues, queue)
		}
	}
	e.qsMux.Unlock()

	for _, queue := range queues {
		queue.Delete()
	}
}

func (e *Engine) setQueue(queueName string, queue *Queue) {
	e.qsMux.Lock()
	defer e.qsMux.Unlock()
	e.qs[queueName] = queue
}

func (e *Engine) fetchQueue(queueName string) (*Queue, bool) {
	e.qsMux.Lock()
	defer e.qsMux.Unlock()
	queue, ok := e.qs[queueName]
	return queue, ok
}

func (e *Engine) removeQueueEntry(queueName string) {
	e.setQueue(queueName, nil)
}

func (e *Engine) setTask(taskName string, task *Task) {
	e.tsMux.Lock()
	defer e.tsMux.Unlock()
	e.ts[taskName] = task
}

func (e *Engine) fetchTask(taskName string) (*Task, bool) {
	e.tsMux.Lock()
	defer e.tsMux.Unlock()
	task, ok := e.ts[taskName]
	return task, ok
}

func (e *Engine) removeTaskEntry(taskName string) {
	e.setTask(taskName, nil)
}

func (e *Engine) hardDeleteTask(taskName string) {
	e.tsMux.Lock()
	defer e.tsMux.Unlock()
	delete(e.ts, taskName)
}

// ListQueues returns all live queues.
func (e *Engine) ListQueues() ([]*Queue, error) {
	// TODO: Implement paging
	e.qsMux.Lock()
	defer e.qsMux.Unlock()

	var queues []*Queue
	for _, queue := range e.qs {
		if queue != nil {
			queues = append(queues, queue)
		}
	}
	return queues, nil
}

// GetQueue returns the named queue.
func (e *Engine) GetQueue(name string) (*Queue, error) {
	queue, ok := e.fetchQueue(name)
	// Cloud responds with the same error message whether the queue was recently deleted or never existed
	if !ok || queue == nil {
		return nil, ErrQueueNotFound
	}
	return queue, nil
}

// CreateQueue creates a new queue under the given parent.
func (e *Engine) CreateQueue(parent string, qs QueueState) (*Queue, error) {
	nameMatched, _ := regexp.MatchString("projects/[A-Za-z0-9-]+/locations/[A-Za-z0-9-]+/queues/[A-Za-z0-9-]+", qs.Name)
	if !nameMatched {
		return nil, ErrInvalidQueueName
	}
	parentMatched, _ := regexp.MatchString("projects/[A-Za-z0-9-]+/locations/[A-Za-z0-9-]+", parent)
	if !parentMatched {
		return nil, ErrInvalidParent
	}
	existing, ok := e.fetchQueue(qs.Name)
	if ok {
		if existing != nil {
			return nil, ErrQueueAlreadyExists
		}
		return nil, ErrQueueRecentlyDeleted
	}

	// Options are read lazily (they may be mutated after New, e.g. by the server
	// wiring), so default the dispatcher here rather than in New.
	dispatcher := e.opts.Dispatcher
	if dispatcher == nil {
		dispatcher = HTTPDispatcher{}
	}

	queue := newQueue(qs, e.opts.OIDC, dispatcher, func(task *Task) {
		e.removeTaskEntry(task.state.Name)
	})
	e.setQueue(qs.Name, queue)
	queue.Run()

	return queue, nil
}

// DeleteQueue removes the named queue.
func (e *Engine) DeleteQueue(name string) error {
	queue, ok := e.fetchQueue(name)
	if !ok || queue == nil {
		return ErrQueueNotFound
	}

	queue.Delete()
	e.removeQueueEntry(name)
	return nil
}

// PurgeQueue purges the named queue. When Options.HardResetOnPurgeQueue is set,
// also releases all task name handles so the names become reusable - this
// mirrors the emulator's optional development-environment behaviour rather
// than production Cloud Tasks.
func (e *Engine) PurgeQueue(name string) (*Queue, error) {
	queue, ok := e.fetchQueue(name)
	if !ok || queue == nil {
		return nil, ErrQueueNotFound
	}
	if e.opts.HardResetOnPurgeQueue {
		e.hardResetQueue(queue)
	} else {
		queue.Purge()
	}
	return queue, nil
}

// hardResetQueue synchronously purges all tasks and releases their name handles.
//
// It cancels every live task and waits for each to reach its terminal state
// (task.done, closed after the task's onDone callback has tombstoned it) before
// releasing the name handles. Waiting on that real completion signal - rather
// than sleeping and hoping - guarantees no late onDone re-inserts a tombstone
// after we delete the entry, so there is nothing to panic about.
func (e *Engine) hardResetQueue(queue *Queue) {
	// Snapshot the live tasks under the queue lock, then release it: the tasks'
	// onDone callbacks need the same lock to tombstone themselves, so we must not
	// hold it while waiting for them.
	queue.tsMux.Lock()
	tasks := make([]*Task, 0, len(queue.ts))
	for _, task := range queue.ts {
		if task != nil {
			tasks = append(tasks, task)
		}
	}
	queue.tsMux.Unlock()

	for _, task := range tasks {
		task.Delete()
	}
	for _, task := range tasks {
		<-task.done
	}

	// Every purged task has now tombstoned itself (a nil map entry). Release only
	// those name handles, leaving any task created concurrently with the purge
	// (a non-nil entry) untouched.
	queue.tsMux.Lock()
	defer queue.tsMux.Unlock()
	for taskName, task := range queue.ts {
		if task == nil {
			delete(queue.ts, taskName)
			e.hardDeleteTask(taskName)
		}
	}
}

// PauseQueue pauses queue dispatch.
func (e *Engine) PauseQueue(name string) (*Queue, error) {
	queue, ok := e.fetchQueue(name)
	if !ok || queue == nil {
		return nil, ErrQueueNotFound
	}
	queue.Pause()
	return queue, nil
}

// ResumeQueue resumes a paused queue.
func (e *Engine) ResumeQueue(name string) (*Queue, error) {
	queue, ok := e.fetchQueue(name)
	if !ok || queue == nil {
		return nil, ErrQueueNotFound
	}
	queue.Resume()
	return queue, nil
}

// ListTasks lists all tasks in the named queue.
func (e *Engine) ListTasks(parent string) ([]*Task, error) {
	// TODO: Implement paging
	queue, ok := e.fetchQueue(parent)
	if !ok || queue == nil {
		return nil, ErrQueueNotFound
	}

	queue.tsMux.Lock()
	defer queue.tsMux.Unlock()

	var taskList []*Task
	for _, task := range queue.ts {
		if task != nil {
			taskList = append(taskList, task)
		}
	}
	return taskList, nil
}

// GetTask returns the named task.
func (e *Engine) GetTask(name string) (*Task, error) {
	task, ok := e.fetchTask(name)
	if !ok {
		return nil, ErrTaskNotFound
	}
	if task == nil {
		return nil, ErrTaskRecentlyDeleted
	}
	return task, nil
}

// CreateTask creates a new task on the queue identified by parent.
// The returned *Task wraps the live engine state; the second return value is a
// snapshot suitable for returning to the caller without observing future mutations.
func (e *Engine) CreateTask(parent string, ts TaskState) (*Task, TaskState, error) {
	queue, ok := e.fetchQueue(parent)
	if !ok {
		return nil, TaskState{}, ErrQueueNotFound
	}
	if queue == nil {
		return nil, TaskState{}, ErrQueueRecentlyDeleted
	}

	if ts.Name != "" {
		// If a name is specified, it must be structurally a task resource name,
		// its task ID must be valid, it must belong to this queue, and it must
		// be unique. A malformed name and a well-formed name carrying an illegal
		// task ID are distinct errors (real Cloud Tasks reports them differently).
		taskID, structured := splitTaskName(ts.Name)
		if !structured {
			return nil, TaskState{}, ErrInvalidTaskName
		}
		if !isValidTaskID(taskID) {
			return nil, TaskState{}, ErrInvalidTaskID
		}
		if !strings.HasPrefix(ts.Name, parent+"/tasks/") {
			return nil, TaskState{}, ErrTaskQueueMismatch
		}
		if _, exists := e.fetchTask(ts.Name); exists {
			return nil, TaskState{}, ErrTaskAlreadyExists
		}
	}

	task, frozen := queue.NewTask(ts)
	e.setTask(frozen.Name, task)
	return task, frozen, nil
}

// DeleteTask removes the named task.
func (e *Engine) DeleteTask(name string) error {
	task, ok := e.fetchTask(name)
	if !ok {
		return ErrTaskNotFound
	}
	if task == nil {
		// Cloud uses NotFound here, not FailedPrecondition.
		return ErrTaskRecentlyDeleted
	}

	// Cancel any pending dispatch, then tombstone the name synchronously so a
	// GetTask immediately following the delete observes it: real Cloud Tasks
	// reports a recently-deleted task as NotFound and keeps the name reserved.
	// The task's onDone callback may also run later; setting the tombstone (a
	// nil map entry) is idempotent, so the two paths don't conflict.
	task.Delete()
	task.queue.removeTask(name)
	e.removeTaskEntry(name)
	return nil
}

// RunTask executes a task immediately and returns its snapshotted state.
func (e *Engine) RunTask(name string) (*Task, TaskState, error) {
	task, ok := e.fetchTask(name)
	if !ok {
		return nil, TaskState{}, ErrTaskNotFound
	}
	if task == nil {
		return nil, TaskState{}, ErrTaskRecentlyDeleted
	}
	frozen := task.Run()
	return task, frozen, nil
}
