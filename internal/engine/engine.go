package engine

import (
	"context"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/oidc"
)

// defaultTombstoneTTL is how long a deleted queue/task name stays reserved
// before it becomes reusable again. It matches the "wait a minute" guidance in
// the recently-deleted error messages; real Cloud Tasks uses a cooldown of a
// few minutes.
const defaultTombstoneTTL = time.Minute

// Options tunes runtime behaviour of the engine.
type Options struct {
	// HardResetOnPurgeQueue makes PurgeQueue synchronously delete tasks and
	// release their name handles. This mirrors a development-environment
	// behaviour rather than production Cloud Tasks.
	HardResetOnPurgeQueue bool

	// OIDC holds the token-signing configuration used when dispatching tasks
	// with an OIDC token, and published via the issuer's HTTP endpoints. It is a
	// pointer only to express optionality: New defaults it to oidc.DefaultConfig
	// when nil and snapshots it by value, so mutating it after New has no effect.
	OIDC *oidc.Config

	// Dispatcher delivers tasks. New defaults it to HTTPDispatcher when nil;
	// tests supply a fake to drive lifecycle/retry logic without network I/O.
	Dispatcher Dispatcher

	// InsecureSkipTLSVerify disables TLS certificate verification when
	// dispatching tasks to HTTPS targets. It exists for local development
	// against targets using self-signed certificates and has no equivalent in
	// production Cloud Tasks; leave it false unless you need it. Read by New
	// only when it defaults the Dispatcher (an injected Dispatcher is
	// responsible for its own transport).
	InsecureSkipTLSVerify bool

	// AppEngineHost is the base URL that App Engine target tasks route to
	// instead of the production https://<project>.appspot.com. It exists for
	// local development against an App Engine emulator and has no equivalent in
	// production Cloud Tasks; leave it empty to keep the appspot.com routing.
	// New captures it once and threads it to each queue's tasks.
	AppEngineHost string

	// TombstoneTTL is how long a deleted queue/task name stays reserved before
	// it becomes reusable. New reads it once at construction (defaulting to
	// defaultTombstoneTTL when zero) and drives the background sweep with it, so
	// unlike the lazily-read fields above it is not observed if mutated later.
	TombstoneTTL time.Duration

	// Logger receives the engine's queue-lifecycle and dispatch diagnostics. New
	// resolves it once (defaulting to slog.Default() when nil) and tags it with a
	// component attribute, so unlike the fields read live it must be set before
	// New; mutating it afterwards has no effect.
	Logger *slog.Logger

	// clock supplies the current time. It defaults to time.Now; tests inject a
	// fake to exercise tombstone expiry without real sleeps.
	clock func() time.Time
}

// Engine owns all queue/task state and the runtime that drives task dispatch.
// It is the core layer; gRPC handlers should wrap an Engine and translate
// proto requests/responses + sentinel errors at the edge.
type Engine struct {
	// qs/ts hold only live queues and tasks. A deleted name is recorded in the
	// matching tombstone map (below) instead of lingering as a nil entry here,
	// so live-object maps never carry tombstones.
	qs map[string]*Queue
	ts map[string]*Task

	// qTombstones/tTombstones record when a queue/task name was deleted. A name
	// with a live tombstone reads back as recently-deleted and cannot be
	// recreated; once TombstoneTTL has elapsed the tombstone is treated as if the
	// name never existed (see the sweep in sweepLoop). Guarded by qsMux/tsMux
	// respectively.
	qTombstones map[string]time.Time
	tTombstones map[string]time.Time

	qsMux sync.Mutex
	tsMux sync.Mutex

	// The fields below are resolved once in New from the supplied Options and
	// never reassigned, so they are safe to read without a lock. The engine keeps
	// no reference to the Options value itself; callers configure it at
	// construction (see NewServer) and later mutation has no effect.

	// dispatcher delivers tasks on every queue. Defaulted to HTTPDispatcher when
	// Options.Dispatcher is nil; tests inject a fake.
	dispatcher Dispatcher

	// oidc is the token-signing configuration, snapshotted by value at New and
	// threaded to each queue. Construction captures it, so later mutation of the
	// Options.OIDC the caller passed is deliberately not observed at dispatch time.
	oidc oidc.Config

	// hardResetOnPurge mirrors Options.HardResetOnPurgeQueue.
	hardResetOnPurge bool

	// appEngineHost mirrors Options.AppEngineHost: the base URL App Engine
	// target tasks route to instead of appspot.com. Empty keeps the production
	// routing. Threaded to each queue at CreateQueue.
	appEngineHost string

	// now supplies the current time, injectable for tests.
	now func() time.Time

	// ttl is the resolved tombstone cooldown, also driving the sweep ticker.
	ttl time.Duration

	// stop is closed by Stop to terminate the tombstone sweep goroutine.
	stop     chan struct{}
	stopOnce sync.Once

	// logger is the engine's diagnostic logger, resolved once in New from
	// Options.Logger (or slog.Default()) and tagged with a component attribute so
	// consumers can filter emulator output from their own. Never nil.
	logger *slog.Logger
}

// New creates a new engine with empty queue/task bookkeeping and starts the
// background tombstone sweep. opts may be nil to accept defaults. Callers must
// invoke Stop to release the sweep goroutine.
func New(opts *Options) *Engine {
	if opts == nil {
		opts = &Options{}
	}
	oidcCfg := oidc.DefaultConfig()
	if opts.OIDC != nil {
		oidcCfg = opts.OIDC
	}
	now := time.Now
	if opts.clock != nil {
		now = opts.clock
	}
	ttl := opts.TombstoneTTL
	if ttl <= 0 {
		ttl = defaultTombstoneTTL
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "cloud-tasks-emulator")
	// The default dispatcher owns the engine's logger; an injected one owns its
	// own logging.
	dispatcher := opts.Dispatcher
	if dispatcher == nil {
		d := HTTPDispatcher{logger: logger}
		if opts.InsecureSkipTLSVerify {
			d.transport = insecureTransport()
			logger.Warn("insecure mode: TLS certificate verification is disabled for task dispatch")
		}
		dispatcher = d
	}
	e := &Engine{
		qs:               make(map[string]*Queue),
		ts:               make(map[string]*Task),
		qTombstones:      make(map[string]time.Time),
		tTombstones:      make(map[string]time.Time),
		dispatcher:       dispatcher,
		oidc:             *oidcCfg,
		hardResetOnPurge: opts.HardResetOnPurgeQueue,
		appEngineHost:    opts.AppEngineHost,
		now:              now,
		ttl:              ttl,
		logger:           logger,
		stop:             make(chan struct{}),
	}
	go e.sweepLoop()
	return e
}

// tombstoneActive reports whether a tombstone recorded at deletedAt is still
// within the cooldown window as of now.
func (e *Engine) tombstoneActive(deletedAt time.Time) bool {
	return e.now().Sub(deletedAt) < e.ttl
}

// sweepLoop periodically prunes expired tombstones so the queue/task
// bookkeeping does not grow without bound. It runs until Stop closes e.stop, so
// no goroutine outlives the engine.
func (e *Engine) sweepLoop() {
	ticker := time.NewTicker(e.ttl)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.sweepTombstones()
		case <-e.stop:
			return
		}
	}
}

// sweepTombstones removes every tombstone whose cooldown has elapsed, freeing
// the name for reuse and bounding map growth.
func (e *Engine) sweepTombstones() {
	e.qsMux.Lock()
	for name, deletedAt := range e.qTombstones {
		if !e.tombstoneActive(deletedAt) {
			delete(e.qTombstones, name)
		}
	}
	e.qsMux.Unlock()

	e.tsMux.Lock()
	for name, deletedAt := range e.tTombstones {
		if !e.tombstoneActive(deletedAt) {
			delete(e.tTombstones, name)
		}
	}
	e.tsMux.Unlock()
}

// Stop cancels every queue - and thereby every queue's token generator,
// dispatcher, workers and pending tasks - and stops the tombstone sweep, so no
// engine goroutine outlives the engine. It is idempotent and safe to call from
// shutdown paths and test teardown. After Stop, the engine's bookkeeping still
// reflects the (now cancelled) queues; callers that want a fresh engine should
// create a new one.
func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		close(e.stop)
	})

	e.qsMux.Lock()
	queues := make([]*Queue, 0, len(e.qs))
	for _, queue := range e.qs {
		queues = append(queues, queue)
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
	// A (re)created name is no longer tombstoned.
	delete(e.qTombstones, queueName)
}

func (e *Engine) fetchQueue(queueName string) (*Queue, bool) {
	e.qsMux.Lock()
	defer e.qsMux.Unlock()
	queue, ok := e.qs[queueName]
	return queue, ok
}

// removeQueueEntry drops a queue from the live map and tombstones its name so
// it reads back as recently-deleted until the cooldown elapses. The deletion
// time is recorded only for a name that is not already tombstoned, so a
// redundant call cannot extend the cooldown.
func (e *Engine) removeQueueEntry(queueName string) {
	e.qsMux.Lock()
	defer e.qsMux.Unlock()
	delete(e.qs, queueName)
	if _, ok := e.qTombstones[queueName]; !ok {
		e.qTombstones[queueName] = e.now()
	}
}

// queueRecentlyDeleted reports whether the name carries a live tombstone. It
// prunes the tombstone opportunistically once the cooldown has elapsed, so an
// expired name is treated as if it never existed.
func (e *Engine) queueRecentlyDeleted(queueName string) bool {
	e.qsMux.Lock()
	defer e.qsMux.Unlock()
	deletedAt, ok := e.qTombstones[queueName]
	if !ok {
		return false
	}
	if e.tombstoneActive(deletedAt) {
		return true
	}
	delete(e.qTombstones, queueName)
	return false
}

func (e *Engine) setTask(taskName string, task *Task) {
	e.tsMux.Lock()
	defer e.tsMux.Unlock()
	e.ts[taskName] = task
	// A (re)created name is no longer tombstoned.
	delete(e.tTombstones, taskName)
}

func (e *Engine) fetchTask(taskName string) (*Task, bool) {
	e.tsMux.Lock()
	defer e.tsMux.Unlock()
	task, ok := e.ts[taskName]
	return task, ok
}

// removeTaskEntry drops a task from the live map and tombstones its name so it
// reads back as recently-deleted until the cooldown elapses. DeleteTask calls
// it to tombstone synchronously; the deletion time is recorded only for a name
// that is not already tombstoned, so the task's later terminal callback cannot
// extend the cooldown.
func (e *Engine) removeTaskEntry(taskName string) {
	e.tsMux.Lock()
	defer e.tsMux.Unlock()
	delete(e.ts, taskName)
	if _, ok := e.tTombstones[taskName]; !ok {
		e.tTombstones[taskName] = e.now()
	}
}

// retireTask is a task's terminal (onDone) callback: it tombstones the name when
// the task reaches a terminal state, but only while the task still owns the live
// entry. A task deleted long ago may fire this callback late; the ownership
// check stops it from clobbering a same-named task created after the name became
// reusable.
func (e *Engine) retireTask(task *Task) {
	e.tsMux.Lock()
	defer e.tsMux.Unlock()
	if cur, ok := e.ts[task.state.Name]; !ok || cur != task {
		return
	}
	delete(e.ts, task.state.Name)
	if _, ok := e.tTombstones[task.state.Name]; !ok {
		e.tTombstones[task.state.Name] = e.now()
	}
}

// taskRecentlyDeleted reports whether the name carries a live tombstone. It
// prunes the tombstone opportunistically once the cooldown has elapsed, so an
// expired name is treated as if it never existed.
func (e *Engine) taskRecentlyDeleted(taskName string) bool {
	e.tsMux.Lock()
	defer e.tsMux.Unlock()
	deletedAt, ok := e.tTombstones[taskName]
	if !ok {
		return false
	}
	if e.tombstoneActive(deletedAt) {
		return true
	}
	delete(e.tTombstones, taskName)
	return false
}

// hardDeleteTask releases a task name entirely, dropping both the live entry and
// any tombstone so the name becomes immediately reusable (hard reset).
func (e *Engine) hardDeleteTask(taskName string) {
	e.tsMux.Lock()
	defer e.tsMux.Unlock()
	delete(e.ts, taskName)
	delete(e.tTombstones, taskName)
}

// ListQueues returns all live queues.
func (e *Engine) ListQueues(ctx context.Context) ([]*Queue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// TODO: Implement paging
	e.qsMux.Lock()
	defer e.qsMux.Unlock()

	var queues []*Queue
	for _, queue := range e.qs {
		queues = append(queues, queue)
	}
	return queues, nil
}

// GetQueue returns the named queue.
func (e *Engine) GetQueue(ctx context.Context, name string) (*Queue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queue, ok := e.fetchQueue(name)
	// Cloud responds with the same error message whether the queue was recently
	// deleted or never existed, so a tombstone does not change the outcome here.
	if !ok {
		return nil, ErrQueueNotFound
	}
	return queue, nil
}

// CreateQueue creates a new queue under the given parent.
func (e *Engine) CreateQueue(ctx context.Context, parent string, qs QueueState) (*Queue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nameMatched, _ := regexp.MatchString("projects/[A-Za-z0-9-]+/locations/[A-Za-z0-9-]+/queues/[A-Za-z0-9-]+", qs.Name)
	if !nameMatched {
		return nil, ErrInvalidQueueName
	}
	parentMatched, _ := regexp.MatchString("projects/[A-Za-z0-9-]+/locations/[A-Za-z0-9-]+", parent)
	if !parentMatched {
		return nil, ErrInvalidParent
	}
	if _, ok := e.fetchQueue(qs.Name); ok {
		return nil, ErrQueueAlreadyExists
	}
	if e.queueRecentlyDeleted(qs.Name) {
		return nil, ErrQueueRecentlyDeleted
	}

	queue := newQueue(qs, e.oidc, e.dispatcher, e.logger, e.appEngineHost, func(task *Task) {
		e.retireTask(task)
	})
	e.setQueue(qs.Name, queue)
	queue.Run()

	return queue, nil
}

// DeleteQueue removes the named queue.
func (e *Engine) DeleteQueue(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	queue, ok := e.fetchQueue(name)
	if !ok {
		return ErrQueueNotFound
	}

	queue.Delete()
	e.removeQueueEntry(name)
	return nil
}

// PurgeQueue purges the named queue. When Options.HardResetOnPurgeQueue is set,
// also releases all task name handles so the names become reusable - this
// mirrors the emulator's optional development-environment behaviour rather
// than production Cloud Tasks. Hard reset can block waiting for in-flight
// tasks to finish; ctx bounds that wait and, if it expires first, PurgeQueue
// returns ctx.Err() promptly instead of hanging the caller.
func (e *Engine) PurgeQueue(ctx context.Context, name string) (*Queue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queue, ok := e.fetchQueue(name)
	if !ok {
		return nil, ErrQueueNotFound
	}
	if e.hardResetOnPurge {
		if err := e.hardResetQueue(ctx, queue); err != nil {
			return nil, err
		}
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
//
// If ctx is cancelled or its deadline expires before every task has finished,
// hardResetQueue returns immediately with ctx.Err() and leaves the still-running
// tasks' name handles reserved; they release themselves normally once their own
// onDone callback runs.
func (e *Engine) hardResetQueue(ctx context.Context, queue *Queue) error {
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
		select {
		case <-task.done:
		case <-ctx.Done():
			return ctx.Err()
		}
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
	return nil
}

// PauseQueue pauses queue dispatch.
func (e *Engine) PauseQueue(ctx context.Context, name string) (*Queue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queue, ok := e.fetchQueue(name)
	if !ok {
		return nil, ErrQueueNotFound
	}
	queue.Pause()
	return queue, nil
}

// ResumeQueue resumes a paused queue.
func (e *Engine) ResumeQueue(ctx context.Context, name string) (*Queue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queue, ok := e.fetchQueue(name)
	if !ok {
		return nil, ErrQueueNotFound
	}
	queue.Resume()
	return queue, nil
}

// ListTasks lists all tasks in the named queue.
func (e *Engine) ListTasks(ctx context.Context, parent string) ([]*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// TODO: Implement paging
	queue, ok := e.fetchQueue(parent)
	if !ok {
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
func (e *Engine) GetTask(ctx context.Context, name string) (*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	task, ok := e.fetchTask(name)
	if ok {
		return task, nil
	}
	// A name within its deletion cooldown reads back as recently-deleted; once
	// the cooldown elapses it is treated as if it never existed.
	if e.taskRecentlyDeleted(name) {
		return nil, ErrTaskRecentlyDeleted
	}
	return nil, ErrTaskNotFound
}

// CreateTask creates a new task on the queue identified by parent.
// The returned *Task wraps the live engine state; the second return value is a
// snapshot suitable for returning to the caller without observing future mutations.
func (e *Engine) CreateTask(ctx context.Context, parent string, ts TaskState) (*Task, TaskState, error) {
	if err := ctx.Err(); err != nil {
		return nil, TaskState{}, err
	}
	queue, ok := e.fetchQueue(parent)
	if !ok {
		if e.queueRecentlyDeleted(parent) {
			return nil, TaskState{}, ErrQueueRecentlyDeleted
		}
		return nil, TaskState{}, ErrQueueNotFound
	}

	if ts.Name != "" {
		// If a name is specified, it must be structurally a task resource name,
		// its task ID must be valid, it must belong to this queue, and it must
		// be unique. A malformed name and a well-formed name carrying an illegal
		// task ID are distinct errors (real Cloud Tasks reports them differently).
		parts, structured := parseTaskName(ts.Name)
		if !structured {
			return nil, TaskState{}, ErrInvalidTaskName
		}
		if !isValidTaskID(parts.taskId) {
			return nil, TaskState{}, ErrInvalidTaskID
		}
		if !strings.HasPrefix(ts.Name, parent+"/tasks/") {
			return nil, TaskState{}, ErrTaskQueueMismatch
		}
		if _, exists := e.fetchTask(ts.Name); exists {
			return nil, TaskState{}, ErrTaskAlreadyExists
		}
		// A recently-deleted name stays reserved for the cooldown; Cloud reports a
		// recreate against a still-reserved name as AlreadyExists. Once the
		// cooldown elapses taskRecentlyDeleted prunes the tombstone and the name
		// becomes reusable.
		if e.taskRecentlyDeleted(ts.Name) {
			return nil, TaskState{}, ErrTaskAlreadyExists
		}
	}

	// Cloud Tasks validates an HTTP-target task's URL at create time, but only
	// shallowly: it must be non-empty and start with http:// or https://. It is
	// deliberately not fully parsed here (an invalid percent-escape, say, is
	// accepted and only fails when the task is dispatched).
	if hr, ok := ts.HTTPRequest.Get(); ok {
		if hr.URL == "" {
			return nil, TaskState{}, ErrHTTPRequestURLRequired
		}
		if !strings.HasPrefix(hr.URL, "http://") && !strings.HasPrefix(hr.URL, "https://") {
			return nil, TaskState{}, ErrHTTPRequestURLScheme
		}
	}

	task, frozen := queue.NewTask(ts)
	e.setTask(frozen.Name, task)
	queue.logger.Debug("task received", "task", frozen.Name, "queue", parent)
	return task, frozen, nil
}

// DeleteTask removes the named task.
func (e *Engine) DeleteTask(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	task, ok := e.fetchTask(name)
	if !ok {
		if e.taskRecentlyDeleted(name) {
			// Cloud uses NotFound here, not FailedPrecondition.
			return ErrTaskRecentlyDeleted
		}
		return ErrTaskNotFound
	}

	// Cancel any pending dispatch, then tombstone the name synchronously so a
	// GetTask immediately following the delete observes it: real Cloud Tasks
	// reports a recently-deleted task as NotFound and keeps the name reserved.
	// The task's onDone callback may also run later; both paths route through
	// removeTaskEntry, which is idempotent, so they don't conflict.
	task.Delete()
	task.queue.removeTask(name)
	e.removeTaskEntry(name)
	return nil
}

// RunTask executes a task immediately and returns its snapshotted state.
func (e *Engine) RunTask(ctx context.Context, name string) (*Task, TaskState, error) {
	if err := ctx.Err(); err != nil {
		return nil, TaskState{}, err
	}
	task, ok := e.fetchTask(name)
	if !ok {
		if e.taskRecentlyDeleted(name) {
			return nil, TaskState{}, ErrTaskRecentlyDeleted
		}
		return nil, TaskState{}, ErrTaskNotFound
	}
	frozen := task.Run()
	return task, frozen, nil
}
