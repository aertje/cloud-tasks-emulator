package engine

import (
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/maybe"
)

var (
	// taskNameRE matches the resource-name shape of a task and captures its four
	// segments. The task-ID capture is deliberately lenient ((.+)) rather than
	// charset-constrained: a structurally-valid name carrying an illegal ID is
	// captured rather than rejected, so callers can tell it apart from a
	// malformed name and report the offending value. Validate the ID separately
	// with isValidTaskID. Real Cloud Tasks reports those two cases with different
	// messages.
	// Format requirements as per https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks#Task.FIELDS.name
	taskNameRE = regexp.MustCompile(`^projects/([a-zA-Z0-9:.-]+)/locations/([a-zA-Z0-9-]+)/queues/([a-zA-Z0-9-]+)/tasks/(.+)$`)

	// taskIDRE matches a syntactically valid task ID.
	taskIDRE = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,500}$`)
)

// parseTaskName splits a task resource name into its parts. ok is false when
// the name does not match the required structure, so callers can avoid a
// nil-index panic on the submatch slice. The captured taskId may contain
// illegal characters; validate it with isValidTaskID when that matters.
func parseTaskName(name string) (TaskNameParts, bool) {
	matches := taskNameRE.FindStringSubmatch(name)
	if matches == nil {
		return TaskNameParts{}, false
	}
	return TaskNameParts{
		project:  matches[1],
		location: matches[2],
		queueId:  matches[3],
		taskId:   matches[4],
	}, true
}

func isValidTaskID(id string) bool {
	return taskIDRE.MatchString(id)
}

type TaskNameParts struct {
	project  string
	location string
	queueId  string
	taskId   string
}

// Task holds all internals for a task
type Task struct {
	queue *Queue

	state TaskState

	cancel chan bool

	// armInterrupt identifies the task's single pending fire, guarded by
	// stateMutex. Schedule installs a fresh channel when it arms the next fire;
	// whoever dispatches the task (the scheduled goroutine or a forced Run)
	// consumes the arming by clearing this field, so the task dispatches at most
	// once per arming. Run additionally closes the consumed channel to wake a
	// scheduled goroutine still waiting on its timer. A nil value means there is
	// no unclaimed pending fire (the task is dispatching, terminal, or between
	// armings).
	armInterrupt chan struct{}

	onDone func(*Task)

	// done is closed exactly once, after onDone has finished running, when the
	// task reaches a terminal state (success, cancellation or retry exhaustion).
	// It lets callers such as hardResetQueue wait for real completion rather
	// than sleeping.
	done chan struct{}

	stateMutex sync.Mutex

	cancelOnce sync.Once

	doneOnce sync.Once
}

// newTask creates a new task for the specified queue
func newTask(queue *Queue, taskState TaskState, onDone func(task *Task)) *Task {
	setInitialTaskState(&taskState, queue.name, queue.appEngineEmulatorHost, queue.appEngineRegionID)

	return &Task{
		queue:  queue,
		state:  taskState,
		onDone: onDone,
		cancel: make(chan bool, 1), // Buffered in case cancel comes when task is not scheduled
		done:   make(chan struct{}),
	}
}

// markDone runs the task's onDone callback exactly once and then signals
// completion by closing done. onDone runs before done is closed so anything
// waiting on done observes the callback's effects (e.g. map tombstoning).
func (task *Task) markDone() {
	task.doneOnce.Do(func() {
		task.onDone(task)
		close(task.done)
	})
}

// State returns a snapshot of the task state.
//
// Note: TaskState and its maybe.M-wrapped members (HTTPRequest /
// AppEngineHTTPRequest / Attempt) are value types and copy by value, but the
// Headers map and Body slice they carry are references, so the snapshot is
// shallow. Callers that need a deep copy should round-trip via taskToProto at
// the edge.
func (t *Task) State() TaskState {
	t.stateMutex.Lock()
	defer t.stateMutex.Unlock()
	return t.state
}

// hasHeaderFold reports whether headers contains a key case-insensitively equal
// to name. HTTP field names are case-insensitive (RFC 7230), so header presence
// checks must be too.
func hasHeaderFold(headers map[string]string, name string) bool {
	for k := range headers {
		if strings.EqualFold(k, name) {
			return true
		}
	}
	return false
}

// Documented Cloud Tasks bounds on task configuration: the dispatch-deadline
// intervals from the tasks.Task field docs (per target family), the schedule
// horizon from the Cloud Tasks quotas page.
const (
	minDispatchDeadline          = 15 * time.Second
	maxHTTPDispatchDeadline      = 30 * time.Minute
	maxAppEngineDispatchDeadline = 24*time.Hour + 15*time.Second
	maxScheduleAhead             = 30 * 24 * time.Hour
)

// validateTaskConfig rejects out-of-range numeric values on a task-creation
// input, as real Cloud Tasks does. Unlike queue configuration these cannot
// crash the emulator (a bad deadline just makes an http.Client timeout), so
// this is purely fidelity: a task real Cloud Tasks would reject must not be
// accepted. Absent fields are valid and get server defaults
// (setInitialTaskState). now anchors the schedule-horizon check and comes from
// the engine's injectable clock.
func validateTaskConfig(s TaskState, now time.Time) error {
	if d, ok := s.DispatchDeadline.Get(); ok {
		if s.AppEngineHTTPRequest.IsPresent() {
			if d < minDispatchDeadline || d > maxAppEngineDispatchDeadline {
				return ErrDispatchDeadlineAppEngineRange
			}
		} else if d < minDispatchDeadline || d > maxHTTPDispatchDeadline {
			return ErrDispatchDeadlineHTTPRange
		}
	}
	if st, ok := s.ScheduleTime.Get(); ok && st.After(now.Add(maxScheduleAhead)) {
		return ErrScheduleTimeTooFarInFuture
	}
	return nil
}

// setInitialTaskState fills in the server-assigned defaults on a freshly created
// task. appEngineEmulatorHost is the base URL App Engine target tasks route to instead
// of the production appspot.com host; an empty value keeps the appspot.com
// routing. appEngineRegionID selects the regional appspot.com host format
// <project>.<region>.r.appspot.com when set; an empty value keeps the legacy
// <project>.appspot.com format. appEngineRegionID is ignored when appEngineEmulatorHost
// is set.
func setInitialTaskState(s *TaskState, queueName string, appEngineEmulatorHost string, appEngineRegionID string) {
	if s.Name == "" {
		taskID := strconv.FormatUint(uint64(rand.Uint64()), 10)
		s.Name = queueName + "/tasks/" + taskID
	}

	// Cloud only sets whole-second precision on CreateTime.
	s.CreateTime = maybe.Some(time.Unix(time.Now().Unix(), 0))

	s.ScheduleTime = s.ScheduleTime.Or(time.Now())
	s.DispatchDeadline = s.DispatchDeadline.Or(600 * time.Second)

	// HTTPRequest / AppEngineHTTPRequest are value-typed Maybes, so their
	// defaults are applied to a copy pulled out with Get and stored back with
	// Some rather than mutated in place.
	if hr, ok := s.HTTPRequest.Get(); ok {
		hr.Method = hr.Method.Or(http.MethodPost)
		headers := hr.Headers.OrZero()
		if headers == nil {
			headers = make(map[string]string)
		}
		// Cloud Tasks overrides any caller-supplied User-Agent.
		headers["User-Agent"] = "Google-Cloud-Tasks"
		hr.Headers = maybe.Some(headers)
		s.HTTPRequest = maybe.Some(hr)
	}

	if ae, ok := s.AppEngineHTTPRequest.Get(); ok {
		ae.Method = ae.Method.Or(http.MethodPost)
		headers := ae.Headers.OrZero()
		if headers == nil {
			headers = make(map[string]string)
		}
		headers["User-Agent"] = "AppEngine-Google; (+http://code.google.com/appengine)"

		if body := ae.Body.OrZero(); len(body) > 0 {
			// HTTP field names are case-insensitive, so a caller-supplied
			// "content-type" must suppress the default just as "Content-Type"
			// would - otherwise the task carries two Content-Type headers, which
			// Cloud Tasks does not allow (see conformance/golden/headers.json).
			// The default itself is added under the canonical casing.
			if !hasHeaderFold(headers, "Content-Type") {
				headers["Content-Type"] = "application/octet-stream"
			}
			// Content-Length is output-only and computed by Cloud Tasks, which
			// materializes it on the stored AppEngine task.
			headers["Content-Length"] = strconv.Itoa(len(body))
		}
		ae.Headers = maybe.Some(headers)

		// Routing is always present on a stored AppEngine task; an absent one
		// defaults to the zero routing, whose Host is then filled in below.
		routing := ae.AppEngineRouting.OrZero()
		if routing.Host.OrZero() == "" {
			var host, domainSeparator string

			if appEngineEmulatorHost == "" {
				// TODO: support custom domains
				// https://cloud.google.com/appengine/docs/standard/python/how-requests-are-routed
				parts, _ := parseTaskName(s.Name)
				if appEngineRegionID != "" {
					// The regional host format production Cloud Tasks emits.
					host = "https://" + parts.project + "." + appEngineRegionID + ".r.appspot.com"
				} else {
					host = "https://" + parts.project + ".appspot.com"
				}
				domainSeparator = "-dot-"
			} else {
				host = appEngineEmulatorHost
				domainSeparator = "."
			}

			// An operator-supplied emulator host is validated at engine.New, and
			// the appspot fallback is built from an already-validated task name, so
			// this parse cannot fail here; the guard is a defensive invariant.
			hostURL, err := url.Parse(host)
			if err != nil {
				panic(err)
			}

			if svc := routing.Service.OrZero(); svc != "" {
				hostURL.Host = svc + domainSeparator + hostURL.Host
			}
			if ver := routing.Version.OrZero(); ver != "" {
				hostURL.Host = ver + domainSeparator + hostURL.Host
			}
			if inst := routing.Instance.OrZero(); inst != "" {
				hostURL.Host = inst + domainSeparator + hostURL.Host
			}

			routing.Host = maybe.Some(hostURL.String())
		}
		ae.AppEngineRouting = maybe.Some(routing)

		ae.RelativeURI = ae.RelativeURI.Or("/")
		s.AppEngineHTTPRequest = maybe.Some(ae)
	}
}

// Attempt tries to execute a task
func (task *Task) Attempt() {
	frozen := updateStateForDispatch(task)

	task.doDispatch(true, frozen)
}

// Run runs the task outside of the normal queueing mechanism.
// This method is called directly by request.
func (task *Task) Run() TaskState {
	// Take over the task's pending fire so it does not also dispatch at its
	// original schedule time (a double dispatch). If the scheduled fire has
	// already been consumed - the task is dispatching through the normal path
	// right now - Run does not dispatch a second time and just returns a current
	// snapshot, matching Cloud Tasks running the task once.
	interrupt, ok := task.disarm()
	if !ok {
		return task.State()
	}
	// Wake the scheduled goroutine so it stands down promptly rather than
	// lingering until its (now-superseded) schedule time.
	close(interrupt)

	frozen := updateStateForDispatch(task)

	go task.doDispatch(false, frozen)

	return frozen
}

// disarm consumes the task's current pending fire, returning its interrupt
// channel, or (nil, false) if there is no unclaimed pending fire. It is how Run
// takes a scheduled task over: the caller owns the resulting dispatch and closes
// the returned channel to release the scheduled goroutine.
func (task *Task) disarm() (chan struct{}, bool) {
	task.stateMutex.Lock()
	defer task.stateMutex.Unlock()
	if task.armInterrupt == nil {
		return nil, false
	}
	interrupt := task.armInterrupt
	task.armInterrupt = nil
	return interrupt, true
}

// claimScheduledFire lets a scheduled goroutine consume the pending fire it was
// armed with. It succeeds only while that arming is still current: a later
// arming (a retry or dispatcher re-arm) or a Run takeover clears or replaces
// armInterrupt, so a stale goroutine stands down instead of dispatching a
// superseded fire.
func (task *Task) claimScheduledFire(interrupt chan struct{}) bool {
	task.stateMutex.Lock()
	defer task.stateMutex.Unlock()
	if task.armInterrupt != interrupt {
		return false
	}
	task.armInterrupt = nil
	return true
}

// Delete cancels the task if it is queued for execution.
// This method is called directly by request.
func (task *Task) Delete() {
	task.cancelOnce.Do(func() {
		task.cancel <- true
	})
}

// Schedule schedules the task for execution.
// It is initially called by the queue, later by the task reschedule.
func (task *Task) Schedule() {
	task.stateMutex.Lock()
	scheduleTime := task.state.ScheduleTime.OrZero()
	// Arm a fresh pending fire. The interrupt channel identifies this arming so a
	// concurrent Run can take the task over (see disarm/claimScheduledFire).
	interrupt := make(chan struct{})
	task.armInterrupt = interrupt
	task.stateMutex.Unlock()

	fromNow := time.Until(scheduleTime)

	go func() {
		select {
		case <-time.After(fromNow):
			// Consume this arming before handing the task to the dispatcher. A
			// forced Run may have taken it over in the meantime, in which case the
			// task is already dispatching and this fire must stand down.
			if !task.claimScheduledFire(interrupt) {
				return
			}
			// The queue may be paused (nothing draining fire) between the timer
			// firing and this send; keep listening on cancel so Delete still
			// takes effect instead of leaking this goroutine or dispatching a
			// deleted task once the queue resumes.
			select {
			case task.queue.fire <- task:
			case <-task.cancel:
				task.markDone()
			}
		case <-task.cancel:
			task.markDone()
		case <-interrupt:
			// A forced Run took the task over before the timer fired; it
			// dispatches the task directly, so this arming stands down.
			return
		}
	}()
}
