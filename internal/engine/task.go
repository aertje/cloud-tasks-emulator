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
	frozen := updateStateForDispatch(task)

	go task.doDispatch(false, frozen)

	return frozen
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
	task.stateMutex.Unlock()

	fromNow := time.Until(scheduleTime)

	go func() {
		select {
		case <-time.After(fromNow):
			task.queue.fire <- task
			return
		case <-task.cancel:
			task.markDone()
			return
		}
	}()
}
