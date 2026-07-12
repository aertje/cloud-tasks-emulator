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
)

var (
	taskNameRE *regexp.Regexp

	// taskNameStructureRE matches the resource-name shape of a task without
	// constraining the task-ID charset: the trailing capture is the task ID,
	// validated separately by taskIDRE. This lets a structurally-valid name
	// carrying an illegal ID be told apart from a malformed name - real Cloud
	// Tasks reports those two cases with different messages.
	taskNameStructureRE = regexp.MustCompile(`^projects/[a-zA-Z0-9:.-]+/locations/[a-zA-Z0-9-]+/queues/[a-zA-Z0-9-]+/tasks/(.+)$`)

	// taskIDRE matches a syntactically valid task ID.
	taskIDRE = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,500}$`)
)

func init() {
	// Format requirements as per https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks#Task.FIELDS.name
	taskNameRE = regexp.MustCompile("projects/([a-zA-Z0-9:.-]+)/locations/([a-zA-Z0-9-]+)/queues/([a-zA-Z0-9-]+)/tasks/([a-zA-Z0-9_-]+)")
}

// parseTaskName splits a task resource name into its parts. ok is false when
// the name does not match the required structure, so callers can avoid a
// nil-index panic on the submatch slice.
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

// splitTaskName returns the task-ID segment of a task resource name and whether
// the name has the required projects/.../queues/.../tasks/<id> structure. The
// ID is returned even when it contains illegal characters so the caller can
// report the offending value; validate it with isValidTaskID.
func splitTaskName(name string) (taskID string, structured bool) {
	m := taskNameStructureRE.FindStringSubmatch(name)
	if m == nil {
		return "", false
	}
	return m[1], true
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
	setInitialTaskState(&taskState, queue.name, queue.appEngineHost)

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
// Note: TaskState is a value type but contains pointers (HTTPRequest /
// AppEngineHTTPRequest / Attempt / Headers map), so the snapshot is shallow.
// Callers that need a deep copy should round-trip via taskToProto at the edge.
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
// task. appEngineHost is the base URL App Engine target tasks route to instead
// of the production appspot.com host; an empty value keeps the appspot.com
// routing.
func setInitialTaskState(s *TaskState, queueName string, appEngineHost string) {
	if s.Name == "" {
		taskID := strconv.FormatUint(uint64(rand.Uint64()), 10)
		s.Name = queueName + "/tasks/" + taskID
	}

	// Cloud only sets whole-second precision on CreateTime.
	s.CreateTime = time.Unix(time.Now().Unix(), 0)

	if s.ScheduleTime.IsZero() {
		s.ScheduleTime = time.Now()
	}
	if s.DispatchDeadline == 0 {
		s.DispatchDeadline = 600 * time.Second
	}

	if s.HTTPRequest != nil {
		if s.HTTPRequest.Method == "" {
			s.HTTPRequest.Method = http.MethodPost
		}
		if s.HTTPRequest.Headers == nil {
			s.HTTPRequest.Headers = make(map[string]string)
		}
		// Cloud Tasks overrides any caller-supplied User-Agent.
		s.HTTPRequest.Headers["User-Agent"] = "Google-Cloud-Tasks"
	}

	if s.AppEngineHTTPRequest != nil {
		ae := s.AppEngineHTTPRequest
		if ae.Method == "" {
			ae.Method = http.MethodPost
		}
		if ae.Headers == nil {
			ae.Headers = make(map[string]string)
		}
		ae.Headers["User-Agent"] = "AppEngine-Google; (+http://code.google.com/appengine)"

		if len(ae.Body) > 0 {
			// HTTP field names are case-insensitive, so a caller-supplied
			// "content-type" must suppress the default just as "Content-Type"
			// would - otherwise the task carries two Content-Type headers, which
			// Cloud Tasks does not allow (see conformance/golden/headers.json).
			// The default itself is added under the canonical casing.
			if !hasHeaderFold(ae.Headers, "Content-Type") {
				ae.Headers["Content-Type"] = "application/octet-stream"
			}
			// Content-Length is output-only and computed by Cloud Tasks, which
			// materializes it on the stored AppEngine task.
			ae.Headers["Content-Length"] = strconv.Itoa(len(ae.Body))
		}

		if ae.AppEngineRouting == nil {
			ae.AppEngineRouting = &AppEngineRouting{}
		}

		if ae.AppEngineRouting.Host == "" {
			var host, domainSeparator string

			if appEngineHost == "" {
				// TODO: the new route format for appengine is <PROJECT_ID>.<REGION_ID>.r.appspot.com
				// TODO: support custom domains
				// https://cloud.google.com/appengine/docs/standard/python/how-requests-are-routed
				parts, _ := parseTaskName(s.Name)
				host = "https://" + parts.project + ".appspot.com"
				domainSeparator = "-dot-"
			} else {
				host = appEngineHost
				domainSeparator = "."
			}

			hostURL, err := url.Parse(host)
			if err != nil {
				panic(err)
			}

			if ae.AppEngineRouting.Service != "" {
				hostURL.Host = ae.AppEngineRouting.Service + domainSeparator + hostURL.Host
			}
			if ae.AppEngineRouting.Version != "" {
				hostURL.Host = ae.AppEngineRouting.Version + domainSeparator + hostURL.Host
			}
			if ae.AppEngineRouting.Instance != "" {
				hostURL.Host = ae.AppEngineRouting.Instance + domainSeparator + hostURL.Host
			}

			ae.AppEngineRouting.Host = hostURL.String()
		}

		if ae.RelativeURI == "" {
			ae.RelativeURI = "/"
		}
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
	scheduleTime := task.state.ScheduleTime
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
