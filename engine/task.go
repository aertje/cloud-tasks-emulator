package engine

import (
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var taskNameRE *regexp.Regexp

func init() {
	// Format requirements as per https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks#Task.FIELDS.name
	taskNameRE = regexp.MustCompile("projects/([a-zA-Z0-9:.-]+)/locations/([a-zA-Z0-9-]+)/queues/([a-zA-Z0-9-]+)/tasks/([a-zA-Z0-9_-]+)")
}

func parseTaskName(name string) TaskNameParts {
	matches := taskNameRE.FindStringSubmatch(name)
	return TaskNameParts{
		project:  matches[1],
		location: matches[2],
		queueId:  matches[3],
		taskId:   matches[4],
	}
}

func isValidTaskName(name string) bool {
	return taskNameRE.MatchString(name)
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

	stateMutex sync.Mutex

	cancelOnce sync.Once
}

// newTask creates a new task for the specified queue
func newTask(queue *Queue, taskState TaskState, onDone func(task *Task)) *Task {
	setInitialTaskState(&taskState, queue.name)

	return &Task{
		queue:  queue,
		state:  taskState,
		onDone: onDone,
		cancel: make(chan bool, 1), // Buffered in case cancel comes when task is not scheduled
	}
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

func setInitialTaskState(s *TaskState, queueName string) {
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
		// Override
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
			if _, ok := ae.Headers["Content-Type"]; !ok {
				ae.Headers["Content-Type"] = "application/octet-stream"
			}
		}

		if ae.AppEngineRouting == nil {
			ae.AppEngineRouting = &AppEngineRouting{}
		}

		if ae.AppEngineRouting.Host == "" {
			var host, domainSeparator string

			emulatorHost := os.Getenv("APP_ENGINE_EMULATOR_HOST")

			if emulatorHost == "" {
				// TODO: the new route format for appengine is <PROJECT_ID>.<REGION_ID>.r.appspot.com
				// TODO: support custom domains
				// https://cloud.google.com/appengine/docs/standard/python/how-requests-are-routed
				host = "https://" + parseTaskName(s.Name).project + ".appspot.com"
				domainSeparator = "-dot-"
			} else {
				host = emulatorHost
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
	updateStateForDispatch(task)

	task.doDispatch(true)
}

// Run runs the task outside of the normal queueing mechanism.
// This method is called directly by request.
func (task *Task) Run() TaskState {
	frozen := updateStateForDispatch(task)

	go task.doDispatch(false)

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
	fromNow := time.Until(task.state.ScheduleTime)

	go func() {
		select {
		case <-time.After(fromNow):
			task.queue.fire <- task
			return
		case <-task.cancel:
			task.onDone(task)
			return
		}
	}()
}
