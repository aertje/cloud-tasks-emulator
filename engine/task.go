package engine

import (
	"math/rand"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	pduration "github.com/golang/protobuf/ptypes/duration"
	tasks "google.golang.org/genproto/googleapis/cloud/tasks/v2"
)

var r *regexp.Regexp

func init() {
	// Format requirements as per https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks#Task.FIELDS.name
	r = regexp.MustCompile("projects/([a-zA-Z0-9:.-]+)/locations/([a-zA-Z0-9-]+)/queues/([a-zA-Z0-9-]+)/tasks/([a-zA-Z0-9_-]+)")
}

func parseTaskName(task *tasks.Task) TaskNameParts {
	matches := r.FindStringSubmatch(task.GetName())
	return TaskNameParts{
		project:  matches[1],
		location: matches[2],
		queueId:  matches[3],
		taskId:   matches[4],
	}
}

func isValidTaskName(name string) bool {
	return r.MatchString(name)
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

	state *tasks.Task

	cancel chan bool

	onDone func(*Task)

	stateMutex sync.Mutex

	cancelOnce sync.Once
}

// newTask creates a new task for the specified queue
func newTask(queue *Queue, taskState *tasks.Task, onDone func(task *Task)) *Task {
	setInitialTaskState(taskState, queue.name)

	task := &Task{
		queue:  queue,
		state:  taskState,
		onDone: onDone,
		cancel: make(chan bool, 1), // Buffered in case cancel comes when task is not scheduled
	}

	return task
}

// State returns the proto-backed task state.
func (t *Task) State() *tasks.Task {
	return t.state
}

func setInitialTaskState(taskState *tasks.Task, queueName string) {
	if taskState.GetName() == "" {
		taskID := strconv.FormatUint(uint64(rand.Uint64()), 10)
		taskState.Name = queueName + "/tasks/" + taskID
	}

	taskState.CreateTime = ptypes.TimestampNow()
	// For some reason the cloud does not set nanos
	taskState.CreateTime.Nanos = 0

	if taskState.GetScheduleTime() == nil {
		taskState.ScheduleTime = ptypes.TimestampNow()
	}
	if taskState.GetDispatchDeadline() == nil {
		taskState.DispatchDeadline = &pduration.Duration{Seconds: 600}
	}

	// This should probably be set somewhere else?
	taskState.View = tasks.Task_BASIC

	httpRequest := taskState.GetHttpRequest()

	if httpRequest != nil {
		if httpRequest.GetHttpMethod() == tasks.HttpMethod_HTTP_METHOD_UNSPECIFIED {
			httpRequest.HttpMethod = tasks.HttpMethod_POST
		}
		if httpRequest.GetHeaders() == nil {
			httpRequest.Headers = make(map[string]string)
		}
		// Override
		httpRequest.Headers["User-Agent"] = "Google-Cloud-Tasks"
	}

	appEngineHTTPRequest := taskState.GetAppEngineHttpRequest()

	if appEngineHTTPRequest != nil {
		if appEngineHTTPRequest.GetHttpMethod() == tasks.HttpMethod_HTTP_METHOD_UNSPECIFIED {
			appEngineHTTPRequest.HttpMethod = tasks.HttpMethod_POST
		}
		if appEngineHTTPRequest.GetHeaders() == nil {
			appEngineHTTPRequest.Headers = make(map[string]string)
		}

		appEngineHTTPRequest.Headers["User-Agent"] = "AppEngine-Google; (+http://code.google.com/appengine)"

		if appEngineHTTPRequest.GetBody() != nil {
			if _, ok := appEngineHTTPRequest.GetHeaders()["Content-Type"]; !ok {
				appEngineHTTPRequest.Headers["Content-Type"] = "application/octet-stream"
			}
		}

		if appEngineHTTPRequest.GetAppEngineRouting() == nil {
			appEngineHTTPRequest.AppEngineRouting = &tasks.AppEngineRouting{}
		}

		if appEngineHTTPRequest.GetAppEngineRouting().Host == "" {
			var host, domainSeparator string

			emulatorHost := os.Getenv("APP_ENGINE_EMULATOR_HOST")

			if emulatorHost == "" {
				// TODO: the new route format for appengine is <PROJECT_ID>.<REGION_ID>.r.appspot.com
				// TODO: support custom domains
				// https://cloud.google.com/appengine/docs/standard/python/how-requests-are-routed
				host = "https://" + parseTaskName(taskState).project + ".appspot.com"
				domainSeparator = "-dot-"
			} else {
				host = emulatorHost
				domainSeparator = "."
			}

			hostURL, err := url.Parse(host)
			if err != nil {
				panic(err)
			}

			if appEngineHTTPRequest.GetAppEngineRouting().GetService() != "" {
				hostURL.Host = appEngineHTTPRequest.GetAppEngineRouting().GetService() + domainSeparator + hostURL.Host
			}
			if appEngineHTTPRequest.GetAppEngineRouting().GetVersion() != "" {
				hostURL.Host = appEngineHTTPRequest.GetAppEngineRouting().GetVersion() + domainSeparator + hostURL.Host
			}
			if appEngineHTTPRequest.GetAppEngineRouting().GetInstance() != "" {
				hostURL.Host = appEngineHTTPRequest.GetAppEngineRouting().GetInstance() + domainSeparator + hostURL.Host
			}

			appEngineHTTPRequest.GetAppEngineRouting().Host = hostURL.String()
		}

		if appEngineHTTPRequest.GetRelativeUri() == "" {
			appEngineHTTPRequest.RelativeUri = "/"
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
func (task *Task) Run() *tasks.Task {
	taskState := updateStateForDispatch(task)

	go task.doDispatch(false)

	return taskState
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
	scheduled, _ := ptypes.Timestamp(task.state.GetScheduleTime())

	fromNow := time.Until(scheduled)

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
