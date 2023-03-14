package emulator

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"sync"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type OIDCTokenCreator interface {
	CreateOIDCToken(email, subject, audience string) (token string, err error)
}

type HTTPDoer interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

type serverOptions struct {
	resetOnPurge bool
}

// Server represents the emulator server
type Server struct {
	oidcTokenCreator OIDCTokenCreator
	httpDoer         HTTPDoer

	queues      map[string]*Queue
	queuesMutex sync.Mutex

	options serverOptions
}

// NewServer creates a new emulator server with its own task and queue bookkeeping
func NewServer(oidcTokenCreator OIDCTokenCreator, httpDoer HTTPDoer, resetOnPurge bool) *Server {
	return &Server{
		oidcTokenCreator: oidcTokenCreator,
		httpDoer:         httpDoer,
		queues:           make(map[string]*Queue),
		options: serverOptions{
			resetOnPurge: resetOnPurge,
		},
	}
}

func (s *Server) setQueue(queueName string, queue *Queue) {
	s.queuesMutex.Lock()
	defer s.queuesMutex.Unlock()
	s.queues[queueName] = queue
}

func (s *Server) fetchQueue(queueName string) (*Queue, bool) {
	s.queuesMutex.Lock()
	defer s.queuesMutex.Unlock()
	queue, ok := s.queues[queueName]
	return queue, ok
}

func (s *Server) fetchTask(taskName string) (*Task, bool) {
	queueName := parseTaskName(taskName).queueName
	queue, found := s.fetchQueue(queueName)
	if !found {
		return nil, false
	}
	return queue.fetchTask(taskName)
}

// ListQueues lists the existing queues
func (s *Server) ListQueues(ctx context.Context, in *taskspb.ListQueuesRequest) (*taskspb.ListQueuesResponse, error) {
	// TODO: Implement pageing
	// TODO: Split per region: parent is a required field, should error if not specified.

	var queueStates []*taskspb.Queue

	s.queuesMutex.Lock()
	defer s.queuesMutex.Unlock()

	for _, queue := range s.queues {
		if queue != nil {
			queueStates = append(queueStates, queue.state)
		}
	}

	return &taskspb.ListQueuesResponse{
		Queues: queueStates,
	}, nil
}

// GetQueue returns the requested queue
func (s *Server) GetQueue(ctx context.Context, in *taskspb.GetQueueRequest) (*taskspb.Queue, error) {
	// TODO: Name is required, should error if not defined
	queue, ok := s.fetchQueue(in.GetName())

	// Cloud responds with the same error message whether the queue was recently deleted or never existed
	if !ok || queue == nil {
		return nil, status.Errorf(codes.NotFound, "Queue does not exist. If you just created the queue, wait at least a minute for the queue to initialize.")
	}

	return queue.state, nil
}

// CreateQueue creates a new queue
func (s *Server) CreateQueue(ctx context.Context, in *taskspb.CreateQueueRequest) (*taskspb.Queue, error) {
	queueState := in.GetQueue()

	name := queueState.GetName()
	nameMatched, _ := regexp.MatchString("projects/[A-Za-z0-9-]+/locations/[A-Za-z0-9-]+/queues/[A-Za-z0-9-]+", name)
	if !nameMatched {
		return nil, status.Errorf(codes.InvalidArgument, `Queue name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>"`)
	}
	parent := in.GetParent()
	parentMatched, _ := regexp.MatchString("projects/[A-Za-z0-9-]+/locations/[A-Za-z0-9-]+", parent)
	if !parentMatched {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid resource field value in the request.")
	}
	queue, ok := s.fetchQueue(name)
	if ok {
		if queue != nil {
			return nil, status.Errorf(codes.AlreadyExists, "Queue already exists")
		}

		return nil, status.Errorf(codes.FailedPrecondition, "The queue cannot be created because a queue with this name existed too recently.")
	}

	queue = NewQueue(
		s,
		queueState,
	)
	s.setQueue(name, queue)

	queueState = queue.Run()

	return queueState, nil
}

// UpdateQueue updates an existing queue (not implemented yet)
func (s *Server) UpdateQueue(ctx context.Context, in *taskspb.UpdateQueueRequest) (*taskspb.Queue, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// DeleteQueue removes an existing queue.
func (s *Server) DeleteQueue(ctx context.Context, in *taskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
	// Lock across whole method as we are reading and removing from the queue map
	s.queuesMutex.Lock()
	defer s.queuesMutex.Unlock()

	queue, ok := s.queues[in.GetName()]

	// Cloud responds with same error for recently deleted queue
	if !ok || queue == nil {
		return nil, status.Errorf(codes.NotFound, "Requested entity was not found.")
	}

	queue.Delete()

	s.queues[in.GetName()] = nil

	return &emptypb.Empty{}, nil
}

// PurgeQueue purges the specified queue
func (s *Server) PurgeQueue(ctx context.Context, in *taskspb.PurgeQueueRequest) (*taskspb.Queue, error) {
	queue, _ := s.fetchQueue(in.GetName())

	if s.options.resetOnPurge {
		// Use the development environment behaviour - synchronously purge the queue and release all task names
		queue.HardReset()
	} else {
		// Mirror production behaviour - spin off an asynchronous purge operation and return
		queue.Purge()
	}

	return queue.state, nil
}

// PauseQueue pauses queue execution
func (s *Server) PauseQueue(ctx context.Context, in *taskspb.PauseQueueRequest) (*taskspb.Queue, error) {
	queue, _ := s.fetchQueue(in.GetName())

	queue.Pause()

	return queue.state, nil
}

// ResumeQueue resumes a paused queue
func (s *Server) ResumeQueue(ctx context.Context, in *taskspb.ResumeQueueRequest) (*taskspb.Queue, error) {
	queue, _ := s.fetchQueue(in.GetName())

	queue.Resume()

	return queue.state, nil
}

// GetIamPolicy doesn't do anything
func (s *Server) GetIamPolicy(ctx context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// SetIamPolicy doesn't do anything
func (s *Server) SetIamPolicy(ctx context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// TestIamPermissions doesn't do anything
func (s *Server) TestIamPermissions(ctx context.Context, in *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// ListTasks lists the tasks in the specified queue
func (s *Server) ListTasks(ctx context.Context, in *taskspb.ListTasksRequest) (*taskspb.ListTasksResponse, error) {
	// TODO: Implement pageing of some sort
	queue, ok := s.fetchQueue(in.GetParent())
	if !ok || queue == nil {
		return nil, status.Errorf(codes.NotFound, "Queue does not exist. If you just created the queue, wait at least a minute for the queue to initialize.")
	}

	var taskStates []*taskspb.Task

	queue.tasksMutex.Lock()
	defer queue.tasksMutex.Unlock()

	for _, task := range queue.tasks {
		if task != nil {
			taskStates = append(taskStates, task.state)
		}
	}

	return &taskspb.ListTasksResponse{
		Tasks: taskStates,
	}, nil
}

// GetTask returns the specified task
func (s *Server) GetTask(ctx context.Context, in *taskspb.GetTaskRequest) (*taskspb.Task, error) {
	task, ok := s.fetchTask(in.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Task does not exist.")
	}
	if task == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.")
	}

	return task.state, nil
}

// CreateTask creates a new task
func (s *Server) CreateTask(ctx context.Context, in *taskspb.CreateTaskRequest) (*taskspb.Task, error) {
	queueName := in.GetParent()
	queue, ok := s.fetchQueue(queueName)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Queue does not exist.")
	}
	if queue == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "The queue no longer exists, though a queue with this name existed recently.")
	}

	if in.Task.Name != "" {
		// If a name is specified, it must be valid, it must be unique, and it must belong to this queue
		if !isValidTaskName(in.Task.Name) {
			return nil, status.Errorf(codes.InvalidArgument, `Task name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>/tasks/<TASK_ID>"`)
		}
		if !strings.HasPrefix(in.Task.Name, queueName+"/tasks/") {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"The queue name from request ('%s') must be the same as the queue name in the named task ('%s').",
				in.Task.Name,
				queueName,
			)
		}
		if _, exists := s.fetchTask(in.Task.Name); exists {
			return nil, status.Errorf(codes.AlreadyExists, "Requested entity already exists")
		}
	}

	_, taskState := queue.NewTask(in.GetTask())

	return taskState, nil
}

// DeleteTask removes an existing task
// TODO: BUG - when task has run out of attempts it can't be deleted
func (s *Server) DeleteTask(ctx context.Context, in *taskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
	task, ok := s.fetchTask(in.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Task does not exist.")
	}
	if task == nil {
		return nil, status.Errorf(codes.NotFound, "The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.")
	}

	// The removal of the task from the queue struct is handled in a callback to the queue
	task.Delete()

	return &emptypb.Empty{}, nil
}

// RunTask executes an existing task immediately
func (s *Server) RunTask(ctx context.Context, in *taskspb.RunTaskRequest) (*taskspb.Task, error) {
	task, ok := s.fetchTask(in.GetName())

	if !ok {
		return nil, status.Errorf(codes.NotFound, "Task does not exist.")
	}
	if task == nil {
		return nil, status.Errorf(codes.NotFound, "The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.")
	}

	taskState := task.Run()

	return taskState, nil
}

func (s *Server) Stop() {
	s.queuesMutex.Lock()
	defer s.queuesMutex.Unlock()

	for queueName, queue := range s.queues {
		if queue != nil {
			queue.Delete()
		}

		delete(s.queues, queueName)
	}
}
