package emulator

import (
	"context"
	"regexp"
	"strings"
	"sync"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type serverOptions struct {
	resetOnPurge bool
}

// Server represents the emulator server
type Server struct {
	qs map[string]*Queue
	ts map[string]*Task

	qsMux   sync.Mutex
	tsMux   sync.Mutex
	options serverOptions
}

// NewServer creates a new emulator server with its own task and queue bookkeeping
func NewServer(resetOnPurge bool) *Server {
	return &Server{
		qs: make(map[string]*Queue),
		ts: make(map[string]*Task),
		options: serverOptions{
			resetOnPurge: resetOnPurge,
		},
	}
}

func (s *Server) setQueue(queueName string, queue *Queue) {
	s.qsMux.Lock()
	defer s.qsMux.Unlock()
	s.qs[queueName] = queue
}

func (s *Server) fetchQueue(queueName string) (*Queue, bool) {
	s.qsMux.Lock()
	defer s.qsMux.Unlock()
	queue, ok := s.qs[queueName]
	return queue, ok
}

func (s *Server) removeQueue(queueName string) {
	s.setQueue(queueName, nil)
}

func (s *Server) setTask(taskName string, task *Task) {
	s.tsMux.Lock()
	defer s.tsMux.Unlock()
	s.ts[taskName] = task
}

func (s *Server) fetchTask(taskName string) (*Task, bool) {
	s.tsMux.Lock()
	defer s.tsMux.Unlock()
	task, ok := s.ts[taskName]
	return task, ok
}

func (s *Server) removeTask(taskName string) {
	s.setTask(taskName, nil)
}

func (s *Server) hardDeleteTask(taskName string) {
	s.tsMux.Lock()
	defer s.tsMux.Unlock()
	delete(s.ts, taskName)
}

// ListQueues lists the existing queues
func (s *Server) ListQueues(ctx context.Context, in *taskspb.ListQueuesRequest) (*taskspb.ListQueuesResponse, error) {
	// TODO: Implement pageing

	var queueStates []*taskspb.Queue

	s.qsMux.Lock()
	defer s.qsMux.Unlock()

	for _, queue := range s.qs {
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
		return nil, status.Errorf(codes.InvalidArgument, "Queue name must be formatted: \"projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>\"")
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

	// Make a deep copy so that the original is frozen for the http response
	queue, queueState = NewQueue(
		name,
		proto.Clone(queueState).(*taskspb.Queue),
		func(task *Task) {
			s.removeTask(task.state.GetName())
		},
	)
	s.setQueue(name, queue)
	queue.Run()

	return queueState, nil
}

// UpdateQueue updates an existing queue (not implemented yet)
func (s *Server) UpdateQueue(ctx context.Context, in *taskspb.UpdateQueueRequest) (*taskspb.Queue, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// DeleteQueue removes an existing queue.
func (s *Server) DeleteQueue(ctx context.Context, in *taskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
	queue, ok := s.fetchQueue(in.GetName())

	// Cloud responds with same error for recently deleted queue
	if !ok || queue == nil {
		return nil, status.Errorf(codes.NotFound, "Requested entity was not found.")
	}

	queue.Delete()

	s.removeQueue(in.GetName())

	return &emptypb.Empty{}, nil
}

// PurgeQueue purges the specified queue
func (s *Server) PurgeQueue(ctx context.Context, in *taskspb.PurgeQueueRequest) (*taskspb.Queue, error) {
	queue, _ := s.fetchQueue(in.GetName())

	if s.options.resetOnPurge {
		// Use the development environment behaviour - synchronously purge the queue and release all task names
		queue.HardReset(s)
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

	queue.tsMux.Lock()
	defer queue.tsMux.Unlock()

	for _, task := range queue.ts {
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

	task, taskState := queue.NewTask(in.GetTask())

	s.setTask(taskState.GetName(), task)

	return taskState, nil
}

// DeleteTask removes an existing task
func (s *Server) DeleteTask(ctx context.Context, in *taskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
	task, ok := s.fetchTask(in.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Task does not exist.")
	}
	if task == nil {
		return nil, status.Errorf(codes.NotFound, "The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.")
	}

	// The removal of the task from the server struct is handled in the queue callback
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
