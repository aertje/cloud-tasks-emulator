package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"regexp"
	"sync"

	tasks "google.golang.org/genproto/googleapis/cloud/tasks/v2beta3"
	v1 "google.golang.org/genproto/googleapis/iam/v1"

	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

// NewServer creates a new emulator server with its own task and queue bookkeeping
func NewServer() *Server {
	return &Server{
		qs: sync.Map{},
		ts: sync.Map{},
	}
}

// Server represents the emulator server
type Server struct {
	qs sync.Map
	ts sync.Map
}

// ListQueues lists the existing queues
func (s *Server) ListQueues(ctx context.Context, in *tasks.ListQueuesRequest) (*tasks.ListQueuesResponse, error) {
	// TODO: Implement pageing

	var queueStates []*tasks.Queue

	s.qs.Range(func(_, v interface{}) bool {
		if v != nil {
			queue := v.(*Queue)
			queueStates = append(queueStates, queue.state)
		}
		return true;
	})

	return &tasks.ListQueuesResponse{
		Queues: queueStates,
	}, nil
}

// GetQueue returns the requested queue
func (s *Server) GetQueue(ctx context.Context, in *tasks.GetQueueRequest) (*tasks.Queue, error) {
	tmpQ, _ := s.qs.Load(in.GetName())
	queue := tmpQ.(*Queue)

	// TODO: handle not found

	return queue.state, nil
}

// CreateQueue creates a new queue
func (s *Server) CreateQueue(ctx context.Context, in *tasks.CreateQueueRequest) (*tasks.Queue, error) {
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
	tmpQ, ok := s.qs.Load(name)
	if ok {
		if tmpQ != nil {
			return nil, status.Errorf(codes.AlreadyExists, "Queue already exists")
		}

		return nil, status.Errorf(codes.FailedPrecondition, "The queue cannot be created because a queue with this name existed too recently.")
	}

	// Make a deep copy so that the original is frozen for the http response
	queue, queueState := NewQueue(
		name,
		proto.Clone(queueState).(*tasks.Queue),
		func(task *Task) {
			s.ts.Store(task.state.GetName(), nil)
		},
	)
	s.qs.Store(name, queue)
	queue.Run()

	return queueState, nil
}

// UpdateQueue updates an existing queue (not implemented yet)
func (s *Server) UpdateQueue(ctx context.Context, in *tasks.UpdateQueueRequest) (*tasks.Queue, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// DeleteQueue removes an existing queue.
func (s *Server) DeleteQueue(ctx context.Context, in *tasks.DeleteQueueRequest) (*empty.Empty, error) {
	tmpQ, ok := s.qs.Load(in.GetName())

	// Cloud responds with same error for recently deleted queue
	if !ok || tmpQ == nil {
		return nil, status.Errorf(codes.NotFound, "Requested entity was not found.")
	}

	queue := tmpQ.(*Queue)
	queue.Delete()

	s.qs.Store(in.GetName(), nil)

	return &empty.Empty{}, nil
}

// PurgeQueue purges the specified queue
func (s *Server) PurgeQueue(ctx context.Context, in *tasks.PurgeQueueRequest) (*tasks.Queue, error) {
	tmpQ, _ := s.qs.Load(in.GetName())
	queue := tmpQ.(*Queue)

	queue.Purge()

	return queue.state, nil
}

// PauseQueue pauses queue execution
func (s *Server) PauseQueue(ctx context.Context, in *tasks.PauseQueueRequest) (*tasks.Queue, error) {
	tmpQ, _ := s.qs.Load(in.GetName())
	queue := tmpQ.(*Queue)

	queue.Pause()

	return queue.state, nil
}

// ResumeQueue resumes a paused queue
func (s *Server) ResumeQueue(ctx context.Context, in *tasks.ResumeQueueRequest) (*tasks.Queue, error) {
	tmpQ, _ := s.qs.Load(in.GetName())
	queue := tmpQ.(*Queue)

	queue.Resume()

	return queue.state, nil
}

// GetIamPolicy doesn't do anything
func (s *Server) GetIamPolicy(ctx context.Context, in *v1.GetIamPolicyRequest) (*v1.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// SetIamPolicy doesn't do anything
func (s *Server) SetIamPolicy(ctx context.Context, in *v1.SetIamPolicyRequest) (*v1.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// TestIamPermissions doesn't do anything
func (s *Server) TestIamPermissions(ctx context.Context, in *v1.TestIamPermissionsRequest) (*v1.TestIamPermissionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// ListTasks lists the tasks in the specified queue
func (s *Server) ListTasks(ctx context.Context, in *tasks.ListTasksRequest) (*tasks.ListTasksResponse, error) {
	// TODO: Implement pageing of some sort
	tmpQ, _ := s.qs.Load(in.GetParent())
	queue := tmpQ.(*Queue)

	var taskStates []*tasks.Task

	queue.ts.Range(func(_, v interface{}) bool {
		if v != nil {
			task := v.(*Task)
			taskStates = append(taskStates, task.state)
		}
		return true
	})

	return &tasks.ListTasksResponse{
		Tasks: taskStates,
	}, nil
}

// GetTask returns the specified task
func (s *Server) GetTask(ctx context.Context, in *tasks.GetTaskRequest) (*tasks.Task, error) {
	tmpT, ok := s.ts.Load(in.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Task does not exist.")
	}
	if tmpT == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "The task no longer exists,  though a task with this name existed recently. The task either successfully completed or was deleted.")
	}

	task := tmpT.(*Task)
	return task.state, nil
}

// CreateTask creates a new task
func (s *Server) CreateTask(ctx context.Context, in *tasks.CreateTaskRequest) (*tasks.Task, error) {
	// TODO: task name validation

	queueName := in.GetParent()
	tmpQ, ok := s.qs.Load(queueName)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Queue does not exist.")
	}
	if tmpQ == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "The queue no longer exists, though a queue with this name existed recently.")
	}
	queue := tmpQ.(*Queue)

	task, taskState := queue.NewTask(in.GetTask())
	s.ts.Store(taskState.GetName(), task)

	return taskState, nil
}

// DeleteTask removes an existing task
func (s *Server) DeleteTask(ctx context.Context, in *tasks.DeleteTaskRequest) (*empty.Empty, error) {
	tmpT, ok := s.ts.Load(in.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Task does not exist.")
	}
	if tmpT == nil {
		return nil, status.Errorf(codes.NotFound, "The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.")
	}

	task := tmpT.(*Task)
	task.Delete()

	return &empty.Empty{}, nil
}

// RunTask executes an existing task immediately
func (s *Server) RunTask(ctx context.Context, in *tasks.RunTaskRequest) (*tasks.Task, error) {
	tmpT, ok := s.ts.Load(in.GetName())

	if !ok {
		return nil, status.Errorf(codes.NotFound, "Task does not exist.")
	}
	if tmpT == nil {
		return nil, status.Errorf(codes.NotFound, "The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.")
	}

	task := tmpT.(*Task)
	taskState := task.Run()

	return taskState, nil
}

func main() {
	host := flag.String("host", "localhost", "The host name")
	port := flag.String("port", "8123", "The port")

	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf("%v:%v", *host, *port))
	if err != nil {
		panic(err)
	}

	print(fmt.Sprintf("Starting cloud tasks emulator, listening on %v:%v", *host, *port))

	grpcServer := grpc.NewServer()
	tasks.RegisterCloudTasksServer(grpcServer, NewServer())
	grpcServer.Serve(lis)
}
