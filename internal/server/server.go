package server

import (
	"context"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/engine"

	tasks "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ServerOptions tunes runtime behaviour of the emulator server.
// It is an alias for engine.Options.
type ServerOptions = engine.Options

// Server is the gRPC CloudTasksServer implementation. It is a thin handler that
// translates proto requests and responses on/off the engine. The one piece of
// state it keeps is the registry of long-running operations returned by the
// batch RPCs, which it also serves as the google.longrunning.Operations
// service; see Register.
type Server struct {
	engine     *engine.Engine
	operations *operationRegistry

	// Options records the options the server was built with. They are consumed by
	// the engine at construction, so mutating this field afterwards has no effect.
	Options ServerOptions
}

// NewServer creates a new emulator server, backed by its own engine, configured
// with the given options. Pass the zero ServerOptions for defaults.
func NewServer(opts ServerOptions) *Server {
	s := &Server{Options: opts, operations: newOperationRegistry()}
	s.engine = engine.New(&s.Options)
	return s
}

// Register registers the server on a gRPC server: the CloudTasks service, and
// the google.longrunning.Operations service clients poll for the operations
// the batch RPCs return.
func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	tasks.RegisterCloudTasksServer(registrar, s)
	longrunningpb.RegisterOperationsServer(registrar, &operationsServer{operations: s.operations})
}

// Stop cancels all engine queues and tasks so no background goroutine outlives
// the server. It does not stop the gRPC server itself (the caller owns that).
func (s *Server) Stop() {
	s.engine.Stop()
}

// ListQueues lists the existing queues
func (s *Server) ListQueues(ctx context.Context, in *tasks.ListQueuesRequest) (*tasks.ListQueuesResponse, error) {
	queues, err := s.engine.ListQueues(ctx)
	if err != nil {
		return nil, mapErr(err)
	}

	var queueStates []*tasks.Queue
	for _, q := range queues {
		queueStates = append(queueStates, queueToProto(q.State()))
	}
	return &tasks.ListQueuesResponse{Queues: queueStates}, nil
}

// GetQueue returns the requested queue
func (s *Server) GetQueue(ctx context.Context, in *tasks.GetQueueRequest) (*tasks.Queue, error) {
	q, err := s.engine.GetQueue(ctx, in.GetName())
	if err != nil {
		return nil, mapErr(err)
	}
	return queueToProto(q.State()), nil
}

// CreateQueue creates a new queue
func (s *Server) CreateQueue(ctx context.Context, in *tasks.CreateQueueRequest) (*tasks.Queue, error) {
	q, err := s.engine.CreateQueue(ctx, in.GetParent(), queueFromProto(in.GetQueue()))
	if err != nil {
		return nil, mapErrForCreateQueue(err, in)
	}
	return queueToProto(q.State()), nil
}

// UpdateQueue updates an existing queue (not implemented yet)
func (s *Server) UpdateQueue(ctx context.Context, in *tasks.UpdateQueueRequest) (*tasks.Queue, error) {
	return nil, mapErr(engine.ErrUnimplemented)
}

// DeleteQueue removes an existing queue.
func (s *Server) DeleteQueue(ctx context.Context, in *tasks.DeleteQueueRequest) (*emptypb.Empty, error) {
	if err := s.engine.DeleteQueue(ctx, in.GetName()); err != nil {
		return nil, mapErrForDeleteQueue(err)
	}
	return &emptypb.Empty{}, nil
}

// PurgeQueue purges the specified queue
func (s *Server) PurgeQueue(ctx context.Context, in *tasks.PurgeQueueRequest) (*tasks.Queue, error) {
	q, err := s.engine.PurgeQueue(ctx, in.GetName())
	if err != nil {
		return nil, mapErr(err)
	}
	return queueToProto(q.State()), nil
}

// PauseQueue pauses queue execution
func (s *Server) PauseQueue(ctx context.Context, in *tasks.PauseQueueRequest) (*tasks.Queue, error) {
	q, err := s.engine.PauseQueue(ctx, in.GetName())
	if err != nil {
		return nil, mapErr(err)
	}
	return queueToProto(q.State()), nil
}

// ResumeQueue resumes a paused queue
func (s *Server) ResumeQueue(ctx context.Context, in *tasks.ResumeQueueRequest) (*tasks.Queue, error) {
	q, err := s.engine.ResumeQueue(ctx, in.GetName())
	if err != nil {
		return nil, mapErr(err)
	}
	return queueToProto(q.State()), nil
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
func (s *Server) ListTasks(ctx context.Context, in *tasks.ListTasksRequest) (*tasks.ListTasksResponse, error) {
	taskList, err := s.engine.ListTasks(ctx, in.GetParent())
	if err != nil {
		return nil, mapErr(err)
	}

	var taskStates []*tasks.Task
	for _, t := range taskList {
		taskStates = append(taskStates, taskToProto(t.State(), in.GetResponseView()))
	}
	return &tasks.ListTasksResponse{Tasks: taskStates}, nil
}

// GetTask returns the specified task
func (s *Server) GetTask(ctx context.Context, in *tasks.GetTaskRequest) (*tasks.Task, error) {
	t, err := s.engine.GetTask(ctx, in.GetName())
	if err != nil {
		return nil, mapErr(err)
	}
	return taskToProto(t.State(), in.GetResponseView()), nil
}

// CreateTask creates a new task
func (s *Server) CreateTask(ctx context.Context, in *tasks.CreateTaskRequest) (*tasks.Task, error) {
	_, frozen, err := s.engine.CreateTask(ctx, in.GetParent(), taskFromProto(in.GetTask()))
	if err != nil {
		return nil, mapErrForCreateTask(err, in)
	}
	return taskToProto(frozen, in.GetResponseView()), nil
}

// BatchCreateTasks creates up to 100 tasks in the same queue
func (s *Server) BatchCreateTasks(ctx context.Context, in *tasks.BatchCreateTasksRequest) (*longrunningpb.Operation, error) {
	if err := s.operations.reserveRequestID(in.GetRequestId()); err != nil {
		return nil, mapErrForBatchCreateTasks(err, in)
	}

	result, err := s.engine.BatchCreateTasks(ctx, in.GetParent(), batchCreateTasksFromProto(in))
	if err != nil {
		s.operations.releaseRequestID(in.GetRequestId())
		return nil, mapErrForBatchCreateTasks(err, in)
	}

	op, err := batchCreateOperationToProto(newOperationName(in.GetParent()), in, result)
	if err != nil {
		s.operations.releaseRequestID(in.GetRequestId())
		return nil, err
	}
	s.operations.add(op)
	return op, nil
}

// BatchDeleteTasks deletes a batch of tasks (not implemented yet)
func (s *Server) BatchDeleteTasks(ctx context.Context, in *tasks.BatchDeleteTasksRequest) (*longrunningpb.Operation, error) {
	return nil, mapErr(engine.ErrUnimplemented)
}

// GetCmekConfig returns the CMEK config of a location (not implemented yet)
func (s *Server) GetCmekConfig(ctx context.Context, in *tasks.GetCmekConfigRequest) (*tasks.CmekConfig, error) {
	return nil, mapErr(engine.ErrUnimplemented)
}

// UpdateCmekConfig updates the CMEK config of a location (not implemented yet)
func (s *Server) UpdateCmekConfig(ctx context.Context, in *tasks.UpdateCmekConfigRequest) (*tasks.CmekConfig, error) {
	return nil, mapErr(engine.ErrUnimplemented)
}

// DeleteTask removes an existing task
func (s *Server) DeleteTask(ctx context.Context, in *tasks.DeleteTaskRequest) (*emptypb.Empty, error) {
	if err := s.engine.DeleteTask(ctx, in.GetName()); err != nil {
		return nil, mapErr(err)
	}
	return &emptypb.Empty{}, nil
}

// RunTask executes an existing task immediately
func (s *Server) RunTask(ctx context.Context, in *tasks.RunTaskRequest) (*tasks.Task, error) {
	_, frozen, err := s.engine.RunTask(ctx, in.GetName())
	if err != nil {
		return nil, mapErr(err)
	}
	return taskToProto(frozen, in.GetResponseView()), nil
}
