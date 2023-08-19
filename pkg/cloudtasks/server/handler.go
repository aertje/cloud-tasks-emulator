package server

import (
	"context"
	"errors"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/aertje/cloud-tasks-emulator/pkg/cloudtasks/emulator"
	"github.com/aertje/cloud-tasks-emulator/pkg/maybe"
	respstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var unhandledError = status.Error(codes.Internal, "Unhandled emulator error.")

type Handler struct {
	emulator *emulator.Emulator
}

func NewHandler(emulator *emulator.Emulator) Handler {
	return Handler{
		emulator: emulator,
	}
}

func (h *Handler) ListQueues(
	ctx context.Context, req *taskspb.ListQueuesRequest,
) (*taskspb.ListQueuesResponse, error) {
	response := &taskspb.ListQueuesResponse{
		Queues: queuesAsPB(h.emulator.GetQueues()),
	}

	return response, nil
}

func (h *Handler) GetQueue(
	ctx context.Context, req *taskspb.GetQueueRequest,
) (*taskspb.Queue, error) {
	queue, err := h.emulator.GetQueue(req.GetName())

	// Cloud responds with the same error message whether the queue never existed (!present)
	// or was recently deleted (!has)
	if err != nil {
		if errors.Is(err, emulator.ErrorQueueNotFound{}) || errors.Is(err, emulator.ErrorQueueDeleted{}) {
			return nil, status.Error(
				codes.NotFound,
				"Queue does not exist. If you just created the queue, wait at least a minute for the queue to initialize.",
			)
		}
		return nil, unhandledError
	}

	return queueAsPB(queue), nil
}

func (h *Handler) CreateQueue(
	ctx context.Context, req *taskspb.CreateQueueRequest,
) (*taskspb.Queue, error) {
	queue, err := h.emulator.CreateQueue(req.Queue.GetName(), queueConfigWithDefaults(req.Queue))

	if err != nil {
		if errors.Is(err, emulator.ErrorQueueAlreadyExists{}) {
			return nil, status.Error(codes.AlreadyExists, "Queue already exists")
		}
		if errors.Is(err, emulator.ErrorQueueExistedBefore{}) {
			return nil, status.Error(
				codes.FailedPrecondition,
				"The queue cannot be created because a queue with this name existed too recently.",
			)
		}
		return nil, unhandledError
	}

	return queueAsPB(queue), nil
}

// UpdateQueue updates an existing queue (not implemented yet)
func (h *Handler) UpdateQueue(ctx context.Context, req *taskspb.UpdateQueueRequest) (*taskspb.Queue, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// DeleteQueue removes an existing queue.
func (h *Handler) DeleteQueue(ctx context.Context, req *taskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
	panic("not implemented")
}

// PurgeQueue purges the specified queue
func (h *Handler) PurgeQueue(ctx context.Context, req *taskspb.PurgeQueueRequest) (*taskspb.Queue, error) {
	panic("not implemented")
}

// PauseQueue pauses queue execution
func (h *Handler) PauseQueue(ctx context.Context, req *taskspb.PauseQueueRequest) (*taskspb.Queue, error) {
	panic("not implemented")
}

// ResumeQueue resumes a paused queue
func (h *Handler) ResumeQueue(ctx context.Context, req *taskspb.ResumeQueueRequest) (*taskspb.Queue, error) {
	panic("not implemented")
}

// GetIamPolicy doesn't do anything
func (h *Handler) GetIamPolicy(ctx context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// SetIamPolicy doesn't do anything
func (h *Handler) SetIamPolicy(ctx context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// TestIamPermissions doesn't do anything
func (h *Handler) TestIamPermissions(ctx context.Context, in *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Not yet implemented")
}

// ListTasks lists the tasks in the specified queue
func (h *Handler) ListTasks(ctx context.Context, req *taskspb.ListTasksRequest) (*taskspb.ListTasksResponse, error) {
	tasks, err := h.emulator.GetTasks(req.Parent)
	if err != nil {
		if errors.Is(err, emulator.ErrorQueueNotFound{}) || errors.Is(err, emulator.ErrorQueueDeleted{}) {
			status.Error(
				codes.NotFound,
				"Queue does not exist. If you just created the queue, wait at least a minute for the queue to initialize.",
			)
		}
		return nil, unhandledError
	}

	tasksPB, err := tasksAsPB(tasks)
	if err != nil {
		return nil, unhandledError
	}

	response := &taskspb.ListTasksResponse{
		Tasks: tasksPB,
	}

	return response, nil
}

// GetTask returns the specified task
func (h *Handler) GetTask(ctx context.Context, req *taskspb.GetTaskRequest) (*taskspb.Task, error) {
	panic("not implemented")
}

// CreateTask creates a new task
func (h *Handler) CreateTask(ctx context.Context, req *taskspb.CreateTaskRequest) (*taskspb.Task, error) {
	panic("not implemented")
}

// DeleteTask removes an existing task
func (h *Handler) DeleteTask(ctx context.Context, req *taskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
	panic("not implemented")
}

// RunTask executes an existing task immediately
func (h *Handler) RunTask(ctx context.Context, req *taskspb.RunTaskRequest) (*taskspb.Task, error) {
	panic("not implemented")
}

func queuesAsPB(queues []emulator.Queue) []*taskspb.Queue {
	var queuesPB []*taskspb.Queue
	for _, q := range queues {
		queuesPB = append(queuesPB, queueAsPB(q))
	}
	return queuesPB
}

func queueAsPB(q emulator.Queue) *taskspb.Queue {
	return &taskspb.Queue{
		Name: q.Name,
		RateLimits: &taskspb.RateLimits{
			MaxDispatchesPerSecond:  q.Config.RateLimits.MaxDispatchesPerSecond,
			MaxBurstSize:            int32(q.Config.RateLimits.MaxBurstSize),
			MaxConcurrentDispatches: int32(q.Config.RateLimits.MaxConcurrentDispatches),
		},
		RetryConfig: &taskspb.RetryConfig{
			MaxAttempts:  int32(q.Config.RetryConfig.MaxAttempts),
			MinBackoff:   durationpb.New(q.Config.RetryConfig.MinBackoff),
			MaxBackoff:   durationpb.New(q.Config.RetryConfig.MaxBackoff),
			MaxDoublings: int32(q.Config.RetryConfig.MaxDoublings),
		},
		State: taskspb.Queue_State(q.State.RunState),
	}
}

func queueConfigWithDefaults(queuePB *taskspb.Queue) emulator.QueueConfig {
	if queuePB == nil {
		queuePB = &taskspb.Queue{}
	}
	ratelimitsPB := queuePB.GetRateLimits()
	if ratelimitsPB == nil {
		ratelimitsPB = &taskspb.RateLimits{}
	}
	retryConfigPB := queuePB.GetRetryConfig()
	if retryConfigPB == nil {
		retryConfigPB = &taskspb.RetryConfig{}
	}

	rateLimits := emulator.RateLimits{
		MaxDispatchesPerSecond:  ratelimitsPB.GetMaxDispatchesPerSecond(),
		MaxBurstSize:            int(ratelimitsPB.GetMaxBurstSize()),
		MaxConcurrentDispatches: int(ratelimitsPB.GetMaxDispatchesPerSecond()),
	}
	if rateLimits.MaxDispatchesPerSecond == 0 {
		rateLimits.MaxDispatchesPerSecond = emulator.DefaultMaxDispatchesPerSecond
	}
	if rateLimits.MaxBurstSize == 0 {
		rateLimits.MaxBurstSize = emulator.DefaultMaxBurstSize
	}
	if rateLimits.MaxConcurrentDispatches == 0 {
		rateLimits.MaxConcurrentDispatches = emulator.DefaultMaxConcurrentDispatches
	}

	retryConfig := emulator.RetryConfig{
		MaxAttempts:  int(retryConfigPB.MaxAttempts),
		MaxDoublings: int(retryConfigPB.MaxDoublings),
		MinBackoff:   retryConfigPB.MinBackoff.AsDuration(),
		MaxBackoff:   retryConfigPB.MaxBackoff.AsDuration(),
	}
	if retryConfig.MaxAttempts == 0 {
		retryConfig.MaxAttempts = emulator.DefaultMaxAttempts
	}
	if retryConfig.MaxDoublings == 0 {
		retryConfig.MaxDoublings = emulator.DefaultMaxDoublings
	}
	if retryConfig.MinBackoff == 0 {
		retryConfig.MinBackoff = emulator.DefaultMinBackOff
	}
	if retryConfig.MaxBackoff == 0 {
		retryConfig.MaxBackoff = emulator.DefaultMaxBackOff
	}

	return emulator.QueueConfig{
		RateLimits:  rateLimits,
		RetryConfig: retryConfig,
	}
}

func tasksAsPB(tasks []emulator.Task) ([]*taskspb.Task, error) {
	var tasksPB []*taskspb.Task
	for _, t := range tasks {
		taskPB, err := taskAsPB(t)
		if err != nil {
			return nil, err
		}
		tasksPB = append(tasksPB, taskPB)
	}
	return tasksPB, nil
}

func taskAsPB(t emulator.Task) (*taskspb.Task, error) {
	var firstAttemptPB *taskspb.Attempt
	firstAttempt, has := t.State.FirstAttempt.Get()
	if has {
		// Only dispatchTime is set for the first attempt
		firstAttemptPB = &taskspb.Attempt{
			DispatchTime: asTimestampPBNearestMicrosecond(firstAttempt.DispatchTime),
		}
	}

	var lastAttemptPB *taskspb.Attempt
	lastAttempt, has := t.State.LastAttempt.Get()
	if has {
		var responseStatusPB *respstatus.Status
		responseStatus, has := lastAttempt.ResponseStatus.Get()
		if has {
			responseStatusPB = &respstatus.Status{
				Code:    int32(toStatusCodePB(responseStatus.Code)),
				Message: responseStatus.Message,
			}
		}
		lastAttemptPB = &taskspb.Attempt{
			ScheduleTime:   asTimestampPBNearestMicrosecond(maybe.Just(lastAttempt.ScheduleTime)),
			DispatchTime:   asTimestampPBNearestMicrosecond(lastAttempt.DispatchTime),
			ResponseTime:   asTimestampPBNearestMicrosecond(lastAttempt.ResponseTime),
			ResponseStatus: responseStatusPB,
		}
	}

	taskPB := &taskspb.Task{
		Name:             t.Name,
		ScheduleTime:     timestamppb.New(t.State.ScheduleTime),
		CreateTime:       timestamppb.New(t.State.CreateTime),
		DispatchDeadline: durationpb.New(t.State.DispatchDeadline),
		DispatchCount:    int32(t.State.DispatchCount),
		ResponseCount:    int32(t.State.ResponseCount),
		FirstAttempt:     firstAttemptPB,
		LastAttempt:      lastAttemptPB,
	}

	if t.Config.MaybeHTTPRequest.Has() && t.Config.MaybeAppengineHTTPRequest.Has() {
		return nil, errors.New("both http and appengine http request set")
	}
	if !t.Config.MaybeHTTPRequest.Has() && !t.Config.MaybeAppengineHTTPRequest.Has() {
		return nil, errors.New("neither http nor appengine http request set")
	}

	httpRequest, has := t.Config.MaybeHTTPRequest.Get()
	if has {
		taskPB.MessageType = &taskspb.Task_HttpRequest{
			HttpRequest: &taskspb.HttpRequest{
				Url:        httpRequest.URL,
				HttpMethod: toHTTPMethodPB(httpRequest.HTTPMethod),
				Headers:    httpRequest.Headers,
				Body:       httpRequest.Body,
			},
		}
	}
	appengineHTTPRequest, has := t.Config.MaybeAppengineHTTPRequest.Get()
	if has {
		taskPB.MessageType = &taskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
				RelativeUri: appengineHTTPRequest.RelatveURI,
				HttpMethod:  toHTTPMethodPB(appengineHTTPRequest.HTTPMethod),
				Headers:     appengineHTTPRequest.Headers,
				Body:        appengineHTTPRequest.Body,
				AppEngineRouting: &taskspb.AppEngineRouting{
					Service:  appengineHTTPRequest.Routing.Service,
					Version:  appengineHTTPRequest.Routing.Version,
					Instance: appengineHTTPRequest.Routing.Instance,
					Host:     appengineHTTPRequest.Routing.Host,
				},
			},
		}
	}

	return taskPB, nil
}

func asTimestampPBNearestMicrosecond(maybeTime maybe.M[time.Time]) *timestamppb.Timestamp {
	t, has := maybeTime.Get()
	if !has {
		return &timestamppb.Timestamp{}
	}
	return timestamppb.New(t.Round(time.Microsecond))
}
