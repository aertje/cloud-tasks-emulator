package server_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	. "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	. "github.com/aertje/cloud-tasks-emulator/v2/internal/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBatchCreateTasks(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)
	namedTask := queue.GetName() + "/tasks/named-task"

	op, err := client.BatchCreateTasks(context.Background(), batchCreateRequest(queue.GetName(),
		futureTask(namedTask),
		futureTask(""),
	))
	require.NoError(t, err)

	assert.True(t, op.Done())
	assert.True(t, strings.HasPrefix(op.Name(), formattedParent+"/operations/"), "operation name %q", op.Name())

	resp, err := op.Wait(context.Background())
	require.NoError(t, err)
	require.Len(t, resp.GetTasks(), 2)
	assert.Equal(t, namedTask, resp.GetTasks()[0].GetName())
	assert.True(t, strings.HasPrefix(resp.GetTasks()[1].GetName(), queue.GetName()+"/tasks/"))
	assert.NotEqual(t, namedTask, resp.GetTasks()[1].GetName())

	metadata, err := op.Metadata()
	require.NoError(t, err)
	assert.Equal(t, taskspb.BatchCreateTasksMetadata_SUCCEEDED, metadata.GetState())
	assert.Empty(t, metadata.GetFailedRequests())
	assert.NotNil(t, metadata.GetStartTime())
	assert.NotNil(t, metadata.GetEndTime())

	assert.ElementsMatch(t,
		[]string{resp.GetTasks()[0].GetName(), resp.GetTasks()[1].GetName()},
		listTaskNames(t, client, queue.GetName()),
	)
}

func TestBatchCreateTasksReportsFailedRequestsByIndex(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)
	existingTask := queue.GetName() + "/tasks/existing-task"

	_, err := client.CreateTask(context.Background(), &taskspb.CreateTaskRequest{
		Parent: queue.GetName(),
		Task:   futureTask(existingTask),
	})
	require.NoError(t, err)

	op, err := client.BatchCreateTasks(context.Background(), batchCreateRequest(queue.GetName(),
		futureTask(queue.GetName()+"/tasks/first-task"),
		futureTask(existingTask),
		noURLTask(),
		futureTask(queue.GetName()+"/tasks/last-task"),
	))
	require.NoError(t, err)

	resp, err := op.Wait(context.Background())
	require.NoError(t, err)
	require.Len(t, resp.GetTasks(), 2)
	assert.Equal(t, queue.GetName()+"/tasks/first-task", resp.GetTasks()[0].GetName())
	assert.Equal(t, queue.GetName()+"/tasks/last-task", resp.GetTasks()[1].GetName())

	metadata, err := op.Metadata()
	require.NoError(t, err)
	assert.Equal(t, taskspb.BatchCreateTasksMetadata_PARTIALLY_SUCCEEDED, metadata.GetState())

	failed := metadata.GetFailedRequests()
	require.Len(t, failed, 2)
	assert.Equal(t, int32(grpcCodes.AlreadyExists), failed[1].GetCode())
	assert.Regexp(t, "^Requested entity already exists", failed[1].GetMessage())
	assert.Equal(t, int32(grpcCodes.InvalidArgument), failed[2].GetCode())
	assert.Regexp(t, "^HttpRequest.url is required", failed[2].GetMessage())
}

func TestBatchCreateTasksReportsMissingQueueInFailedOperation(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queueName := formatQueueName(formattedParent, queueIDForTest(t))

	op, err := client.BatchCreateTasks(context.Background(), batchCreateRequest(queueName, futureTask(""), futureTask("")))
	require.NoError(t, err)
	assert.True(t, op.Done())

	_, err = op.Wait(context.Background())
	assert.ErrorContains(t, err, "unsupported result type <nil>", "an operation that created nothing carries no result")

	metadata, err := op.Metadata()
	require.NoError(t, err)
	assert.Equal(t, taskspb.BatchCreateTasksMetadata_FAILED, metadata.GetState())
	failed := metadata.GetFailedRequests()
	require.Len(t, failed, 2)
	for _, index := range []int32{0, 1} {
		assert.Equal(t, int32(grpcCodes.NotFound), failed[index].GetCode())
		assert.Regexp(t, "^Queue does not exist", failed[index].GetMessage())
	}
}

func TestBatchCreateTasksRejectsBatchWhenEveryTaskFails(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)
	existingTask := queue.GetName() + "/tasks/existing-task"

	_, err := client.CreateTask(context.Background(), &taskspb.CreateTaskRequest{
		Parent: queue.GetName(),
		Task:   futureTask(existingTask),
	})
	require.NoError(t, err)

	op, err := client.BatchCreateTasks(context.Background(), batchCreateRequest(queue.GetName(),
		futureTask(existingTask),
		noURLTask(),
	))

	assert.Nil(t, op)
	assertIsGrpcError(t, "^Requested entity already exists", grpcCodes.AlreadyExists, err)
	assert.Len(t, listTaskNames(t, client, queue.GetName()), 1)
}

func TestBatchCreateTasksRejectsInvalidBatch(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)
	otherQueue := formatQueueName(formattedParent, "other-queue")

	tooMany := make([]*taskspb.Task, 101)
	for i := range tooMany {
		tooMany[i] = futureTask("")
	}
	withParent := func(request *taskspb.BatchCreateTasksRequest, index int, parent string) *taskspb.BatchCreateTasksRequest {
		request.Requests[index].Parent = parent
		return request
	}

	tests := []struct {
		name        string
		request     *taskspb.BatchCreateTasksRequest
		wantMessage string
	}{
		{
			name:        "empty batch",
			request:     batchCreateRequest(queue.GetName()),
			wantMessage: `^Request must contain at least one task\.$`,
		},
		{
			name:        "too many tasks",
			request:     batchCreateRequest(queue.GetName(), tooMany...),
			wantMessage: `^A maximum of 100 tasks can be created in a single batch, but got 101 tasks\.$`,
		},
		{
			name:        "request for another queue",
			request:     withParent(batchCreateRequest(queue.GetName(), futureTask(""), futureTask("")), 1, otherQueue),
			wantMessage: `^All tasks in a batch request must target the same queue\. Found task for queue ` + otherQueue + ` in batch request for queue ` + queue.GetName() + `\.$`,
		},
		{
			name:        "request with no queue",
			request:     withParent(batchCreateRequest(queue.GetName(), futureTask("")), 0, ""),
			wantMessage: `^All tasks in a batch request must target the same queue\. Found task for queue  in batch request for queue ` + queue.GetName() + `\.$`,
		},
		{
			name:        "invalid task id",
			request:     batchCreateRequest(queue.GetName(), futureTask(""), futureTask(queue.GetName()+"/tasks/invalid.task")),
			wantMessage: `^Task ID "invalid\.task" can contain only letters`,
		},
		{
			name:        "duplicate task name",
			request:     batchCreateRequest(queue.GetName(), futureTask(queue.GetName()+"/tasks/dupe"), futureTask(queue.GetName()+"/tasks/dupe")),
			wantMessage: `^Duplicate task name in the batch request: ` + queue.GetName() + `/tasks/dupe$`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			op, err := client.BatchCreateTasks(context.Background(), tc.request)

			assert.Nil(t, op)
			assertIsGrpcError(t, tc.wantMessage, grpcCodes.InvalidArgument, err)
		})
	}
	assert.Empty(t, listTaskNames(t, client, queue.GetName()), "a rejected batch must create nothing")
}

func TestBatchCreateTasksRejectsRepeatedRequestID(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)

	request := batchCreateRequest(queue.GetName(), futureTask(""), futureTask(""))
	request.RequestId = "1b4e28ba-2fa1-41d2-883f-0016d3cca427"

	first, err := client.BatchCreateTasks(context.Background(), request)
	require.NoError(t, err)
	_, err = first.Wait(context.Background())
	require.NoError(t, err)
	assert.Equal(t, formattedParent+"/operations/"+request.RequestId, first.Name())

	_, err = client.BatchCreateTasks(context.Background(), request)
	assertIsGrpcError(t, "^Duplicate operation id in the batch request: "+request.RequestId+"$", grpcCodes.InvalidArgument, err)
	assert.Len(t, listTaskNames(t, client, queue.GetName()), 2)

	request.RequestId = "6f9619ff-8b86-4d01-b42d-00c04fc964ff"
	other, err := client.BatchCreateTasks(context.Background(), request)
	require.NoError(t, err)

	assert.NotEqual(t, first.Name(), other.Name())
	assert.Len(t, listTaskNames(t, client, queue.GetName()), 4)
}

func TestBatchCreateTasksRunsConcurrentRequestIDOnce(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)

	request := batchCreateRequest(queue.GetName(), futureTask(""), futureTask(""))
	request.RequestId = "9b2f6a4e-3c1d-4f8a-b5e7-2d6c8a1f0e93"

	const callers = 8
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			_, errs[i] = client.BatchCreateTasks(context.Background(), request)
		})
	}
	wg.Wait()

	succeeded := 0
	for _, err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		assertIsGrpcError(t, "^Duplicate operation id in the batch request", grpcCodes.InvalidArgument, err)
	}
	assert.Equal(t, 1, succeeded)
	assert.Len(t, listTaskNames(t, client, queue.GetName()), 2)
}

func TestBatchCreateTasksAcceptsRequestIDOfInvalidBatch(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)

	request := batchCreateRequest(queue.GetName(), futureTask(queue.GetName()+"/tasks/invalid.task"))
	request.RequestId = "4c8e2b7a-1f3d-4a6b-9e5c-7d2a8f1b3c64"

	_, err := client.BatchCreateTasks(context.Background(), request)
	assertIsGrpcError(t, "^Task ID", grpcCodes.InvalidArgument, err)

	request.Requests = batchCreateRequest(queue.GetName(), futureTask("")).GetRequests()
	op, err := client.BatchCreateTasks(context.Background(), request)
	require.NoError(t, err)
	resp, err := op.Wait(context.Background())
	require.NoError(t, err)
	assert.Len(t, resp.GetTasks(), 1)
}

func TestBatchCreateTasksKeepsRequestIDWhenEveryTaskFails(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)
	existingTask := queue.GetName() + "/tasks/existing-task"

	_, err := client.CreateTask(context.Background(), &taskspb.CreateTaskRequest{
		Parent: queue.GetName(),
		Task:   futureTask(existingTask),
	})
	require.NoError(t, err)

	request := batchCreateRequest(queue.GetName(), futureTask(existingTask))
	request.RequestId = "7e1c3a9b-5d2f-4b8e-a6c4-1f9d3e7b2a05"

	_, err = client.BatchCreateTasks(context.Background(), request)
	assertIsGrpcError(t, "^Requested entity already exists", grpcCodes.AlreadyExists, err)

	request.Requests = batchCreateRequest(queue.GetName(), futureTask("")).GetRequests()
	_, err = client.BatchCreateTasks(context.Background(), request)
	assertIsGrpcError(t, "^Duplicate operation id in the batch request: "+request.RequestId+"$", grpcCodes.InvalidArgument, err)
	assert.Len(t, listTaskNames(t, client, queue.GetName()), 1)
}

func TestGetBatchCreateTasksOperation(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)

	op, err := client.BatchCreateTasks(context.Background(), batchCreateRequest(queue.GetName(), futureTask("")))
	require.NoError(t, err)
	resp, err := op.Wait(context.Background())
	require.NoError(t, err)

	polled := client.BatchCreateTasksOperation(op.Name())
	polledResp, err := polled.Poll(context.Background())
	require.NoError(t, err)

	assert.True(t, polled.Done())
	assert.Equal(t, taskNames(resp.GetTasks()), taskNames(polledResp.GetTasks()))

	metadata, err := polled.Metadata()
	require.NoError(t, err)
	assert.Equal(t, taskspb.BatchCreateTasksMetadata_SUCCEEDED, metadata.GetState())
}

func TestGetOperationRejectsUnknownName(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	_, err := client.BatchCreateTasksOperation(formattedParent + "/operations/unknown").Poll(context.Background())

	assertIsGrpcError(t, "^Requested entity was not found", grpcCodes.NotFound, err)
}

// futureTask returns an HTTP task scheduled an hour ahead, so it is never
// dispatched (and removed) while a test inspects it. An empty name lets the
// emulator generate one.
func futureTask(name string) *taskspb.Task {
	return &taskspb.Task{
		Name:         name,
		ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
		MessageType: &taskspb.Task_HttpRequest{
			HttpRequest: &taskspb.HttpRequest{Url: "http://localhost/success"},
		},
	}
}

// noURLTask returns an HTTP task with no URL, which CreateTask rejects per task.
func noURLTask() *taskspb.Task {
	task := futureTask("")
	task.GetHttpRequest().Url = ""
	return task
}

func batchCreateRequest(queueName string, tasks ...*taskspb.Task) *taskspb.BatchCreateTasksRequest {
	requests := make([]*taskspb.CreateTaskRequest, len(tasks))
	for i, task := range tasks {
		requests[i] = &taskspb.CreateTaskRequest{Parent: queueName, Task: task}
	}
	return &taskspb.BatchCreateTasksRequest{Parent: queueName, Requests: requests}
}

func listTaskNames(t *testing.T, client *Client, queueName string) []string {
	t.Helper()

	var names []string
	it := client.ListTasks(context.Background(), &taskspb.ListTasksRequest{Parent: queueName})
	for {
		task, err := it.Next()
		if err == iterator.Done {
			return names
		}
		require.NoError(t, err)
		names = append(names, task.GetName())
	}
}

func taskNames(tasks []*taskspb.Task) []string {
	names := make([]string, len(tasks))
	for i, task := range tasks {
		names[i] = task.GetName()
	}
	return names
}
