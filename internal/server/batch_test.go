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
		futureTask(queue.GetName()+"/tasks/invalid.task"),
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
	assert.Regexp(t, `^Task ID "invalid.task"`, failed[2].GetMessage())
}

func TestBatchCreateTasksReportsFailedStateWhenNoTaskIsCreated(t *testing.T) {
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
		futureTask(queue.GetName()+"/tasks/invalid.task"),
	))
	require.NoError(t, err)

	resp, err := op.Wait(context.Background())
	require.NoError(t, err)
	assert.Empty(t, resp.GetTasks())

	metadata, err := op.Metadata()
	require.NoError(t, err)
	assert.Equal(t, taskspb.BatchCreateTasksMetadata_FAILED, metadata.GetState())
	assert.Len(t, metadata.GetFailedRequests(), 2)
}

func TestBatchCreateTasksRejectsMissingQueue(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queueName := formatQueueName(formattedParent, queueIDForTest(t))

	op, err := client.BatchCreateTasks(context.Background(), batchCreateRequest(queueName, futureTask("")))

	assert.Nil(t, op)
	assertIsGrpcError(t, "^Queue does not exist", grpcCodes.NotFound, err)
}

func TestBatchCreateTasksRejectsTooManyRequests(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)

	tasks := make([]*taskspb.Task, 101)
	for i := range tasks {
		tasks[i] = futureTask("")
	}

	op, err := client.BatchCreateTasks(context.Background(), batchCreateRequest(queue.GetName(), tasks...))

	assert.Nil(t, op)
	assertIsGrpcError(t, "less than or equal to 100", grpcCodes.InvalidArgument, err)
	assert.Empty(t, listTaskNames(t, client, queue.GetName()))
}

func TestBatchCreateTasksRejectsEmptyBatch(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)

	request := batchCreateRequest(queue.GetName())
	request.RequestId = "c9bf9e57-1685-4c89-bafb-ff5af830be8a"

	op, err := client.BatchCreateTasks(context.Background(), request)

	assert.Nil(t, op)
	assertIsGrpcError(t, "requests is required", grpcCodes.InvalidArgument, err)

	request.Requests = batchCreateRequest(queue.GetName(), futureTask("")).GetRequests()
	op, err = client.BatchCreateTasks(context.Background(), request)
	require.NoError(t, err)
	resp, err := op.Wait(context.Background())
	require.NoError(t, err)
	assert.Len(t, resp.GetTasks(), 1)
}

func TestBatchCreateTasksRejectsRequestForOtherQueue(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)

	request := batchCreateRequest(queue.GetName(), futureTask(""), futureTask(""))
	request.Requests[1].Parent = formatQueueName(formattedParent, "other-queue")

	op, err := client.BatchCreateTasks(context.Background(), request)

	assert.Nil(t, op)
	assertIsGrpcError(t, "must be the same as the queue name of the batch", grpcCodes.InvalidArgument, err)
	assert.Empty(t, listTaskNames(t, client, queue.GetName()))
}

func TestBatchCreateTasksDeduplicatesRequestID(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)

	request := batchCreateRequest(queue.GetName(), futureTask(""), futureTask(""))
	request.RequestId = "1b4e28ba-2fa1-41d2-883f-0016d3cca427"

	first, err := client.BatchCreateTasks(context.Background(), request)
	require.NoError(t, err)
	firstResp, err := first.Wait(context.Background())
	require.NoError(t, err)

	retried, err := client.BatchCreateTasks(context.Background(), request)
	require.NoError(t, err)
	retriedResp, err := retried.Wait(context.Background())
	require.NoError(t, err)

	assert.Equal(t, first.Name(), retried.Name())
	assert.Equal(t, taskNames(firstResp.GetTasks()), taskNames(retriedResp.GetTasks()))
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
	names := make([]string, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			op, err := client.BatchCreateTasks(context.Background(), request)
			errs[i] = err
			if err == nil {
				names[i] = op.Name()
			}
		})
	}
	wg.Wait()

	for i := range callers {
		require.NoError(t, errs[i])
		assert.Equal(t, names[0], names[i])
	}
	assert.Len(t, listTaskNames(t, client, queue.GetName()), 2)
}

func TestBatchCreateTasksRetriesRequestIDAfterFailure(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queueName := formatQueueName(formattedParent, queueIDForTest(t))

	request := batchCreateRequest(queueName, futureTask(""))
	request.RequestId = "4c8e2b7a-1f3d-4a6b-9e5c-7d2a8f1b3c64"

	_, err := client.BatchCreateTasks(context.Background(), request)
	assertIsGrpcError(t, "^Queue does not exist", grpcCodes.NotFound, err)

	createTestQueue(t, client)

	op, err := client.BatchCreateTasks(context.Background(), request)
	require.NoError(t, err)
	resp, err := op.Wait(context.Background())
	require.NoError(t, err)
	assert.Len(t, resp.GetTasks(), 1)
}

func TestBatchCreateTasksReturnsStoredOperationAfterQueueDeleted(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := createTestQueue(t, client)

	request := batchCreateRequest(queue.GetName(), futureTask(""))
	request.RequestId = "0f8fad5b-d9cb-469f-a165-70867728950e"

	first, err := client.BatchCreateTasks(context.Background(), request)
	require.NoError(t, err)
	firstResp, err := first.Wait(context.Background())
	require.NoError(t, err)

	require.NoError(t, client.DeleteQueue(context.Background(), &taskspb.DeleteQueueRequest{Name: queue.GetName()}))

	retried, err := client.BatchCreateTasks(context.Background(), request)
	require.NoError(t, err)
	retriedResp, err := retried.Wait(context.Background())
	require.NoError(t, err)

	assert.Equal(t, first.Name(), retried.Name())
	assert.Equal(t, taskNames(firstResp.GetTasks()), taskNames(retriedResp.GetTasks()))

	request.RequestId = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
	_, err = client.BatchCreateTasks(context.Background(), request)
	assertIsGrpcError(t, "^Queue does not exist", grpcCodes.NotFound, err)
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
