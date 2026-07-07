package server_test

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	. "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/aertje/cloud-tasks-emulator/v2/internal/oidc"
	. "github.com/aertje/cloud-tasks-emulator/v2/internal/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	errdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var formattedParent = formatParent("TestProject", "TestLocation")

// setUp starts an in-process emulator on an ephemeral port and returns a client
// connected to it. Teardown (gRPC server stop, engine goroutine cancellation and
// client close) is registered with t.Cleanup so every test tears down
// deterministically and leaves no queue/task goroutines running into the next.
func setUp(t *testing.T, options ServerOptions) (*Server, *Client) {
	t.Helper()

	serv := grpc.NewServer()
	emulatorServer := NewServer(options)
	taskspb.RegisterCloudTasksServer(serv, emulatorServer)

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	go serv.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client, err := NewClient(context.Background(), option.WithGRPCConn(conn))
	require.NoError(t, err)

	t.Cleanup(func() {
		// Stop accepting RPCs first, then cancel the engine's queue/task
		// goroutines so none of them dispatch into a later test's target.
		serv.Stop()
		emulatorServer.Stop()
		client.Close()
	})

	return emulatorServer, client
}

func TestCloudTasksCreateQueue(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})
	queue := newQueue(formattedParent, t.Name())
	request := taskspb.CreateQueueRequest{
		Parent: formattedParent,
		Queue:  queue,
	}

	resp, err := client.CreateQueue(context.Background(), &request)
	require.NoError(t, err)
	assert.Equal(t, request.GetQueue().Name, resp.Name)
	assert.Equal(t, taskspb.Queue_RUNNING, resp.State)
}

func TestCreateTask(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://www.google.com",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)
	assert.NotEmpty(t, createdTask.GetName())
	assert.Contains(t, createdTask.GetName(), createdQueue.GetName()+"/tasks/")
	assert.Equal(t, "http://www.google.com", createdTask.GetHttpRequest().GetUrl())
	assert.Equal(t, taskspb.HttpMethod_POST, createdTask.GetHttpRequest().GetHttpMethod())
	assert.EqualValues(t, 0, createdTask.GetDispatchCount())
}

func TestCreateTaskRejectsDuplicateName(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueue(t, client)

	target := startTestServer(t)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: createdQueue.GetName() + "/tasks/dedupe-this-task",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: target.URL + "/success",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	// First creation worked OK

	dupeTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	assert.Nil(t, dupeTask)
	assertIsGrpcError(t, "^Requested entity already exists", grpcCodes.AlreadyExists, err)

	// Wait for it to perform the http request
	_, err = awaitHttpRequest(target.receivedRequests)
	require.NoError(t, err)

	// Check the task has been removed now (to ensure state is valid for the
	// recreate-even-after-executed-and-removed case following)
	requireTaskEventuallyGone(t, client, createdTask.GetName())

	// Check still can't create even after removal
	_, err = client.CreateTask(context.Background(), &createTaskRequest)
	assertIsGrpcError(t, "^Requested entity already exists", grpcCodes.AlreadyExists, err)

	// Verify that it only sent the original HTTP request, nothing after that
	_, err = awaitHttpRequestWithTimeout(target.receivedRequests, 1*time.Second)
	assert.Error(t, err, "Should not receive any further HTTP requests within timeout")
}

func TestCreateTaskRejectsInvalidName(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: "is-this-a-name",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://www.google.com",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	assert.Nil(t, createdTask)
	assertIsGrpcError(t, "^Task name must be formatted", grpcCodes.InvalidArgument, err)
}

func TestCreateTaskRejectsInvalidTaskID(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			// Structurally a valid task name, but the ID contains spaces - a
			// different failure from a wholly malformed name (see above).
			Name: createdQueue.GetName() + "/tasks/not a valid id",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://www.google.com",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	assert.Nil(t, createdTask)
	assertIsGrpcError(t, `^Task ID "not a valid id" can contain only`, grpcCodes.InvalidArgument, err)

	// Real Cloud Tasks attaches a Help detail pointing at the task-name field.
	rsp, _ := grpcStatus.FromError(err)
	require.Len(t, rsp.Details(), 1)
	help, ok := rsp.Details()[0].(*errdetails.Help)
	require.True(t, ok, "detail should be a Help")
	require.Len(t, help.GetLinks(), 1)
	assert.Equal(t, "Definition of task ID", help.GetLinks()[0].GetDescription())
}

func TestCreateTaskRejectsNameForOtherQueue(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: "projects/TestProject/locations/TestLocation/queues/SomeOtherQueue/tasks/valid-name",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://www.google.com",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	assert.Nil(t, createdTask)
	assertIsGrpcError(t, "^The queue name from request", grpcCodes.InvalidArgument, err)
}

func TestCreateTaskRejectsMissingURL(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{Url: ""},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	assert.Nil(t, createdTask)
	assertIsGrpcError(t, "^HttpRequest.url is required.", grpcCodes.InvalidArgument, err)
}

func TestCreateTaskRejectsNonHTTPURL(t *testing.T) {
	t.Parallel()

	// Cloud Tasks rejects any URL that does not start with http:// or https://,
	// whether the scheme is missing entirely or simply not http(s).
	for name, url := range map[string]string{
		"no scheme":       "www.google.com",
		"non-http scheme": "ftp://www.google.com",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, client := setUp(t, ServerOptions{})

			createdQueue := createTestQueue(t, client)

			createTaskRequest := taskspb.CreateTaskRequest{
				Parent: createdQueue.GetName(),
				Task: &taskspb.Task{
					MessageType: &taskspb.Task_HttpRequest{
						HttpRequest: &taskspb.HttpRequest{Url: url},
					},
				},
			}

			createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)

			assert.Nil(t, createdTask)
			assertIsGrpcError(t, "^HttpTarget.url must start with", grpcCodes.InvalidArgument, err)
		})
	}
}

// TestCreateTaskAcceptsUnparseableURL locks in that create-time validation is
// shallow: a URL with the right scheme is accepted even if it is not fully
// parseable (real Cloud Tasks behaves the same - the task only fails at
// dispatch). See conformance case task/create/invalid-url-bad-escape.
func TestCreateTaskAcceptsUnparseableURL(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			// Schedule far out so it never actually dispatches; the point is that
			// create accepts the malformed URL.
			ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{Url: "http://example.com/%zz"},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	require.NoError(t, err)
	assert.NotNil(t, createdTask)
}

func TestDeleteTaskTombstonesName(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueue(t, client)

	// Schedule the task well into the future so it never dispatches; the test is
	// purely about delete semantics, not execution.
	scheduleTime := timestamppb.New(time.Now().Add(time.Hour))

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name:         createdQueue.GetName() + "/tasks/to-be-deleted",
			ScheduleTime: scheduleTime,
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://localhost/success",
				},
			},
		},
	}
	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	err = client.DeleteTask(context.Background(), &taskspb.DeleteTaskRequest{Name: createdTask.GetName()})
	require.NoError(t, err)

	// A GetTask immediately after the delete must observe the tombstone, not the
	// task, and re-creating the name must report it as still reserved.
	assertGetTaskFails(t, grpcCodes.NotFound, client, createdTask.GetName())

	_, err = client.CreateTask(context.Background(), &createTaskRequest)
	assertIsGrpcError(t, "^Requested entity already exists", grpcCodes.AlreadyExists, err)
}

func TestPausedThenResumedQueueStillDispatches(t *testing.T) {
	// Regression test for the worker-cancellation rewrite: a queue that is
	// paused and then resumed must still dispatch tasks. The previous design
	// left a stale cancellation token buffered, so the first resumed worker
	// re-killed the whole pool and the queue never dispatched again.
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	target := startTestServer(t)

	createdQueue := createTestQueue(t, client)

	_, err := client.PauseQueue(context.Background(), &taskspb.PauseQueueRequest{Name: createdQueue.GetName()})
	require.NoError(t, err)

	_, err = client.ResumeQueue(context.Background(), &taskspb.ResumeQueueRequest{Name: createdQueue.GetName()})
	require.NoError(t, err)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: target.URL + "/success",
				},
			},
		},
	}
	_, err = client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	_, err = awaitHttpRequest(target.receivedRequests)
	require.NoError(t, err, "task should dispatch after the queue is paused and resumed")
}

func TestGetQueueExists(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueue(t, client)

	getQueueRequest := taskspb.GetQueueRequest{
		Name: createdQueue.GetName(),
	}

	gettedQueue, err := client.GetQueue(context.Background(), &getQueueRequest)

	assert.NoError(t, err)
	assert.Equal(t, createdQueue.GetName(), gettedQueue.GetName())
}

func TestGetQueueNeverExisted(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	getQueueRequest := taskspb.GetQueueRequest{
		Name: "hello_q",
	}

	gettedQueue, err := client.GetQueue(context.Background(), &getQueueRequest)

	assert.Nil(t, gettedQueue)
	st, _ := grpcStatus.FromError(err)
	assert.Equal(t, grpcCodes.NotFound, st.Code())
}

func TestGetQueuePreviouslyExisted(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueue(t, client)

	deleteQueueRequest := taskspb.DeleteQueueRequest{
		Name: createdQueue.GetName(),
	}

	err := client.DeleteQueue(context.Background(), &deleteQueueRequest)

	assert.NoError(t, err)

	getQueueRequest := taskspb.GetQueueRequest{
		Name: createdQueue.GetName(),
	}

	gettedQueue, err := client.GetQueue(context.Background(), &getQueueRequest)

	assert.Nil(t, gettedQueue)
	st, _ := grpcStatus.FromError(err)
	assert.Equal(t, grpcCodes.NotFound, st.Code())
}

func TestPurgeQueueDoesNotReleaseTaskNamesByDefault(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueueWithSlowRetry(t, client)

	target := startTestServer(t)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: createdQueue.GetName() + "/tasks/any-task",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					// Use the not_found handler to prove that purge stops any further retries
					Url: target.URL + "/not_found",
				},
			},
		},
	}
	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	// Task was created OK, verify that the first HTTP request was sent
	_, err = awaitHttpRequest(target.receivedRequests)
	require.NoError(t, err)

	// Now purge the queue
	purgeQueueRequest := taskspb.PurgeQueueRequest{
		Name: createdQueue.GetName(),
	}
	_, err = client.PurgeQueue(context.Background(), &purgeQueueRequest)
	require.NoError(t, err)

	// Soft purge is asynchronous; poll until nothing is in the list and the task
	// cannot be retrieved by name.
	requireTaskListEventuallyEmpty(t, client, createdQueue)
	assertGetTaskFails(t, grpcCodes.NotFound, client, createdTask.GetName())

	// BUT - Verify that the task name is still not available for new tasks
	_, err = client.CreateTask(context.Background(), &createTaskRequest)
	assertIsGrpcError(t, "^Requested entity already exists", grpcCodes.AlreadyExists, err)

	// Verify that it only sent the original HTTP request, it purged before the retries
	_, err = awaitHttpRequestWithTimeout(target.receivedRequests, 1*time.Second)
	assert.Error(t, err, "Should not receive any further HTTP requests within timeout")
}

func TestPurgeQueueOptionallyPerformsHardReset(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{HardResetOnPurgeQueue: true})

	createdQueue := createTestQueueWithSlowRetry(t, client)

	target := startTestServer(t)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: createdQueue.GetName() + "/tasks/any-task",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					// Use the not_found handler to prove that purge stops any further retries
					Url: target.URL + "/not_found",
				},
			},
		},
	}
	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	// Task was created OK, verify that the first HTTP request was sent
	_, err = awaitHttpRequest(target.receivedRequests)
	require.NoError(t, err)

	// Now purge the queue
	purgeQueueRequest := taskspb.PurgeQueueRequest{
		Name: createdQueue.GetName(),
	}
	_, err = client.PurgeQueue(context.Background(), &purgeQueueRequest)
	require.NoError(t, err)

	// In this mode, purging the queue is synchronous so we should be in the empty state straight away
	assertTaskListIsEmpty(t, client, createdQueue)
	assertGetTaskFails(t, grpcCodes.NotFound, client, createdTask.GetName())

	// And verify that we can now create the task with that name again and it will fire again
	_, err = client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	// Verify that it has now sent the request from the new task
	receivedRequest, err := awaitHttpRequest(target.receivedRequests)
	require.NotNil(t, receivedRequest, "Request was received")
	require.NoError(t, err)
	// Note that the execution count is reset to 0
	assertHeadersMatch(
		t,
		map[string]string{
			"X-CloudTasks-TaskExecutionCount": "0",
			"X-CloudTasks-TaskRetryCount":     "0",
		},
		receivedRequest,
	)
}

func TestListTasks(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	createdQueue := createTestQueue(t, client)

	// Schedule the task in the future so it does not dispatch (and get removed)
	// before ListTasks observes it - otherwise the list races the dispatch.
	scheduleTime := timestamppb.New(time.Now().Add(time.Hour))

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name:         createdQueue.GetName() + "/tasks/my-test-task",
			ScheduleTime: scheduleTime,
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://localhost/success",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	listTasksRequest := taskspb.ListTasksRequest{
		Parent: createdQueue.GetName(),
	}

	tasksIterator := client.ListTasks(context.Background(), &listTasksRequest)

	listedTask, err := tasksIterator.Next()
	assert.NoError(t, err)
	assert.Equal(t, createdTask.GetName(), listedTask.GetName())
	_, err = tasksIterator.Next()
	assert.EqualError(t, err, "no more items in iterator")

	deleteQueueRequest := taskspb.DeleteQueueRequest{
		Name: createdQueue.GetName(),
	}
	err = client.DeleteQueue(context.Background(), &deleteQueueRequest)
	require.NoError(t, err)

	tasksIterator = client.ListTasks(context.Background(), &listTasksRequest)

	listedTask, err = tasksIterator.Next()
	assertIsGrpcError(t, "^Queue does not exist", grpcCodes.NotFound, err)
	assert.Nil(t, listedTask)
}

func TestSuccessTaskExecution(t *testing.T) {
	t.Parallel()
	_, client := setUp(t, ServerOptions{})

	target := startTestServer(t)

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: createdQueue.GetName() + "/tasks/my-test-task",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: target.URL + "/success",
				},
			},
		},
	}
	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	receivedRequest, err := awaitHttpRequest(target.receivedRequests)
	require.NoError(t, err)

	// A successful dispatch removes the task; poll until GetTask reports it gone.
	requireTaskEventuallyGone(t, client, createdTask.GetName())

	// Validate that the call was actually made properly
	require.NotNil(t, receivedRequest, "Request was received")

	// Simple predictable headers
	assertHeadersMatch(
		t,
		map[string]string{
			"X-CloudTasks-TaskExecutionCount": "0",
			"X-CloudTasks-TaskRetryCount":     "0",
			"X-CloudTasks-TaskName":           "my-test-task",
			"X-CloudTasks-QueueName":          t.Name(),
		},
		receivedRequest,
	)
	assertIsRecentTimestamp(t, receivedRequest.Header.Get("X-CloudTasks-TaskETA"))
}

func TestSuccessAppEngineTaskExecution(t *testing.T) {
	// Not parallel: it sets the process-wide APP_ENGINE_EMULATOR_HOST env var.
	_, client := setUp(t, ServerOptions{})

	target := startTestServer(t)
	t.Setenv("APP_ENGINE_EMULATOR_HOST", target.URL)

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: createdQueue.GetName() + "/tasks/my-test-task",
			MessageType: &taskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
					RelativeUri: "/success",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)
	assert.NotNil(t, createdTask)

	// Wait for it to perform the http request
	receivedRequest, err := awaitHttpRequest(target.receivedRequests)
	require.NoError(t, err)

	require.NotNil(t, receivedRequest, "Request was received")
	assertHeadersMatch(
		t,
		map[string]string{
			"X-AppEngine-TaskExecutionCount": "0",
			"X-AppEngine-TaskRetryCount":     "0",
			"X-AppEngine-TaskName":           "my-test-task",
			"X-AppEngine-QueueName":          t.Name(),
		},
		receivedRequest,
	)

	assertIsRecentTimestamp(t, receivedRequest.Header.Get("X-AppEngine-TaskETA"))
}

func TestErrorTaskExecution(t *testing.T) {
	// Not parallel: it asserts on wall-clock retry timing, which is sensitive to
	// scheduler contention from other concurrently-running tests.
	_, client := setUp(t, ServerOptions{})

	target := startTestServer(t)

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: target.URL + "/not_found",
				},
			},
		},
	}

	start := time.Now()

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	// With the default retry backoff, we expect 4 calls within the first second:
	// at t=0, 0.1, 0.3 (+0.2), 0.7 (+0.4) seconds (plus some buffer) ==> 4 calls
	for attempt := range 4 {
		receivedRequest, err := awaitHttpRequest(target.receivedRequests)
		require.NoErrorf(t, err, "Should have received request %d", attempt+1)
		assertHeadersMatch(
			t,
			map[string]string{
				"X-CloudTasks-TaskExecutionCount": strconv.Itoa(attempt),
				"X-CloudTasks-TaskRetryCount":     strconv.Itoa(attempt),
			},
			receivedRequest,
		)
	}

	expectedCompleteBy := start.Add(700 * time.Millisecond)
	assert.WithinDuration(
		t,
		expectedCompleteBy,
		time.Now(),
		300*time.Millisecond,
		"4 retries should take roughly 0.7 seconds",
	)

	// Check the state of the task has been updated with the number of dispatches
	requireTaskDispatchCountEventually(t, client, createdTask.GetName(), 4)
}

func TestOIDCAuthenticatedTaskExecution(t *testing.T) {
	t.Parallel()
	oidcConfig := oidc.DefaultConfig()
	oidcConfig.IssuerURL = "http://localhost:8980"
	_, client := setUp(t, ServerOptions{OIDC: oidcConfig})

	target := startTestServer(t)

	createdQueue := createTestQueue(t, client)

	targetURL := target.URL + "/success?foo=bar"

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: targetURL,
					AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
						OidcToken: &taskspb.OidcToken{
							ServiceAccountEmail: "emulator@service.test",
						},
					},
				},
			},
		},
	}
	_, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	// Wait for it to perform the http request
	receivedRequest, err := awaitHttpRequest(target.receivedRequests)
	require.NoError(t, err)

	// Validate that the call was actually made properly
	require.NotNil(t, receivedRequest, "Request was received")
	authHeader := receivedRequest.Header.Get("Authorization")
	assert.NotNil(t, authHeader, "Has Authorization header")
	assert.Regexp(t, "^Bearer [a-zA-Z0-9_-]+\\.[a-zA-Z0-9_-]+\\.[a-zA-Z0-9_-]+$", authHeader)
	tokenStr := strings.Replace(authHeader, "Bearer ", "", 1)

	// Full token validation is done in the docker smoketests and the oidc internal tests
	token, _, err := new(jwt.Parser).ParseUnverified(tokenStr, &oidc.Claims{})
	require.NoError(t, err)

	claims := token.Claims.(*oidc.Claims)
	assert.Equal(t, jwt.ClaimStrings{targetURL}, claims.Audience, "Specifies audience")
	assert.Equal(t, "emulator@service.test", claims.Email, "Specifies email")
	assert.Equal(t, "http://localhost:8980", claims.Issuer, "Specifies issuer")
}

func newQueue(formattedParent, name string) *taskspb.Queue {
	return &taskspb.Queue{Name: formatQueueName(formattedParent, name)}
}

func formatQueueName(formattedParent, name string) string {
	return fmt.Sprintf("%s/queues/%s", formattedParent, name)
}

func formatParent(project, location string) string {
	return fmt.Sprintf("projects/%s/locations/%s", project, location)
}

func assertHeadersMatch(t *testing.T, expectHeaders map[string]string, request *http.Request) {
	t.Helper()
	actualHeaders := make(map[string]string)

	for hdr := range expectHeaders {
		actualHeaders[hdr] = request.Header.Get(hdr)
	}

	assert.Equal(t, expectHeaders, actualHeaders)
}

func assertIsRecentTimestamp(t *testing.T, etaString string) {
	t.Helper()
	assert.Regexp(t, "^[0-9]+\\.[0-9]+$", etaString)
	float, err := strconv.ParseFloat(etaString, 64)
	require.NoError(t, err)
	seconds, fraction := math.Modf(float)
	etaTime := time.Unix(int64(seconds), int64(fraction*1e9))

	assert.WithinDuration(
		t,
		time.Now(),
		etaTime,
		2*time.Second,
		"task eta should be within last few seconds",
	)
}

func assertIsGrpcError(t *testing.T, expectMessageRegexp string, expectCode grpcCodes.Code, err error) {
	t.Helper()
	require.Error(t, err, "Should return error")
	rsp, ok := grpcStatus.FromError(err)
	require.True(t, ok, "Should be grpc error")
	assert.Regexp(t, expectMessageRegexp, rsp.Message())
	assert.Equal(t, expectCode, rsp.Code(), "Expected code %s, got %s", expectCode.String(), rsp.Code().String())
}

func assertTaskListIsEmpty(t *testing.T, client *Client, queue *taskspb.Queue) {
	t.Helper()
	listTasksRequest := taskspb.ListTasksRequest{
		Parent: queue.GetName(),
	}
	tasksIterator := client.ListTasks(context.Background(), &listTasksRequest)
	firstTask, err := tasksIterator.Next()
	assert.Nil(t, firstTask, "Should not get a task in the tasks list")
	assert.Same(t, iterator.Done, err, "task iterator should be done")
}

// requireTaskListEventuallyEmpty polls the task list until it is empty, for the
// asynchronous (soft) purge path.
func requireTaskListEventuallyEmpty(t *testing.T, client *Client, queue *taskspb.Queue) {
	t.Helper()
	require.Eventually(t, func() bool {
		tasksIterator := client.ListTasks(context.Background(), &taskspb.ListTasksRequest{Parent: queue.GetName()})
		_, err := tasksIterator.Next()
		return err == iterator.Done
	}, 2*time.Second, 10*time.Millisecond, "task list should become empty")
}

func assertGetTaskFails(t *testing.T, expectCode grpcCodes.Code, client *Client, name string) {
	t.Helper()
	getTaskRequest := taskspb.GetTaskRequest{
		Name: name,
	}
	gettedTask, err := client.GetTask(context.Background(), &getTaskRequest)
	if assert.Error(t, err) {
		rsp, ok := grpcStatus.FromError(err)
		assert.True(t, ok, "Should be grpc error")
		assert.Equal(t, expectCode, rsp.Code())
	}
	assert.Nil(t, gettedTask)
}

// requireTaskEventuallyGone polls until GetTask reports the task as NotFound,
// which happens once the emulator has processed the target's response and
// removed the task. This replaces sleeping for a fixed grace period.
func requireTaskEventuallyGone(t *testing.T, client *Client, name string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := client.GetTask(context.Background(), &taskspb.GetTaskRequest{Name: name})
		return grpcStatus.Code(err) == grpcCodes.NotFound
	}, 2*time.Second, 10*time.Millisecond, "task %s should be removed after dispatch", name)
}

// requireTaskDispatchCountEventually polls until the task reports at least the
// expected number of dispatches, avoiding a race with the emulator's async
// post-response bookkeeping.
func requireTaskDispatchCountEventually(t *testing.T, client *Client, name string, want int32) {
	t.Helper()
	require.Eventually(t, func() bool {
		task, err := client.GetTask(context.Background(), &taskspb.GetTaskRequest{Name: name})
		return err == nil && task.GetDispatchCount() >= want
	}, 2*time.Second, 10*time.Millisecond, "task %s should reach dispatch count %d", name, want)
}

func createTestQueue(t *testing.T, client *Client) *taskspb.Queue {
	t.Helper()
	// The queue name is derived from the test name so every test gets a unique
	// queue and tests can run in parallel without colliding.
	queue := newQueue(formattedParent, t.Name())

	createQueueRequest := taskspb.CreateQueueRequest{
		Parent: formattedParent,
		Queue:  queue,
	}

	createdQueue, err := client.CreateQueue(context.Background(), &createQueueRequest)
	require.NoError(t, err)

	return createdQueue
}

// createTestQueueWithSlowRetry creates a queue whose retry backoff is an hour,
// so a failed task's retry is scheduled far beyond the test window. This lets
// the purge tests deterministically prove that purging cancels the pending
// retry, without racing the default 100ms backoff.
func createTestQueueWithSlowRetry(t *testing.T, client *Client) *taskspb.Queue {
	t.Helper()
	queue := newQueue(formattedParent, t.Name())
	queue.RetryConfig = &taskspb.RetryConfig{
		MinBackoff: durationpb.New(time.Hour),
		MaxBackoff: durationpb.New(time.Hour),
	}

	createdQueue, err := client.CreateQueue(context.Background(), &taskspb.CreateQueueRequest{
		Parent: formattedParent,
		Queue:  queue,
	})
	require.NoError(t, err)

	return createdQueue
}

func awaitHttpRequest(receivedRequests <-chan *http.Request) (*http.Request, error) {
	return awaitHttpRequestWithTimeout(receivedRequests, 1*time.Second)
}

func awaitHttpRequestWithTimeout(receivedRequests <-chan *http.Request, timeout time.Duration) (*http.Request, error) {
	select {
	case request := <-receivedRequests:
		return request, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timed out waiting for HTTP request after %s", timeout)
	}
}

// testTarget is an ephemeral HTTP server used as a task dispatch target.
type testTarget struct {
	URL              string
	receivedRequests <-chan *http.Request
}

// startTestServer starts an HTTP target on an ephemeral port and blocks until it
// is accepting connections. It is torn down via t.Cleanup. The handlers publish
// the received request on the channel only after writing the response status, so
// observing a request implies the response has been written.
func startTestServer(t *testing.T) *testTarget {
	t.Helper()

	requestChannel := make(chan *http.Request, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/success", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		requestChannel <- r
	})
	mux.HandleFunc("/not_found", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		requestChannel <- r
	})

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	srv := &http.Server{Handler: mux}
	go srv.Serve(lis)

	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	return &testTarget{
		URL:              "http://" + lis.Addr().String(),
		receivedRequests: requestChannel,
	}
}
