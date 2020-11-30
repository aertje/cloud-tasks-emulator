package main_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	. "cloud.google.com/go/cloudtasks/apiv2"
	. "github.com/PwC-Next/cloud-tasks-emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

var formattedParent = formatParent("TestProject", "TestLocation")

type serverRequestCallback = func(req *http.Request)

func TestMain(m *testing.M) {
	flag.Parse()

	os.Exit(m.Run())
}

func setUp(t *testing.T) (*grpc.Server, *Client) {
	serv := grpc.NewServer()
	taskspb.RegisterCloudTasksServer(serv, NewServer())

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		log.Fatal(err)
	}
	go serv.Serve(lis)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	clientOpt := option.WithGRPCConn(conn)

	client, err := NewClient(context.Background(), clientOpt)

	return serv, client
}

func tearDown(t *testing.T, serv *grpc.Server) {
	serv.Stop()
}

func TestCloudTasksCreateQueue(t *testing.T) {
	serv, client := setUp(t)
	defer tearDown(t, serv)
	queue := newQueue(formattedParent, "testCloudTasksCreateQueue")
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
	serv, client := setUp(t)
	defer tearDown(t, serv)

	testQueue := createTestQueue(t, client, "test")

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: testQueue.GetName(),
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
	assert.Contains(t, createdTask.GetName(), "projects/TestProject/locations/TestLocation/queues/test/tasks/")
	assert.Equal(t, "http://www.google.com", createdTask.GetHttpRequest().GetUrl())
	assert.Equal(t, taskspb.HttpMethod_POST, createdTask.GetHttpRequest().GetHttpMethod())
	assert.EqualValues(t, 0, createdTask.GetDispatchCount())
}

func TestCreateTaskRejectsDuplicateName(t *testing.T) {
	serv, client := setUp(t)
	defer tearDown(t, serv)

	testQueue := createTestQueue(t, client, "test")

	var receivedCount = 0
	srv := startTestServer(
		func(req *http.Request) { receivedCount++ },
		func(req *http.Request) {},
	)
	defer srv.Shutdown(context.Background())

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: testQueue.GetName(),
		Task: &taskspb.Task{
			Name: testQueue.GetName() + "/tasks/dedupe-this-task",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://localhost:5000/success",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	// First creation worked OK

	dupeTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	assert.Nil(t, dupeTask)
	if assert.Error(t, err, "Should return error") {
		rsp, ok := grpcStatus.FromError(err)
		assert.True(t, ok, "Should be grpc error")
		assert.Regexp(t, "^Requested entity already exists", rsp.Message())
		assert.Equal(t, grpcCodes.AlreadyExists, rsp.Code())
	}

	// Need to give it a chance to make the actual call
	time.Sleep(100 * time.Millisecond)
	// Validate that the call was actually made once
	assert.Equal(t, 1, receivedCount, "Request was received once only")

	// Check the task has been removed now (to ensure state is valid for the
	// recreate-even-after-executed-and-removed case following)
	getTaskRequest := taskspb.GetTaskRequest{
		Name: createdTask.GetName(),
	}
	gettedTask, err := client.GetTask(context.Background(), &getTaskRequest)
	assert.Error(t, err)
	assert.Nil(t, gettedTask)

	// Check still can't create even after removal
	_, err = client.CreateTask(context.Background(), &createTaskRequest)
	if assert.Error(t, err, "Should still give error after task executes") {
		rsp, _ := grpcStatus.FromError(err)
		assert.Equal(t, grpcCodes.AlreadyExists, rsp.Code())
	}
}

func TestCreateTaskRejectsInvalidName(t *testing.T) {
	serv, client := setUp(t)
	defer tearDown(t, serv)

	testQueue := createTestQueue(t, client, "test")

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: testQueue.GetName(),
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
	if assert.Error(t, err, "Should return error") {
		rsp, ok := grpcStatus.FromError(err)
		assert.True(t, ok, "Should be grpc error")
		assert.Regexp(t, "^Task name must be formatted", rsp.Message())
		assert.Equal(t, grpcCodes.InvalidArgument, rsp.Code())
	}
}

func TestSuccessTaskExecution(t *testing.T) {
	serv, client := setUp(t)
	defer tearDown(t, serv)

	var receivedRequest *http.Request
	srv := startTestServer(
		func(req *http.Request) { receivedRequest = req },
		func(req *http.Request) {},
	)

	testQueue := createTestQueue(t, client, "test")

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: testQueue.GetName(),
		Task: &taskspb.Task{
			Name: testQueue.GetName() + "/tasks/my-test-task",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://localhost:5000/success",
				},
			},
		},
	}
	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	getTaskRequest := taskspb.GetTaskRequest{
		Name: createdTask.GetName(),
	}

	// Need to give it a chance to make the actual call
	time.Sleep(100 * time.Millisecond)
	gettedTask, err := client.GetTask(context.Background(), &getTaskRequest)
	assert.Error(t, err)
	assert.Nil(t, gettedTask)

	// Validate that the call was actually made properly
	assert.NotNil(t, receivedRequest, "Request was received")

	// Simple predictable headers
	expectHeaders := map[string]string{
		"X-CloudTasks-TaskExecutionCount": "0",
		"X-CloudTasks-TaskRetryCount":     "0",
		"X-CloudTasks-TaskName":           "my-test-task",
		"X-CloudTasks-QueueName":          "test",
	}
	actualHeaders := make(map[string]string)
	for hdr := range expectHeaders {
		actualHeaders[hdr] = receivedRequest.Header.Get(hdr)
	}

	assert.Equal(t, expectHeaders, actualHeaders)
	assertIsRecentTimestamp(t, receivedRequest.Header.Get("X-CloudTasks-TaskEta"))

	srv.Shutdown(context.Background())
}

func TestErrorTaskExecution(t *testing.T) {
	serv, client := setUp(t)
	defer tearDown(t, serv)

	called := 0
	srv := startTestServer(
		func(req *http.Request) {},
		func(req *http.Request) { called++ },
	)

	testQueue := createTestQueue(t, client, "test")

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: testQueue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://localhost:5000/not_found",
				},
			},
		},
	}
	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	getTaskRequest := taskspb.GetTaskRequest{
		Name: createdTask.GetName(),
	}

	time.Sleep(time.Second)
	gettedTask, err := client.GetTask(context.Background(), &getTaskRequest)
	require.NoError(t, err)

	// at t=0, 0.1, 0.3 (+0.2), 0.7 (+0.4) seconds (plus some buffer) ==> 4 calls
	assert.EqualValues(t, 4, gettedTask.GetDispatchCount())
	assert.Equal(t, 4, called)

	srv.Shutdown(context.Background())
}

func createTestQueue(t *testing.T, client *Client, qName string) *taskspb.Queue {
	queue := newQueue(formattedParent, "test")
	createQueueRequest := taskspb.CreateQueueRequest{
		Parent: formattedParent,
		Queue:  queue,
	}

	createdQueue, err := client.CreateQueue(context.Background(), &createQueueRequest)
	require.NoError(t, err)

	return createdQueue
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

func assertIsRecentTimestamp(t *testing.T, etaString string) {
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

func startTestServer(successCallback serverRequestCallback, notFoundCallback serverRequestCallback) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/success", func(w http.ResponseWriter, r *http.Request) {
		successCallback(r)
		w.WriteHeader(200)
	})
	mux.HandleFunc("/not_found", func(w http.ResponseWriter, r *http.Request) {
		notFoundCallback(r)
		w.WriteHeader(404)
	})

	srv := &http.Server{Addr: "localhost:5000", Handler: mux}

	go srv.ListenAndServe()

	return srv
}
