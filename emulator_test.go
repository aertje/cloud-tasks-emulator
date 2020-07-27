package main_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	. "cloud.google.com/go/cloudtasks/apiv2"
	. "github.com/PwC-Next/cloud-tasks-emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/grpc"
)

var formattedParent = formatParent("TestProject", "TestLocation")

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

	queue := newQueue(formattedParent, "test")
	createQueueRequest := taskspb.CreateQueueRequest{
		Parent: formattedParent,
		Queue:  queue,
	}

	createdQueue, err := client.CreateQueue(context.Background(), &createQueueRequest)
	require.NoError(t, err)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			PayloadType: &taskspb.Task_HttpRequest{
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

func TestSuccessTaskExecution(t *testing.T) {
	serv, client := setUp(t)
	defer tearDown(t, serv)

	called := false
	srv := startTestServer(func() { called = true }, func() {})

	queue := newQueue(formattedParent, "test")
	createQueueRequest := taskspb.CreateQueueRequest{
		Parent: formattedParent,
		Queue:  queue,
	}

	createdQueue, err := client.CreateQueue(context.Background(), &createQueueRequest)
	require.NoError(t, err)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			PayloadType: &taskspb.Task_HttpRequest{
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
	assert.Equal(t, true, called)

	srv.Shutdown(context.Background())
}

func TestErrorTaskExecution(t *testing.T) {
	serv, client := setUp(t)
	defer tearDown(t, serv)

	called := 0
	srv := startTestServer(func() {}, func() { called++ })

	queue := newQueue(formattedParent, "test")

	createQueueRequest := taskspb.CreateQueueRequest{
		Parent: formattedParent,
		Queue:  queue,
	}

	createdQueue, err := client.CreateQueue(context.Background(), &createQueueRequest)
	require.NoError(t, err)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			PayloadType: &taskspb.Task_HttpRequest{
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

func newQueue(formattedParent, name string) *taskspb.Queue {
	return &taskspb.Queue{Name: formatQueueName(formattedParent, name)}
}

func formatQueueName(formattedParent, name string) string {
	return fmt.Sprintf("%s/queues/%s", formattedParent, name)
}

func formatParent(project, location string) string {
	return fmt.Sprintf("projects/%s/locations/%s", project, location)
}

func startTestServer(successCallback func(), notFoundCallback func()) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/success", func(w http.ResponseWriter, r *http.Request) {
		successCallback()
		w.WriteHeader(200)
	})
	mux.HandleFunc("/not_found", func(w http.ResponseWriter, r *http.Request) {
		notFoundCallback()
		w.WriteHeader(404)
	})

	srv := &http.Server{Addr: "localhost:5000", Handler: mux}

	go srv.ListenAndServe()

	return srv
}
