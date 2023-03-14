package emulator_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/aertje/cloud-tasks-emulator/pkg/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

type FakeOIDCTokenGenerator struct {
}

func (g FakeOIDCTokenGenerator) CreateOIDCToken(email, subject, audience string) (token string, err error) {
	return "", nil
}

type FakeHTTPDoer struct {
}

func (d FakeHTTPDoer) Do(req *http.Request) (resp *http.Response, err error) {
	return &http.Response{
		Body: io.NopCloser(nil),
	}, nil
}

var defaultQueue = &cloudtaskspb.Queue{
	Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
	RateLimits: &cloudtaskspb.RateLimits{
		MaxDispatchesPerSecond:  500,
		MaxBurstSize:            100,
		MaxConcurrentDispatches: 1000,
	},
	RetryConfig: &cloudtaskspb.RetryConfig{
		MaxAttempts: 100,
		MinBackoff: &durationpb.Duration{
			Nanos: 100000000,
		},
		MaxBackoff: &durationpb.Duration{
			Seconds: 3600,
		},
		MaxDoublings: 16,
	},
	State: cloudtaskspb.Queue_RUNNING,
}

func TestListQueues(t *testing.T) {
	server := createServer()

	_, err := server.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/my-project/locations/australia-southeast1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
		},
	})
	require.NoError(t, err)

	response, err := server.ListQueues(context.Background(), &cloudtaskspb.ListQueuesRequest{})
	assert.NoError(t, err)

	expected := &cloudtaskspb.ListQueuesResponse{
		Queues: []*cloudtaskspb.Queue{
			defaultQueue,
		},
	}
	assert.Equal(t, expected.String(), response.String())
}

func TestGetQueue(t *testing.T) {
	server := createServer()

	_, err := server.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/my-project/locations/australia-southeast1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
		},
	})
	require.NoError(t, err)

	response, err := server.GetQueue(context.Background(), &cloudtaskspb.GetQueueRequest{
		Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
	})

	assert.NoError(t, err)
	assert.Equal(t, defaultQueue.String(), response.String())
}

func TestGetQueueNotFound(t *testing.T) {
	server := createServer()

	_, err := server.GetQueue(context.Background(), &cloudtaskspb.GetQueueRequest{
		Name: "projects/some-project/locations/us-west1/queues/not-a-queue",
	})

	assert.Equal(
		t,
		"rpc error: code = NotFound desc = Queue does not exist. If you just created the queue, wait at least a minute for the queue to initialize.",
		err.Error(),
	)
}

func TestCreateQueue(t *testing.T) {
	server := createServer()

	response, err := server.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/my-project/locations/australia-southeast1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
		},
	})

	assert.NoError(t, err)
	assert.Equal(t, defaultQueue.String(), response.String())
}

func TestCreateQueueAlreadyExists(t *testing.T) {
	server := createServer()

	_, err := server.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/my-project/locations/australia-southeast1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
		},
	})
	require.NoError(t, err)

	_, err = server.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/my-project/locations/australia-southeast1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
		},
	})
	assert.Error(t, err)
	assert.Equal(t, "rpc error: code = AlreadyExists desc = Queue already exists", err.Error())
}

func TestCreateQueueFormatError(t *testing.T) {
	server := createServer()

	for _, tt := range []struct {
		Name          string
		Parent        string
		QueueName     string
		ExpectedError string
	}{
		{
			Name:          "InvalidParent",
			Parent:        "not/right",
			QueueName:     "projects/my-project/locations/australia-southeast1/queues/my-queue",
			ExpectedError: `rpc error: code = InvalidArgument desc = Invalid resource field value in the request.`,
		},
		{
			Name:          "InvalidName",
			Parent:        "projects/my-project/locations/australia-southeast1",
			QueueName:     "not/right",
			ExpectedError: `rpc error: code = InvalidArgument desc = Queue name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>"`,
		},
	} {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := server.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
				Parent: tt.Parent,
				Queue: &cloudtaskspb.Queue{
					Name: tt.QueueName,
				},
			})
			assert.Error(t, err)
			assert.Equal(t, tt.ExpectedError, err.Error())
		})
	}
}

func TestDeleteQueue(t *testing.T) {
	server := createServer()

	_, err := server.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/my-project/locations/australia-southeast1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
		},
	})
	require.NoError(t, err)

	_, err = server.DeleteQueue(context.Background(), &cloudtaskspb.DeleteQueueRequest{
		Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
	})
	assert.NoError(t, err)
}

func TestDeleteQueueNeverExisted(t *testing.T) {
	server := createServer()

	_, err := server.DeleteQueue(context.Background(), &cloudtaskspb.DeleteQueueRequest{
		Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
	})
	assert.Equal(t, "rpc error: code = NotFound desc = Requested entity was not found.", err.Error())
}

func TestDeleteQueueAlreadyDeleted(t *testing.T) {
	server := createServer()

	_, err := server.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/my-project/locations/australia-southeast1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
		},
	})
	require.NoError(t, err)

	_, err = server.DeleteQueue(context.Background(), &cloudtaskspb.DeleteQueueRequest{
		Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
	})
	require.NoError(t, err)

	_, err = server.DeleteQueue(context.Background(), &cloudtaskspb.DeleteQueueRequest{
		Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
	})
	assert.Equal(t, "rpc error: code = NotFound desc = Requested entity was not found.", err.Error())
}

func TestPurgeQueue(t *testing.T) {
	server := createServer()

	_, err := server.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/my-project/locations/australia-southeast1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
		},
	})
	require.NoError(t, err)

	_, err = server.CreateTask(context.Background(), &cloudtaskspb.CreateTaskRequest{
		Parent: "projects/my-project/locations/australia-southeast1/queues/my-queue",
		Task: &cloudtaskspb.Task{
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{},
			},
		},
	})
	require.NoError(t, err)

	response, err := server.PurgeQueue(context.Background(), &cloudtaskspb.PurgeQueueRequest{
		Name: "projects/my-project/locations/australia-southeast1/queues/my-queue",
	})
	assert.NoError(t, err)
	assert.Equal(t, defaultQueue.String(), response.String())

	assert.Eventually(t, func() bool {
		tasks, err := server.ListTasks(context.Background(), &cloudtaskspb.ListTasksRequest{
			Parent: "projects/my-project/locations/australia-southeast1/queues/my-queue",
		})

		return err == nil && len(tasks.Tasks) == 0
	}, 10*time.Millisecond, time.Millisecond)
}

func createServer() *emulator.Server {
	oidcTokenCreator := FakeOIDCTokenGenerator{}
	httpDoer := FakeHTTPDoer{}

	return emulator.NewServer(oidcTokenCreator, httpDoer, false)
}
