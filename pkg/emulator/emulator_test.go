package emulator_test

import (
	"context"
	"net/http"
	"testing"

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
	return nil, nil
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

func createServer() *emulator.Server {
	oidcTokenCreator := FakeOIDCTokenGenerator{}
	httpDoer := FakeHTTPDoer{}

	return emulator.NewServer(oidcTokenCreator, httpDoer, false)
}
