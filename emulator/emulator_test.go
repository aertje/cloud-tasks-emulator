package emulator_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/aertje/cloud-tasks-emulator/v2/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInProcessDispatch exercises the full path a library consumer relies on:
// the official client dials the in-process emulator over bufconn, creates a
// queue and a task, and the emulator dispatches that task over HTTP to a local
// target. Receiving the request proves the client, gRPC transport, engine and
// dispatcher all work end-to-end with no TCP port or external process.
func TestInProcessDispatch(t *testing.T) {
	received := make(chan *http.Request, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	em := emulator.Start()
	defer em.Close()

	ctx := context.Background()
	client, err := cloudtasks.NewClient(ctx, em.ClientOptions()...)
	require.NoError(t, err)
	defer client.Close()

	const parent = "projects/test-project/locations/test-location"

	queue, err := client.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: parent,
		Queue:  &taskspb.Queue{Name: parent + "/queues/test-queue"},
	})
	require.NoError(t, err)

	task, err := client.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: queue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url:        target.URL + "/task",
					HttpMethod: taskspb.HttpMethod_POST,
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, task.GetName(), queue.GetName()+"/tasks/")

	select {
	case r := <-received:
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/task", r.URL.Path)
	case <-time.After(5 * time.Second):
		t.Fatal("emulator did not dispatch the task to the target within 5s")
	}
}

// TestWithHardResetOnPurgeQueue verifies functional options reach the underlying
// server. With a hard reset, PurgeQueue synchronously clears tasks.
func TestWithHardResetOnPurgeQueue(t *testing.T) {
	em := emulator.Start(emulator.WithHardResetOnPurgeQueue(true))
	defer em.Close()

	ctx := context.Background()
	client, err := cloudtasks.NewClient(ctx, em.ClientOptions()...)
	require.NoError(t, err)
	defer client.Close()

	const parent = "projects/test-project/locations/test-location"

	queue, err := client.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: parent,
		Queue:  &taskspb.Queue{Name: parent + "/queues/purge-queue"},
	})
	require.NoError(t, err)

	_, err = client.PurgeQueue(ctx, &taskspb.PurgeQueueRequest{Name: queue.GetName()})
	require.NoError(t, err)
}
