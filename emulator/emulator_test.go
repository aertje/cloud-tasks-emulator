package emulator_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/aertje/cloud-tasks-emulator/v2/emulator"
	"github.com/golang-jwt/jwt/v5"
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

	em := emulator.New()
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
	em := emulator.New(emulator.WithHardResetOnPurgeQueue(true))
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

// TestWithAppEngineEmulatorHost verifies the option routes App Engine target
// tasks to the configured host instead of production appspot.com, letting an
// embedded consumer point them at a local target.
func TestWithAppEngineEmulatorHost(t *testing.T) {
	received := make(chan *http.Request, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	em := emulator.New(emulator.WithAppEngineEmulatorHost(target.URL))
	defer em.Close()

	ctx := context.Background()
	client, err := cloudtasks.NewClient(ctx, em.ClientOptions()...)
	require.NoError(t, err)
	defer client.Close()

	const parent = "projects/test-project/locations/test-location"

	queue, err := client.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: parent,
		Queue:  &taskspb.Queue{Name: parent + "/queues/appengine-queue"},
	})
	require.NoError(t, err)

	_, err = client.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: queue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
					RelativeUri: "/task",
					HttpMethod:  taskspb.HttpMethod_POST,
				},
			},
		},
	})
	require.NoError(t, err)

	select {
	case r := <-received:
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/task", r.URL.Path)
	case <-time.After(5 * time.Second):
		t.Fatal("emulator did not dispatch the App Engine task to the configured host within 5s")
	}
}

// TestWithOIDCSigningKey verifies the option signs dispatched OIDC tokens with
// the supplied key: the token in the Authorization header validates against that
// key's public half, which it would not if the baked-in default key were used.
func TestWithOIDCSigningKey(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})

	received := make(chan *http.Request, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	em := emulator.New(emulator.WithOIDCSigningKey(keyPEM))
	defer em.Close()

	ctx := context.Background()
	client, err := cloudtasks.NewClient(ctx, em.ClientOptions()...)
	require.NoError(t, err)
	defer client.Close()

	const parent = "projects/test-project/locations/test-location"

	queue, err := client.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: parent,
		Queue:  &taskspb.Queue{Name: parent + "/queues/oidc-queue"},
	})
	require.NoError(t, err)

	_, err = client.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: queue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url:        target.URL + "/task",
					HttpMethod: taskspb.HttpMethod_POST,
					AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
						OidcToken: &taskspb.OidcToken{
							ServiceAccountEmail: "test@example.com",
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	select {
	case r := <-received:
		authHeader := r.Header.Get("Authorization")
		require.True(t, strings.HasPrefix(authHeader, "Bearer "), "expected Bearer token, got %q", authHeader)
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		_, err := jwt.Parse(tokenStr, func(*jwt.Token) (any, error) {
			return &key.PublicKey, nil
		}, jwt.WithValidMethods([]string{"RS256"}))
		assert.NoError(t, err, "dispatched token should validate against the supplied signing key")
	case <-time.After(5 * time.Second):
		t.Fatal("emulator did not dispatch the OIDC task to the target within 5s")
	}
}

// TestWithOIDCSigningKeyPanicsOnInvalidPEM verifies the option fails fast on key
// material that does not parse, mirroring the binary's construction-time check.
func TestWithOIDCSigningKeyPanicsOnInvalidPEM(t *testing.T) {
	assert.Panics(t, func() { emulator.WithOIDCSigningKey([]byte("not a pem key")) })
}

// TestStartPanicsOnSecondCall verifies Start guards against being started twice.
func TestStartPanicsOnSecondCall(t *testing.T) {
	em := emulator.NewUnstarted()
	em.Start()
	defer em.Close()

	assert.Panics(t, func() { em.Start() })
}
