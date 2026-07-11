# Code examples

Runnable client examples for the [Cloud Tasks emulator](./README.md). Each one
points a standard Cloud Tasks client at the emulator over an insecure local
channel and uses it exactly as you would against production.

## Connecting a client

### Python

```python
import grpc
from google.cloud.tasks_v2 import CloudTasksClient
from google.cloud.tasks_v2.services.cloud_tasks.transports import CloudTasksGrpcTransport

channel = grpc.insecure_channel('localhost:8123')
transport = CloudTasksGrpcTransport(channel=channel)
client = CloudTasksClient(transport=transport)

parent = 'projects/my-sandbox/locations/us-central1'
queue_name = parent + '/queues/test'
client.create_queue(queue={'name': queue_name}, parent=parent)

# An HTTP task that should succeed (200)
client.create_task(task={'http_request': {'http_method': 'GET', 'url': 'https://www.google.com'}}, parent=queue_name)
# An HTTP task that returns 405 and will get retried
client.create_task(task={'http_request': {'http_method': 'POST', 'url': 'https://www.google.com'}}, parent=queue_name)
# An App Engine task targeting `/`
client.create_task(task={'app_engine_http_request': {}}, parent=queue_name)
```

### Go

```go
import (
	"context"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

ctx := context.Background()

client, _ := cloudtasks.NewClient(ctx,
	option.WithEndpoint("localhost:8123"),
	option.WithoutAuthentication(),
	option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
)

parent := "projects/test-project/locations/us-central1"
queue, _ := client.CreateQueue(ctx, &taskspb.CreateQueueRequest{
	Parent: parent,
	Queue:  &taskspb.Queue{Name: parent + "/queues/test"},
})

client.CreateTask(ctx, &taskspb.CreateTaskRequest{
	Parent: queue.GetName(),
	Task: &taskspb.Task{
		MessageType: &taskspb.Task_HttpRequest{
			HttpRequest: &taskspb.HttpRequest{Url: "https://www.google.com"},
		},
	},
})
```

### PHP

```php
use Grpc\ChannelCredentials;
use Google\Cloud\Core\InsecureCredentialsWrapper;
use Google\Cloud\Tasks\V2\Task;
use Google\Cloud\Tasks\V2\HttpMethod;
use Google\Cloud\Tasks\V2\HttpRequest;
use Google\Cloud\Tasks\V2\CloudTasksClient;

$client = new CloudTasksClient([
    'apiEndpoint' => 'localhost:8123',
    'transport' => 'grpc',
    'credentials' => new InsecureCredentialsWrapper(),
    'transportConfig' => [
        'grpc' => [
            'stubOpts' => [
                'credentials' => ChannelCredentials::createInsecure(),
            ],
        ],
    ],
]);

$http = new HttpRequest();
$http->setHttpMethod(HttpMethod::GET)->setUrl('https://google.com');

$task = new Task();
$task->setHttpRequest($http);

$queuePath = $client->queueName('dev', 'here', 'tasks');
$response = $client->createTask($queuePath, $task);
```

### JavaScript

```js
import { CloudTasksClient } from '@google-cloud/tasks';
import { credentials } from '@grpc/grpc-js';

const client = new CloudTasksClient({
  port: 8123,
  servicePath: 'localhost',
  sslCreds: credentials.createInsecure(),
});

const parent = 'projects/my-sandbox/locations/us-central1';
const queueName = `${parent}/queues/test`;
await client.createQueue({ parent, queue: { name: queueName } });

// An HTTP task that should succeed (200)
await client.createTask({
  parent: queueName,
  task: { httpRequest: { httpMethod: 'GET', url: 'https://www.google.com' } },
});

// An HTTP task with an OIDC token (see the OIDC token verification example below)
const payload = { foo: 'bar' };
await client.createTask({
  parent: queueName,
  task: {
    httpRequest: {
      url: 'https://myapp.example.com/worker',
      httpMethod: 'POST',
      body: Buffer.from(JSON.stringify(payload)).toString('base64'),
      headers: { 'Content-Type': 'application/json' },
      oidcToken: {
        serviceAccountEmail: 'account@project_id.iam.gserviceaccount.com',
      },
    },
  },
});
```

## Embedding in Go tests

The `emulator` package serves over an in-memory (bufconn) connection, so no TCP
port is opened and your tests stay hermetic. `em.ClientOptions()` returns the
`option.ClientOption` values that wire the standard client to the in-process
emulator; pass them to any client construction that accepts client options.

```go
import (
	"context"
	"testing"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/aertje/cloud-tasks-emulator/v2/emulator"
)

func TestMyWorker(t *testing.T) {
	em := emulator.New()
	defer em.Close()

	ctx := context.Background()
	client, err := cloudtasks.NewClient(ctx, em.ClientOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Use `client` exactly as you would against real Cloud Tasks.
	parent := "projects/my-sandbox/locations/us-central1"
	queue, _ := client.CreateQueue(ctx, &taskspb.CreateQueueRequest{
		Parent: parent,
		Queue:  &taskspb.Queue{Name: parent + "/queues/test"},
	})
	client.CreateTask(ctx, &taskspb.CreateTaskRequest{
		Parent: queue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{Url: "https://www.google.com"},
			},
		},
	})
}
```

## Verifying OIDC tokens at runtime (Go)

With the emulator started using `-openid-issuer http://localhost:8980` and an
HTTP task created with an OIDC token (as in the JavaScript example above), an
application can verify the token with the full online discovery flow. This
example uses [`github.com/coreos/go-oidc/v3/oidc`](https://pkg.go.dev/github.com/coreos/go-oidc/v3/oidc),
which fetches the emulator's public keys via its
`/.well-known/openid-configuration` discovery document:

```go
import (
	"context"
	"log"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Discover the emulator's keys once at startup and reuse the verifier.
func newVerifier(ctx context.Context) (*oidc.IDTokenVerifier, error) {
	provider, err := oidc.NewProvider(ctx, "http://localhost:8980")
	if err != nil {
		return nil, err
	}
	// ClientID is checked against the token's `aud` claim.
	return provider.Verifier(&oidc.Config{
		ClientID: "https://myapp.example.com/worker",
	}), nil
}

// Handles the incoming request to https://myapp.example.com/worker,
// protected by the OIDC token supplied at task creation.
func httpRequestHandler(ctx context.Context, verifier *oidc.IDTokenVerifier) {
	rawIDToken := "..." // from the Authorization header

	idToken, err := verifier.Verify(ctx, rawIDToken)
	if err != nil {
		log.Fatalf("token verification failed: %v", err)
	}

	var claims map[string]any
	if err := idToken.Claims(&claims); err != nil {
		log.Fatalf("failed to parse claims: %v", err)
	}
	log.Printf("Payload %v", claims)
}
```

## Configuring rate limits and retries (Go)

Rate limits and retry behavior are per-queue properties that must be set at
queue creation time. For example, to create a queue that dispatches at most one
task per second, one at a time, and gives up after three attempts:

```go
_, err := client.CreateQueue(ctx, &taskspb.CreateQueueRequest{
	Parent: "projects/my-project/locations/us-central1",
	Queue: &taskspb.Queue{
		Name: "projects/my-project/locations/us-central1/queues/my-queue",
		RateLimits: &taskspb.RateLimits{
			MaxDispatchesPerSecond:  1,
			MaxConcurrentDispatches: 1,
		},
		RetryConfig: &taskspb.RetryConfig{
			MaxAttempts: 3,
		},
	},
})
```
