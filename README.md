# Cloud Tasks emulator

An emulator for [Google Cloud Tasks](https://cloud.google.com/tasks), for local
development and testing. Google does not (yet) ship an official Cloud Tasks
emulator, so this project fills the gap until they do.

It implements the Cloud Tasks **v2** API and speaks the standard gRPC protocol,
so you point any official Cloud Tasks client library at it and use it as you
normally would.

This project is not affiliated with Google.

## Features

Supported:

- HTTP target tasks and App Engine target tasks.
- Rate limiting, honoring the queue's `RateLimits` (max dispatches per second,
  max burst, max concurrent) - see [Rate limits and retries](#rate-limits-and-retries).
- Retries, honoring the queue's `RetryConfig` (max attempts, max doublings,
  backoff) - see [Rate limits and retries](#rate-limits-and-retries).
- Self-signed, verifiable OIDC authentication tokens for HTTP target tasks -
  see [OIDC authentication](#oidc-authentication).

Known limitations:

- `UpdateQueue` is not implemented, so a queue's rate-limit and retry settings
  can only be set at creation time.
- Some response headers and formats differ from production Cloud Tasks.

## Running the emulator

### Prebuilt Docker image (recommended)

Pull and run the published image from the GitHub Container Registry:

```sh
docker run -p 8123:8123 ghcr.io/aertje/cloud-tasks-emulator:latest
```

Pass flags (see [Configuration](#configuration)) after the image name:

```sh
docker run -p 8123:8123 ghcr.io/aertje/cloud-tasks-emulator:latest \
  -host 0.0.0.0 -port 8123 \
  -initial-queue projects/dev/locations/here/queues/anotherq
```

### Docker Compose

```yml
gcloud-tasks-emulator:
  image: ghcr.io/aertje/cloud-tasks-emulator:latest
  command: -host 0.0.0.0 -port 8123 -initial-queue "projects/dev/locations/here/queues/anotherq"
  ports:
    - "${TASKS_PORT:-8123}:8123"
  environment:
    APP_ENGINE_EMULATOR_HOST: http://localhost:8080
```

### Building the image yourself

```sh
docker build ./ -t tasks_emulator
docker run -p 8123:8123 tasks_emulator -host 0.0.0.0 -port 8123
```

### From source

Run directly with Go:

```sh
go run ./cmd/emulator -host localhost -port 8000
```

Or install the binary:

```sh
go install github.com/aertje/cloud-tasks-emulator/v2/cmd/emulator@latest
```

### Configuration

Every setting is a startup flag. Each flag can equivalently be set via an
environment variable derived from its name (uppercased, dashes replaced by
underscores). Explicit flags take precedence over environment variables.

| Flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `-host` | `HOST` | `localhost` | Host to bind. Use `0.0.0.0` in Docker/k8s. |
| `-port` | `PORT` | `8123` | Port to bind. |
| `-initial-queue` | `INITIAL_QUEUE` | (none) | Queue to create on startup. Repeat the flag for multiple queues; the env var accepts a comma-separated list. |
| `-openid-issuer` | `OPENID_ISSUER` | (none) | Serve an OIDC discovery endpoint at this URL and use it as the JWT `iss`. See [OIDC authentication](#oidc-authentication). |
| `-openid-signing-key` | `OPENID_SIGNING_KEY` | (baked-in dev key) | Path to a PEM-encoded RSA private key used to sign OIDC tokens. |
| `-hard-reset-on-purge-queue` | `HARD_RESET_ON_PURGE_QUEUE` | `false` | Make `PurgeQueue` wipe all task-name history and run synchronously. See [Flushing task state](#flushing-task-state). |
| `-insecure-skip-tls-verify` | `INSECURE_SKIP_TLS_VERIFY` | `false` | Skip TLS verification when dispatching to HTTPS targets. See [Skipping TLS verification](#skipping-tls-verification-for-https-targets). |

For example, to configure the emulator entirely through the environment:

```sh
export HOST=localhost
export PORT=8124
export INITIAL_QUEUE=projects/dev/locations/here/queues/1,projects/dev/locations/here/queues/2
export OPENID_ISSUER=http://localhost:8080
export HARD_RESET_ON_PURGE_QUEUE=true

./emulator
```

## Connecting a client

The emulator listens for plaintext gRPC with no authentication, since it runs
locally. Point the official Cloud Tasks client at its `host:port` (default
`localhost:8123`) over an insecure channel with credentials disabled, then use
the client exactly as you would against production.

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

// An HTTP task with an OIDC token (see the OIDC section below)
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

If your code is written in Go, you can run the emulator in-process instead of
starting a separate binary or container. The `emulator` package serves over an
in-memory (bufconn) connection, so no TCP port is opened and your tests stay
hermetic.

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

`em.ClientOptions()` returns the `option.ClientOption` values that wire the
standard client to the in-process emulator; pass them to any client
construction that accepts client options.

## App Engine

To make calls to a local
[App Engine emulator](https://cloud.google.com/appengine/docs/standard/python3/testing-and-deploying-your-app#local-dev-server)
instance, set the appropriate environment variable, e.g.:

```sh
export APP_ENGINE_EMULATOR_HOST=http://localhost:8080
```

### Targeting services

The App Engine emulator runs services on individual localhost ports (e.g.
`default` on `http://localhost:8080`, `worker` on `http://localhost:8081`),
while the task emulator targets subdomains when a service is specified (e.g.
`http://worker.localhost:8080`). To bridge the two, use one of these
workarounds:

- Use a proxy that maps the subdomain to the right destination, and set
  `APP_ENGINE_EMULATOR_HOST` to match the proxy. A straightforward way is to
  leverage docker-compose networking to route task emulator traffic through an
  nginx instance and pass it on to the container(s) running the App Engine
  service(s). I.e. target `http://worker.my-proxy`.
- Update your code to use `relative_uri` instead of `service`, and include a
  `dispatch.yaml` in your App Engine configuration. I.e. target
  `http://localhost:8080/worker`.

The following also work, but are not recommended as they will likely result in
different code for local testing versus cloud deployment:

- If you are only targeting one App Engine service, set
  `APP_ENGINE_EMULATOR_HOST` to match that service. I.e. target
  `http://localhost:8081`.
- Use `http_request` instead of `app_engine_http_request` and specify the
  target URL directly. I.e. target `http://localhost:8081`.

## OIDC authentication

The emulator supports [OIDC token](https://cloud.google.com/tasks/docs/creating-http-target-tasks#token)
authentication for HTTP target tasks. Tokens are issued and signed by the
emulator's (insecure) private key. The emulator will accept, and issue tokens
for, **any** `ServiceAccountEmail` provided by the client.

By default the JWT `iss` (issuer) field is `http://cloud-tasks-emulator`.

### Verifying tokens at runtime

Optionally, the emulator can host an HTTP OIDC discovery endpoint so your
application can verify tokens with the full online flow. Enable it by specifying
an issuer at startup:

```sh
go run ./cmd/emulator -openid-issuer http://localhost:8980
```

With this flag:

- JWTs have an `iss` field of `http://localhost:8980`.
- The [discovery document](https://developers.google.com/identity/protocols/oauth2/openid-connect#discovery)
  is served at `http://localhost:8980/.well-known/openid-configuration`.
- The emulator's public key(s), in JWK format, are served at
  `http://localhost:8980/jwks`.

The `-openid-issuer` URL can be any `http://hostname:port` value your
application can route to. The endpoint listens on `0.0.0.0` for easy use in
docker / k8s environments. You can also export the contents of `/jwks` if you
prefer to hardcode the public keys in your application.

For example, verifying a token in Node.js:

```js
// Started the emulator with `-openid-issuer http://localhost:8980` and created
// an HTTP task with an OIDC token, as in the JavaScript example above.
import { OAuth2Client } from "google-auth-library";

const client = new OAuth2Client({
  endpoints: {
    // JWK certs served by the emulator
    oauth2FederatedSignonJwkCertsUrl: "http://localhost:8980/jwks",
  },
  issuers: ["http://localhost:8980"],
});

// Handles the incoming request to https://myapp.example.com/worker,
// protected by the OIDC token supplied at task creation.
async function httpRequestHandler() {
  const idToken = "..."; // from the Authorization header

  const ticket = await client.verifyIdToken({
    idToken,
    audience: "https://myapp.example.com/worker",
  });
  const payload = ticket.getPayload();
  console.info("Payload", payload);
}
```

### Signing with your own key

By default the emulator signs tokens with a baked-in (insecure, publicly known)
development key. To sign with your own RSA private key instead, pass a path to a
PEM-encoded key:

```sh
go run ./cmd/emulator -openid-issuer http://localhost:8980 -openid-signing-key ./oidc.key
```

The matching public key is derived from it automatically and published at the
`/jwks` endpoint, so verification via the discovery flow keeps working. You can
generate a suitable key with:

```sh
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out oidc.key
```

## Rate limits and retries

Rate limits and retry behavior are **per-queue properties**, exactly as in
production Cloud Tasks. There is no emulator-specific flag or environment
variable for them: you set them on the queue itself through the API, using the
standard Cloud Tasks client. The emulator then honors them when dispatching.

The catch is *when* you can set them:

- Set them when you **create the queue**. `UpdateQueue` is not implemented, so
  you cannot change a queue's limits after creation.
- The `-initial-queue` startup flag only takes a queue *name*, so queues created
  that way get the default limits. To use custom limits, create the queue
  programmatically with the config set.

The honored fields and their defaults (matching production Cloud Tasks) are:

| Field | Default |
| --- | --- |
| `RateLimits.MaxDispatchesPerSecond` | 500 |
| `RateLimits.MaxBurstSize` | 100 |
| `RateLimits.MaxConcurrentDispatches` | 1000 |
| `RetryConfig.MaxAttempts` | 100 |
| `RetryConfig.MaxDoublings` | 16 |
| `RetryConfig.MinBackoff` | 100ms |
| `RetryConfig.MaxBackoff` | 1h |

For example, to create a queue that dispatches at most one task per second, one
at a time, and gives up after three attempts (Go):

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

## Flushing task state

By default, the emulator tracks the names of every task created since it
launched. The list of task names survives task completion, deletion, and purge
queue operations. Completed / removed tasks do not appear in `ListTasks`, but
calling `GetTask` or `CreateTask` with a name that has been used in the past
returns an error. This mirrors the behavior of Cloud Tasks - although note that,
unlike Cloud Tasks, the emulator does not attempt to garbage collect the list of
task names over time.

For some use cases you may want to completely reset the list of task names
without restarting the emulator - e.g. between scenarios in a test run.

The optional `-hard-reset-on-purge-queue` flag makes `PurgeQueue` remove all
record of past tasks. It also switches `PurgeQueue` to be a synchronous
operation that only returns once all tasks have been cancelled and the queue is
empty. Queued tasks may still fire during the `PurgeQueue` operation, but they
cannot fire after it has returned.

```sh
go run ./cmd/emulator -hard-reset-on-purge-queue
```

## Skipping TLS verification for HTTPS targets

When developing against a target served over HTTPS with a self-signed or
otherwise untrusted certificate, task dispatch fails TLS verification. The
optional `-insecure-skip-tls-verify` flag disables certificate verification for
task dispatch so those deliveries succeed. It is a development convenience with
no production equivalent, so leave it off unless you need it; the emulator logs
a warning on startup when it is enabled.

```sh
go run ./cmd/emulator -insecure-skip-tls-verify
```
