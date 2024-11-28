# Cloud tasks emulator

## Introduction
This emulator tries to emulate the behaviour of Google Cloud Tasks.
As of this writing, Google does not provide a Cloud Tasks emulator, which makes local development and testing a bit tricky. This project aims to help you out until they do release an official emulator.

This project is not associated with Google.

## Status and features
This project uses the v2 version of cloud tasks, to support both http and appengine requests.

It supports the following:
- Targeting normal http and appengine endpoints.
- Rate limiting and honors rate limiting configuration (max burst, max concurrent, and dispatch rate)
- Retries and honors retry configuration (max attempts, max doublings, backoff)
- Self-signed, verifiable, OIDC authentication tokens for HTTP requests

It also has a few outstanding things to address;
- Updating of queues
- Use of context / cleaning up of the signaling
- Certain headers and response formats.

## Running the emulator
Fire it up; you can specify host and port (defaults to localhost:8123):
```sh
go run ./ -host localhost -port 8000
```

You can also optionally specify one or more queues to create automatically on startup:

```sh
go run ./ -host localhost \
  -port 8000 \
  -queue projects/dev/locations/here/queues/firstq \
  -queue projects/dev/locations/here/queues/anotherq
```

Alternatively, you can define environment variables and then run the shell script `./emulator_from_env.sh` to start the emulator. The following environment variables are supported:

 ```sh
 export PORT=8124
 export HOST=localhost
 export HARD_RESET_ON_PURGE=true
 export INITIAL_QUEUES=projects/dev/locations/here/queues/1,projects/dev/locations/here/queues/2
 export OPENID_ISSUER=http://localhost:8080

./emulator_from_env.sh
 ```

Once running, you connect to it using the standard google cloud tasks GRPC libraries.

### Docker
You can use the dockerfile if you don't want to install a Go build environment:
```sh
docker build ./ -t tasks_emulator
docker run -p 8123:8123 tasks_emulator -host 0.0.0.0 -port 8123 -queue projects/dev/locations/here/queues/anotherq
```

### Docker image
Or even easier - pull and run it directly from GitHub Container Registry:
```sh
docker run ghcr.io/aertje/cloud-tasks-emulator:latest
```

### Docker Compose
If you are planning on using docker-compose the above configuration translates to :
```yml
gcloud-tasks-emulator:
  image: ghcr.io/aertje/cloud-tasks-emulator:latest
  command: >
      -host 0.0.0.0 
      -port 8123 
      -queue projects/my-project/locations/us-west1/queues/queue1 
      -queue projects/my-project/locations/us-west1/queues/queue2
      -rate-limits-max-dispatches-per-second 50 
      -rate-limits-max-burst-size 50 
      -rate-limits-max-concurrent-dispatches 101 
      -retry-config-max-attempts 1 
  ports:
    - "${TASKS_PORT:-8123}:8123"
  environment:
    APP_ENGINE_EMULATOR_HOST: http://localhost:8080
```


## App Engine
If you want to use it to make calls to a local [App Engine emulator](https://cloud.google.com/appengine/docs/standard/python3/testing-and-deploying-your-app#local-dev-server) instance, you'll need to set the appropriate environment variable, e.g.:
```sh
export APP_ENGINE_EMULATOR_HOST=http://localhost:8080
```

### Targeting services
Since the App Engine emulator runs services on individual localhost ports (e.g. `default` on `http://localhost:8080`, `worker` on `http://localhost:8081`), and the task emulator targets subdomains when specified (e.g. `http://worker.localhost:8080`), you can use one of these workarounds:
- Use a proxy that will map the subdomain to the right destination, and set the `APP_ENGINE_EMULATOR_HOST` to match the proxy. A straightforward way is to leverage the docker-compose networking to route the task emulator traffic through an nginx instance and pass the traffic on to the container(s) running the AppEngine service(s). I.e. target `http://worker.my-proxy`.
- Update your code to use `relative_uri` instead of the `service`, and include a `dispatch.yaml` in your AppEngine configuration. I.e. target `http://localhost:8080/worker`.

The following methods will also work, but are not recommended as they will likely result in different code for your local testing and cloud deployment:
- If you are only targeting one App Engine service with the cloud tasks emulator, update the `APP_ENGINE_EMULATOR_HOST` to match that service. I.e. target `http://localhost:8081`.
- Use `http_request` instead of `app_engine_http_request` and simply specify the target URL. I.e. target `http://localhost:8081`.

## OIDC authentication
The emulator supports [OIDC token](https://cloud.google.com/tasks/docs/creating-http-target-tasks#token)
authentication for HTTP target tasks. Tokens will be issued and signed by the
emulator's (insecure) private key. The emulator will accept, and issue tokens
for, **any** ServiceAccountEmail provided by the client.

By default, the JWT `iss` (issuer) field is `http://cloud-tasks-emulator`.

Optionally, the emulator can host an HTTP OIDC discovery endpoint. This allows
your application to verify tokens at runtime with the full online flow.
To enable this, specify an issuer value at startup:

```sh
go run ./ -openid-issuer http://localhost:8980
```

With this flag:

* JWTs will have an `iss` field of `http://localhost:8980`
* The [discovery document](https://developers.google.com/identity/protocols/oauth2/openid-connect#discovery)
  will be available at `http://localhost:8980/.well-known/openid-configuration`
* The emulator's public key(s) (in JWK format) will be available at
  `http://localhost:8980/jwks`

The `-openid-issuer` URL can be any `http://hostname:port` value that your
application code can route to. The endpoint listens on `0.0.0.0` for easy
use in docker / k8s environments.

You can, of course, export the content of the `/jwks` url if you prefer to
hardcode the public keys in your application.

## Flushing task state

By default, the emulator tracks the names of every task created since the emulator launched. The list
of task names survives task completion, deletion, and purge queue operations. Completed / removed tasks
do not appear in ListTasks, but calling GetTask or CreateTask with a name that has been used in the
past will return an error. This mirrors the behaviour of Cloud Tasks - although note that unlike
Cloud Tasks the emulator does not attempt to garbage collect the list of task names over time.

For some usecases, you may want to completely reset the list of task names without restarting the
emulator - e.g. between each scenario in a test run.

The optional `hard-reset-on-purge-queue` flag configures the emulator so that calling `PurgeQueue`
will remove all record of past tasks. It also switches `PurgeQueue` to be a synchronous operation
which only returns once all tasks have been cancelled and the queue is empty. Queued tasks may, of
course, still fire during the PurgeQueue operation - but they cannot fire after PurgeQueue has
returned.

```sh
go run ./ --hard-reset-on-purge-queue
```

## Examples

### Python example
Here's a little snippet of python code that you can use to talk to the emulator.

```python
import grpc
from google.cloud.tasks_v2 import CloudTasksClient
from google.cloud.tasks_v2.services.cloud_tasks.transports import CloudTasksGrpcTransport

channel = grpc.insecure_channel('localhost:8123')

# Before v2.0.0 of the client
# client = CloudTasksClient(channel=channel)

transport = CloudTasksGrpcTransport(channel=channel)
client = CloudTasksClient(transport=transport)

parent = 'projects/my-sandbox/locations/us-central1'
queue_name = parent + '/queues/test'
client.create_queue(queue={'name': queue_name}, parent=parent)

# Create a normal http task that should succeed
client.create_task(task={'http_request': {'http_method': 'GET', 'url': 'https://www.google.com'}}, parent=queue_name) # 200
# Create a normal http task that will throw 405s and will get retried
client.create_task(task={'http_request': {'http_method': 'POST', 'url': 'https://www.google.com'}}, parent=queue_name) # 405
# Create an appengine task that will target `/`
client.create_task(task={'app_engine_http_request': {}}, parent=queue_name)
```

### Go example
In Go it would go something like this.

```go
import (
	"context"

	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/grpc"
)

conn, _ := grpc.Dial("localhost:8123", grpc.WithInsecure())
clientOpt := option.WithGRPCConn(conn)
client, _ := NewClient(context.Background(), clientOpt)

parent := "projects/test-project/locations/us-central1"
createQueueRequest := taskspb.CreateQueueRequest{
    Parent: parent,
    Queue: parent + "/queues/test",
}

createQueueResp, _ := client.CreateQueue(context.Background(), &createQueueRequest)

createTaskRequest := taskspb.CreateTaskRequest{
    Parent: createQueueResp.GetName(),
    Task: &taskspb.Task{
        PayloadType: &taskspb.Task_HttpRequest{
            HttpRequest: &taskspb.HttpRequest{
                Url: "http://www.google.com",
            },
        },
    },
}
createdTaskResp, _ := client.CreateTask(context.Background(), &createTaskRequest)
```

### PHP example
The following example can be used for PHP.
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
                        'credentials' => ChannelCredentials::createInsecure()
                    ]
                ]
            ]
        ]);

$http = new HttpRequest();
$http->setHttpMethod(HttpMethod::GET)->setUrl('https://google.com');

$task = new Task();

$task->setHttpRequest($http);
$queuePath = $client->queueName('dev', 'here', 'tasks');

$response = $this->client->createTask($queuePath, $task);
```

### JavaScript example
The following example can be used for JavaScript.
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
client.createQueue({ parent, queue: { name: queueName } });

// Create a normal http task that should succeed
await client.createTask({
  parent: queueName,
  task: { httpRequest: { httpMethod: 'GET', url: 'https://www.google.com' } },
});
// Create a normal http task that will throw 405s and will get retried
await client.createTask({
  parent: queueName,
  task: { httpRequest: { httpMethod: 'POST', url: 'https://www.google.com' } },
});
```

## Publishing the image
docker build -t cloud-tasks-emulator .
docker tag cloud-tasks-emulator llay/cloud-tasks-emulator:1.3.0
docker login
docker push llay/cloud-tasks-emulator:1.3.0

check https://hub.docker.com/r/llay/cloud-tasks-emulator