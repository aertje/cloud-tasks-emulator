# Cloud Tasks emulator

An emulator for [Google Cloud Tasks](https://cloud.google.com/tasks), for local
development and testing. Google does not (yet) ship an official Cloud Tasks
emulator, so this project fills the gap until they do.

It implements the Cloud Tasks v2 API and speaks the standard gRPC protocol,
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
- Pagination is not supported. `ListQueues` and `ListTasks` ignore the
  `page_size` and `page_token` request fields and return every result in a
  single response with an empty `next_page_token`.
- App Engine target tasks route to the `appspot.com` host. Custom domains are
  not supported. See [App Engine](#app-engine) for the appspot.com routing
  options.

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
| `-hard-reset-on-purge-queue` | `HARD_RESET_ON_PURGE_QUEUE` | `false` | Make `PurgeQueue` release reserved task names immediately and run synchronously. See [Flushing task state](#flushing-task-state). |
| `-insecure-skip-tls-verify` | `INSECURE_SKIP_TLS_VERIFY` | `false` | Skip TLS verification when dispatching to HTTPS targets. See [Skipping TLS verification](#skipping-tls-verification-for-https-targets). |
| `-app-engine-emulator-host` | `APP_ENGINE_EMULATOR_HOST` | (none) | Base URL that App Engine target tasks route to instead of `https://<project>.appspot.com`. See [App Engine](#app-engine). |
| `-app-engine-region-id` | `APP_ENGINE_REGION_ID` | (none) | App Engine region ID (e.g. `uc`) used to emit the regional host format `https://<project>.<region>.r.appspot.com` that production Cloud Tasks uses. Leave unset for the legacy `https://<project>.appspot.com` format. Ignored when `-app-engine-emulator-host` is set. See [App Engine](#app-engine). |

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

See [EXAMPLES.md](./EXAMPLES.md#connecting-a-client) for connection snippets in
Python, Go, PHP, and JavaScript.

## Embedding in Go tests

If your code is written in Go, you can run the emulator in-process instead of
starting a separate binary or container. The `emulator` package serves over an
in-memory (bufconn) connection, so no TCP port is opened and your tests stay
hermetic. `em.ClientOptions()` returns the `option.ClientOption` values that
wire the standard client to the in-process emulator; pass them to any client
construction that accepts client options.

See [EXAMPLES.md](./EXAMPLES.md#embedding-in-go-tests) for a full test example.

## App Engine

To make calls to a local
[App Engine emulator](https://cloud.google.com/appengine/docs/standard/python3/testing-and-deploying-your-app#local-dev-server)
instance, point App Engine target tasks at it with the
`-app-engine-emulator-host` flag (or its `APP_ENGINE_EMULATOR_HOST` env var), e.g.:

```sh
go run ./cmd/emulator -app-engine-emulator-host http://localhost:8080
```

### Targeting services

The App Engine emulator runs services on individual localhost ports (e.g.
`default` on `http://localhost:8080`, `worker` on `http://localhost:8081`),
while the task emulator targets subdomains when a service is specified (e.g.
`http://worker.localhost:8080`). To bridge the two, use one of these
workarounds:

- Use a proxy that maps the subdomain to the right destination, and set
  `-app-engine-emulator-host` to match the proxy. A straightforward way is to
  leverage docker-compose networking to route task emulator traffic through an
  nginx instance and pass it on to the container(s) running the App Engine
  service(s). I.e. target `http://worker.my-proxy`.
- Update your code to use `relative_uri` instead of `service`, and include a
  `dispatch.yaml` in your App Engine configuration. I.e. target
  `http://localhost:8080/worker`.

The following also work, but are not recommended as they will likely result in
different code for local testing versus cloud deployment:

- If you are only targeting one App Engine service, set
  `-app-engine-emulator-host` to match that service. I.e. target
  `http://localhost:8081`.
- Use `http_request` instead of `app_engine_http_request` and specify the
  target URL directly. I.e. target `http://localhost:8081`.

### Production appspot.com routing

When `-app-engine-emulator-host` is not set, App Engine target tasks route to
`https://<project>.appspot.com` by default. Production Cloud Tasks instead uses
the regional host format `https://<project>.<region>.r.appspot.com`, where
`<region>` is the App Engine region ID (a short code such as `uc` for
`us-central1`, not the full location name).

To match production, set `-app-engine-region-id` (or `APP_ENGINE_REGION_ID`) to
your app's region ID:

```sh
go run ./cmd/emulator -app-engine-region-id uc
```

Leave the flag unset to keep the legacy `https://<project>.appspot.com` format.
The flag is ignored when `-app-engine-emulator-host` is set.

## OIDC authentication

The emulator supports [OIDC token](https://cloud.google.com/tasks/docs/creating-http-target-tasks#token)
authentication for HTTP target tasks. Tokens are issued and signed by the
emulator's (insecure) private key. The emulator will accept, and issue tokens
for, any `ServiceAccountEmail` provided by the client.

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

See [EXAMPLES.md](./EXAMPLES.md#verifying-oidc-tokens-at-runtime-go) for a Go
token verification example.

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

Rate limits and retry behavior are per-queue properties, exactly as in
production Cloud Tasks. There is no emulator-specific flag or environment
variable for them: you set them on the queue itself through the API, using the
standard Cloud Tasks client. The emulator then honors them when dispatching.

The catch is *when* you can set them:

- Set them when you create the queue. `UpdateQueue` is not implemented, so
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

See [EXAMPLES.md](./EXAMPLES.md#configuring-rate-limits-and-retries-go) for a Go
example that creates a queue with custom rate limits and a retry cap.

## Flushing task state

When a task completes, is deleted, or is removed by a purge queue operation, the
emulator reserves its name for a short cooldown rather than freeing it
immediately. During the cooldown the task no longer appears in `ListTasks`, but
calling `GetTask` or `CreateTask` with that name returns an error, mirroring the
recently-deleted behavior of Cloud Tasks. Once the cooldown elapses a background
sweep reclaims the name and it becomes reusable again. The cooldown defaults to
one minute. (Queue names are reserved the same way.)

For some use cases you may want to release reserved names immediately - e.g.
between scenarios in a test run - rather than waiting out the cooldown.

The optional `-hard-reset-on-purge-queue` flag makes `PurgeQueue` drop the
reserved names outright, so they can be reused without waiting out the cooldown.
It also switches `PurgeQueue` to be a synchronous
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
