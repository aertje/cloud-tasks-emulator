# Dispatch-header receiver

A tiny HTTP target that records the request headers Cloud Tasks attaches when it
dispatches (and re-dispatches) a task. It exists to capture the two *optional*
retry headers whose value format is undocumented:

- HTTP targets: `X-CloudTasks-TaskPreviousResponse`, `X-CloudTasks-TaskRetryReason`
- App Engine targets: `X-AppEngine-TaskPreviousResponse`, `X-AppEngine-TaskRetryReason`

The same handler backs two consumers, so what the conformance test drives
locally is byte-for-byte the handler that recorded the golden:

- **Deployed to App Engine** it observes real Cloud Tasks dispatches. Both target
  families reach it: an HTTP-target task points its URL at
  `https://PROJECT.appspot.com/recv/http`, and an App Engine-target task routes to
  `/recv/appengine` on the same default service. A deployed App Engine app is the
  only vantage point that can see the `X-AppEngine-*` retry headers, because App
  Engine-target tasks route through internal App Engine routing that cannot be
  pointed at a tunnel (this is why ngrok was rejected).
- **Run as a plain local server** it lets the conformance harness drive the
  emulator against an identical target, keeping validation hermetic.

## How it forces the optional headers to appear

The optional headers only appear on a *re-dispatch*. The handler forces two
retries per task, with a different failure status each time: it fails the first
delivery with `503` and the second with `404`, then succeeds (`200`) on every
later attempt, decided from the task's `X-*-TaskRetryCount` header. Two differing
failures capture the optional headers for both a 5XX and a 4XX prior response,
since their `X-*-TaskRetryReason` (and, for HTTP, `X-*-TaskExecutionCount`, which
excludes 5XX) may differ between the two.

Every request's headers are recorded in memory, keyed by task name + attempt.
In-memory is sufficient because the deploy is pinned to a single instance
(`basic_scaling: max_instances: 1`). Read back what was seen with:

    GET /captures?run=<prefix>

`run` is the run-scoped resource-name prefix; captures whose queue or task name
contains it are returned as a JSON array of `receiver.Capture`. Omit `run` to get
everything.

> Note: net/http canonicalises header *keys* on receipt (the wire's
> `X-CloudTasks-TaskName` is stored as `X-Cloudtasks-Taskname`); header *values*
> are preserved exactly, which is what matters for the undocumented retry-reason
> format. The documented header names remain authoritative for spelling.

## Layout

    receiver.go        package receiver: the shared handler, capture store, readback
    cmd/recv/main.go   package main: serves NewHandler() on $PORT (App Engine entrypoint)
    app.yaml           App Engine deploy config (single instance, scales to zero)

It is a lean, standard-library-only module, kept separate from the conformance
harness so its App Engine deploy stays free of the heavy Cloud Tasks client
graph. The conformance module pulls it in via a local `replace`.

## Run locally

    go run ./cmd/recv          # listens on :8080 (override with PORT)
    curl -s 'localhost:8080/captures'   # -> []

## Deploy to App Engine

First-time project setup (once per project):

    # gcloud app deploy builds the app with Cloud Build. Without this API enabled
    # the deploy fails with an "invalid bucket ... does not have access to the
    # bucket" error. Enabling it is the only prerequisite - no extra IAM grants
    # were needed.
    gcloud services enable cloudbuild.googleapis.com --project=$PROJECT

    # Create the App Engine application.
    gcloud app create --project=$PROJECT --region=us-central

Deploy the receiver (run from this directory — it is the module root):

    gcloud app deploy app.yaml --project=$PROJECT

Then record the golden from the repo's `conformance/` directory:

    go run ./cmd/record -target=real -kind=dispatch \
      -project=$PROJECT -location=us-central1 \
      -receiver-url=https://$PROJECT.appspot.com -out=golden/dispatch.json

The full raw headers of every dispatch are also written to the App Engine logs
(`gcloud app logs tail`) for eyeballing while recording.

## Security

The deployed endpoints are **public and unauthenticated** - there is no
`login: admin`, ingress rule or auth check. Anyone who knows the URL can `POST`
to `/recv/*` (skewing a capture) or `GET /captures` to read back the recorded
headers, which for App Engine-target dispatches include an internal
`X-Appengine-Api-Ticket` token and the caller IP.

This is acceptable only because the app is **ephemeral**: it holds nothing but
synthetic conformance traffic, keeps it **in memory** (a restart, scale-to-zero
or redeploy wipes it), and exists only for the minutes it takes to record a
golden. Deploy it in a throwaway project.

The usual App Engine lock-downs don't fit a dual-family receiver: `login: admin`
would 302 the HTTP-target path (`/recv/http` is a plain external request), and
IAP blocks Cloud Tasks entirely. So the intended protection is **lifecycle** -
tear it down once the golden is recorded:

    gcloud app versions list --project=$PROJECT
    gcloud app versions stop VERSION --project=$PROJECT   # stop serving (redeploy to record again)

Re-deploying is a single command when you next need to record.
