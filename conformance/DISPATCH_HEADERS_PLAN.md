# Plan: capture & implement the optional dispatch headers

Status: **DONE.** Receiver built and deployed, golden captured from real Cloud
Tasks, engine implementation done, and the hermetic `TestEmulatorDispatch`
passes. All work items complete. See "Open questions" below for what the capture
resolved. This doc can be removed; it is retained as a record of the design and
findings (also captured in `conformance/README.md` and the code comments).

## Goal

The emulator does not send the two *optional* dispatch headers that production
Cloud Tasks includes on retried task requests. Implement them faithfully:

- HTTP targets: `X-CloudTasks-TaskPreviousResponse`, `X-CloudTasks-TaskRetryReason`
- App Engine targets: `X-AppEngine-TaskPreviousResponse`, `X-AppEngine-TaskRetryReason`

They are currently listed as a known limitation in the root `README.md`.

Blocker: the *value format* of `TaskRetryReason` is not documented anywhere
public, so we want to capture the real headers from production before
implementing, rather than guess.

## What we already know

- `TaskPreviousResponse` is unambiguous: the HTTP status code of the **previous**
  attempt, present **only** on retries. Google's own App Engine stub emits it
  conditionally (`if task_response.has_runlog() and
  task_response.runlog().has_response_code()`), value = the prior response code.
  This one we can implement without capture.
- `TaskRetryReason` is documented only as "the reason for retrying the task."
  Exact string format unknown; the App Engine stub does not set it at all. This
  is the one we need real data for.
- Docs: https://docs.cloud.google.com/tasks/docs/creating-http-target-tasks
- App Engine stub reference:
  https://github.com/GoogleCloudPlatform/python-compat-runtime/blob/master/appengine-compat/exported_appengine_sdk/google/appengine/api/taskqueue/taskqueue_stub.py

## Chosen capture approach: a small App Engine receiver (NOT ngrok)

ngrok was considered and rejected: it can only capture the HTTP family. App
Engine target tasks route to `PROJECT.appspot.com` via Cloud Tasks' internal App
Engine routing, which cannot be pointed at a tunnel, so ngrok can never observe
the `X-AppEngine-*` retry headers (half of what's missing). ngrok also injects
proxy/forwarding headers and needs an authtoken.

A single deployed App Engine app covers **both** families and is GCP-native:

- **HTTP target** -> `HttpRequest.Url = https://PROJECT.appspot.com/recv/http`
  (the app is just a public HTTPS endpoint). Captures `X-CloudTasks-*`.
- **App Engine target** -> `AppEngineHttpRequest{RelativeUri: "/recv/appengine"}`
  routed to the same default service. Captures `X-AppEngine-*`.

### Receiver behaviour

A tiny Go App Engine app whose handler:

- Decides its own response from the retry-count header (`retrycount < 1` -> `503`,
  else `200`), so every task deterministically fails once then succeeds. That
  single forced retry is what makes the optional headers appear.
- Records each request's headers, keyed by task name + attempt, in memory.
- Exposes `GET /captures?run=<prefix>` so the recorder reads back what it saw.

In-memory capture is fine if pinned to a single instance
(`basic_scaling: max_instances: 1`, which also scales to zero when idle so there
is no standing cost). Firestore was considered but is overkill at this scale.

Decision taken: **in-memory + single instance + readback endpoint.**

### Reuse for emulator validation (hermetic)

Factor the handler into a shared package. The conformance test runs it as a
plain local HTTP server, points emulator HTTP tasks straight at it, and sets
`APP_ENGINE_EMULATOR_HOST` to it so App Engine-target tasks hit it too. So the
App Engine deploy is only needed to record the golden; ongoing validation stays
hermetic, matching the errors/happypath batteries.

## Work items

1. **DONE.** `conformance/receiver/` - a lean, stdlib-only sibling module
   (`.../conformance/receiver`, own `go.mod`) so the App Engine deploy stays free
   of the harness' heavy client graph; pulled into the conformance module via a
   local `replace`. `receiver.go` (`package receiver`): forced-retry response
   policy (503 then 200, keyed off `X-*-TaskRetryCount`), in-memory per-attempt
   capture keyed by task name + attempt, `GET /captures?run=` readback returning
   `[]receiver.Capture`, and raw headers logged to stderr for eyeballing.
   `cmd/recv/main.go` serves it on `$PORT`; `app.yaml`
   (`basic_scaling: max_instances: 1`, `main: ./cmd/recv`). Verified locally.
2. **DONE.** `-kind=dispatch` battery in `conformance/dispatch.go`:
   - `DispatchSnapshot` / `DispatchAttempt` types, `RunDispatch(ctx, c, opts,
     receiverURL)`, `SaveDispatch` / `LoadDispatch` / `CompareDispatch`.
   - Cases: one HTTP-target, one App Engine-target, each forced to retry once.
   - Queue created with a fast retry config (`MinBackoff` 1s, `MaxBackoff` 5s,
     `MaxAttempts` 5). Polls the receiver readback (2s interval, 90s deadline)
     until a retry (`Attempt>=1`) is observed; a shortfall is reported, not fatal.
   - Normalizes volatile header values before diffing: `QueueName` -> `{queue}`,
     `TaskName` -> `{task}`, `TaskETA` -> `{eta}`. Keeps **only** the
     `X-CloudTasks-*` / `X-AppEngine-*` / `User-Agent` family; drops transport
     noise. Reuses `receiver.Capture` as the single source of truth for the
     readback JSON shape.
3. **DONE.** Wired `-kind=dispatch` + `-receiver-url` into `cmd/record/main.go`
   (errors if `-receiver-url` is missing; logs per-case whether a retry was seen).
4. **DONE.** `TestEmulatorDispatch` in `conformance_test.go` (hermetic: local
   receiver via `receiver.NewHandler()` + emulator, `APP_ENGINE_EMULATOR_HOST`
   set via a child-env `startEmulator`). Passes.
5. **DONE.** Engine implementation (`internal/engine/dispatch.go`, `types.go`):
   - `TaskPreviousResponse` (both families): `Attempt.ResponseCode` now stores the
     raw HTTP status; `updateStateForDispatch` captures the prior attempt's code
     onto the dispatch snapshot (`TaskState.PreviousResponseCode`) before
     overwriting `LastAttempt`. Emitted (with `TaskRetryReason`) only on retries
     of an attempt that received an HTTP response.
   - `TaskRetryReason` (both families): implemented to the captured values - empty
     for HTTP, `App Error` for App Engine (see `httpRetryReason` /
     `appEngineRetryReason`).
   - Also fixed two gaps the capture exposed (see Open questions): HTTP
     `TaskExecutionCount` now excludes 5XX (`TaskState.ExecutionCount`), and HTTP
     dispatch now sends `User-Agent: Google-Cloud-Tasks`.
   - Removed the `// TODO: optional headers` comments.
6. **DONE.** Dropped the optional-headers bullet from root `README.md`.
7. **DONE.** Documented the dispatch battery + receiver in `conformance/README.md`
   (and updated the happy-path note that used to say dispatch headers were out of
   scope).

## Setup to record the golden (once implemented)

```sh
gcloud app create --project=$PROJECT --region=us-central          # one-time per project
gcloud app deploy conformance/receiver/app.yaml --project=$PROJECT # deploy the receiver
go run ./cmd/record -target=real -kind=dispatch \
  -project=$PROJECT -location=us-central1 \
  -receiver-url=https://$PROJECT.appspot.com -out=golden/dispatch.json
```

## Open questions - resolved by the capture

- **Exact `TaskRetryReason` string format** (the whole reason for capturing): it
  is **family-specific**. App Engine sends `App Error`; the HTTP family sends an
  **empty string** (header present, value blank). The receiver forces a 503 then
  a 404, so both a 5XX and a 4XX prior response were captured: the HTTP reason is
  empty for both (not failure-class specific), and `TaskPreviousResponse` chains
  `503` -> `404` across the two retries. Transport failures (no HTTP response)
  remain unobserved; the emulator omits the optional headers in that case.
- **Does real Cloud Tasks send `X-AppEngine-TaskRetryReason`?** Yes - `App Error`.
- **Does App Engine strip/rewrite `X-CloudTasks-*` on the HTTP-target path to
  appspot?** It does not strip them, but it *adds* a raft of App Engine frontend
  headers (`X-Appengine-Api-Ticket`, `-User-Ip`, `-Request-Log-Id`, …) that real
  Cloud Tasks never sends to an arbitrary HTTP target. These are a capture-rig
  artifact (the receiver is itself an App Engine app), so `normalizeDispatchHeaders`
  allowlists only the real dispatch headers + `User-Agent` and drops them.
- **Bonus finding - `TaskExecutionCount`:** the HTTP header **excludes** 5XX
  failures; the App Engine header counts them. After one forced `503` the HTTP
  retry reports `0`, App Engine reports `1`. The emulator previously counted all
  responses for both; now fixed per family. Consistent with the existing
  404-based `TestErrorTaskExecution` (404 is not 5XX, so it still counts).
- **Bonus finding - HTTP `User-Agent`:** real sends `Google-Cloud-Tasks`; the
  emulator sent Go's default. Now injected on HTTP dispatch.
