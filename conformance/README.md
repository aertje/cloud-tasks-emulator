# Conformance harness

Captures the observable error behaviour of Cloud Tasks and validates the
emulator against it. One battery of deliberately-malformed RPCs runs against
either target through the **official Cloud Tasks client**
(`cloud.google.com/go/cloudtasks/apiv2`) — so what we record is exactly what a
real caller using the SDK sees (routing headers, deadlines and all), not a
reconstruction.

This is a **separate Go module** (`conformance/go.mod`) so it can depend on the
current client without disturbing the emulator's intentionally-pinned dependency
graph. Run all commands below from inside the `conformance/` directory (or with
`go -C conformance ...`).

One battery of deliberately-malformed RPCs runs against either target:

- against **real Cloud Tasks** → committed golden snapshot (`golden/errors.json`)
- against the **emulator** → diffed against the golden by the conformance test

This is what `mapErr` (and its per-handler variants in `protohelpers.go`) should
be derived from, replacing the hand-guessed messages whose comments flag them as
unverified.

## Why templates, not literal messages

Cloud Tasks interpolates request values into some messages (e.g. the
queue/task-name-mismatch error embeds both names). Each case therefore runs with
several variants whose queue/task IDs differ. `Normalize` replaces those known
input substrings with placeholders (`{queue_path}`, `{task_id}`, …). The
resulting **template** is what we store and diff.

If a variant value leaks through normalization, the variants disagree and the
case is flagged **unstable** — that means there is an interpolated slot we
haven't modeled yet. Add a replacement in `normalize.go` (or vary that input)
until it's stable, then trust the template.

## Record the golden from real Cloud Tasks

Needs a throwaway GCP project with the Cloud Tasks API enabled. Control-plane
calls only (no task dispatch) — comfortably within the free tier.

```sh
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/cloud-platform,openid,https://www.googleapis.com/auth/userinfo.email
gcloud services enable cloudtasks.googleapis.com --project $PROJECT

cd conformance
go run ./cmd/record \
  -target=real -project=$PROJECT -location=us-central1 \
  -out=golden/errors.json
```

Resource names are run-scoped (random prefix) and cases clean up after
themselves, so re-runs don't collide. Watch stderr for `UNSTABLE` lines before
committing the snapshot.

## Validate the emulator

```sh
cd conformance
go test -tags conformance ./...
```

The test builds and starts the emulator (from the parent module) on a free port,
replays the battery, and reports every status code, message template or error
detail that differs from the golden. It skips if no golden snapshot is present.

Error details (the `*errdetails.*` payloads Cloud Tasks attaches to some errors,
e.g. a `Help` link on an invalid-name `InvalidArgument`) are compared as part of
the contract. Their text is prototext, whose field separator is deliberately
unstable, so the comparison collapses whitespace before diffing.

To eyeball the emulator's current behaviour without a golden:

```sh
go run ./cmd/emulator -port 8123 &                                  # from repo root
cd conformance
go run ./cmd/record -target=emulator -addr=localhost:8123 -out=/tmp/emu.json
```

## Adding cases

Append to `Cases()` in `cases.go`. Each case names the RPC under test, an error
category, and an `Invoke`; use `Setup`/`Teardown` for preconditions (e.g.
create-then-delete to reach a "recently deleted" state). Names are golden keys —
don't rename casually.

## Known divergences

`knowndiff.go` is the explicit ledger of cases where the emulator is *knowingly*
unfaithful for reasons beyond message mapping (real behaviour gaps we've
deferred). The validation test reports these as `KNOWN` instead of failing, and
fails if one starts matching (so the entry gets removed). Current entries:

- `queue/create/invalid-parent` — real resolves any parent string to a project
  and returns `PermissionDenied` via IAM; the emulator has no project/IAM concept
  and returns `InvalidArgument`. Not reproducible by design.

## Happy-path battery

A second, separate battery captures a *success-response shape* rather than an
error: what Cloud Tasks echoes back after a task is created and read. It exists
to settle behaviour the proto docs leave ambiguous and that issues #111/#53 turn
on:

- **Key casing at rest** - is a submitted `content-type` stored verbatim or
  canonicalized to `Content-Type`?
- **Default `Content-Type`** - for an AppEngine task with a body, does the
  `application/octet-stream` default appear in the stored task, or only on the
  dispatched wire request? This decides whether the emulator should inject it at
  rest or at dispatch.
- **View sensitivity** - which fields does the `BASIC` response view withhold?
  The body is documented as omitted under `BASIC` (forcing `FULL`), while headers
  are returned under both. Each stage captures both, so the golden records the
  real division.

Each observation creates a task carrying a lowercase `content-type` and a
mixed-case custom header plus a body, then reads it back via `CreateTask` (FULL),
`GetTask` (BASIC) and `GetTask` (FULL), capturing the headers and body at each
stage. See `snapshot.go`.

Record it against real Cloud Tasks. `FULL` view requires the
`cloudtasks.tasks.fullView` IAM permission on the queue (owner/editor have it):

```sh
cd conformance
go run ./cmd/record \
  -target=real -kind=happypath -project=$PROJECT -location=us-central1 \
  -out=golden/happypath.json
```

`TestEmulatorErrors`'s sibling `TestEmulatorHappyPath` diffs the
emulator against `golden/happypath.json` (and skips if it's absent). Dispatch-time
wire headers are out of scope here - this battery is control-plane only; cover
those with a hermetic emulator dispatch unit test.

## Scope

Error states plus the happy-path battery above. Other success-response shapes
remain out of scope.
