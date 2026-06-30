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

- against **real Cloud Tasks** → committed golden snapshot (`golden/realcloud.json`)
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
  -out=golden/realcloud.json
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
go run . -port 8123 &                                  # from repo root
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

## Scope

Error states only, for now (the gap `mapErr` needs filled). Success-response
shapes are out of scope.
