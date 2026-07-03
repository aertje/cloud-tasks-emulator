# v2 Cleanup Plan

Consolidated findings from a four-way review sweep (core code, testing setup, CI, repo hygiene) on branch `v2`, 2026-07-03. Items are ordered by suggested execution phase. Each item is self-contained enough to pick up in a fresh session.

Verified context: `go build ./...` and `go vet ./...` are clean. `go test -race` fails on a confirmed data race. The root package vs `engine/` split is complete and intentional (root `oidc.go`/`protohelpers.go` are the gRPC/HTTP edge layer, `engine/*` is the domain layer); nothing needs consolidating there.

> **Status (2026-07-03):** Phases 1-4 complete and the Phase 5 small cleanups done; `go build`, `go vet`, `golangci-lint`, and `go test -race` are clean across all three modules. Engine coverage rose 13.4% -> 78.7%. The three larger Phase 5 design items (tombstone expiry, worker pool, gRPC context propagation) are left unchecked for a separate, discussed pass. Notes: the Phase 2 test handler was de-flaked by replacing sleeps with condition polling on the emulator's own state rather than reordering the handler send; the optional Dockerfile HEALTHCHECK was skipped (the OIDC port is only known at runtime).

## Phase 1: Engine concurrency correctness

Do these first; everything in Phase 2/3 builds on a race-clean engine.

- [x] **Fix data race on task headers during dispatch** (`engine/dispatch.go:103-139`). `dispatch` takes `state *TaskState = &task.state` and mutates the live shared headers map in place (`Authorization`, `X-CloudTasks-*`) with no lock, while gRPC handlers concurrently read it via `State()` -> `taskToProto` -> `copyHeaders`. Confirmed by `-race` (worker write vs `CreateTask` read). Also causes injected headers to bleed into task state returned by later `GetTask` calls. Fix: dispatch from a deep copy of the request (clone the headers map); read `task.state` under `stateMutex`. Note `doDispatch` already builds a `frozen` snapshot for `Run()` but still dispatches from `&task.state`.
- [x] **Fix unsynchronized `Queue.state` / `paused` / `cancelled`** (`engine/queue.go:71`, `queue.go:235-254`). `State()` reads the whole struct with no lock while `Pause()`/`Resume()`/`Delete()` write these fields; all reachable concurrently from gRPC handlers. Add a mutex around queue lifecycle fields (the existing `tsMux` only guards the task map).
- [x] **Rewrite worker cancellation; fixes broken Resume and a Delete deadlock** (`engine/queue.go:118-129`, `199-209`, `235-254`). Current design cascades a single token through a cap-1 `cancelWorkers` channel; after the cascade one token always remains buffered. Consequences: (a) `Resume()` starts new workers and the first one consumes the stale token, re-killing all workers, so a paused-then-resumed queue never dispatches again; (b) `Delete()` after `Pause()` sends to the already-full buffer with no readers and blocks the gRPC handler forever. Replace with a close-to-broadcast pattern (`chan struct{}` + `close`, or `context.Context`). Add a regression test: pause, resume, then assert a task still dispatches.
- [x] **Guard latent panics in dispatch path**:
  - `engine/dispatch.go:101` discards the `http.NewRequest` error; a failure (or neither `HTTPRequest` nor `AppEngineHTTPRequest` set) nil-panics at `req.Header[k]=` / `client.Do(req)` inside a worker. Check the error, record a failed attempt. Prefer `http.NewRequestWithContext`.
  - `engine/task.go:33-41` `parseTaskName` indexes `FindStringSubmatch` results without a nil check. Return an error/ok flag instead.
- [x] **Make hard-reset-on-purge deterministic** (`engine/engine.go:176-202`). Currently sleeps 10ms then `panic("Expected task to be deleted by now!")`. `PurgeQueue` already returns a `*sync.WaitGroup` (`engine/queue.go:213`); wait on real completion signals instead of sleeping, and never panic the server on a timing race.

## Phase 2: Test suite de-flaking, then race-enabled CI

The suite failed 2 of 3 runs under `go test ./... -cover` (`TestListTasks`, `TestCreateTaskRejectsDuplicateName`) and hung once past 30s. Root causes are known:

- [x] **Ephemeral ports in root tests** (`emulator_test.go:857-874` `startTestServer`). Every test binds a hardcoded `localhost:5000` handler server via `go srv.ListenAndServe()` with the error discarded. Use `net.Listen("tcp", "localhost:0")`, inject the resulting URL into each task, and block until listening before creating tasks. Model to copy: `conformance/conformance_test.go` `startEmulator` (free port, poll-until-listening, `t.Cleanup`).
- [x] **Add `Engine.Stop()` and call it in teardown** (`engine/engine.go`, `emulator_test.go:68-70`). `tearDown` only stops the gRPC server; queue and task goroutines keep running and leaked retrying tasks (e.g. the `/not_found` retrier in `TestErrorTaskExecution`) deliver into the next test's server, corrupting its buffer-1 request channel. `Engine.Stop()` should cancel all queues/tasks deterministically.
- [x] **Make `TestListTasks` deterministic** (`emulator_test.go:466-515`). The task has no `ScheduleTime`, so it dispatches immediately and races the `ListTasks` call. Schedule it in the future like `TestDeleteTaskTombstonesName` does (`time.Now().Add(time.Hour)`).
- [x] **Remove sleep-based synchronization** (`emulator_test.go:79`, `:393`, `:443` (1s in hard-reset test), `:850`). Replace with condition polling or the engine's own completion signals (the purge WaitGroup). Also fix the test handler ordering bug: it sends the request on the channel before writing the HTTP response, which is what the 20ms sleep in `awaitHttpRequestWithTimeout` papers over; send after responding.
- [x] **Add `t.Parallel()` + unique queue names per test** once the above land (all root tests currently share queue name `"test"` and the fixed port; those are the only blockers).
- [x] **Rebalance the pyramid: unit tests in `engine/`**. Coverage today: root 51.5%, engine 13.4%. All lifecycle logic (create/delete/purge/pause/resume, duplicate-name rejection, tombstoning, retry/backoff, invalid-ID validation) is tested only via slow e2e gRPC round-trips. Add table-driven tests against `Engine` with a fake dispatcher (introduce an interface seam over `doDispatch`) and a fake clock for retry/backoff. Keep root tests as a thin wiring/serialization layer.
- [x] **CI test step: `go test -race ./...`** (`.github/workflows/workflow.yml:35` currently runs `go test -v .`, so `engine/` tests never run in CI and no race detection happens). Do this after the flakiness and race fixes so it goes green immediately. Also change `go build -v .` (line 33) to `./...`.
- [x] **Run conformance in CI**. The conformance module (`conformance/`, separate go.mod, `-tags conformance`) is never invoked by any workflow despite being the source of truth for error mapping. It runs against the committed golden (`conformance/golden/realcloud.json`) without GCP credentials and self-skips when no golden is present: add `go -C conformance test -tags conformance ./...`.

## Phase 3: Workflow and Docker modernization (one mechanical PR)

`.github/workflows/workflow.yml` and `release.yml`:

- [x] **Drop the ghcr.io PAT** (`workflow.yml:69` uses `secrets.GH_CR_IMG_PUSH_PAT`). Use the built-in `GITHUB_TOKEN` with `permissions: packages: write` on the `docker-publish` job. Biggest security win; the standing secret can then be revoked.
- [x] **Update action versions**: `actions/checkout@v2`/`@v3` -> v5 (`workflow.yml:17,40,58`, `release.yml:22`); `docker/setup-buildx-action@v2` -> v3 (`workflow.yml:60`); delete `actions/cache@v2` steps (`workflow.yml:18-24,43-49`) in favor of `setup-go` built-in caching.
- [x] **Go version from go.mod**: replace hardcoded `go-version: 1.26.4` (`workflow.yml:14`) with `go-version-file: go.mod`. Same drift issue in `docker-smoketests/smoketests.sh:30` (hardcoded `golang:1.26.4-alpine`).
- [x] **Delete pre-modules boilerplate**: the "Get dependencies" step (`workflow.yml:25-31`, `go get -d` plus a `Gopkg.toml`/`dep` fallback) is dead weight; optionally replace with `go mod verify`.
- [x] **Baseline hygiene**: top-level `permissions: contents: read` with per-job overrides; `concurrency` group with `cancel-in-progress: true`; `timeout-minutes` on every job (currently unbounded, default 6h).
- [x] **Add `.github/dependabot.yml`**: `github-actions` plus three `gomod` entries (root, `conformance/`, `docker-smoketests/`).
- [x] **Add golangci-lint** (no `.golangci.yml` exists, no lint step in CI). Cover root, `engine/`, and the two nested modules.
- [x] **Dockerfile multi-arch: cross-compile instead of QEMU**. `docker buildx --platform linux/amd64,linux/arm64` (`workflow.yml:84`) has no QEMU setup and the Dockerfile has no cross-compile args. Use `FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder` + `ARG TARGETOS TARGETARCH` + `CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build`.
- [x] **Dockerfile hardening**: non-root user (`adduser -D -u 10001` + `USER`) in the final stage (`Dockerfile:12-22`); pin the `alpine:latest` base; optionally add a `HEALTHCHECK` against the OIDC discovery endpoint.
- [x] **Release pipeline** (`release.yml` uses third-party `wangyoucao577/go-release-action@v1.28`): migrate to GoReleaser triggered on tag push, with checksums and provenance attestation (`actions/attest-build-provenance`). Lower priority than the rest of this phase.

## Phase 4: Docs, scripts, hygiene (quick wins; can also go first)

- [x] **Fix `emulator_from_env.sh`** (verified broken by running it): lines 14/18 pass `-hard_reset_on_purge_queue` / `-openid_issuer` but `emulator.go:57-58` defines hyphenated `-hard-reset-on-purge-queue` / `-openid-issuer`, so both fail with `flag provided but not defined`. Line 29's backtick command substitution swallows the failure and exit status; use `exec` with a properly quoted array instead.
- [x] **Fix README.md**: line 43 documents `HARD_RESET_ON_PURGE` but the script reads `HARD_RESET_ON_PURGE_QUEUE`. Line 182's Go example imports deprecated `google.golang.org/genproto/googleapis/cloud/tasks/v2` (codebase migrated to `cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb` in `e5283c0`) and the snippet doesn't compile as written (bare `NewClient`, missing `option` import). Make it compile-clean.
- [x] **Stage the readme rename**: `readme.MD` is an unstaged deletion, `README.md` is untracked; `git add` both so git records the rename.
- [x] **Untrack `.claude/settings.local.json`** and add it to `.gitignore`.
- [x] **Harden `.gitignore`**: add the `emulator` binary, `*.test`, coverage output.

## Phase 5: Engine design improvements (larger, discuss before doing)

- [ ] **Tombstone expiry** (`engine/engine.go:67-69,84-86,120-144`). Deleted queue/task names are tombstoned forever: names can never be reused (real Cloud Tasks reuses after a cooldown; the emulator's own error messages promise "wait a minute") and the `qs`/`ts` maps grow unbounded. Add timestamped tombstones with a sweep, or explicitly document the divergence in the conformance `KnownDivergences` ledger.
- [x] **Worker pool instead of eager goroutines** (`engine/queue.go:92-94,112-116`). Every queue eagerly starts `MaxConcurrentDispatches` workers (default 1000) regardless of load. Use a semaphore bounding concurrent attempts, or a pool that scales with pending work. Interacts with the Phase 1 cancellation rewrite; consider doing them together.
- [ ] **Propagate gRPC context** (`server.go`): handlers discard `ctx`; thread it into long operations and dispatch.
- [x] **Small cleanups**: drop the dead `retry` parameter on `dispatch` (`engine/dispatch.go:86`); rename `engine/protohelpers.go` to `engine/statuscode.go` (it contains no proto mapping, only `toRPCStatusCode`/`toCodeName`); `interface{}` -> `any` and `time.LoadLocation("UTC")` -> `time.UTC` in `oidc.go`; replace builtin `print` with `fmt.Printf`/logger in `emulator.go:35,81`; range loop in `emulator.go:86-88`.

## Verified fine (do not redo)

- Root vs `engine/` package split: complete, no duplication to consolidate.
- All three go.mod files (root, `conformance/`, `docker-smoketests/`): tidy (`go mod tidy -diff` clean), consistent `go 1.26.4`, aligned shared dep versions. Dependency updates available are routine drift, nothing urgent.
- Module path matches the repo; LICENSE (MIT) fine; `APP_ENGINE_EMULATOR_HOST` docs accurate; README's "Updating of queues" outstanding item still accurate (`server.go:70` `UpdateQueue` unimplemented).
- Conformance harness design (golden snapshots, normalization, `KnownDivergences` ledger) got strong marks; only gap is CI wiring (Phase 2).
- testify usage and assertion helper quality in root tests are good; keep them.
