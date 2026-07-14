# Code review findings

Review of the main codebase (engine, server, emulator, oidc, maybe, cmd), 2026-07-14.
Tests and CI were out of scope. Work through these one by one; check items off as they land.

## Major findings

### 1. Unvalidated queue rate limits can panic and crash the emulator

- [x] Status: done. `validateQueueConfig` (queue.go) rejects out-of-range
  `RateLimits`/`RetryConfig` at `CreateQueue` with per-violation sentinels
  mapped to InvalidArgument at the server edge; messages verified against the
  re-recorded conformance golden (`queue-invalid-config` cases), which also
  showed real v2 ignores client-supplied `max_burst_size` (output-only), so the
  proto edge now drops it instead of rejecting it. Token-generator period now
  computed in float64 (fractional rates work, overflow saturates). Adjacent
  fixes the validation exposed: `MaxAttempts` -1 is honored as unlimited in
  `reschedule`, and the backoff doubling term saturates instead of overflowing
  `time.Duration` for large `MaxDoublings`.
- Severity: high (process crash from client input)
- Locations: `internal/engine/queue.go:97,105,173`

Two distinct crash paths, plus a correctness bug, all from client-supplied
`RateLimits` values that are never validated:

- `newQueue` does `make(chan bool, MaxBurstSize)` and
  `make(chan struct{}, MaxConcurrentDispatches)` directly from client input.
  A negative value panics inside the gRPC handler goroutine; grpc-go does not
  recover panics, so one bad `CreateQueue` kills the process.
- `runTokenGenerator` computes `time.Second / time.Duration(maxDispatchesPerSecond)`.
  The float64-to-Duration conversion truncates to integer nanoseconds, so a
  fractional rate such as `0.5` (legal in real Cloud Tasks) becomes `0` and the
  division panics in a background goroutine, crashing the process.
- Non-panicking fractional rates are silently wrong: `1.5` behaves as `1.0`.
  The period should be computed as `time.Duration(float64(time.Second) / rate)`.

Fix direction: validate `RateLimits`/`RetryConfig` at `CreateQueue` (real Cloud
Tasks rejects out-of-range values with InvalidArgument) and fix the float
period arithmetic.

### 2. Task schedule goroutine's send to the queue is not cancellable

- [ ] Status: open
- Severity: high (goroutine leak, delete does not stick, hard reset hangs)
- Location: `internal/engine/task.go:270`

Once the schedule timer fires, the goroutine commits to a bare blocking
`task.queue.fire <- task` with no `select` on `task.cancel`. On a paused queue
(no dispatcher generation receiving) that goroutine blocks indefinitely.
Consequences:

- `DeleteTask` on such a task cannot stop it; after `Resume` the deleted task
  is dispatched anyway.
- `Delete`/`Stop` of a paused queue leaks the goroutine permanently. This
  matters for the embedded `emulator` package, where test suites create and
  close many emulators in one process.
- Hard-reset `PurgeQueue` blocks on `<-task.done` until the caller's ctx
  expires, because `markDone` never runs.

Fix direction: make the send cancellable:

```go
select {
case task.queue.fire <- task:
case <-task.cancel:
    task.markDone()
}
```

### 3. RunTask double-dispatches

- [ ] Status: open
- Severity: high (behavioral: forced task runs twice)
- Locations: `internal/engine/task.go:242`, `internal/engine/engine.go:683`

`Task.Run` dispatches immediately but never disarms the pending `Schedule`
goroutine. A task scheduled for the future runs now via `RunTask` and again at
its original schedule time. On a successful forced run, `markDone` fires, yet
the armed goroutine still pushes the completed task into `queue.fire` later,
and the dispatcher attempts it again; `reschedule(retry=true)` can then keep
retrying it. Real Cloud Tasks resets the schedule so the task runs once.

Fix direction: `Run` must take over the task's single pending schedule (disarm
the goroutine, e.g. via the cancel channel plus re-arm bookkeeping, or by
restructuring scheduling so there is one owner of "next fire").

### 4. Retry backoff is anchored to the previous schedule time

- [ ] Status: open
- Severity: medium (hot retry loop against slow/timing-out targets)
- Location: `internal/engine/dispatch.go:82`

`updateStateForReschedule` sets the next `ScheduleTime` to the old
`ScheduleTime` plus backoff. When attempts take long (worst case a
dispatch-deadline timeout, 600s by default), the computed time is already in
the past, so `time.After` fires immediately and the target gets hammered with
effectively zero backoff until doubling catches up. Backoff should be measured
from the failure time (attempt completion), matching real Cloud Tasks.

### 5. Check-then-insert races in CreateQueue and CreateTask

- [ ] Status: open
- Severity: medium (duplicate names under concurrency, leaked queue goroutines)
- Locations: `internal/engine/engine.go:424` vs `:434`, `:625` vs `:652`

Both creates do an existence check and a later insert under separate lock
acquisitions. Two concurrent creates with the same name both succeed. For
queues, the loser's token-generator and dispatcher goroutines run forever with
no `Delete` path (permanent leak). For tasks, both copies get scheduled and
dispatch.

Fix direction: make the uniqueness check and insert one atomic operation under
`qsMux`/`tsMux` (a set-if-absent that also consults the tombstone map).

### 6. Queue-name validation is loose

- [ ] Status: open
- Severity: medium (accepts invalid names real Cloud Tasks rejects)
- Location: `internal/engine/engine.go:416,420`

- The `CreateQueue` regexes are unanchored, so
  `junk/projects/a/locations/b/queues/c/junk` passes.
- There is no check that the queue name falls under `parent` (unlike
  `CreateTask`, which does the prefix check).
- The regexes are recompiled on every call; `task.go` correctly uses
  package-level compiled patterns.

## Minor findings

### 7. AppEngineEmulatorHost parse panic in the request path

- [ ] Status: open
- Location: `internal/engine/task.go:211`

`setInitialTaskState` panics on an unparseable `AppEngineEmulatorHost`. That is
an operator flag, but the panic happens at first App Engine task creation
inside a request handler and crashes the server. Validate the flag at startup
in `main` (and at `engine.New` for embedded use).

### 8. Inconsistent clock injection

- [ ] Status: open
- Locations: `internal/engine/task.go:144,146`, `internal/engine/dispatch.go:89,132`, `internal/oidc/token.go:94`

The engine has an injectable `now` for tombstones, but `setInitialTaskState`,
`updateStateForDispatch`, `updateStateAfterDispatch`, and `oidc.CreateToken`
call `time.Now` directly, so task/attempt timing is untestable with a fake
clock.

### 9. Stale Purge comment and dead return value

- [ ] Status: open
- Location: `internal/engine/queue.go:296`

`Queue.Purge`'s comment says it returns a `WaitGroup` "to allow HardReset to
wait", but `hardResetQueue` does not use `Purge` and no caller consumes the
return value.

### 10. Auto-generated task IDs skip uniqueness/tombstone checks

- [ ] Status: open
- Location: `internal/engine/task.go:139`

Names generated from `rand.Uint64` bypass the exists/recently-deleted checks
that client-named tasks get; a collision would silently clobber the existing
task. Astronomically unlikely, but routing both cases through the same
reservation logic removes the gap for free (and composes with finding 5's
set-if-absent).

### 11. Unlocked reads of queue.state.RetryConfig from the task path

- [ ] Status: open
- Location: `internal/engine/dispatch.go:77,169`

Safe today only because `UpdateQueue` is unimplemented and those fields are
never written after construction. If `UpdateQueue` is ever implemented this
becomes a data race. Add a comment or a snapshot accessor now.

### 12. Embedded emulator option surface is thinner than the binary's

- [ ] Status: open
- Location: `emulator/emulator.go`

No `With*` option for the App Engine emulator host, region ID, or OIDC config,
all of which the binary exposes as flags. Embedded test users of App Engine
targets would plausibly want these.

## Architecture notes (no action required, context for the fixes)

- The task registry is duplicated between `engine.ts` and each `queue.ts`, with
  two different tombstone conventions (timestamp map at the engine, nil entries
  in the queue map). It works, but it is the most intricate part of the design;
  a single registry keyed by name with an entry state would remove a class of
  "who owns this entry" reasoning. Findings 2, 3, 5 and 10 all touch this
  machinery; if fixing them gets awkward, consolidating the registry first may
  be the cheaper path.
- Strengths to preserve while fixing: the engine/server layering with sentinel
  errors mapped at the edge, the `maybe.M` optionality convention, the
  channel-generation pause/resume design with the queue-level semaphore shared
  across generations, the `markDone`-before-`done`-close ordering guarantee,
  and the conformance-golden-driven fidelity (error messages, header rules,
  the two execution-count families).
