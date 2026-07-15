# Code review findings

Review of the main codebase (engine, server, emulator, oidc, maybe, cmd), 2026-07-14.
Tests and CI were out of scope. Work through these one by one; check items off as they land.

## Major findings

### 2. Retry backoff is anchored to the previous schedule time

- [ ] Status: open
- Severity: medium (hot retry loop against slow/timing-out targets)
- Location: `internal/engine/dispatch.go:82`

`updateStateForReschedule` sets the next `ScheduleTime` to the old
`ScheduleTime` plus backoff. When attempts take long (worst case a
dispatch-deadline timeout, 600s by default), the computed time is already in
the past, so `time.After` fires immediately and the target gets hammered with
effectively zero backoff until doubling catches up. Backoff should be measured
from the failure time (attempt completion), matching real Cloud Tasks.

### 3. Check-then-insert races in CreateQueue and CreateTask

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

### 4. Queue-name validation is loose

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

### 5. AppEngineEmulatorHost parse panic in the request path

- [ ] Status: open
- Location: `internal/engine/task.go:211`

`setInitialTaskState` panics on an unparseable `AppEngineEmulatorHost`. That is
an operator flag, but the panic happens at first App Engine task creation
inside a request handler and crashes the server. Validate the flag at startup
in `main` (and at `engine.New` for embedded use).

### 6. Inconsistent clock injection

- [ ] Status: open
- Locations: `internal/engine/task.go:144,146`, `internal/engine/dispatch.go:89,132`, `internal/oidc/token.go:94`

The engine has an injectable `now` for tombstones, but `setInitialTaskState`,
`updateStateForDispatch`, `updateStateAfterDispatch`, and `oidc.CreateToken`
call `time.Now` directly, so task/attempt timing is untestable with a fake
clock.

### 7. Stale Purge comment and dead return value

- [ ] Status: open
- Location: `internal/engine/queue.go:296`

`Queue.Purge`'s comment says it returns a `WaitGroup` "to allow HardReset to
wait", but `hardResetQueue` does not use `Purge` and no caller consumes the
return value.

### 8. Auto-generated task IDs skip uniqueness/tombstone checks

- [ ] Status: open
- Location: `internal/engine/task.go:139`

Names generated from `rand.Uint64` bypass the exists/recently-deleted checks
that client-named tasks get; a collision would silently clobber the existing
task. Astronomically unlikely, but routing both cases through the same
reservation logic removes the gap for free (and composes with finding 3's
set-if-absent).

### 9. Unlocked reads of queue.state.RetryConfig from the task path

- [ ] Status: open
- Location: `internal/engine/dispatch.go:77,169`

Safe today only because `UpdateQueue` is unimplemented and those fields are
never written after construction. If `UpdateQueue` is ever implemented this
becomes a data race. Add a comment or a snapshot accessor now.

### 10. Embedded emulator option surface is thinner than the binary's

- [ ] Status: open
- Location: `emulator/emulator.go`

No `With*` option for the App Engine emulator host, region ID, or OIDC config,
all of which the binary exposes as flags. Embedded test users of App Engine
targets would plausibly want these.

### 11. Task size limits are not validated at CreateTask

- [ ] Status: open
- Severity: minor (fidelity gap only; cannot crash the emulator)
- Location: `internal/engine/task.go` (`validateTaskConfig`)

Real Cloud Tasks caps task size at 100KB for App Engine targets and 1MB for
HTTP targets, enforced with InvalidArgument at `CreateTask`. The emulator
accepts any size. Deferred out of the (now resolved) task-config validation
work because the measured "size" (which fields count, and how) needs probing
against real Cloud Tasks before it can be enforced correctly.

## Architecture notes (no action required, context for the fixes)

- The task registry is duplicated between `engine.ts` and each `queue.ts`, with
  two different tombstone conventions (timestamp map at the engine, nil entries
  in the queue map). It works, but it is the most intricate part of the design;
  a single registry keyed by name with an entry state would remove a class of
  "who owns this entry" reasoning. Findings 1, 3 and 8 all touch this
  machinery; if fixing them gets awkward, consolidating the registry first may
  be the cheaper path.
- Strengths to preserve while fixing: the engine/server layering with sentinel
  errors mapped at the edge, the `maybe.M` optionality convention, the
  channel-generation pause/resume design with the queue-level semaphore shared
  across generations, the `markDone`-before-`done`-close ordering guarantee,
  and the conformance-golden-driven fidelity (error messages, header rules,
  the two execution-count families).
