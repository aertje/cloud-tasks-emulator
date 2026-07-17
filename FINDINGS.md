# Code review findings

Review of the main codebase (engine, server, emulator, oidc, maybe, cmd), 2026-07-14.
Tests and CI were out of scope. Work through these one by one; check items off as they land.

## Major findings

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

- [ ] Status: partially done (dispatch path fixed with finding 2)
- Locations: `internal/engine/task.go:144,146`, `internal/oidc/token.go:94`

The engine has an injectable `now` for tombstones. The dispatch-path callers
(`updateStateForDispatch`, `updateStateForReschedule`, `updateStateAfterDispatch`)
now use the queue's threaded `now` clock (done alongside finding 2). Still
outstanding: `setInitialTaskState` and `oidc.CreateToken` call `time.Now`
directly, so task creation timing and OIDC token `iat`/`exp` remain untestable
with a fake clock.

### 7. Stale Purge comment and dead return value

- [ ] Status: open
- Location: `internal/engine/queue.go:296`

`Queue.Purge`'s comment says it returns a `WaitGroup` "to allow HardReset to
wait", but `hardResetQueue` does not use `Purge` and no caller consumes the
return value.

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
  "who owns this entry" reasoning. The now-resolved create-race and
  auto-generated-name findings touched this machinery; the engine map is now the
  atomic reservation authority (insertQueueIfAbsent/insertTaskIfAbsent), but the
  duplication between the engine and queue maps remains.
- Strengths to preserve while fixing: the engine/server layering with sentinel
  errors mapped at the edge, the `maybe.M` optionality convention, the
  channel-generation pause/resume design with the queue-level semaphore shared
  across generations, the `markDone`-before-`done`-close ordering guarantee,
  and the conformance-golden-driven fidelity (error messages, header rules,
  the two execution-count families).
