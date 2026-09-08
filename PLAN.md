# gorch — Remediation Plan

Derived from the full project review (2026-09-08). Every item below was
verified empirically against the current code. Items are grouped into phases
so that non-breaking bug fixes can ship as **v0.3.2** before the breaking API
changes land as **v0.4.0**.

Global verification after every phase:

```bash
gofmt -l .                      # must be empty
go vet ./...
go test ./... -race -coverprofile=coverage.out
go tool cover -func=coverage.out | grep total   # must be 100.0%
go build ./...                  # includes examples/
```

---

## Phase 1 — Critical bug fixes (non-breaking) → v0.3.2

### 1.1 Fix process crash on shutdown (send on closed channel)

**Problem.** `Stop()` closes `logCh` (gorch.go:721) *before* `wg.Wait()`
(gorch.go:731). A service still running (or timing out) that logs afterwards
panics in `ServiceLogger.emit` (service.go:87); the recovery defer then calls
`Logger.Error` to report the panic → panics again on the same closed channel →
unrecoverable, `exit status 2`. Same window in `stopStartedServices`
(gorch.go:642). Violates the "no panics in library code" rule.

**Fix.**

- Never close `logCh`. Add `logQuit chan struct{}` to `Orchestrator`; close
  *that* to signal shutdown. Sending on a channel that is never closed can
  never panic.
- `logPump`: `select` on `logCh` and `logQuit`; on quit, drain remaining
  buffered entries (`for len(o.logCh) > 0`) and return.
- `ServiceLogger.emit`: `select { case ch <- e: case <-quit: default: }`.
  `ServiceLogger` needs a `quit <-chan struct{}` field, set at creation.
- Remove the log-pump from `o.wg`; give it a dedicated `logPumpDone` channel
  so shutdown can wait for the drain without deadlocking (`wg` includes the
  pump today, so `close(logCh)` can never be moved after `wg.Wait()`).
- Apply the same pattern in `stopStartedServices`.

**Files:** `gorch/gorch.go` (Orchestrator struct, New, Start, Stop,
stopStartedServices, logPump), `gorch/service.go` (ServiceLogger, emit).

**Tests:**

- Regression test: service that keeps logging after ctx cancel while Stop
  runs (the verified reproducer) — must exit cleanly.
- Test Stop-timeout path: service that ignores ctx entirely, `Stop(50ms)` —
  no panic, pump exits.
- Test that buffered entries are flushed to stderr before pump exit.

### 1.2 Make crash semantics real (`StatusCrashed`, `OnCrash`, real error)

**Problem.** `handleServiceDone` sets `StatusStopped` — never
`StatusCrashed` — when a persistent service dies. `OnCrash` only fires for
`runOnce` failures, and when it fires it receives a fabricated
`fmt.Errorf("service %s crashed")` instead of the real error (gorch.go:666).
`metricsCrashes` never increments for real crashes.

**Fix.**

- Thread the exit cause through: `runService` / `startOneService`'s goroutine
  already recover panics and see the returned error — capture it
  (`exitErr error`, including `fmt.Errorf("panic: %v", r)` on recover) and
  pass it to `handleServiceDone(entry, sc, exitErr)`.
- In `handleServiceDone`, non-self-heal path, when the orchestrator is NOT
  shutting down:
  - `exitErr == nil` → `StatusStopped` (clean exit), as today.
  - `exitErr != nil` (and not `context.Canceled`) → `StatusCrashed`.
- Self-heal path, `maxRetries` exhausted → `StatusCrashed` (not Stopped).
- Change `setStatus` so the `OnCrash` callback receives the real error:
  add an internal `setStatusErr(entry, status, err)`; `OnCrash(name, err)`
  gets `exitErr`.

**Files:** `gorch/gorch.go`.

**Tests:**

- Update the tests that assert the old behavior: `TestOnCrash`
  (gorch_test.go:3642), `TestOnCrash_WithSelfHeal` (gorch_test.go:4078), and
  any dependency-skip tests relying on `StatusCrashed` semantics
  (gorch_test.go:1690, 3158).
- New: persistent service returns error → status `crashed`, `OnCrash` fires
  with the *actual* error, `Metrics().Crashes == 1`.
- New: service panics → same, error contains the panic value.
- New: clean `nil` return without ctx cancel → `stopped`, no `OnCrash`.
- New: maxRetries exhausted → `crashed`, `OnCrash` fires.

### 1.3 Fix `Start()` idempotence and failed-start bricking

**Problem.** The `ErrAlreadyStarted` branch inside `startOnce.Do` is dead
code — a second `Start()` returns `nil` (verified), contradicting the doc
comment. Worse: after a *failed* `Start()`, a retry returns `nil` without
starting anything and `Register` returns `ErrAlreadyStarted` — the
orchestrator is bricked while reporting success.

**Fix.**

- Hoist the started-check out of `startOnce`:

  ```go
  o.mu.Lock()
  if o.started { o.mu.Unlock(); return ErrAlreadyStarted }
  o.mu.Unlock()
  ```

- Decide policy for failed Start, recommended: **allow retry**. In every
  failure path of `Start` (after `stopStartedServices()`), reset state:
  `o.started = false`, `o.startOnce = sync.Once{}`, `o.stopOnce = sync.Once{}`,
  and reset per-service `wgDone`/status to `StatusRegistered`. This is the
  useful semantics for `runOnce` gates (e.g. waiting for a DB to come up).
- If retry proves too invasive, fallback: return a distinct
  `ErrStartFailedPermanent` on subsequent calls and document it. (Only if the
  reset approach fails review.)

**Files:** `gorch/gorch.go`, `gorch/service.go` (sentinel error if needed).

**Tests:**

- Second `Start()` → `ErrAlreadyStarted`.
- Failed `Start()` then `Register()` → succeeds (not `ErrAlreadyStarted`).
- Failed `Start()` then `Start()` → services actually start.
- `Stop()` after failed `Start()` → clean no-op, no goroutine leak.

### 1.4 Fix or remove `TypedRequest`

**Problem.** Always fails out of the box: `RequestAsync` gob-encodes the
`Message` wrapper as `any` → `gob: type not registered for interface:
gorch.Message`. Tests only pass because each secretly calls
`gob.Register(Message{})` (service_test.go:1242) — documented nowhere. It
also double-encodes (Message inside Message), and the README responder
example (`TypedSubscribe` + `TypedPublish`) cannot interoperate with it.

**Fix (preferred).**

- Auto-register the envelope: call `gob.Register(Message{})` inside
  `newMessenger()`.
- Eliminate double-encoding: extract an internal
  `requestMessage(ctx, wrapper Message, topic)` that publishes the
  already-built `Message` directly (sets `ReplyTopic`, subscribes reply) —
  `RequestAsync` and `TypedRequest` both use it.
- Make the responder story real: add `TypedSubscribeRequest[TReq](m, topic)`
  returning `(ch <-chan TypedEnvelope[TReq], unsub)` where the envelope
  carries the decoded value and `ReplyTopic`, and
  `TypedRespond[TResp](m, resp, replyTopic)`. Rewrite the README example with
  this pair.
- Fallback (ponytail): if the above grows beyond ~80 lines, delete
  `TypedRequest` and document request-reply via `Request` + `RegisterType`.

**Files:** `gorch/typed.go`, `gorch/service.go` (newMessenger),
`README.md`.

**Tests:**

- Remove every `gob.Register(Message{})` crutch from TypedRequest tests —
  they must pass without it.
- End-to-end: `TypedRequest` ↔ `TypedSubscribeRequest`/`TypedRespond` round
  trip, no manual gob anywhere in user code.
- Keep the existing error-path tests (encode error, non-Message response,
  decode error, no responder).

### 1.5 Fix `DependsOnSoft` ordering

**Problem.** `topoSort` only reads `dependsOn`; soft deps are only a
crash-gate at start. Verified: a dependent starts while its soft dep is still
booting, contradicting "start after if present" (doc + README).

**Fix.** In `topoSort`, add edges for `softDependsOn` when the target is
present in the entry set. Cycle note: soft-dep cycles among registered
services will now surface as `ErrDependencyCycle` from `topoSort` at
`Start` — document this; extend the Register-time cycle check to soft edges
only when both ends are already registered (cheap win, no new API).

**Files:** `gorch/gorch.go` (topoSort, Register/dependsOnRecursive).

**Tests:** soft dep present → dependent starts strictly after soft dep's
`Start` returns/enters Running; soft dep missing → no ordering, no error;
soft-dep cycle → detected.

---

## Phase 2 — Smaller bugs & consistency (non-breaking) → v0.3.2

### 2.1 Per-probe health timeout

**Problem.** `runHealthChecks` (gorch.go:1167) and `Health()` (gorch.go:904)
create one `HealthTimeout` context shared across *all* probes, though the doc
says "per-probe deadline". One slow checker fails all later probes with an
expired ctx → false unhealthy → spurious restarts.

**Fix.** Create a fresh `context.WithTimeout` per service inside the loop;
`cancel()` immediately after each probe.

**Tests:** two `HealthChecker`s — first sleeps ~80% of timeout, second is
instant; second must report healthy. Cover `Health()` and the loop.

### 2.2 Self-heal restart data races

**Problem.** The restart path writes `entry.svc`, `entry.logger`,
`entry.cancel` (gorch.go:1360–1377) while `runHealthChecks`, `Health()`, and
`IsReady()` read them without synchronization. `-race` passes only because no
test overlaps a restart with a probe.

**Fix.** Add per-entry `stateMu sync.Mutex` (or funnel all through
`statusMu`) guarding `svc`, `logger`, `cancel`, `retryCount`, `stableSince`,
`healthFailures`; add small accessor helpers (`entry.getSvc()`,
`entry.setSvc()`, …) and use them at every read/write site.

**Tests:** new `-race` test — self-healing service that crashes repeatedly
while `Health()` and the health loop probe concurrently.

### 2.3 `RequestAsync` goroutine leak

**Problem.** The cleanup goroutine (service.go:443) blocks on `<-ctx.Done()`
forever if the caller never cancels — one leaked goroutine per request.

**Fix.** Spawn a forwarding goroutine instead: internal reply channel,
`select { case resp := <-internal: deliver, unsub, return; case <-ctx.Done():
unsub, return }` — it exits on either reply or cancel.

**Tests:** request that gets a reply with a never-cancelled ctx → goroutine
count returns to baseline (use `runtime.NumGoroutine` with eventual
assertion).

### 2.4 Messenger teardown

**Problem.** `Stop()` sets `subs = nil` (via `messengerDone`) — subscriber
channels are never closed, so a service blocked on `<-ch` hangs after
shutdown; a late `Subscribe` would panic on nil-map write. `Drain()` also
needlessly consumes one buffered message per channel before closing.

**Fix.**

- Replace the `messengerDone` cleanup with `o.messenger.Drain()`.
- Simplify `Drain`: close every subscriber channel (receivers drain the
  buffer then see close), clear the map. Do not pre-consume.
- Make `Subscribe`/`SubscribeWithBuffer` safe after Drain: lazy-reinit
  `subs` if nil.
- Document: `Publish` after Drain is a no-op.

**Tests:** subscriber blocked on receive gets channel-close on `Stop()`;
`Subscribe` after `Stop` doesn't panic; Drain with pending buffered messages
delivers them before close.

### 2.5 Cron service status lifecycle

**Problem.** Cron services stay `StatusRegistered` forever (never transition
on ticks), yet `Stop()` runs stop-hooks on them as if they had started.

**Fix.** After `cronSched.Start()`, set cron entries to `StatusRunning`; in
`Stop()`, after `cronSched.Stop()`, transition them `StatusStopping →
StatusStopped` via the existing `stopOneService` path (which already calls
their hooks). Document that cron services receive `Stop()` once at shutdown.

**Tests:** status of a cron service is `running` after Start, `stopped`
after Stop; hooks fire exactly once.

### 2.6 Log-level filtering at emit, not just the pump

**Problem.** Filtering happens in `logPump` — a Debug flood fills the
256-entry buffer and causes INFO/ERROR messages to be silently dropped
(service.go:88).

**Fix.** Give `ServiceLogger` a `minLevel LogLevel` (set in `Start` from
`cfg`) and drop below-min entries in `emit` before the channel send. Keep the
pump check as a harmless backstop or remove it.

**Tests:** with level=Info, flood 1000 Debug + 1 Error from a service →
Error always appears on stderr.

### 2.7 `Run()` default signals

**Problem.** README says "blocks until SIGINT/SIGTERM"; default is only
`os.Interrupt` (gorch.go:829).

**Fix.** Default `sigSet = []os.Signal{os.Interrupt, syscall.SIGTERM}` —
matches the doc and universal expectation. Update the doc comment.

**Tests:** send `SIGTERM` to own process in a subprocess test (or refactor
to make the signal set injectable — it already is via variadic arg; test the
default via docs/example only if subprocess test is flaky on CI).

### 2.8 Nits (batch into one commit each)

- `go mod tidy` — drop the bogus `// indirect` on robfig/cron.
- Remove the no-op `entry := entry` loop copies (Go ≥1.22 semantics).
- Delete `gobBuf`; use `bytes.Buffer` directly in `RequestAsync` (its own
  ponytail comment admits this).
- Deduplicate the unsubscribe closure: `Subscribe(topic)` calls
  `SubscribeWithBuffer(topic, 16)`.
- `Status`/`Names`/`Count`/`Statuses`: use read locks (`mu` → `sync.RWMutex`
  or RLock where only reading).
- `Done()` spawns a goroutine per call — cache the channel lazily with
  `sync.OnceValue` like `messengerDone` does.
- Document that health-based restart requires `WithSelfHeal`; without a
  factory, failures only log/increment metrics (README currently implies
  auto-restart for everyone).

---

## Phase 3 — Breaking API cleanup → v0.4.0

These change the public surface; bundle them into one release with a
migration section in the release notes.

### 3.1 `Config` struct → functional options

**Problem.** `Config` has 14 fields (8 of them hooks) — violates the CLAUDE.md
"functional options, never a 12-field config struct" rule. It also makes two
bugs unfixable without breakage:

- `LogLevelDebug == 0`, so `New()` treats it as unset and overwrites it with
  `LogLevelInfo` (gorch.go:168) — **Debug is unselectable** (verified;
  `examples/advanced` sets it and silently gets Info).
- `HealthInterval == 0` is documented as "disables health checks" but
  `New()` rewrites it to 30s (gorch.go:172) — **health checks can never be
  disabled** (verified).

**Fix.**

- `gorch.New(opts ...gorch.Option)` with options:
  `WithLogger(l)`, `WithLogLevel(lvl)` (absent ⇒ Info; Debug selectable),
  `WithDefaultStartTimeout(d)`, `WithHealthChecks(interval, timeout,
  threshold)` and `WithHealthChecksDisabled()` (explicit disable),
  `WithOnBeforeStart(fn)`, … one per hook.
- Keep the zero-option call equivalent to today's defaults.
- Delete the `Config` struct (or keep it deprecated for one release if
  migration pain is a concern — decide before v0.4.0).
- Update CLAUDE.md if the conventions section needs to reflect the new
  constructor pattern.

**Tests:** `WithLogLevel(LogLevelDebug)` actually enables debug output;
`WithHealthChecksDisabled()` → no health goroutine; all existing behavior
tests ported to options.

### 3.2 `Service.Start` takes `ServiceContext`

**Problem.** `Service.Start(ctx context.Context)` actually requires a
`ServiceContext`; every user must do the unsafe `ctx.(gorch.ServiceContext)`
assertion (all examples do). The interface lies about its contract.

**Fix.** Change the interface to:

```go
type Service interface {
    Start(ctx ServiceContext) error
    Stop() error
}
```

`ServiceContext` embeds `context.Context`, so existing `<-ctx.Done()` /
`ctx.Err()` bodies keep compiling unchanged — only the method signature
changes. Removes the `funcService` wrapper cast and the
`ctx.(ServiceContext)` in `RegisterFunc`.

**Tests/docs:** update all examples; add a compile-time assertion
`var _ Service = (*funcService)(nil)`.

---

## Phase 4 — Structure & tooling

### 4.1 Split oversized files (CLAUDE.md: ~400 lines/file)

Current: `gorch.go` 1418 lines, `service.go` 488 lines. Target layout (no
behavior change, pure moves — do this *after* Phases 1–3 so diffs stay
reviewable):

| File | Contents |
|---|---|
| `gorch.go` | `Orchestrator` struct, `New`, `Register`, `Start`, `Stop`, `Run` |
| `lifecycle.go` | `startOneService`, `stopOneService`, `handleServiceDone`, `runService`, `stopStartedServices`, hooks, `setStatus` |
| `cron.go` | cron setup in Start (extract helper), `invokeCron`, `CronMode` |
| `health.go` | existing interfaces + `healthCheckLoop`, `runHealthChecks`, `Health()` |
| `status.go` | `Status`, `Statuses`, `Names`, `Count`, groups/labels queries, `WaitFor`, `IsReady`, `Metrics`, `Done`, `topoSort` |
| `log.go` | `LogLevel`, `logEntry`, `ServiceLogger`, `logPump`, `Logger` interface |
| `messenger.go` | `Messenger`, subscribe/publish/request/drain, `newUUID` |
| `service.go` | `Service`, `ServiceContext`, `registerConfig` + options, sentinel errors, `funcService` |
| `typed.go` | unchanged |
| `backoff.go` | unchanged |

### 4.2 CI

- Add `-race` to the test step.
- Add `go build ./...` (compiles `examples/`).
- Coverage gate already enforces 100% on tag builds; also run it on
  `workflow_dispatch` (already covered) — verify Phase 1 fixes restore
  100.0% (currently 99.9%, one branch in `stopStartedServices`; the 1.1
  rewrite removes that branch — ensure the new code is fully covered).
- Format workflow: restrict `git-auto-commit` to non-PR pushes (it currently
  commits on every push, noisy on fork PRs).

### 4.3 Docs & release hygiene

- README: fix the SIGINT/SIGTERM claim (subsumed by 2.7), rewrite the
  TypedRequest section (1.4), clarify health-restart requires self-heal
  (2.8), update the custom-logger/Config section after 3.1.
- Add `CHANGELOG.md` (Keep a Changelog format); backfill v0.1.0–v0.3.1 from
  git history, then document v0.3.2 and v0.4.0.
- Doc-test drift guard: after 1.4, the request-reply README snippets should
  mirror a compiling example — add `examples/typedreq/` or fold into
  `examples/pubsub/`.

---

## Execution order & milestones

1. **v0.3.2** — Phases 1 + 2. All bug fixes, no API breakage. Tag after CI
   passes with `-race` and 100% coverage.
2. **v0.4.0** — Phase 3 (breaking) + Phase 4. Release notes must include a
   migration guide (`New(Config{...})` → `New(With...(…))`, `Start(ctx
   context.Context)` → `Start(ctx ServiceContext)`).
3. Update `CLAUDE.md` if any conventions change (constructor pattern, file
   layout table can be added to "Structure rules").

## Explicitly out of scope (ponytail)

- Parallel `stopStartedServices` — sequential is fine at current scale.
- Health probes running concurrently with each other — serial with
  per-probe timeout is bounded enough.
- Backoff jitter — existing ponytail comment covers it; add only if
  thundering-herd reports appear.
- Replacing gob with another codec — keep stdlib-only.
