# gorch — Go Orchestrator Library

Manage goroutine lifecycles — start, stop, cron scheduling, pub-sub messaging, dependency ordering, health checks, and self-healing — with a small, composable API.

```go
import "github.com/lorenzo-vecchio/gorch"
```

## Install

```bash
go get github.com/lorenzo-vecchio/gorch@latest
```

Requires Go 1.25+.

## Features

- **Service lifecycle** — Start/Stop with context cancellation and graceful shutdown.
- **Dynamic membership** — `Register` adds a service to a running orchestrator, and `StartService`/`StopService`/`Unregister` start, stop, and remove services while it runs; `WithCascadeStop` extends a removal to hard dependents.
- **Run() convenience** — single call starts, blocks on OS signals, then stops.
- **Dependency ordering** — declare dependencies with `DependsOn`, cycle detection at registration, topological start and reverse-topological stop.
- **Start timeout** — per-service start deadline via `WithStartTimeout`, with a `DefaultStartTimeout` config default.
- **Bounded failed-Start rollback** — `WithFailedStartTimeout` bounds the cleanup of a failed `Start` (default 30s), unwinding the services that had started in reverse topological order so a dependency is never stopped before its dependent; a service that blocks in `Stop()` does not hang it; a negative value removes the bound.
- **Cron scheduling** — 6-field cron (seconds included) with three concurrency modes: Parallel, Queue, Skip.
- **Pub-sub Messenger** — topic-based messaging between services (Socket.IO rooms style), non-blocking sends, request-reply, and typed messages.
- **Self-healing** — auto-restart crashed services with a factory-provided fresh instance and configurable backoff/retry.
- **Health checks** — `HealthChecker` interface; orchestrator probes services on configurable intervals, auto-restarts unhealthy services.
- **Backoff & retry** — `ExponentialBackoff` and `ConstantBackoff` strategies, max retries, stability-window retry reset.
- **One-shot services** — init/gate tasks that run once before persistent services; `Stop()` is called at shutdown.
- **Lifecycle hooks** — `OnBeforeStart`, `OnAfterStart`, `OnBeforeStop`, `OnAfterStop` (global or per-service overrides).
- **Status introspection** — `Status`, `Statuses`, `Names`, `Count`, `CountRunning`, `RunningNames`, and `Busy(name)` for runtime observability. `Count`/`Names`/`Statuses` report every *registered* entry; `CountRunning`/`RunningNames` report the `StatusRunning` subset; `Busy(name)` polls an in-flight membership reservation, which status does not encode.
- **Error aggregation** — `errors.Join` in `Start`/`Stop` so all failures are reported, not just the first.
- **Nestable orchestrators** — a service can create its own gorch for sub-services.
- **Structured logging** — channel-based log-pump writes to stderr; services call `Info/Error/Debug/Warn` on a `ServiceLogger`.
- **Custom logger** — inject any logger satisfying the `Logger` interface (e.g. `*slog.Logger`); service name is prepended as a key-value pair to every call.
- **RegisterFunc** — closure-based services for simple cases; no boilerplate struct needed.
- **Service groups** — `WithGroup`, `StartGroup`, `StopGroup`, `StatusesByGroup` for operating on subsets.
- **Labels** — `WithLabel` + `StatusesByLabel` for metadata filtering.
- **Soft dependencies** — `DependsOnSoft` for optional service ordering; start after if present, no error if missing.
- **Readiness checks** — `ReadinessChecker` interface + `IsReady()`; separate "alive" from "ready to serve."
- **State-change hooks** — `OnStateChange` + `OnCrash` callbacks for external observability without polling.
- **WaitFor** — Block until a service reaches a target status.
- **TypedRequest** — Typed request-reply without losing type safety: `TypedRequest[TReq, TResp](messenger, ctx, req, topic)`.
- **Metrics** — atomic int64 counters (`Starts`, `Stops`, `Crashes`, `Restarts`, `HealthFails`, `AbandonedGoroutines`), exposed via `Metrics()` snapshot.
- **Validator interface** — `Validate() error` called at `Register` for early config checks.
- **WithStartCondition** — Skip a service at runtime via a `func() bool`.
- **Per-service stop timeout** — `WithStopTimeout` controls how long to wait for `Stop()`.
- **Configurable channel buffer** — `SubscribeWithBuffer` for the Messenger.
- **Health check hooks** — `BeforeHealthCheck` / `AfterHealthCheck` for instrumenting probes.
- **Messenger.Drain** — Gracefully close all subscriber channels and clear subscriptions.
- **Done() channel** — Non-blocking shutdown notification; closes once `Stop`/`Run` returns (a shutdown-completed signal, not a goroutine count).

## Concurrency

All methods are safe to call from multiple goroutines unless noted otherwise. The
table below summarizes what may run concurrently with a live `Start`/`Stop`.

| Method group | Concurrent with `Start`/`Stop` |
|--------------|-------------------------------|
| `Register`, `RegisterFunc` | Before `Start` the registry is static (whole graph validated at once). A hot add made while `Start` runs lands either in `Start`'s snapshot or, after it, live as `StatusRegistered` (never auto-started); before `Start` it is part of the static graph. It is rejected with `ErrOrchestratorStopping` while `Stop` runs and `ErrOrchestratorStopped` after `Stop`. |
| `StartService` | Yes — starts a registered service once every hard dependency is `StatusRunning`; an already-`Running` persistent/cron entry is a no-op (a `runOnce` entry is the deliberate re-run exception). It never restarts a live instance: to replace one, `StopService` and then `StartService` — and `StopService` is itself refused with `ErrHasDependents` while a hard dependent is `Running` or `Starting`, unless `WithCascadeStop` is used, so restarting a depended-on service bounces those dependents first. The start decision and its reservation are claimed atomically under the membership lock (released before any user code), so concurrent `StartService` calls cannot double-start or orphan an instance. Returns `ErrOrchestratorNotStarted` before `Start`, and is rejected with an error once whole-orchestrator `Stop` has begun. A hard dependency must have been registered before the dependent, both statically and on a hot add. Colliding with another goroutine's reservation returns the transient `ErrMembershipBusy` (poll `Busy`); re-entering from the entry's own `Start` returns `ErrReentrantMembership`. |
| `StopService`, `Unregister` | Yes — stop (keep registered) or stop-and-remove a service while the lifecycle runs. Serialized against each other and against `StartGroup`/`StopGroup` by the membership lock. A hard dependent that is `Running` or `Starting` blocks the call with `ErrHasDependents`, and the error names each blocker with its status (e.g. `api (starting)`); every other dependent status — `Stopping`, `Registered`, `Crashed`, `Stopped`, `Succeeded` — does not block, so a plain stop proceeds and leaves the dependent untouched. With `WithCascadeStop`, every transitive hard dependent is stopped/removed in reverse topological order, except one already `Stopping`, which is left to its own in-flight teardown rather than stopped a second time (no double `Stop()` or hooks). A collision with another goroutine's in-flight reservation returns the transient `ErrMembershipBusy` (poll `Busy`), while a re-entry from the target's own `Start`/`Stop` returns `ErrReentrantMembership`. |
| `Start`, `Stop` | Yes — against each other. Guarded by `sync.Once`; the **whole-orchestrator lifecycle** is single-shot, so after a successful `Stop` neither can run again. `Stop`'s `timeout` bounds the whole shutdown, including every service's before/after-stop hooks and `Stop()` call. |
| `Status`, `Statuses`, `Names`, `Count`, `CountRunning`, `RunningNames`, `Busy` | Yes — safe to read while services run, while membership churns, and during shutdown. `Count`/`Names`/`Statuses` are the **registered** surface (a hot-added, not-yet-started entry and a staged cron entry both appear as `StatusRegistered`); `CountRunning`/`RunningNames` are the `StatusRunning` subset, so `"N of M running"` is `CountRunning()` of `Count()`. `Busy(name)` is the predicate for the transient `ErrMembershipBusy`: it reports whether a registered entry currently holds an in-flight reservation, and is the only observable for the reservation window. |
| `Health`, `IsReady`, `WaitFor` | Yes — each probe/tick takes its own read lock; `IsReady` honors the caller's `ctx`. |
| `Metrics`, `Done` | Yes — atomic counters and a shutdown-completed channel created once in `New` and returned unchanged. |
| `StartGroup`, `StopGroup` | Drive one group per orchestrator. Serialized with `StopService`/`Unregister` by the membership lock; a group start reserves each member and rolls back the members it already started if a later one fails, and a group stop honours a concurrent start's reservation by skipping that entry. Both are gated by shutdown (`ErrOrchestratorStopping`/`ErrOrchestratorStopped`). Group ops are not synchronized with the whole-orchestrator `Start`/`Stop` beyond that shutdown gate: do not drive them from inside a concurrent `Start`/`Stop`. |
| `Messenger` (`Subscribe`, `Publish`, `Request`, `RequestAsync`, `Drain`) and the typed helpers | Yes — all Messenger methods are safe for concurrent use. |

Service implementations are responsible for their own internal concurrency:
`Start` runs in its own goroutine and `Stop` may be called from another after
context cancellation.

## Contract

These guarantees are part of the public API and are relied upon by callers.

- **Wire format is gob.** `encoding/gob` is the serialization format for typed
  messages and request-reply payloads. It is public contract: types passed
  through `TypedPublish`/`TypedSubscribe`/`TypedRequest`/`TypedRespond` must be
  gob-compatible, and changing a field layout is a breaking change.
- **`Publish` is drop-only.** Delivery is best-effort. When a subscriber's
  channel is full the message is dropped for that subscriber; gorch never blocks
  the publisher and never replays dropped messages.
- **Membership is dynamic.** Before `Start`, registration is static. After a
  successful `Start`, `Register` adds a service to the live graph as
  `StatusRegistered` (it is not auto-started), and `StartService`,
  `StopService`, and `Unregister` start, stop, and remove services while the
  orchestrator runs. `StopService` keeps the entry registered so it can be
  started again; `Unregister` removes it from the graph. Both cancel the
  service's context and release its Messenger subscriptions, which are scoped to
  the instance or tick that created them; a persistent service's instance and any
  in-flight cron ticks are then awaited until they exit. A tick abandoned for
  outliving the deadline still has its subscriptions released: teardown drains
  every live owner id of the entry, so the release its abandoned goroutine skips
  cannot leak the id. Owner ids are monotonic and never reused, so a drained id
  is never minted into a later view. `Unregister` also drops
  the cron schedule and discards the entry's per-instance state — instance and
  teardown contexts, exit channel, retry/health counters, stability window,
  liveness/accounting flags, owner ids, and cron accounting — from both the
  `entries` slice and the name index, using the same "fresh" definition as a
  failed-`Start` retry. Re-registering the same name therefore creates a
  brand-new entry that inherits nothing from the previous incarnation (a
  monotonic auto-name counter means `$N` is never reused either). A stop is refused
  with `ErrHasDependents` while a hard dependent is `Running` or `Starting` (the
  error names each blocker and its status), unless `WithCascadeStop` is passed;
  a dependent that is `Stopping`, `Registered`, `Crashed`, `Stopped`, or
  `Succeeded` does not block, and a cascade leaves an already-`Stopping`
  dependent to its own teardown instead of stopping it twice. Soft dependencies
  never block and are never cascaded. A membership
  op re-entered from the target's own `Start`/`Stop` returns
  `ErrReentrantMembership` (a programming error), while one colliding with
  another goroutine's in-flight reservation returns the retryable
  `ErrMembershipBusy`; `Busy(name)` observes the latter without racing.
- **Whole-orchestrator lifecycle is single-shot.** After a successful `Stop`,
  neither `Start` nor `Register` can be used again; `Start` returns
  `ErrAlreadyStarted` and `Register` returns `ErrOrchestratorStopped`. A failed
  `Start` does not consume the lifecycle and may be retried.
- **Errors are aggregated.** `Start` and `Stop` join every failure with
  `errors.Join`, so a single call reports all causes, not just the first. Use
  `errors.Is`/`errors.As` to inspect them.
- **No user code runs under an orchestrator lock.** The orchestrator never calls
  service code — `Start`, `Stop`, `Validate`, health/readiness probes, or any
  lifecycle hook — while holding `o.mu` or `membershipMu`. A hook or `Validate`
  may therefore block or call back into a membership operation without
  deadlocking. This is enforced as a class-wide invariant (static and dynamic
  registration, `Health` and the periodic probe path, `Start`, and group
  selection), not per call site.
- **A misbehaving callback does not panic the caller.** A panic raised by a
  lifecycle hook, `Validator`, start condition, or health/readiness probe is
  recovered and reported as an error (or as unhealthy/unready); it never unwinds
  through a public entry point. `Register(nil, …)` and a nil `RegisterFunc`
  `startFn` are rejected with `ErrNilService` rather than deferred to a panic at
  `Start`.
- **A blocked before-stop hook cannot strand a service.** `Stop`'s budget is
  split so the hook gets at most half of what remains; the service's own `Stop()`
  is always invoked, and a hook that overruns is reported as `ErrHookTimeout`
  (also matching `ErrStopTimeout`). A hook that never returns cannot consume the
  entire deadline and leave the service's resources unreleased.
- **A failed `Start` rolls back within a bounded budget.** The cleanup stops the
  already-started services and then waits for their instance and log-pump
  goroutines under one budget shared by every step (`WithFailedStartTimeout`,
  default 30s), mirroring `Stop`. A service that ignores cancellation and blocks
  in `Stop()` is reported as `ErrStopTimeout` and does not hang `Start` — unless
  `WithFailedStartTimeout` is set negative, which removes the bound. The reset
  that follows is best-effort: a goroutine that ignores cancellation may outlive
  the failed `Start`, but the orchestrator is left restartable so `Start` can be
  retried.
- **A timed-out stop is reported honestly.** A stop that does not finish inside
  the caller's timeout — because a hook overran, `Stop()` was still running, or
  the instance had not exited — leaves the entry `StatusStopping`, not
  `StatusStopped`, and does not increment `Metrics().Stops`. The terminal
  `StatusStopped` is committed only once the whole teardown is verified complete,
  so `Status()`/`Statuses()` never claim a service stopped while it may still be
  alive. `WaitFor(name, StatusStopped, …)` therefore does not succeed for a
  timed-out stop.

Sentinel errors returned by the orchestrator:

| Error | Returned by | Meaning |
|-------|-------------|---------|
| `ErrAlreadyStarted` | `Start` | Called after the orchestrator already started (including after a `Stop`). |
| `ErrDuplicateName` | `Register` | Two services share a `WithName`. |
| `ErrDependencyCycle` | `Register`, `Start`, `Stop` (defensive), `StartGroup`, `StopGroup` | A hard or soft dependency chain loops. `Register` rejects it up front; `Start`, `Stop`, and the group ops re-check their selected subset and surface it defensively if a cycle survived registration. |
| `ErrStartAborted` | `Start` | A hard/soft dependency failed or was skipped. |
| `ErrInvalidCron` | `Start`, `Register` (hot add) | A `WithCron` spec is invalid. |
| `ErrUnsupportedOption` | `Register` | `WithSelfHeal` combined with `WithCron`/`WithRunOnce`. |
| `ErrStopTimeout` | `Stop`, `StopService`, `Unregister`, `Start` (failed-start rollback) | A stop did not finish within the caller's timeout: the before/after-stop hooks, `Stop()`, or the wait for the instance to exit was still in flight. On the stop methods the entry is left `StatusStopping` (not `StatusStopped`) and the stop is not counted in `Metrics().Stops`. On a failed `Start` it means the bounded rollback budget was exceeded; the rollback still resets the snapshotted entries to `StatusRegistered`, leaving the orchestrator retryable. |
| `ErrHookTimeout` | `Stop`, `StopService`, `Unregister`, `Start` (failed-start rollback) | A before-stop hook overran the share of the deadline reserved for it. Always joined with `ErrStopTimeout`, so callers that only classify whole-stop timeouts still match. On the stop methods the teardown is unverified, so the entry stays `StatusStopping`; on a failed `Start` the rollback resets it to `StatusRegistered`. |
| `ErrNilService` | `Register`, `RegisterFunc` | A nil `Service`, or a nil `Start` closure passed to `RegisterFunc`. |
| `ErrOrchestratorNotStarted` | `StartService` | Called before the orchestrator was started. |
| `ErrReentrantMembership` | `StartService`, `StopService`, `Unregister` | A membership op re-entered from the target's own `Start`/`Stop` on the same goroutine (e.g. a service stopping itself from its `Start`). A programming error: fix the code, do not retry. Group ops skip a reserved entry instead of returning it. |
| `ErrMembershipBusy` | `StartService`, `StopService`, `Unregister` | A membership op collided with a reservation held by another goroutine's in-flight `Start`/`Stop`/group operation. Transient: poll `Busy(name)` or retry once the reservation clears. |
| `ErrServiceNotFound` | `StartService`, `StopService`, `Unregister` | No registered service has that name. |
| `ErrHasDependents` | `StopService`, `Unregister` | A stop/removal would break a hard dependent that is `Running` or `Starting`; the message names each blocker and its status. Pass `WithCascadeStop` to tear those dependents down too. Dependents in `Stopping`, `Registered`, `Crashed`, `Stopped`, or `Succeeded` do not block. |
| `ErrDependencyNotFound` | `StartService`, `Register` | A hard dependency is not registered (dynamically removed, or never added). |
| `ErrDependencyNotRunning` | `StartService` | A hard dependency exists but is not `StatusRunning`. |
| `ErrDependencyRemoving` | `Register` | A hot-added service names a hard dependency that is being torn down (being stopped/removed concurrently); retry after the teardown completes. |
| `ErrDependencyDepthExceeded` | `Register` | A dependency walk needed to check for a cycle ran deeper than the 10 000-edge limit. The registered graph is a bounded, startup-sized acyclic graph; reaching this depth means an unbounded registration/reload loop has grown it. The walk stops with this sentinel instead of overflowing the goroutine stack (a fatal error). Free or prune the graph and retry. |
| `ErrOrchestratorStopping` | `Register`, `StartService`, `StopService`, `Unregister`, `StartGroup`, `StopGroup` | Whole-orchestrator `Stop` is in progress. |
| `ErrOrchestratorStopped` | `Register`, `StartService`, `StopService`, `Unregister`, `StartGroup`, `StopGroup` | Whole-orchestrator `Stop` has completed. |

## Quick start

```go
package main

import (
    "context"
    "time"

    "github.com/lorenzo-vecchio/gorch"
)

type MyService struct{}

func (s *MyService) Start(ctx gorch.ServiceContext) error {
    <-ctx.Done()
    return nil
}

func (s *MyService) Stop() error { return nil }

func main() {
    orch := gorch.New(gorch.WithLogLevel(gorch.LogLevelInfo))
    orch.Register(&MyService{})

    // Blocks until SIGINT/SIGTERM, then stops gracefully.
    if err := orch.Run(10 * time.Second); err != nil {
        panic(err)
    }
}
```

## API

### Service interface

```go
type Service interface {
    Start(ctx gorch.ServiceContext) error
    Stop() error
}
```

`ServiceContext` (the `ctx` passed to `Start`) embeds `context.Context` and carries a `*ServiceLogger` and `*Messenger`. Existing `<-ctx.Done()` / `ctx.Err()` bodies keep working unchanged.

### Orchestrator

```go
orch := gorch.New(
    gorch.WithLogLevel(gorch.LogLevelInfo),
    gorch.WithDefaultStartTimeout(5 * time.Second),
)
orch.Register(svc, gorch.WithCron("@every 5s", gorch.CronSkip))
orch.Register(svc, gorch.WithSelfHeal(func() gorch.Service { return &MyService{} }))
orch.Start()
orch.Stop(10 * time.Second)
```

Configuration uses functional options. `New()` with no options uses the defaults (Info log level, health checks every 30s with a 5s probe timeout, a 30s failed-`Start` rollback budget).

### Run() convenience

`Run` starts the orchestrator, blocks until a signal is received (SIGINT and SIGTERM by default, configurable via variadic signals), then stops.

```go
// Default: waits for SIGINT or SIGTERM.
orch.Run(10 * time.Second)

// Custom signals.
orch.Run(10 * time.Second, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
```

### Dependency ordering

Services declare names and dependencies via `WithName` and `DependsOn`. Cycles are detected at `Register` time. Services start in topological order (independent services in parallel within each level) and stop in reverse topological order.

```go
orch.Register(dbSvc,   gorch.WithName("db"))
orch.Register(cacheSvc, gorch.WithName("cache"))
orch.Register(apiSvc,  gorch.WithName("api"), gorch.DependsOn("db", "cache"))
// Start: (db, cache) in parallel → api. Stop: api → (cache, db).
```

### Dynamic membership

Before `Start` the registry is static. After `Start`, `Register` is a hot add:
the service enters the graph as `StatusRegistered` and is **not** started
automatically — start it with `StartService`. `StartService` also (re)starts a
service previously stopped with `StopService`; on a service that is still
`Running` it is a no-op, so calling it from a reconciliation loop is safe and
restarting a live instance is always explicit (`StopService` then
`StartService`). A hot-added cron service follows
the same rule: `Register` validates and stages the schedule but does not install
it, so the entry stays `StatusRegistered` with no ticks until `StartService`
schedules it and reports `StatusRunning`.

```go
orch.Start()

// Hot-add while the orchestrator runs.
_ = orch.Register(late, gorch.WithName("late"), gorch.DependsOn("db"))
_ = orch.StartService("late") // every hard dependency must be Running

// Stop but keep the entry: Names()/Statuses() still report it, and
// StartService can bring it back.
_ = orch.StopService("late", 5*time.Second)

// Stop and remove: it leaves Names()/Statuses(), and its cron schedule and
// Messenger subscriptions are released.
_ = orch.Unregister("late", 5*time.Second)
```

A plain stop refuses to break a hard dependent that is `Running` or `Starting`:

```go
_ = orch.StopService("db", 5*time.Second)
// ErrHasDependents: service has active dependents: db is depended on by api (running)

// Tear down the target and its transitive hard dependents in reverse
// topological order, under one shared timeout. A dependent already stopping is
// left to its own teardown and never stopped twice.
_ = orch.StopService("db", 5*time.Second, gorch.WithCascadeStop())
```

`timeout` bounds the whole stop — the before/after-stop hooks, the service's own
`Stop()`, and the wait for its instance or in-flight cron ticks to exit — and is
shared across a cascade (one budget, not one per service). A per-service
`WithStopTimeout` still caps `Stop()` when it is smaller than the remaining
budget; a non-positive `timeout` waits indefinitely. Soft dependencies never
block a stop and are never cascaded. On a running orchestrator, `Register` and
every membership method return `ErrOrchestratorStopping` during `Stop` and
`ErrOrchestratorStopped` afterwards. `StartService` before `Start` returns
`ErrOrchestratorNotStarted`. For a cron service every in-flight tick is
cancelled, the schedule is removed, and the stop waits for those ticks to return.
The budget is split so a before-stop hook cannot consume all of it: the hook is
bounded and an overrun is reported as `ErrHookTimeout` while the service's own
`Stop()` still runs with the remainder. A stop that does not complete inside the
budget — a hook overran, `Stop()` was capped away, or the instance had not yet
exited — leaves the entry `StatusStopping` rather than `StatusStopped`, and is
not counted in `Metrics().Stops`; `StatusStopped` is committed only once the
teardown is verified complete.

Bounding a stop means walking away from user code that will not return. A
before-stop hook or a `Stop()` that outlives its budget is abandoned in its
goroutine, logged at `Error` level naming the service, and counted in
`Metrics().AbandonedGoroutines`; the same applies to each wait a failed `Start`
abandons when its rollback budget expires. `Done()` closes as soon as `Stop`
returns even while such a goroutine still runs, so the counter is the only
visibility into it. The library cannot
force user code to return — the contract is that a hook returns and `Stop()`
does not block past `WithStopTimeout` — so a non-zero counter means user code
may be leaked for the process lifetime.

A membership operation can be blocked by an in-flight reservation on the target,
and the sentinel tells the caller which situation it is in. A genuine
same-goroutine re-entry — a service calling a membership op on itself from its
own `Start` or `Stop` — is a programming error and returns
`ErrReentrantMembership`; it must never be retried. A collision with a
reservation held by *another* goroutine's in-flight `Start`/`Stop`/group
operation is benign and transient, and returns `ErrMembershipBusy`: the
reservation is released when that operation returns, so poll `Busy(name)` (or
retry) instead of racing.

**Status reports the lifecycle, `Busy` reports the reservation.** The two are
deliberately separate, and the gap between them is the reservation window:
`Start`/`StartGroup` claim an entry's reservation (`Busy(name) == true`) *before*
`startOneService` commits `StatusStarting`. During that window — the entry's
before-start hook and start condition run — `Status`/`Statuses` still report the
entry's previous status (typically `StatusRegistered` for a hot add), so an entry
actively being started can read as `Registered`. `StatusStarting` means
"`startOneService` has committed the start", not merely "a start was claimed";
`StatusStopping` covers a `StopService`/`Unregister`/`StopGroup` teardown. Poll
`Busy(name)` for "is anything in flight?", and `Statuses()` for what the entry
is. No `ServiceStatus` value encodes the reservation: a new value would make
`ServiceStatus` an unstable surface for a transient internal state.

Hard dependencies must be registered before the service that names them, both
statically and on a hot add: `Register` rejects an unknown hard dependency
immediately (dynamically with `ErrDependencyNotFound`). `Count`, `Names`, and
`Statuses` include an entry the moment it is **registered** — not when it starts:
a hot-added, not-yet-started persistent service and a staged (registered,
unscheduled) cron entry both count and both read `StatusRegistered`, so
`Count()` is "registered", not "live". For "N of M running", use the
`StatusRunning` subset: `CountRunning()` of `Count()`, or `RunningNames()`. For a
cron entry `StatusRunning` means the schedule is installed, not that a tick is
working (see [Cron modes](#cron-modes)). `Done()` is a shutdown-completed signal:
it closes once `Stop` returns — even if every service was unregistered first, or
a timed-out stop abandoned a goroutine — and stays open until then.

#### Naming policy

- `WithName` assigns the name used as the key everywhere else: `DependsOn` edges,
  `StartService`/`StopService`/`Unregister`, `Status`/`WaitFor`, and the
  lifecycle hooks. Names must be unique across the registry.
- Omitting `WithName` auto-assigns `$1`, `$2`, … The counter advances on every
  registration — named services included — so it is not the count of unnamed
  services, and it never winds back: `Unregister` frees an explicitly named
  entry, but the auto-name sequence never reuses a value.
- Re-registering an explicitly named service after its `Unregister` completes is
  allowed.
- Registering a hard dependency on a service that is mid-removal fails with
  `ErrDependencyRemoving`, so a torn-down entry cannot acquire new dependents.

### Start timeout

Per-service start deadline, with a config-level default.

```go
orch := gorch.New(gorch.WithDefaultStartTimeout(5 * time.Second))
orch.Register(svc, gorch.WithStartTimeout(30 * time.Second)) // per-service override
```

### Failed-Start rollback timeout

`WithFailedStartTimeout` bounds the whole cleanup of a failed `Start`: the
per-service stop sequences (before/after-stop hooks and `Stop()`) and the final
wait for instance and log-pump goroutines share one budget, exactly like `Stop`.
The services that had started are stopped in reverse topological order — the same
order as `Stop` — so a dependency is never torn down before its dependent,
regardless of registration order. The default is 30s; a negative value removes
the bound, which is not recommended
because a service that blocks in `Stop()` would then hang `Start` forever. When
the budget is exceeded, the error returned by `Start` matches `ErrStopTimeout`
(and `ErrHookTimeout` for an overrunning before-stop hook), and the reset is
best-effort so `Start` can still be retried.

```go
orch := gorch.New(gorch.WithFailedStartTimeout(5 * time.Second))
```

### Cron modes

| Mode | Behavior |
|------|----------|
| `CronParallel` | Fire every tick, overlapping runs allowed. |
| `CronQueue` | Serialize — wait for the previous run to finish. |
| `CronSkip` | Drop ticks that would overlap. |

Each tick receives a fresh `ServiceContext` whose context is cancelled when that
tick returns; `StopService`/`Unregister` cancel every in-flight tick through it
and wait for them to return.

**For a cron entry, `StatusRunning` means "schedule installed", not "working".**
Scheduling and health are separate facts, and the status reports only the first:
a tick whose `Start` returns an error is logged at Error level by `invokeCron`
and nothing else — the status stays `StatusRunning`, the entry is not restarted,
and the error never crashes the orchestrator. `Health()` and `IsReady()` inherit
this: a cron entry in `StatusRunning` is eligible for a health probe exactly like
a persistent one, but neither a failing tick nor a successful one is reflected in
its `ServiceStatus`. Tick failures are **not** currently observable through the
public API (only via the log, which the library's log contract does not promise
to parse); a tick-failure counter in `Metrics()` is tracked by
[#30](https://github.com/lorenzo-vecchio/gorch/issues/30) and is not part of this
contract.

A statically registered cron entry starts as `StatusRunning`; a hot-added one is
staged by `Register` (spec validated, no schedule installed) and reports
`StatusRegistered` until `StartService` schedules it and marks it running.
`StopService` removes the schedule and a later `StartService` re-creates it,
while `Unregister` drops the entry itself. A staged cron entry and a persistent
entry awaiting `StartService` both report `StatusRegistered`, so status alone
does not tell them apart: the distinction is the registration mode the caller
itself chose (`WithCron` vs not), which introspection does not expose. A staged
cron entry never ticks before `StartService`.

### Status introspection

```go
status, ok := orch.Status("db")            // ServiceStatus, bool
all := orch.Statuses()                     // map[string]ServiceStatus (all registered)
names := orch.Names()                      // []string in registration order (all registered)
count := orch.Count()                      // total registered services (not live)
runningCount := orch.CountRunning()        // number of StatusRunning services ("N of M running")
runningNames := orch.RunningNames()        // []string of StatusRunning services
busy := orch.Busy("db")                    // true if an in-flight reservation blocks membership ops
```

`Count`, `Names`, and `Statuses` are the **registered** surface: an entry appears
the moment `Register` accepts it, so a hot-added, not-yet-started persistent
service and a staged cron entry are both present and both read
`StatusRegistered`. `CountRunning` and `RunningNames` are the `StatusRunning`
subset, backed by the same `Statuses` snapshot. The reservation window is not
part of either: a start claimed but not yet committed to `StatusStarting` still
reads as the previous status, so poll `Busy(name)` to see an in-flight
reservation.

`ServiceStatus` values: `StatusRegistered`, `StatusStarting`, `StatusRunning`, `StatusStopping`, `StatusStopped`, `StatusCrashed`, `StatusSucceeded`. Each has a `String()` method. `StatusSucceeded` marks a one-shot service whose `Start` completed without error (a successful gate); dependents are not aborted by it. For a cron entry, `StatusRunning` means the schedule is installed, not that a tick is working or healthy.

### One-shot / init services

`WithRunOnce` marks a service as a one-shot init task. It runs before persistent services and transitions to `StatusSucceeded` when `Start` returns. `Stop()` is called at orchestrator shutdown (make it idempotent). If `Start` returns an error, startup aborts.

```go
orch.Register(migrator, gorch.WithRunOnce())
```

### Lifecycle hooks

Global hooks are set via functional options, or per-service via `RegisterOption`.

```go
orch := gorch.New(
    gorch.WithGlobalOnBeforeStart(func(name string) error {
        log.Printf("starting %s", name)
        return nil
    }),
    gorch.WithGlobalOnAfterStop(func(name string, err error) {
        log.Printf("stopped %s, err=%v", name, err)
    }),
)

// Per-service override:
orch.Register(svc, gorch.WithOnBeforeStart(func(name string) error {
    return checkPrerequisites()
}))
```

### Self-healing with backoff & retry

Self-heal restarts crashed services with backoff, retry limits, and a stability window.

A crash is observable before the restart: the entry transitions `Running -> Crashed`, `OnCrash` fires with the real exit error, and `Metrics().Crashes` increments. The restart then re-establishes `Running` once the new instance is live, so `Status`/`Statuses` never advertise a self-healed service as dead.

```go
orch.Register(svc,
    gorch.WithSelfHeal(func() gorch.Service { return &MyService{} }),
    gorch.WithBackoff(gorch.ExponentialBackoff{
        Initial: 1 * time.Second,
        Max:     30 * time.Second,
        Factor:  2.0,
    }),
    gorch.WithMaxRetries(5),              // give up after 5 retries (0 = unlimited)
    gorch.WithResetAfter(2 * time.Minute),  // reset retry count if service runs this long
)
```

`ConstantBackoff` returns the same delay every time.

```go
gorch.WithBackoff(gorch.ConstantBackoff{Delay: 3 * time.Second})
```

### Health checks

Services implement `HealthChecker` to report their health. The orchestrator probes on a configurable interval. After `HealthThreshold` consecutive failures, a self-healing service is restarted.

Only services whose status is `StatusRunning` are probed — both by the periodic loop and by `Health()`. Non-running entries (registered, starting, stopping, stopped, crashed, succeeded) are omitted from the `Health()` result, so a stopped service is never reported as healthy. A running service that does not implement `HealthChecker` is reported with a nil (healthy) error.

```go
type HealthChecker interface {
    Health(ctx context.Context) error
}

orch := gorch.New(
    gorch.WithHealthChecks(30*time.Second,
        gorch.WithProbeTimeout(5*time.Second),
        gorch.WithFailureThreshold(3)),
    // interval, per-probe timeout, consecutive failures before restart
)

// Or disable the health-check loop entirely:
orch := gorch.New(gorch.WithHealthChecksDisabled())
```

> **Note:** automatic restart on health failure requires `WithSelfHeal` (a factory to create a fresh instance). Without a factory, health failures are only logged and counted in `Metrics().HealthFails`.

Manual health check:

```go
results := orch.Health() // map[string]error, nil = healthy
// Only StatusRunning services appear; non-running entries are omitted.
```

### Messenger

```go
ch, unsub := messenger.Subscribe("topic")
messenger.Publish(msg, "topic")  // send to topic subscribers
messenger.Publish(msg)           // broadcast to ALL subscribers
```

### Request-reply

`Request` publishes a message and blocks until a response arrives (or ctx expires). The responder receives a `Message` with a `ReplyTopic` field and publishes its reply there. The payload is gob-encoded, so decoding it by hand is error-prone: implement the responder with the typed helpers (`TypedSubscribeRequest`/`TypedRespond`) instead.

```go
// Requestor:
resp, err := messenger.Request(ctx, payload, "orders.create")

// Responder (inside a service goroutine): messages arrive already decoded,
// and the reply is encoded by TypedRespond — see "Typed Request-Reply".
reqCh, _ := gorch.TypedSubscribeRequest[CreateOrderReq](messenger, "orders.create")
for env := range reqCh {
    gorch.TypedRespond(messenger, processOrder(env.Value), env.ReplyTopic)
}
```

For a fully type-safe round trip (typed requestor included), use `TypedRequest` — described in "Typed Request-Reply" below. `RequestAsync` returns a response channel immediately without blocking.

### Typed messages

`RegisterType`, `TypedPublish`, and `TypedSubscribe` provide gob-encoded type-safe messaging.

```go
type OrderEvent struct {
    OrderID string
    Status  string
}

gorch.RegisterType[OrderEvent](messenger)

// Publisher:
gorch.TypedPublish(messenger, OrderEvent{OrderID: "42", Status: "shipped"}, "orders")

// Subscriber:
ch, unsub := gorch.TypedSubscribe[OrderEvent](messenger, "orders")
for evt := range ch {
    fmt.Println(evt.OrderID) // typed, no cast needed
}
```

### Logging

Services log via `ServiceLogger`:

```go
sc.Logger.Info("request completed", "status", 200, "latency", 12*time.Millisecond)
// 2026-07-27 14:30:05.123 INFO  my-service --- request completed status=200 latency=12ms
// (the prefix is the service name: WithName, or the auto-assigned $N)
```

The built-in log-pump writes to `os.Stderr`. Log level filters entries: `Debug < Info < Warn < Error`.

`ServiceLogger` never blocks: the send to the log channel is non-blocking, so an entry is dropped when the channel is full or its pump has been signalled to stop. A torn-down log channel can therefore never stall a service or a `Stop`; the cost of a dead channel is lost lines, never a hang.

A service hot-added while a `Start` is in flight takes a logger bound to that `Start`'s channel. If that `Start` fails, its pump exits during the rollback and the survivor's entries are dropped until the next successful `Start` rebinds its logger. The rebind is automatic: every `Start` reassigns a logger to every entry in its snapshot, and that snapshot includes a survivor from a previous failed `Start`. So the window is low severity and self-healing — no hang, only dropped lines until the retry.

#### Custom logger

Inject any logger that satisfies the `Logger` interface via `WithLogger`. `*slog.Logger` from the standard library satisfies this interface directly.

```go
import "log/slog"

orch := gorch.New(gorch.WithLogger(slog.Default()))
```

When a custom logger is set, the built-in log-pump is disabled entirely. The service name is prepended as `"service"=<name>` to every log call so the custom logger can include or exclude it as needed. `WithLogLevel` is ignored — the custom logger manages its own level filtering.

```go
// With slog, the service name appears as a structured key-value pair:
// level=INFO msg="request completed" service=api status=200 latency=12ms
```

### RegisterFunc

For simple services where a struct is boilerplate, `RegisterFunc` accepts closures directly.

```go
orch.RegisterFunc("health-server", func(ctx gorch.ServiceContext) error {
    srv := &http.Server{Addr: ":8080"}
    go func() { <-ctx.Done(); srv.Shutdown(context.Background()) }()
    return srv.ListenAndServe()
}, nil) // nil Stop func — stops purely via context cancellation
```

A `Stop` func can be nil if the service cleans up via context cancellation alone.

### Groups

Assign services to named groups with `WithGroup`, then operate on subsets.

```go
orch.Register(dbSvc, gorch.WithName("db"), gorch.WithGroup("infra"))
orch.Register(cacheSvc, gorch.WithName("cache"), gorch.WithGroup("infra"))
orch.Register(apiSvc, gorch.WithName("api"), gorch.WithGroup("app"), gorch.DependsOn("db", "cache"))

// Start or stop only a group.
err := orch.StartGroup("infra")
err = orch.StopGroup("app", 5*time.Second)

// Filter statuses by group.
infra := orch.StatusesByGroup("infra") // map[string]ServiceStatus
```

### Labels

Attach arbitrary key-value tags for filtering and introspection.

```go
orch.Register(svc, gorch.WithLabel("tier", "critical"))
orch.Register(svc, gorch.WithLabel("team", "payments"))

critical := orch.StatusesByLabel("tier", "critical")
```

### Soft dependencies

`DependsOnSoft` orders a service after its soft dependencies if they are registered, but does not fail if they are missing.

```go
orch.Register(apiSvc,
    gorch.WithName("api"),
    gorch.DependsOn("db"),          // hard: must exist
    gorch.DependsOnSoft("metrics"), // soft: start after if present, ignore if missing
)
```

### Readiness

`ReadinessChecker` separates "running" from "ready to serve." Use `IsReady()` to gate traffic routing without killing the service.

```go
type ReadinessChecker interface {
    Ready(ctx context.Context) error
}

// On the orchestrator:
if orch.IsReady(ctx, "api") {
    // route traffic
}
```

### State-change hooks

`OnStateChange` fires on every status transition. `OnCrash` fires specifically on `Running -> Crashed`. Wire these to Prometheus counters, Slack webhooks, or a status page instead of polling.

```go
orch := gorch.New(
    gorch.WithOnStateChange(func(name string, from, to gorch.ServiceStatus) {
        log.Printf("%s: %s -> %s", name, from, to)
    }),
    gorch.WithOnCrash(func(name string, err error) {
        notifications.Send(name + " crashed")
    }),
)
```

### WaitFor

Block until a service reaches a target status (or times out). Useful for tests and services that need external coordination.

```go
err := orch.WaitFor("db", gorch.StatusRunning, 10*time.Second)
```

### Typed Request-Reply

`TypedRequest` provides type-safe request-reply without falling back to the untyped `Message` API. Pair it with `TypedSubscribeRequest` (decodes requests into a `TypedEnvelope` carrying the value and `ReplyTopic`) and `TypedRespond` (encodes and publishes the reply).

```go
type CreateOrderReq struct {
    ItemID string
    Qty    int
}
type CreateOrderResp struct {
    OrderID string
    Status  string
}

// Responder (inside a service goroutine):
reqCh, unsub := gorch.TypedSubscribeRequest[CreateOrderReq](messenger, "orders.create")
defer unsub()
go func() {
    for env := range reqCh {
        result := processOrder(env.Value)
        gorch.TypedRespond(messenger, result, env.ReplyTopic)
    }
}()

// Requestor:
resp, err := gorch.TypedRequest[CreateOrderReq, CreateOrderResp](
    messenger, ctx, req, "orders.create",
)
```

No manual gob encoding is required anywhere in user code.

### Metrics

`Metrics()` returns a snapshot of atomic counters for orchestrator-level events. The user wires these into their own monitoring system — no metrics library dependency. Each stop of an instance is counted exactly once, even when a teardown and the instance's own exit race. `Stops` counts only stops that completed; a stop that timed out (`ErrStopTimeout`/`ErrHookTimeout`, or a `WithStopTimeout` cap that fired) is not counted, matching its unverified `StatusStopping`. `AbandonedGoroutines` counts teardown goroutines abandoned because a deadline won: a blocking hook or `Stop()`, or a failed-`Start` wait that outlived its rollback budget. It is monotonic — never decremented, since there is no reliable signal that an abandoned goroutine later returned — so a non-zero value means user code may be leaked for the process lifetime.

```go
stats := orch.Metrics()
fmt.Printf("starts=%d stops=%d crashes=%d restarts=%d healthFails=%d abandonedGoroutines=%d\n",
    stats.Starts, stats.Stops, stats.Crashes, stats.Restarts, stats.HealthFails, stats.AbandonedGoroutines)
```

### Validator

Implement the `Validator` interface to catch config errors at `Register` time (before `Start`). `Validate` runs outside the orchestrator's locks, so it may block or call back into membership operations without deadlocking.

```go
type Validator interface {
    Validate() error
}

func (s *MyService) Validate() error {
    if s.Port == 0 {
        return fmt.Errorf("port must be set")
    }
    return nil
}

// Register returns the validation error immediately:
err := orch.Register(svc)
```

### WithStartCondition

Skip a service at runtime without removing its registration. The condition function is evaluated just before startup.

```go
orch.Register(svc, gorch.WithStartCondition(func() bool {
    return os.Getenv("FEATURE_ENABLED") == "true"
}))
```

### Per-service StopTimeout

`WithStopTimeout` sets a per-service deadline on `Stop()`. The orchestrator proceeds with shutdown even if this service takes longer.

```go
orch.Register(svc, gorch.WithStopTimeout(3 * time.Second))
```

### Messenger buffer size

`SubscribeWithBuffer` lets callers set the buffer capacity to prevent slow consumers from blocking publishers.

```go
ch, unsub := messenger.SubscribeWithBuffer("high-throughput", 256)
```

### Health check hooks

`BeforeHealthCheck` and `AfterHealthCheck` provide instrumentation points around every health probe without wrapping every `HealthChecker`.

```go
orch := gorch.New(
    gorch.WithHealthChecks(30*time.Second,
        gorch.WithProbeTimeout(5*time.Second),
        gorch.WithFailureThreshold(3)),
    gorch.WithBeforeHealthCheck(func(name string) error {
        metrics.Inc("health_checks_total")
        return nil
    }),
    gorch.WithAfterHealthCheck(func(name string, err error) {
        if err != nil {
            metrics.Inc("health_checks_failed")
        }
    }),
)
```

### Drain and Done

`Drain()` closes all subscriber channels and clears subscriptions. `Done()` returns a channel that closes once `Stop` (or `Run`, which calls `Stop`) has returned — useful for non-blocking shutdown. It is a **shutdown-completed** signal, not a count of live goroutines: it stays open through hot adds and restarts, stays open after a failed `Start`, and a `Stop` that times out still closes it while an abandoned goroutine may be running. The same channel is returned by every call.

A `ServiceContext.Messenger` is a scoped view: its subscriptions are released when the instance or cron tick that created them ends. After that release (or a global `Drain`), `Subscribe` on that same view returns an already-closed channel, and `Request`/`RequestAsync` return an error instead of a nil reply — a goroutine that outlived its service cannot resurrect its subscriptions. A newly created view still subscribes normally.

```go
// Gracefully flush pending messages before shutdown.
messenger.Drain()

// Non-blocking wait for shutdown to complete (Stop/Run returned).
// Done() is not a guarantee that every goroutine has exited: on a
// timed-out Stop an abandoned goroutine may still be running. Watch
// Metrics().AbandonedGoroutines for that.
select {
case <-orch.Done():
case <-time.After(10 * time.Second):
}
```

## Compatibility & versioning

gorch follows [Semantic Versioning](https://semver.org/). Until 1.0, minor
releases may contain breaking changes; every one is marked **Breaking** in the
[release notes](https://github.com/lorenzo-vecchio/gorch/releases).

- **Stable** — exported identifiers (types, functions, methods), the `Service`,
  `HealthChecker`, `ReadinessChecker`, `Validator`, and `Logger` interfaces, the
  `ServiceStatus` values, the sentinel errors above, and the gob wire format.
- **Not stable** — the built-in logger's exact output format and key ordering,
  internal goroutine counts, and anything unexported. Do not parse log lines.
- **Deprecations** — a deprecated exported identifier keeps working for at least
  one minor release and is listed as `Deprecated:` in its doc comment and in the
  release notes before removal.

See the [release notes](https://github.com/lorenzo-vecchio/gorch/releases) for upgrade steps between releases.

## Examples

- [`examples/basic/`](examples/basic/) — Service lifecycle, cron scheduling, graceful shutdown.
- [`examples/pubsub/`](examples/pubsub/) — Inter-service messaging with topics.
- [`examples/typedreq/`](examples/typedreq/) — Typed request-reply via `TypedRequest`/`TypedRespond`.
- [`examples/advanced/`](examples/advanced/) — Groups, labels, hooks, health checks, and more.

## Development

```bash
go test . -coverprofile=coverage.out
go tool cover -func=coverage.out | grep total  # must be 100.0%
go test . -bench . -benchmem
go test -fuzz=FuzzMembershipTransitions -fuzztime=30s .
go vet ./...
gofmt -w .
```

## License

MIT
