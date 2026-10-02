# gorch — a composable orchestrator for goroutine lifecycles in a single process

Start, stop, and supervise a set of long-lived goroutines with dependency ordering, cron
scheduling, a topic pub-sub Messenger, health checks, and self-healing — without adopting a
framework.

![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/lorenzo-vecchio/gorch/main/.github/badges/coverage.json)
![library LOC](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/lorenzo-vecchio/gorch/main/.github/badges/loc-library.json)
![test LOC](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/lorenzo-vecchio/gorch/main/.github/badges/loc-tests.json)
![benchmark LOC](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/lorenzo-vecchio/gorch/main/.github/badges/loc-bench.json)
![tooling LOC](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/lorenzo-vecchio/gorch/main/.github/badges/loc-tooling.json)
![examples LOC](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/lorenzo-vecchio/gorch/main/.github/badges/loc-examples.json)
![total LOC](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/lorenzo-vecchio/gorch/main/.github/badges/loc-total.json)

<!-- fragment -->
```go
import "github.com/lorenzo-vecchio/gorch"
```

`gorch` is a runtime supervisor for the composition root of one process. You describe the
services you want to run as `Service` values, give each a name and, if needed, the names it
depends on, and the orchestrator starts them in dependency order, keeps them running (health
probes, self-heal restarts, cron schedules, pub-sub messaging), and stops them in reverse
dependency order. The graph is not frozen: services can be added, started, stopped, replaced,
and removed while the orchestrator runs.

It is deliberately small. The only external dependency is `robfig/cron/v3`; there is no code
generation, no framework, and no global registry. The public API and its contracts are frozen
by tests, and the package overview in `doc.go` renders on
[pkg.go.dev](https://pkg.go.dev/github.com/lorenzo-vecchio/gorch); the split of
responsibilities between `doc.go` and this README is stated in
[Versioning and breaking changes](#versioning-and-breaking-changes).

> **Pre-1.0.** Until `v1.0.0`, minor releases may contain breaking changes and silent behaviour
> changes; each is recorded in [Upgrading](#upgrading). The versioning policy is in
> [Versioning and breaking changes](#versioning-and-breaking-changes).

## Table of contents

- [Why gorch](#why-gorch)
- [When not to use gorch](#when-not-to-use-gorch)
- [How it compares](#how-it-compares)
- [Quickstart](#quickstart)
  - [Install](#install)
  - [Minimal program](#minimal-program)
  - [A realistic five-minute example](#a-realistic-five-minute-example)
  - [Where to go next](#where-to-go-next)
- [Core concepts](#core-concepts)
  - [Service](#service)
  - [ServiceContext](#servicecontext)
  - [Orchestrator](#orchestrator)
  - [Lifecycle states](#lifecycle-states)
  - [The dependency graph](#the-dependency-graph)
- [Guides](#guides)
  - [Starting and stopping services](#starting-and-stopping-services)
  - [Dependency ordering](#dependency-ordering)
  - [One-shot init gates](#one-shot-init-gates)
  - [Lifecycle hooks](#lifecycle-hooks)
  - [Self-healing](#self-healing)
  - [Health and readiness](#health-and-readiness)
  - [Cron scheduling](#cron-scheduling)
  - [Messaging](#messaging)
  - [Groups and labels](#groups-and-labels)
  - [Logging](#logging)
- [Dynamic membership](#dynamic-membership)
  - [Hot add](#hot-add)
  - [StopService vs Unregister](#stopservice-vs-unregister)
  - [Cascade](#cascade)
  - [Reservations](#reservations)
  - [Names and auto-naming](#names-and-auto-naming)
- [Concurrency](#concurrency)
- [Errors](#errors)
  - [Sentinel taxonomy](#sentinel-taxonomy)
- [Observability](#observability)
- [Contract and guarantees](#contract-and-guarantees)
- [Versioning and breaking changes](#versioning-and-breaking-changes)
- [Upgrading](#upgrading)
  - [Breaking changes by release](#breaking-changes-by-release)
  - [API added since v0.9.0](#api-added-since-v090)
  - [Silent behaviour changes](#silent-behaviour-changes)
- [Pitfalls and gotchas](#pitfalls-and-gotchas)
- [Performance](#performance)
- [Examples](#examples)
- [Statistics](#statistics)
- [Development](#development)
- [FAQ](#faq)
- [License](#license)

## Why gorch

The composition root of a Go process tends to grow into a `main` that starts an HTTP server, a
worker pool, a cache refresher, and a migration step, each in its own goroutine, with an
ad-hoc ordering and a `defer`-stack shutdown that is wrong the moment one component depends on
another. Its goroutines die silently, its startup order is implicit, and its shutdown is a
best-effort race.

gorch turns that into an explicit lifecycle:

- Each component is a `Service` with a name and optional dependency edges.
- `Start` launches services in topological order; independent services start in parallel.
- `Stop` tears them down in reverse topological order, so a dependency outlives its dependents.
- Between start and stop the orchestrator supervises: health probes, self-heal restarts, cron
  schedules, and a pub-sub Messenger.
- The graph changes at runtime: `Register`, `StartService`, `StopService`, `Unregister`,
  `ReplaceService`.
- Failures are aggregated with `errors.Join` and classified with stable sentinel errors.

You keep the wiring in your own `main`; gorch only owns the lifecycle.

## When not to use gorch

This section is deliberately as prominent as the feature list, because gorch is the wrong tool
for several things people reach for it to do.

- **Not a distributed scheduler.** gorch supervises goroutines inside one process. It does not
  schedule work across machines, elect leaders, or coordinate with other processes.
- **Not a durable queue.** The Messenger is drop-only and in-memory. Nothing survives a
  process restart, and nothing is replayed.
- **Not a service mesh.** There is no service discovery, no transport, no retries across the
  network, and no mTLS. gorch does not know other processes exist.
- **Not a replacement for `context`.** `ServiceContext` embeds a `context.Context` and is the
  cancellation mechanism. gorch adds lifecycle around contexts; it does not replace them.
- **Not an OS process supervisor.** gorch does not spawn or restart processes. It supervises
  goroutines. Use systemd, Kubernetes, or similar for processes.
- **Not a workflow engine.** There is no per-step persistence, no compensating transactions,
  and no cross-restart retries. A `runOnce` gate is a startup ordering primitive, not a
  durable saga.
- **Not a generic job queue.** Cron schedules run in-process on a timer. Missed ticks in the
  past are not backfilled.

If you need durable messaging, use a broker. If you need cross-process orchestration, use a
scheduler. If you need dependency injection across a large codebase, use a DI framework; gorch
starts at the point where the graph is already assembled.

## How it compares

| Alternative | What it gives you | What gorch adds |
|---|---|---|
| [`oklog/run`](https://github.com/oklog/run) | an actor group: `Add(execute, interrupt)` and a single two-way failure signal | dependency-ordered start and reverse-ordered stop, health/readiness, cron, `runOnce` gates, runtime membership, a Messenger |
| [`uber-go/fx`](https://github.com/uber-go/fx) | dependency injection plus `OnStart`/`OnStop` lifecycle hooks | start/stop ordering derived from explicit edges, health probing, self-healing, runtime membership, no code generation |
| [`golang.org/x/sync/errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup) | structured concurrency for a fixed set of tasks, with error propagation | a lifecycle with observable states, ordered teardown, and supervision *after* start |
| `signal.Notify` + `defer` | the minimum viable graceful shutdown | everything here, with a similar dependency-light footprint but a real state machine |

gorch does not try to beat these at their own jobs. It exists for the specific moment when you
have several cooperating components, a required order, and a need to observe and act on their
lifecycles at runtime.

## Quickstart

### Install

```bash
go get github.com/lorenzo-vecchio/gorch@latest
```

Requires Go 1.25+. The import path is the module root,
`github.com/lorenzo-vecchio/gorch`; the pre-v0.6 flattened `.../gorch/gorch` path is gone.

### Minimal program

The smallest useful program: one service, one call to `Run`, and clean shutdown on a signal.
It is complete — copy it into a file and `go run` it.

```go
package main

import (
	"time"

	"github.com/lorenzo-vecchio/gorch"
)

type MyService struct{}

func (s *MyService) Start(ctx gorch.ServiceContext) error {
	<-ctx.Done() // run until the orchestrator cancels us
	return nil
}

func (s *MyService) Stop() error { return nil }

func main() {
	orch := gorch.New()
	orch.Register(&MyService{})

	// Start, block until SIGINT/SIGTERM, then stop gracefully.
	if err := orch.Run(10 * time.Second); err != nil {
		panic(err)
	}
}
```

### A realistic five-minute example

A migration gate that must finish first, a database service that self-heals and reports
health, and an API service that depends on both. This is also complete and runnable.

```go
package main

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/lorenzo-vecchio/gorch"
)

// migrateOnce is a one-shot gate: it runs to completion before its dependents
// start, then reports StatusSucceeded.
type migrateOnce struct{}

func (m *migrateOnce) Start(ctx gorch.ServiceContext) error {
	ctx.Logger.Info("applying migrations")
	return nil
}

func (m *migrateOnce) Stop() error { return nil }

// db is a persistent service with a health probe and a self-heal factory.
type db struct{}

func (d *db) Start(ctx gorch.ServiceContext) error {
	<-ctx.Done()
	return nil
}

func (d *db) Stop() error                      { return nil }
func (d *db) Health(ctx context.Context) error { return nil }

// api starts only after the gate has succeeded and the database is running.
type api struct{}

func (a *api) Start(ctx gorch.ServiceContext) error {
	<-ctx.Done()
	return nil
}

func (a *api) Stop() error { return nil }

func main() {
	orch := gorch.New(
		gorch.WithHealthChecks(10*time.Second,
			gorch.WithProbeTimeout(time.Second),
			gorch.WithFailureThreshold(3)),
	)

	must(orch.Register(&migrateOnce{}, gorch.WithName("migrate"), gorch.WithRunOnce()))
	must(orch.Register(&db{}, gorch.WithName("db"),
		gorch.WithSelfHeal(func() gorch.Service { return &db{} })))
	must(orch.Register(&api{}, gorch.WithName("api"), gorch.DependsOn("migrate", "db")))

	// Start order: migrate -> db -> api. Run blocks until SIGINT/SIGTERM,
	// then stops in reverse order: api -> db -> migrate.
	if err := orch.Run(10*time.Second, syscall.SIGINT, syscall.SIGTERM); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
```

### Where to go next

| I want to… | Read |
|---|---|
| understand the lifecycle states and who triggers each | [Lifecycle states](#lifecycle-states) |
| start/stop a service and control the timeouts | [Starting and stopping services](#starting-and-stopping-services) |
| express "api starts after db" | [Dependency ordering](#dependency-ordering) |
| run a migration before everything else | [One-shot init gates](#one-shot-init-gates) |
| restart a crashed service automatically | [Self-healing](#self-healing) |
| probe health and gate traffic on readiness | [Health and readiness](#health-and-readiness) |
| run a task on a schedule | [Cron scheduling](#cron-scheduling) |
| pass messages between services | [Messaging](#messaging) |
| add or remove a service while running | [Dynamic membership](#dynamic-membership) |
| know which calls are safe concurrently | [Concurrency](#concurrency) |
| classify an error without reading internals | [Errors](#errors) |
| observe status, health, metrics, and shutdown | [Observability](#observability) |
| upgrade from an older release | [Upgrading](#upgrading) |

## Core concepts

### Service

<!-- fragment -->
```go
type Service interface {
	Start(ctx ServiceContext) error // blocks; for cron: runs per-tick; for non-cron: runs until ctx cancelled
	Stop() error                    // cleanup signal beyond context cancellation
}
```

A service is **a unit of lifecycle, not a unit of work**. The usual shape is one goroutine per
`Service` whose `Start` blocks until `ctx.Done()`; the orchestrator owns when that goroutine is
created and cancelled. If you find yourself wanting many goroutines with different lifetimes
inside one `Service`, register several services instead — that is what gives the orchestrator
something to order, observe, and heal.

`Start` may return before cancelling only for a `runOnce` gate (see
[One-shot init gates](#one-shot-init-gates)). For a persistent service, returning early is how
a crash or a clean exit is observed; self-heal decides whether to restart.

### ServiceContext

<!-- fragment -->
```go
type ServiceContext struct {
	context.Context
	Logger    *ServiceLogger
	Messenger *Messenger
}
```

The context handed to `Start` embeds `context.Context`, so `<-ctx.Done()` / `ctx.Err()` work
unchanged and it can be passed to any standard-library function that takes a context. It also
carries the service's `Logger` and a `Messenger` view scoped to this instance (or this cron
tick) — subscriptions made on that view are released when the instance or tick ends, and a view
whose owner has been released cannot subscribe again. `ServiceContext` is owned by gorch; a
service must not mutate it.

### Orchestrator

<!-- fragment -->
```go
orch := gorch.New(
	gorch.WithLogLevel(gorch.LogLevelInfo),
	gorch.WithDefaultStartTimeout(5 * time.Second),
)
```

Configuration uses functional options; `New()` with no options uses the defaults: Info log
level, health checks every 30s with a 5s probe timeout and a 3-failure threshold, and a 30s
failed-`Start` rollback budget.

An `Orchestrator` zero value is usable: `var o Orchestrator` lazily initialises on the first
public call with `New()`'s defaults, so it can be registered against and started without
panicking on a nil registry, Messenger, or shutdown channel. A zero-value `Messenger` is
likewise usable and lazily initialised.

### Lifecycle states

Every registered service carries one of seven `ServiceStatus` values:

| Status | Meaning | Entered from | Left by |
|---|---|---|---|
| `StatusRegistered` | Registered, but no instance or schedule is live. Static entries before `Start`, plus hot-added and staged entries. | `Register`; failed-`Start` rollback | `Start`/`StartService`/`StartGroup` (→ `Starting`); a before-start hook error or a false start condition (→ `Stopped`); `Unregister` removes the entry |
| `StatusStarting` | `startOneService` committed the start: hooks passed, the condition was true, the contexts are built. | `Registered` | `Running` (persistent or cron instance launched); `Succeeded` (a `runOnce` `Start` returned nil); `Crashed` (a `runOnce` failure); `Stopped` (rollback) |
| `StatusRunning` | A persistent instance is live, or a cron schedule is installed. | `Starting`; `Stopped` (via `StartService`); `Crashed` (self-heal restart) | `Stopping` (a teardown claimed the entry); `Stopped` (clean exit, non-self-heal); `Crashed` (error exit) |
| `StatusStopping` | A `StopService`/`Unregister`/`StopGroup` (or `Stop`) teardown owns the entry. | `Running`/`Starting` | `Stopped` once the teardown is verified complete; it stays `Stopping` if the stop times out |
| `StatusStopped` | Verified complete teardown, a clean instance exit, or a started-then-failed service. | `Running`, `Stopping`, `Starting` | `Starting` (a later `StartService`/`StartGroup`) |
| `StatusCrashed` | The instance exited with a real error (not `context.Canceled`), or the retry budget was exhausted. | `Running`/`Starting` | `Starting`/`Running` (self-heal restart) |
| `StatusSucceeded` | A `runOnce` gate whose `Start` returned nil. **Permanent**: a later stop runs `Stop()` but never demotes it. | `Starting` | never |

`StatusSucceeded` is the distinction that matters for gates: a dependency is satisfied by a
hard dependency that is either `StatusRunning` or a `runOnce` gate in `StatusSucceeded`, which
is why a succeeding migration does not have to stay "running" for its dependents to start.
Each value has a `String()` method.

### The dependency graph

`DependsOn(names...)` declares **hard** edges: the named services must exist (or the
registration is rejected) and must be `Running` — or a succeeded `runOnce` gate — before the
dependent starts. A hard dependent that is `Running`/`Starting` also blocks a plain
`StopService`/`Unregister` of its dependency (see [Cascade](#cascade)).

`DependsOnSoft(names...)` declares **soft** edges: if the named service is registered, the
dependent starts after it; if it is missing, the edge is ignored and nothing fails. Soft edges
never block a stop and are never cascaded.

Cycles are rejected at `Register` for both hard and soft edges. Start is topological
(independent services in parallel within a level) and stop is reverse topological. The cycle
walk visits each node once and is depth-capped: a chain deeper than 10 000 edges fails
registration with `ErrDependencyDepthExceeded` instead of overflowing the stack. The registered
graph is expected to be a bounded, startup-sized acyclic graph; the cap guards an unbounded
reload loop, not normal use. `Dependents(name)` and `DependenciesOf(name)` expose the hard
edges in the reverse and forward directions (see [Observability](#observability)).

## Guides

### Starting and stopping services

`Start` starts the registered graph and returns once it is up; `Stop(timeout)` shuts it down.

```go
// Complete program.
package main

import (
	"context"
	"errors"
	"time"

	"github.com/lorenzo-vecchio/gorch"
)

type svc struct{}

func (s *svc) Start(ctx gorch.ServiceContext) error { <-ctx.Done(); return nil }
func (s *svc) Stop() error                          { return nil }

func main() {
	orch := gorch.New()
	_ = orch.Register(&svc{}, gorch.WithName("db"))

	_ = orch.Start()
	// ... do work ...
	err := orch.Stop(10 * time.Second)
	if err != nil && !errors.Is(err, context.Canceled) {
		panic(err)
	}
}
```

`Stop`'s `timeout` bounds the whole shutdown — every service's before/after-stop hooks, its
`Stop()`, and the wait for instances and in-flight cron ticks to exit. A non-positive timeout
waits indefinitely. See [Concurrency](#concurrency) for how concurrent `Start`/`Stop` calls
behave.

Three timeouts refine the default:

| Option | Scope | Default |
|---|---|---|
| `WithStartTimeout` (per service) / `WithDefaultStartTimeout` (config) | the synchronous window in which a persistent service's `Start` may return an error and abort the orchestrator's `Start` | 0 = fire-and-forget |
| `WithStopTimeout` (per service) | a cap on the service's own `Stop()`, smaller than the caller's budget when needed | 0 = the caller's budget |
| `WithFailedStartTimeout` (config) | the whole rollback of a failed `Start`, sharing one budget across every step | 30s; negative removes the bound |

A failed `Start` rolls back the services that had started in reverse topological order, so a
dependency is never torn down before its dependent regardless of registration order. The
rollback drains Messenger owners, so an instance that ignores cancellation and outlives the
budget does not leak subscriptions across retries. A failed `Start` does not consume the
lifecycle; `Start` may be retried.

### Dependency ordering

<!-- fragment -->
```go
orch.Register(dbSvc, gorch.WithName("db"))
orch.Register(cacheSvc, gorch.WithName("cache"))
orch.Register(apiSvc, gorch.WithName("api"), gorch.DependsOn("db", "cache"))
// Start: (db, cache) in parallel -> api. Stop: api -> (cache, db).
```

A hard dependency must be registered before the service that names it, both statically and on a
hot add. A missing hard dependency fails `Register` with `ErrDependencyNotFound`; a hard
dependency that exists but is not running makes `StartService` fail with
`ErrDependencyNotRunning`.

### One-shot init gates

`WithRunOnce` marks a service as a one-shot init task. It runs before persistent services
(started synchronously, subject to `WithStartTimeout`) and transitions to `StatusSucceeded`
when `Start` returns nil; if `Start` returns an error, startup aborts. A gate that succeeded
keeps `StatusSucceeded` through any later stop — `Stop()`, a `StopService`/`Unregister`/cascade,
or a whole-orchestrator `Stop` still run `Stop()` (make it idempotent) but never demote the
gate to `StatusStopped`.

<!-- fragment -->
```go
orch.Register(migrator, gorch.WithRunOnce())
```

A hard dependency is satisfied by a gate in `StatusSucceeded`, so a dependent can start even
though the gate no longer runs.

### Lifecycle hooks

Global hooks are set via functional options; per-service hooks via `RegisterOption` and
override the global hook of the same name.

<!-- fragment -->
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

The four hooks are `OnBeforeStart`, `OnAfterStart`, `OnBeforeStop`, `OnAfterStop`. The
before-stop hook is bounded so it cannot consume the whole shutdown budget: it gets at most
half of what remains, the service's own `Stop()` always runs with the remainder, and a hook
that overruns is reported as `ErrHookTimeout` (joined with `ErrStopTimeout`).

Both the **before-stop** and **after-stop** hooks also run on every self-heal restart,
immediately around the dead instance's `Stop()`: a hook that releases a lease or deregisters an
instance from a load balancer therefore fires on every crash, not only on an explicit stop. A
restart has no caller-supplied timeout, so its before-stop hook is capped by the service's
`WithStopTimeout` (30s when unset); an overrun is abandoned — logged and counted in
`Metrics().AbandonedGoroutines` — and the restart proceeds.

### Self-healing

`WithSelfHeal` supplies a factory for a fresh instance; when the service's `Start` returns an
error (or panics), the orchestrator creates a new instance after a backoff delay.

<!-- fragment -->
```go
orch.Register(svc,
	gorch.WithSelfHeal(func() gorch.Service { return &MyService{} }),
	gorch.WithBackoff(gorch.ExponentialBackoff{
		Initial: 1 * time.Second,
		Max:     30 * time.Second,
		Factor:  2.0,
	}),
	gorch.WithMaxRetries(5),             // give up after 5 retries (0 = unlimited)
	gorch.WithResetAfter(2*time.Minute), // reset the retry count if the service runs this long
)
```

`ConstantBackoff{Delay: 3 * time.Second}` returns the same delay every time; the default when
no backoff is set is `ConstantBackoff{Delay: 1 * time.Second}`.

A crash is **observable before the restart**: the entry transitions `Running -> Crashed`,
`OnCrash` fires with the real exit error, and `Metrics().Crashes` increments. The restart then
re-establishes `Running` once the new instance is live, so `Status`/`Statuses` never advertise
a self-healed service as dead. A clean or `context.Canceled` exit of a self-heal service is not
a crash, but is still restarted.

A crash of a service **without** a factory is observable the same way (`Crashed`, `OnCrash`,
`Crashes`) and is not restarted.

### Health and readiness

Implement `HealthChecker` for liveness and `ReadinessChecker` to distinguish "running" from
"ready to serve".

<!-- fragment -->
```go
type HealthChecker interface {
	Health(ctx context.Context) error
}

type ReadinessChecker interface {
	Ready(ctx context.Context) error
}
```

Only services whose status is `StatusRunning` are probed — by both the periodic loop and
`Health()`. Non-running entries are omitted from the `Health()` result rather than reported
healthy. A running service that does not implement `HealthChecker` is reported with a nil
(healthy) error. Each probe gets a fresh deadline (`WithProbeTimeout`, default 5s) and a panic
is recovered and returned as an error.

<!-- fragment -->
```go
orch := gorch.New(
	gorch.WithHealthChecks(30*time.Second,
		gorch.WithProbeTimeout(5*time.Second),
		gorch.WithFailureThreshold(3)),
	// interval, per-probe timeout, consecutive failures before restart
)

// Or disable the periodic loop entirely:
orch := gorch.New(gorch.WithHealthChecksDisabled())

results := orch.Health() // map[string]error, nil = healthy; only Running services appear
```

After `WithFailureThreshold` consecutive failed probes, a **self-healing** service is restarted
(the threshold resets on a successful probe or a restart). Without a factory, health failures
are only logged and counted in `Metrics().HealthFails`.

Readiness is on demand:

<!-- fragment -->
```go
if orch.IsReady(ctx, "api") {
	// route traffic
}
```

`IsReady` honours the caller's context and returns false for a service that is not ready, does
not exist, or does not implement `ReadinessChecker`.

### Cron scheduling

`WithCron(spec, mode)` runs a service's `Start` once per matching tick. The grammar is six
fields with seconds, plus descriptors such as `@daily` and `@every 5s`.

| Mode | Behaviour |
|---|---|
| `CronParallel` | Fire every tick; overlapping runs are allowed. |
| `CronQueue` | Serialize — a tick waits for the previous run to finish. |
| `CronSkip` | Drop a tick that would overlap a still-running one. |

Each tick receives a fresh `ServiceContext` whose context is cancelled when that tick returns;
`StopService`/`Unregister` cancel every in-flight tick and wait for them to return.

**For a cron entry, `StatusRunning` means "schedule installed", not "working".** A tick whose
`Start` returns an error is logged at `Error` level and counted in `Metrics().CronFailures`;
the status stays `StatusRunning`, the entry is not restarted, and the error never crashes the
orchestrator. `Health()` and `IsReady()` inherit this: a cron entry in `StatusRunning` is
eligible for a health probe exactly like a persistent one, but neither a failing nor a
successful tick is reflected in its `ServiceStatus`.

`WithCron` is validated by `Register` on both the static and the hot-add path, so the same spec
is accepted or rejected identically: an empty or malformed spec returns `ErrInvalidCron` rather
than silently registering a non-cron service. A sub-second `@every` interval (including
`@every 0s`) is accepted and clamped to one second by the parser.

A statically registered cron entry starts as `StatusRunning`; a hot-added one is **staged** by
`Register` (spec validated, no schedule installed) and reports `StatusRegistered` until
`StartService` schedules it. `StopService` removes the schedule and a later `StartService`
re-creates it, while `Unregister` drops the entry itself.

### Messaging

Every `ServiceContext` carries a `*Messenger`: a topic-based, in-process pub-sub hub.

<!-- fragment -->
```go
ch, unsub := messenger.Subscribe("topic")
messenger.Publish(msg, "topic") // send to the subscribers of "topic"
messenger.Publish(msg)          // broadcast to ALL subscribers
```

`Publish` is **drop-only**: when a subscriber's buffered channel is full, the message is
dropped for that subscriber. The publisher never blocks and dropped messages are never
replayed. Use `SubscribeWithBuffer` to size a slow consumer's buffer (a negative capacity
returns `ErrInvalidBufferSize` instead of panicking); `Drain` closes every subscriber channel
and clears subscriptions.

Subscriptions are scoped to the instance or cron tick that created them. When that owner ends,
its subscriptions are released: `Subscribe` on the released view returns an already-closed
channel and `Request`/`RequestAsync` return an error instead of a nil reply. A newly created
view still subscribes normally.

Request-reply uses a `ReplyTopic` on the incoming `Message`:

<!-- fragment -->
```go
// Requestor:
resp, err := messenger.Request(ctx, payload, "orders.create")

// Responder (inside a service goroutine):
reqCh, _ := gorch.TypedSubscribeRequest[CreateOrderReq](messenger, "orders.create")
for env := range reqCh {
	gorch.TypedRespond(messenger, processOrder(env.Value), env.ReplyTopic)
}
```

`RequestAsync` returns a response channel immediately instead of blocking. For a fully
type-safe round trip, use `TypedRequest`, `TypedSubscribeRequest`, and `TypedRespond`:

<!-- fragment -->
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
		gorch.TypedRespond(messenger, processOrder(env.Value), env.ReplyTopic)
	}
}()

// Requestor:
resp, err := gorch.TypedRequest[CreateOrderReq, CreateOrderResp](
	messenger, ctx, req, "orders.create",
)
```

Typed publish/subscribe works for one-way events too:

<!-- fragment -->
```go
type OrderEvent struct {
	OrderID string
	Status  string
}

gorch.RegisterType[OrderEvent](messenger)

gorch.TypedPublish(messenger, OrderEvent{OrderID: "42", Status: "shipped"}, "orders")

ch, _ := gorch.TypedSubscribe[OrderEvent](messenger, "orders")
for evt := range ch {
	fmt.Println(evt.OrderID) // typed, no cast needed
}
```

The wire format is `encoding/gob` and is public contract: types passed through the typed
helpers must be gob-compatible, and changing a field layout is a breaking change. Hand-decoding
the gob payload is error-prone; the typed helpers exist so you do not have to.

### Groups and labels

`WithGroup` assigns a service to a named group for bulk operations; `WithLabel` attaches
arbitrary key-value tags for filtering.

<!-- fragment -->
```go
orch.Register(dbSvc, gorch.WithName("db"), gorch.WithGroup("infra"))
orch.Register(cacheSvc, gorch.WithName("cache"), gorch.WithGroup("infra"))
orch.Register(apiSvc, gorch.WithName("api"), gorch.WithGroup("app"), gorch.DependsOn("db", "cache"))

err := orch.StartGroup("infra")
err = orch.StopGroup("app", 5*time.Second)
err = orch.UnregisterGroup("infra", 5*time.Second, gorch.WithCascadeStop())

infra := orch.StatusesByGroup("infra")           // map[string]ServiceStatus
critical := orch.StatusesByLabel("tier", "critical")
```

A group is a **filter tag, not a registered entity**: an unknown or empty group selects
nothing and returns `nil`. `StartGroup` before `Start` returns `ErrOrchestratorNotStarted`
(the members would build a context from a nil parent), while `StopGroup` before `Start` is a
no-op, consistent with `StopService`/`Unregister`. A mid-group `StartGroup` failure rolls back
the members already started. `UnregisterGroup` is the group-level analogue of `Unregister`:
it removes every member (cron and `runOnce` members included), refuses to break a
`Running`/`Starting` hard dependent outside the group with `ErrHasDependents`, and accepts
`WithCascadeStop` or `Orphans` to resolve that (the two are mutually exclusive).

### Logging

Services log through `ServiceContext.Logger`:

<!-- fragment -->
```go
sc.Logger.Info("request completed", "status", 200, "latency", 12*time.Millisecond)
// 2026-07-27 14:30:05.123 INFO  my-service --- request completed status=200 latency=12ms
// (the prefix is the service name: WithName, or the auto-assigned $N)
```

The built-in log-pump writes to `os.Stderr`, and `WithLogLevel` filters entries
(`Debug < Info < Warn < Error`). `ServiceLogger` never blocks: the send is non-blocking, so an
entry is dropped when the channel is full or its pump has been signalled to stop. A torn-down
log channel can therefore never stall a service or a `Stop`; the cost of a dead channel is
lost lines, never a hang.

Inject any logger satisfying the `Logger` interface — `*slog.Logger` satisfies it directly:

<!-- fragment -->
```go
orch := gorch.New(gorch.WithLogger(slog.Default()))
// level=INFO msg="request completed" service=api status=200 latency=12ms
```

When a custom logger is set, the built-in log-pump is disabled entirely and `WithLogLevel` is
ignored (the logger manages its own filtering). The service name is prepended as
`"service"=<name>` to every call.

The exact built-in output format and key ordering are **not stable**; see
[Versioning and breaking changes](#versioning-and-breaking-changes).

## Dynamic membership

Before `Start` the registry is static and the whole graph is validated at once. After a
successful `Start`, membership can change while the orchestrator runs.

### Hot add

`Register` on a running orchestrator adds the service as `StatusRegistered`; it is **not**
started automatically. Start it with `StartService`, which requires every hard dependency to
be `Running` (or a succeeded `runOnce` gate):

<!-- fragment -->
```go
orch.Start()

_ = orch.Register(late, gorch.WithName("late"), gorch.DependsOn("db"))
_ = orch.StartService("late") // every hard dependency must be Running

// Stop but keep the entry: Names()/Statuses() still report it, and
// StartService can bring it back.
_ = orch.StopService("late", 5*time.Second)

// Stop and remove: it leaves Names()/Statuses(), and its cron schedule and
// Messenger subscriptions are released.
_ = orch.Unregister("late", 5*time.Second)
```

`StartService` also (re)starts a service previously stopped with `StopService`. On a service
that is still `Running` it is a no-op, so calling it from a reconciliation loop is safe;
restarting a live instance is always explicit (`StopService` then `StartService`). A hot-added
cron entry follows the same rule: `Register` stages the schedule, and `StartService` installs
it.

Registering a hard dependency that is mid-removal fails with `ErrDependencyRemoving`, a
retryable condition distinct from `ErrHasDependents`.

### StopService vs Unregister

| | `StopService` | `Unregister` |
|---|---|---|
| Stops the instance / cancels in-flight ticks | yes | yes |
| Keeps the name in the graph | yes | no |
| `Names()`/`Statuses()` after | still present, `StatusStopped` | gone |
| `StartService` afterwards | restarts it | `ErrServiceNotFound` |
| Drops the cron schedule | yes (re-created by `StartService`) | yes |
| Releases Messenger subscriptions | yes | yes |
| Discards per-instance state (contexts, counters, owner ids) | yes | yes |

`Unregister` leaves nothing behind: re-registering the same name creates a brand-new entry
that inherits nothing from the previous incarnation. `ReplaceService` is the third mode — it
keeps the entry registered and swaps only its implementation, so hard dependents are neither
torn down nor blocked (they read `IsReady` false only while the swap is in progress):

<!-- fragment -->
```go
_ = orch.ReplaceService("db", newDBSvc, 5*time.Second)
```

`ReplaceService` applies the same hard-dependency gate as `StartService`, freezes the entry's
registration-time config, and aborts the swap (leaving the old implementation in place) if the
old instance's teardown does not complete cleanly. It never cascades: `WithCascadeStop` is
rejected with `ErrUnsupportedOption`, and `Orphans` is accepted as a no-op.

### Cascade

A plain stop refuses to break a hard dependent that is `Running` or `Starting`:

<!-- fragment -->
```go
err := orch.StopService("db", 5*time.Second)
// ErrHasDependents: service has active dependents: db is depended on by api, worker

// The error is a *HasDependentsError carrying the same set, so a CLI can offer
// a confirmation prompt without parsing the message:
var depErr *gorch.HasDependentsError
if errors.As(err, &depErr) {
	fmt.Println("stopping", depErr.Name, "will also stop:", depErr.Dependents)
}

// Plan before acting: the same set is available as a query.
blockers, _ := orch.Dependents("db")   // [api worker], reverse topological
deps, _ := orch.DependenciesOf("api")  // [db], direct hard dependencies

// Tear down the target and its transitive hard dependents in reverse
// topological order, under one shared timeout. A dependent already stopping is
// left to its own teardown and never stopped twice.
_ = orch.StopService("db", 5*time.Second, gorch.WithCascadeStop())
```

`Dependents(name)` returns exactly the transitive hard dependents that block a plain stop —
the ones `Running` or `Starting` — in reverse topological order, so it is empty exactly when
the stop would proceed, and it backs the `*HasDependentsError` payload. A non-blocking hard
dependent (`Registered`, `Stopping`, `Crashed`, `Stopped`, `Succeeded`) is still part of the
graph but is not returned. Soft dependencies never block and are never cascaded.

`Orphans()` is the explicit opposite: it stops only the target and leaves running hard
dependents `StatusRunning` but degraded — `IsReady` reports them not ready until the dependency
is back, and `StartService` of the orphan keeps failing with `ErrDependencyNotRunning` until
then. `Orphans` and `WithCascadeStop` are mutually exclusive and are rejected with
`ErrUnsupportedOption`.

### Reservations

A membership operation can be blocked by an **in-flight reservation** on the target, and the
sentinel tells the caller which situation it is in:

- A genuine same-goroutine re-entry — a service calling a membership op on itself from its own
  `Start` or `Stop` — is a programming error and returns `ErrReentrantMembership`; never
  retry. The whole callback counts: a persistent service owns its `Start` goroutine for the
  instance's lifetime, so `StartService("self")` from that goroutine is a re-entry even after
  the entry reached `StatusRunning`, not the idempotent no-op an *external* caller gets.
- A collision with a reservation held by *another* goroutine — an in-flight start transaction,
  teardown, or group operation — is transient and returns `ErrMembershipBusy`. The reservation
  is released when that operation returns, so poll `Busy(name)` (or retry) instead of racing.

**The reservation is the membership transaction, not a live instance.** A persistent service's
user `Start` runs in the instance goroutine long after the start transaction committed; the
entry is `StatusRunning` and `Busy(name)` is `false` throughout. Stopping it is an ordinary
stop, never `ErrMembershipBusy`. A cron entry holds no reservation while its schedule is
installed: a tick in flight is reached through the schedule's shared context, so `Busy` is
`false` and a stop cancels the tick rather than colliding.

**Status reports the lifecycle; `Busy` reports the reservation.** `Start`/`StartGroup` claim an
entry's reservation (`Busy(name) == true`) *before* `startOneService` commits `StatusStarting`,
so during the before-start hook and start condition the entry still reads its previous status
(typically `StatusRegistered`). `StatusStarting` means "`startOneService` has committed the
start", not merely "a start was claimed". No `ServiceStatus` value encodes the reservation.

### Names and auto-naming

- `WithName` assigns the name used as the key everywhere else: `DependsOn` edges,
  `StartService`/`StopService`/`Unregister`, `Status`/`WaitFor`, and the lifecycle hooks. Names
  must be unique across the registry.
- Omitting `WithName`, or passing an explicit empty string, auto-assigns `$1`, `$2`, … The
  counter advances on every registration — named services included — so it is not the count of
  unnamed services, and it never winds back: `Unregister` frees an explicitly named entry, but
  the auto-name sequence never reuses a value.
- Re-registering an explicitly named service after its `Unregister` completes is allowed.
- Registering a hard dependency on a service that is mid-removal fails with
  `ErrDependencyRemoving`, so a torn-down entry cannot acquire new dependents.

## Concurrency

All methods are safe to call from multiple goroutines unless noted otherwise. The table below
summarises what may run concurrently with a live `Start`/`Stop`.

| Method group | Concurrent with `Start`/`Stop` |
|--------------|-------------------------------|
| `Register`, `RegisterFunc` | Before `Start` the registry is static (whole graph validated at once). A hot add made while `Start` runs lands either in `Start`'s snapshot or, after it, live as `StatusRegistered` (never auto-started); before `Start` it is part of the static graph. It is rejected with `ErrOrchestratorStopping` while `Stop` runs and `ErrOrchestratorStopped` after `Stop`. |
| `StartService` | Yes — starts a registered service once every hard dependency is `StatusRunning` (or is a `runOnce` gate that reached `StatusSucceeded`, which satisfies the edge); an already-`Running` persistent/cron entry is a no-op (a `runOnce` entry is the deliberate re-run exception). It never restarts a live instance: to replace one, `StopService` and then `StartService` — and `StopService` is itself refused with `ErrHasDependents` while a hard dependent is `Running` or `Starting`, unless `WithCascadeStop` is used, so restarting a depended-on service bounces those dependents first. The start decision and its reservation are claimed atomically under the membership lock (released before any user code), so concurrent `StartService` calls cannot double-start or orphan an instance. Returns `ErrOrchestratorNotStarted` before `Start`, and is rejected with an error once whole-orchestrator `Stop` has begun. A hard dependency must have been registered before the dependent, both statically and on a hot add. Colliding with a reservation held by *another* goroutine — an in-flight start transaction, or a teardown that has claimed the entry but not yet removed it — returns the transient `ErrMembershipBusy` (poll `Busy`); a start re-entered from the target's own `Start` or `Stop` on the same goroutine returns the permanent `ErrReentrantMembership`. A persistent service owns its `Start` goroutine for the instance's lifetime, so a self-start from that callback stays a re-entry even after the reservation cleared. |
| `StopService`, `Unregister` | Yes — stop (keep registered) or stop-and-remove a service while the lifecycle runs. Serialized against each other and against `StartGroup`/`StopGroup` by the membership lock. A hard dependent that is `Running` or `Starting` blocks the call with `ErrHasDependents`, returned as a `*HasDependentsError` that names each blocker; every other dependent status — `Stopping`, `Registered`, `Crashed`, `Stopped`, `Succeeded` — does not block, so a plain stop proceeds and leaves the dependent untouched. With `WithCascadeStop`, every transitive hard dependent is stopped/removed in reverse topological order, except one already `Stopping`, which is left to its own in-flight teardown rather than stopped a second time (no double `Stop()` or hooks). A collision with another goroutine's in-flight reservation returns the transient `ErrMembershipBusy` (poll `Busy`), while a re-entry from the target's own `Start`/`Stop` returns `ErrReentrantMembership`. |
| `ReplaceService` | Yes — atomically swaps a registered service's implementation while keeping its name in the graph, so hard dependents are not torn down or blocked. Serialized against `StartService`/`StopService`/`Unregister` and the group ops by the membership lock and the entry reservation. Unlike `StopService`, it applies no dependent guard and never cascades: `WithCascadeStop` is rejected with `ErrUnsupportedOption` and `Orphans` is accepted as a no-op. The old instance is torn down exactly as `StopService` would tear down the target, and only if that completes cleanly is `svc` installed and a fresh instance started; a timeout or a `Stop()` error aborts the swap and leaves the old implementation in place. `svc` goes through `Validator.Validate()`. The same hard-dependency gate as `StartService` applies, and the entry's registration-time config is frozen. Returns `ErrOrchestratorNotStarted` before `Start`, `ErrServiceNotFound` for an unknown name, and is gated by shutdown like the other membership ops; a collision returns `ErrMembershipBusy` and a re-entry from the target's own `Start`/`Stop` returns `ErrReentrantMembership`. |
| `Start`, `Stop` | Yes — against each other. The lifecycle is single-shot and each entry point is guarded by its own `sync.Once`. Concurrent `Start` calls are claimed atomically: the caller that wins the claim runs the start and every other caller returns `ErrAlreadyStarted` immediately, without waiting for the winner, so a `nil` return always identifies the call that actually ran it. Concurrent `Stop` calls are a no-op for the losers: the first runs the shutdown, the others return `nil` once it completes and do not observe its errors. After a successful `Stop` neither can run again; a failed `Start` does not consume the lifecycle and may be retried. `Stop`'s `timeout` bounds the whole shutdown, including every service's before/after-stop hooks and `Stop()` call. |
| `Status`, `Statuses`, `Names`, `Count`, `CountRunning`, `RunningNames`, `Dependents`, `DependenciesOf`, `Busy` | Yes — safe to read while services run, while membership churns, and during shutdown. `Count`/`Names`/`Statuses` are the **registered** surface (a hot-added, not-yet-started entry and a staged cron entry both appear as `StatusRegistered`); `CountRunning`/`RunningNames` are the `StatusRunning` subset, so `"N of M running"` is `CountRunning()` of `Count()`. `Dependents`/`DependenciesOf` walk the hard-dependency edges under the graph lock and return snapshots. `Busy(name)` is the predicate for the transient `ErrMembershipBusy`: it reports whether a registered entry currently holds an in-flight reservation — an active start transaction or teardown — and is the only observable for that window. It is not "a live instance": once a persistent service's goroutine is spawned the transaction is complete and `Busy` is false for the rest of the instance's life, and a cron entry is never busy while its schedule is installed, so neither a running persistent service nor an in-flight cron tick makes a stop return `ErrMembershipBusy`. |
| `Health`, `IsReady`, `WaitFor` | Yes — each probe/tick takes its own read lock; `IsReady` honours the caller's `ctx`. |
| `Metrics`, `Done` | Yes — atomic counters and a shutdown-completed channel created once in `New` and returned unchanged. |
| `StartGroup`, `StopGroup`, `UnregisterGroup` | Drive one group per orchestrator. Serialized with `StopService`/`Unregister` by the membership lock; a group start reserves each member and rolls back the members it already started if a later one fails, and a group stop honours a concurrent start's reservation by skipping that entry. `StartGroup` before `Start` returns `ErrOrchestratorNotStarted` (a member would otherwise build its context from a nil parent), while `StopGroup` before `Start` is a no-op returning `nil`, consistent with `StopService`/`Unregister`. An unknown or empty group selects nothing and returns `nil`: a group is a filter tag, not a registered entity. Both are gated by shutdown (`ErrOrchestratorStopping`/`ErrOrchestratorStopped`). Group ops are not synchronized with the whole-orchestrator `Start`/`Stop` beyond those gates: do not drive them from inside a concurrent `Start`/`Stop`. |
| `Messenger` (`Subscribe`, `SubscribeWithBuffer`, `Publish`, `Request`, `RequestAsync`, `Drain`) and the typed helpers | Yes — all Messenger methods are safe for concurrent use. `SubscribeWithBuffer` rejects a negative capacity with `ErrInvalidBufferSize` instead of panicking, and `Request`/`RequestAsync`/`TypedRequest` reject a nil context with `ErrNilContext` instead of panicking on `ctx.Done()`. |

Service implementations are responsible for their own internal concurrency: `Start` runs in
its own goroutine and `Stop` may be called from another after context cancellation.

## Errors

`Start`, `Stop`, and the group operations aggregate failures with `errors.Join`, so one call
reports every cause. Use `errors.Is`/`errors.As` to inspect them. Sentinel errors returned by
the orchestrator:

| Error | Returned by | Meaning |
|-------|-------------|---------|
| `ErrAlreadyStarted` | `Start` | Called after the orchestrator already started (including after a `Stop`). |
| `ErrDuplicateName` | `Register` | Two services share a `WithName`. |
| `ErrDependencyCycle` | `Register`, `Start`, `Stop` (defensive), `StartGroup`, `StopGroup` | A hard or soft dependency chain loops. `Register` rejects it up front; `Start`, `Stop`, and the group ops re-check their selected subset and surface it defensively if a cycle survived registration. |
| `ErrStartAborted` | `Start` | A hard/soft dependency failed or was skipped. |
| `ErrInvalidCron` | `Register` (static and hot add), `Start` (defensive re-check) | A `WithCron` spec is empty or invalid. `Register` validates the spec on both paths, so the same spec is accepted or rejected identically; `Start` re-checks defensively in case an entry's spec was changed after registration. A sub-second `@every` interval (including `@every 0s`) is *not* invalid: the underlying parser clamps it to one second. |
| `ErrUnsupportedOption` | `Register`, `ReplaceService`, `UnregisterGroup` | `WithSelfHeal` combined with `WithCron`/`WithRunOnce`; `WithCascadeStop` passed to `ReplaceService` (replace never cascades); `WithCascadeStop` and `Orphans` passed together. |
| `ErrStopTimeout` | `Stop`, `StopService`, `Unregister`, `ReplaceService`, `UnregisterGroup`, `Start` (failed-start rollback) | A stop did not finish within the caller's timeout: the before/after-stop hooks, `Stop()`, or the wait for the instance to exit was still in flight. On the stop methods the entry is left `StatusStopping` (not `StatusStopped`) and the stop is not counted in `Metrics().Stops`. `ReplaceService` aborts the swap and keeps the old implementation. On a failed `Start` it means the bounded rollback budget was exceeded; the rollback still resets the snapshotted entries to `StatusRegistered`, leaving the orchestrator retryable. |
| `ErrHookTimeout` | `Stop`, `StopService`, `Unregister`, `ReplaceService`, `UnregisterGroup`, `Start` (failed-start rollback) | A before-stop hook overran the share of the deadline reserved for it. Always joined with `ErrStopTimeout`, so callers that only classify whole-stop timeouts still match. On the stop methods the teardown is unverified, so the entry stays `StatusStopping` (and `ReplaceService` does not swap); on a failed `Start` the rollback resets it to `StatusRegistered`. |
| `ErrNilService` | `Register`, `RegisterFunc`, `ReplaceService` | A nil `Service`, or a nil `Start` closure passed to `RegisterFunc`. |
| `ErrOrchestratorNotStarted` | `StartService`, `StartGroup`, `ReplaceService` | Called before the orchestrator was started, so there is no service context or scheduler yet. |
| `ErrReentrantMembership` | `StartService`, `StopService`, `Unregister`, `ReplaceService` | A membership op re-entered from the target's own `Start`/`Stop` on the same goroutine (e.g. a service stopping itself from its `Start`). The whole callback counts, so a persistent service self-starting from its `Start` is a re-entry for the instance's lifetime, not the running-service no-op an external caller gets. A programming error: fix the code, do not retry. Group ops skip a reserved entry instead of returning it. |
| `ErrMembershipBusy` | `StartService`, `StopService`, `Unregister`, `ReplaceService` | A membership op collided with a reservation held by another goroutine — an in-flight start transaction, a teardown, or a group operation. Not a running persistent service or an in-flight cron tick (those are ordinary stops). Transient: poll `Busy(name)` or retry once the reservation clears. |
| `ErrInvalidBufferSize` | `SubscribeWithBuffer` | The buffer capacity was negative. A permanent caller bug: fix the size, do not retry. |
| `ErrNilContext` | `Request`, `RequestAsync`, `TypedRequest` | A nil `context.Context` was passed. A permanent caller bug: pass `context.Background()` for no cancellation or deadline, do not retry. |
| `ErrServiceNotFound` | `StartService`, `StopService`, `Unregister`, `ReplaceService` | No registered service has that name (or it has already been `Unregister`ed). An entry mid-teardown is still registered and reports `ErrMembershipBusy`, not this. |
| `ErrHasDependents` | `StopService`, `Unregister`, `UnregisterGroup` | A stop/removal would break a hard dependent that is `Running` or `Starting`. Returned as a `*HasDependentsError` whose `Name` is the target and whose `Dependents` names each blocker (the same set as `Dependents(name)`). Pass `WithCascadeStop` to tear those dependents down too, or `Orphans` to leave them running but degraded. Dependents in `Stopping`, `Registered`, `Crashed`, `Stopped`, or `Succeeded` do not block. |
| `ErrDependencyNotFound` | `StartService`, `Register`, `ReplaceService` | A hard dependency is not registered (dynamically removed, or never added). |
| `ErrDependencyNotRunning` | `StartService`, `ReplaceService` | A hard dependency exists but is neither `StatusRunning` nor a `runOnce` gate in `StatusSucceeded`. |
| `ErrDependencyRemoving` | `Register` | A hot-added service names a hard dependency that is being torn down (being stopped/removed concurrently); retry after the teardown completes. |
| `ErrDependencyDepthExceeded` | `Register` | A dependency walk needed to check for a cycle ran deeper than the 10 000-edge limit. The registered graph is a bounded, startup-sized acyclic graph; reaching this depth means an unbounded registration/reload loop has grown it. The walk stops with this sentinel instead of overflowing the goroutine stack (a fatal error). Free or prune the graph and retry. |
| `ErrOrchestratorStopping` | `Register`, `StartService`, `StopService`, `Unregister`, `ReplaceService`, `StartGroup`, `StopGroup`, `UnregisterGroup` | Whole-orchestrator `Stop` is in progress. |
| `ErrOrchestratorStopped` | `Register`, `StartService`, `StopService`, `Unregister`, `ReplaceService`, `StartGroup`, `StopGroup`, `UnregisterGroup` | Whole-orchestrator `Stop` has completed. |

### Sentinel taxonomy

Sentinels are stable API surface, so each one is classified along two axes: a **class** and
whether it is **retryable**.

| Class | Retryable | Meaning | Sentinels |
|-------|-----------|---------|-----------|
| permanent | no | A bug in the caller's code or configuration; an identical call keeps failing until code or configuration changes. | `ErrDuplicateName`, `ErrNilService`, `ErrInvalidCron`, `ErrDependencyCycle`, `ErrDependencyNotFound`, `ErrServiceNotFound`, `ErrDependencyDepthExceeded`, `ErrUnsupportedOption`, `ErrReentrantMembership`, `ErrInvalidBufferSize`, `ErrNilContext` |
| transient | yes | A condition that may clear on its own or after another operation; retry once it does. | `ErrOrchestratorStopping`, `ErrOrchestratorNotStarted`, `ErrDependencyNotRunning`, `ErrDependencyRemoving`, `ErrHasDependents`, `ErrMembershipBusy`, `ErrStartAborted` |
| terminal | no | The whole-orchestrator lifecycle has already begun or ended; by design the operation can never succeed. | `ErrAlreadyStarted`, `ErrOrchestratorStopped` |
| environmental | no | User code or teardown overran a deadline and the outcome is unverified; the timed-out teardown is not retried. | `ErrStopTimeout`, `ErrHookTimeout` |

Only a **transient** sentinel is retryable. Classification is `errors.Is`-based: `Start`,
`Stop`, and the group ops join failures with `errors.Join`, so one returned error can match
several sentinels (for example `ErrHookTimeout` and `ErrStopTimeout` together). A caller
classifies like this:

<!-- fragment -->
```go
switch {
case errors.Is(err, gorch.ErrMembershipBusy):
	// transient: poll Busy(name), then retry
case errors.Is(err, gorch.ErrStopTimeout):
	// environmental: the teardown was abandoned; do not retry the same stop
case errors.Is(err, gorch.ErrAlreadyStarted):
	// terminal: the lifecycle is spent; do not retry
default:
	// permanent (a bug in the caller) unless a joined cause says otherwise
}
```

Decisions behind the table ([#29](https://github.com/lorenzo-vecchio/gorch/issues/29)):

- **`ErrHookTimeout` is a permanent companion, not a migration aid.** `ErrStopTimeout`
  attributes the deadline, `ErrHookTimeout` its cause (a before-stop hook that would not
  return); the pair is always joined so a caller that only classifies `ErrStopTimeout` still
  matches.
- **Reentrancy and contention are split.** `ErrReentrantMembership` is a same-goroutine
  programming error — permanent, never retried; a reservation held by *another* goroutine is
  the transient, retryable `ErrMembershipBusy`.
- **`ErrHasDependents` answers one direction only, and carries the names.** It is returned by
  `StopService`/`Unregister` when the *target* has active dependents, as a
  `*HasDependentsError` whose `Dependents` is the same set `Dependents(name)` returns;
  `DependenciesOf(name)` is the forward direction. Naming a dependency that is mid-removal is
  `ErrDependencyRemoving` instead, a distinct transient condition.
- **The not-found family is uniform.** `ErrServiceNotFound` (the named service),
  `ErrDependencyNotFound` (a named hard dependency), and `ErrDependencyNotRunning` (a named
  hard dependency not yet running) all follow `Err<Subject><Problem>`.
- **Every sentinel is reachable.** Each has a documented entry point and a producer test.
  `TestSentinelsTable_EverySentinelHasATest` fails when a new exported sentinel is added
  without a classification, and `TestSentinelsTable_ProducibleFromDocumentedEntry` fails when
  one cannot be produced from its entry point.

## Observability

Status introspection is a snapshot; every method is safe to call while services run.

<!-- fragment -->
```go
status, ok := orch.Status("db")         // ServiceStatus, bool
all := orch.Statuses()                  // map[string]ServiceStatus (all registered)
names := orch.Names()                   // []string in registration order (all registered)
count := orch.Count()                   // total registered services (not live)
runningCount := orch.CountRunning()     // number of StatusRunning services ("N of M running")
runningNames := orch.RunningNames()     // []string of StatusRunning services
blockers, err := orch.Dependents("db")  // transitive hard dependents blocking a stop (reverse topo)
deps, err := orch.DependenciesOf("api") // direct hard dependencies in declaration order
busy := orch.Busy("db")                 // true while another goroutine's start/teardown holds "db"
```

`Count`, `Names`, and `Statuses` are the **registered** surface: an entry appears the moment
`Register` accepts it, so a hot-added, not-yet-started persistent service and a staged cron
entry are both present and both read `StatusRegistered`. `CountRunning` and `RunningNames` are
the `StatusRunning` subset, backed by the same `Statuses` snapshot. `Dependents` and
`DependenciesOf` are the two directions of the hard-dependency graph; neither includes soft
dependencies, and both report `ErrServiceNotFound` for an unknown name. `WaitFor` blocks until
a service reaches a target status (or times out); a non-positive timeout fires immediately.

`Metrics()` returns a snapshot of monotonic atomic counters. Take a baseline and subtract — the
counters are never reset. They count lifecycle events, not per-tick work: a self-heal restart
is a `Restart` and never a `Stop`, a cron tick is not a `Start`, and a failed probe is counted
per probe, not per incident. You wire them into your own monitoring; gorch takes no metrics
dependency. Each stop of an instance is counted exactly once, even when a teardown and the
instance's own exit race.

| Counter | Definition |
|---------|------------|
| `Starts` | Service starts initiated through the lifecycle API (`Start`, `StartService`, `StartGroup`, `Run`): one per persistent/`runOnce` instance launched and one per cron schedule installed. It counts invocations, not successes, so a start that immediately fails is still counted. Self-heal re-launches are in `Restarts`, so `Starts + Restarts` is the total number of instances launched. |
| `Stops` | Completed instance stops: a lifecycle-API stop (`Stop`, `StopService`, `StopGroup`, `Unregister`, `UnregisterGroup`) and a non-self-heal instance that exits on its own and is committed `StatusStopped` both count. A stop that timed out (`ErrStopTimeout`/`ErrHookTimeout`, or a `WithStopTimeout` cap that fired) is not counted at that moment, matching its unverified `StatusStopping`; it counts only once the abandoned instance finally exits, so one that never does is never counted. The internal `Stop()` a self-heal restart runs is not counted either. Counted at most once per instance. |
| `Crashes` | `Running -> Crashed` transitions: an instance that exited with a real error, or whose retry budget was exhausted. A self-heal crash is counted before the restart, so it is observable even when the service comes straight back up. Clean or `context.Canceled` exits are not crashes. |
| `Restarts` | Self-heal re-launches: a fresh instance spawned after an instance exited on its own (crash, clean return, or a health-threshold cancellation) without a caller-initiated teardown. |
| `HealthFails` | Failed periodic health probes, incremented once per failed probe rather than once per service that crossed the failure threshold. Probes issued on demand by `Health()` are not counted; the counter instruments the supervision loop. |
| `CronFailures` | Cron tick invocations that failed: the tick's `Start` returned a non-`context.Canceled` error, or panicked. A tick skipped by `CronSkip` while the previous invocation is still running, and a tick cancelled by teardown, are not failures. Ticks are not counted as `Starts`, so this is the counter for per-tick cron activity. |
| `AbandonedGoroutines` | Teardown goroutines abandoned because a deadline won: a blocking hook or `Stop()`, the whole-`Stop` final wait for the instance goroutines and the log-pump, or a failed-`Start` wait that outlived its rollback budget. Monotonic — never decremented, since there is no reliable signal that an abandoned goroutine later returned — so a non-zero value means user code may be leaked for the process lifetime. |

<!-- fragment -->
```go
stats := orch.Metrics()
fmt.Printf("starts=%d stops=%d crashes=%d restarts=%d healthFails=%d cronFailures=%d abandonedGoroutines=%d\n",
	stats.Starts, stats.Stops, stats.Crashes, stats.Restarts, stats.HealthFails, stats.CronFailures, stats.AbandonedGoroutines)
```

`OnStateChange` fires on every status transition; `OnCrash` fires specifically on
`Running -> Crashed`. `Done()` is a **shutdown-completed** signal: its channel closes once
`Stop` (or `Run`, which calls `Stop`) returns — even if every service was unregistered first,
even after a failed `Start`, and even if a timed-out stop abandoned a goroutine — and stays
open until then. It is not a "every goroutine has exited" guarantee; watch
`Metrics().AbandonedGoroutines` for that.

<!-- fragment -->
```go
select {
case <-orch.Done(): // Stop/Run returned
case <-time.After(10 * time.Second):
}
```

## Contract and guarantees

These guarantees are part of the public API and are relied upon by callers.

- **Wire format is gob.** `encoding/gob` is the serialization format for typed messages and
  request-reply payloads. It is public contract: types passed through
  `TypedPublish`/`TypedSubscribe`/`TypedRequest`/`TypedRespond` must be gob-compatible, and
  changing a field layout is a breaking change.
- **`Publish` is drop-only.** Delivery is best-effort. When a subscriber's channel is full the
  message is dropped for that subscriber; gorch never blocks the publisher and never replays
  dropped messages.
- **Membership is dynamic.** Before `Start`, registration is static. After a successful
  `Start`, `Register` adds a service to the live graph as `StatusRegistered` (it is not
  auto-started), and `StartService`, `StopService`, `Unregister`, `UnregisterGroup`, and
  `ReplaceService` change the graph while the orchestrator runs. `StopService` keeps the entry
  registered so it can be started again; `Unregister` removes it. `StopService` and
  `Unregister` both cancel the service's context and release its Messenger subscriptions,
  which are scoped to the instance or tick that created them; the live instance and any
  in-flight cron ticks are then awaited until they exit. A tick abandoned for outliving the
  deadline still has its subscriptions released: teardown drains every live owner id of the
  entry. The failed-`Start` rollback drains the same way. Owner ids are monotonic and never
  reused. `Unregister` also drops the cron schedule and discards the entry's per-instance
  state (instance and teardown contexts, exit channel, retry/health counters, stability
  window, liveness/accounting flags, owner ids, and cron accounting) from both the `entries`
  slice and the name index, using the same "fresh" definition as a failed-`Start` retry.
  Re-registering the same name therefore creates a brand-new entry that inherits nothing. A
  stop is refused with `ErrHasDependents` while a hard dependent is `Running` or `Starting`
  (the returned `*HasDependentsError` names each blocker), unless `WithCascadeStop` is passed;
  a dependent that is `Stopping`, `Registered`, `Crashed`, `Stopped`, or `Succeeded` does not
  block, and a cascade leaves an already-`Stopping` dependent to its own teardown instead of
  stopping it twice. Soft dependencies never block and are never cascaded. A membership op
  re-entered from the target's own `Start`/`Stop` returns `ErrReentrantMembership` (a
  programming error), while one colliding with a reservation held by another goroutine returns
  the retryable `ErrMembershipBusy`; `Busy(name)` observes the latter without racing. The
  reservation is the membership transaction, not a service's own callback: a running persistent
  service and an in-flight cron tick are not reserved, so stopping them is an ordinary stop.
- **Whole-orchestrator lifecycle is single-shot.** After a successful `Stop`, neither `Start`
  nor `Register` can be used again; `Start` returns `ErrAlreadyStarted` and `Register` returns
  `ErrOrchestratorStopped`. A failed `Start` does not consume the lifecycle and may be retried.
  Concurrent `Start` calls are claimed atomically: one caller performs the start and the rest
  return `ErrAlreadyStarted` without waiting, so a `nil` return always identifies the caller
  that actually ran it.
- **Errors are aggregated.** `Start` and `Stop` join every failure with `errors.Join`, so a
  single call reports all causes, not just the first. Use `errors.Is`/`errors.As` to inspect
  them.
- **No user code runs under an orchestrator lock.** The orchestrator never calls service code —
  `Start`, `Stop`, `Validate`, health/readiness probes, or any lifecycle hook — while holding
  `o.mu` or `membershipMu`. A hook or `Validate` may therefore block or call back into a
  membership operation without deadlocking. This is enforced as a class-wide invariant (static
  and dynamic registration, `Health` and the periodic probe path, `Start`, and group
  selection), not per call site.
- **A misbehaving callback does not panic the caller.** A panic raised by a lifecycle hook,
  `Validator`, start condition, or health/readiness probe is recovered and reported as an error
  (or as unhealthy/unready); it never unwinds through a public entry point. `Register(nil, …)`
  and a nil `RegisterFunc` `startFn` are rejected with `ErrNilService` rather than deferred to a
  panic at `Start`. `StartGroup` before `Start` is gated with `ErrOrchestratorNotStarted`,
  `SubscribeWithBuffer` rejects a negative capacity with `ErrInvalidBufferSize`, and
  `Request`/`RequestAsync`/`TypedRequest` reject a nil context with `ErrNilContext`; none of
  these unwind.
- **An `Orchestrator` zero value is usable.** Build one with `New` to configure it, or declare
  `var o Orchestrator`: the first public call lazily initialises a zero-value instance with
  `New()`'s defaults, so it can be registered against and started without panicking on a nil
  registry, Messenger, or shutdown channel. A zero-value `Messenger` is likewise usable.
- **A blocked before-stop hook cannot strand a service.** `Stop`'s budget is split so the hook
  gets at most half of what remains; the service's own `Stop()` is always invoked, and a hook
  that overruns is reported as `ErrHookTimeout` (also matching `ErrStopTimeout`).
- **A failed `Start` rolls back within a bounded budget.** The cleanup stops the already-started
  services and then waits for their instance and log-pump goroutines under one budget shared by
  every step (`WithFailedStartTimeout`, default 30s), mirroring `Stop`. A service that ignores
  cancellation and blocks in `Stop()` is reported as `ErrStopTimeout` and does not hang `Start`
  — unless `WithFailedStartTimeout` is set negative, which removes the bound. The reset that
  follows is best-effort, and the orchestrator is left restartable so `Start` can be retried.
- **A timed-out stop is reported honestly.** A stop that does not finish inside the caller's
  timeout — because a hook overran, `Stop()` was still running, or the instance had not exited
  — leaves the entry `StatusStopping`, not `StatusStopped`, and does not increment
  `Metrics().Stops`. `Status()`/`Statuses()` never claim a service stopped while it may still
  be alive, and `WaitFor(name, StatusStopped, …)` does not succeed for a timed-out stop. A
  succeeded `runOnce` gate is the one entry that never enters `StatusStopping`: it keeps
  `StatusSucceeded` even when its stop times out.

## Versioning and breaking changes

gorch follows [Semantic Versioning](https://semver.org/). Until 1.0, minor releases may contain
breaking changes; every one is marked **Breaking** in the
[release notes](https://github.com/lorenzo-vecchio/gorch/releases) and recorded in
[Upgrading](#upgrading).

- **Stable** — exported identifiers (types, functions, methods), the `Service`,
  `HealthChecker`, `ReadinessChecker`, `Validator`, and `Logger` interfaces, the
  `ServiceStatus` values, the sentinel errors above, and the gob wire format.
- **Not stable** — the built-in logger's exact output format and key ordering, internal
  goroutine counts, and anything unexported. Do not parse log lines.
- **Deprecations** — a deprecated exported identifier keeps working for at least one minor
  release and is listed as `Deprecated:` in its doc comment and in the release notes before
  removal.

`doc.go` is the what-and-why (short, stable; rendered on pkg.go.dev) and this README is the
how-to (long, navigable). Where the two overlap they must not contradict; the contract in
`doc.go` is the source of truth, and this README expands it with examples.

## Upgrading

There is no separate migration guide: `README.md` is the only markdown document until `v1.0.0`,
and this section is the cumulative pre-1.0 break record. Release-by-release prose lives in the
[release notes](https://github.com/lorenzo-vecchio/gorch/releases); below is what a consumer has
to change.

### Breaking changes by release

| Release | Breaking change | Before | After |
|---|---|---|---|
| v0.6.0 | Import path flattened | `github.com/lorenzo-vecchio/gorch/gorch` | `github.com/lorenzo-vecchio/gorch` |
| v0.6.0 | `WithHealthChecks` signature | `WithHealthChecks(interval, timeout, threshold)` | `WithHealthChecks(interval, WithProbeTimeout(...), WithFailureThreshold(...))` |
| v0.6.0 | Silently ignored options rejected | `WithSelfHeal` + `WithCron`/`WithRunOnce` registered a non-healing service | `Register` returns `ErrUnsupportedOption` |
| v0.7.0 | `Register` after `Start` is a hot add | returned `ErrAlreadyStarted` | adds the service as `StatusRegistered`; start it with `StartService` |
| v0.7.0 | Messenger subscriptions scoped per instance | a subscription lived until `Drain` | released when the instance or cron tick that created it ends |
| v0.8.0 | `StopService`/`Unregister` timeout scope | bounded only the wait for a persistent instance to exit | bounds the whole stop, `Stop()` included |
| v0.8.0 | Stop waits for in-flight cron ticks | cancelled but did not await them | cancelled and awaited |
| v0.8.0 | A released Messenger view cannot re-subscribe | subscriptions could be revived after `Drain` | `Subscribe` returns a closed channel; `Request` returns an error |
| v0.9.0 | Caller deadline bounds the whole stop | a blocking hook could outlast the deadline | hooks are capped and an overrun is `ErrHookTimeout` |
| v0.9.0 | `StartService` before `Start` | panicked on a nil scheduler | returns `ErrOrchestratorNotStarted` |
| v0.9.0 | Hot-added cron entries are staged | ticked immediately as `StatusRegistered` | registered but not scheduled until `StartService` |

### API added since v0.9.0

These additions are unreleased (`main` will ship them in `v1.0.0`), and none of them renames or
removes an existing identifier — existing code keeps compiling:

- `ReplaceService`, `UnregisterGroup`, `Orphans()`, `Dependents()`, `DependenciesOf()`,
  `CountRunning()`, and `RunningNames()`.
- `ErrMembershipBusy` (transient) and `ErrDependencyRemoving` (transient); reentrancy is the
  exported, permanent `ErrReentrantMembership`.
- `ErrHasDependents` is now returned as a `*HasDependentsError` carrying the blocking names; it
  still matches with `errors.Is`.

### Silent behaviour changes

These compile and run identically but behave differently — the category that bites without a
compiler error.

- **A stop that times out leaves `StatusStopping`, not `StatusStopped`, and is not counted in
  `Metrics().Stops`.** Before v0.9.0 a timeout claimed `StatusStopped` even while the service
  might still be alive.
- **A self-heal crash now reports `Running -> Crashed` before the restart** (with `OnCrash` and
  the `Crashes` counter), instead of silently restarting.
- **`Done()` is a shutdown-completed signal.** It closes when `Stop` returns, not when every
  goroutine has exited, and a failed `Start` leaves it open so a retry can still close it.
- **A hot-added cron entry no longer ticks until `StartService` schedules it**, even though
  `Register` accepts it.
- **A failed `Start` no longer accumulates Messenger owner ids across retries**, because the
  rollback drains them.
- **Failed-`Start` rollback and `Stop` unwind in reverse topological order**, so a dependency
  is never torn down before its dependent regardless of registration order.
- **After `Unregister`, re-registering the same name yields a fresh entry** with no inherited
  counters, contexts, or owner ids.
- **`Metrics().Stops` counts each instance's stop exactly once**, even when a teardown and the
  instance's own exit race.

## Pitfalls and gotchas

- **A before-stop hook is capped at half the remaining budget.** A slow hook does not starve
  the service's own `Stop()`; an overrun is `ErrHookTimeout` (joined with `ErrStopTimeout`).
- **A timed-out stop is honest, not optimistic.** The entry stays `StatusStopping` and
  `WaitFor(..., StatusStopped, ...)` will not succeed; the stop is not counted until the
  instance finally exits.
- **`Done()` does not mean "all goroutines exited".** A timed-out stop closes it while an
  abandoned goroutine may still run. Watch `Metrics().AbandonedGoroutines`.
- **Hot adds are not auto-started.** `Register` on a running orchestrator yields
  `StatusRegistered`; call `StartService`.
- **`StatusRunning` for a cron entry means "scheduled".** A failing tick is logged and counted
  but changes no status.
- **`Publish` is lossy.** A full subscriber buffer drops messages for that subscriber; there is
  no replay.
- **The built-in log format is not stable.** Do not parse log lines; use `OnStateChange` or
  `Metrics` for machine-readable signals.
- **A self-heal restart runs the stop hooks.** A lease-releasing before-stop hook fires on
  every crash, not only on an explicit stop.
- **A succeeded `runOnce` gate never demotes to `StatusStopped`.** Its `Stop()` still runs, so
  make it idempotent.
- **A false `WithStartCondition` marks the service `StatusStopped`**, not `StatusRegistered`.
- **A hard dependency must be registered before its dependent**, both statically and on a hot
  add.
- **The auto-name counter is monotonic.** `$N` is never reused, even after `Unregister`.
- **A released Messenger view cannot resurrect its subscriptions.**
- **`Orphans` leaves dependents `Running` but not ready.** `IsReady` reports false until the
  dependency is back.
- **`Orphans` and `WithCascadeStop` are mutually exclusive**; passing both is
  `ErrUnsupportedOption`.
- **`Metrics` counts lifecycle events, not per-tick work.** A cron tick is not a `Start`; a
  self-heal re-launch is a `Restart`, never a `Stop`.

## Performance

The benchmarks live in `bench_test.go` and `bench_schedule_test.go`. Run the lifecycle
scaling curve, the publish matrix, the churn benchmarks, and the schedule timings with:

```bash
go test . -run '^$' -bench . -benchmem
go test . -run '^$' -bench 'BenchmarkLifecycleScaling|BenchmarkMessengerPublish' -benchmem
```

The expected shapes:

- `BenchmarkLifecycleScaling` — start/stop cost grows roughly linearly with the number of
  services and their edges.
- `BenchmarkMessengerPublish` — publish scales with the number of subscribers, not with
  message size.
- `BenchmarkStatusesChurn`, `BenchmarkNamesChurn`, `BenchmarkRegister_ReloadChurn` — contended
  introspection and membership churn.
- `BenchmarkStatusesDuringSlowHealthProbe` — introspection stays responsive while a slow probe
  holds the probe path.

A checked-in baseline (`testdata/bench/baseline.txt`) backs the release-only regression gate in
[`.github/workflows/bench.yml`](.github/workflows/bench.yml): a lock-across-user-code regression
shows up as a lock-latency tripwire, and allocation regressions are compared against the same
baseline. See [`cmd/benchgate`](cmd/benchgate/) and the comments in `bench_test.go` for how to
refresh the baseline.

## Examples

Each directory is a runnable `main` package, built by CI.

- [`examples/basic/`](examples/basic/) — service lifecycle, cron scheduling, graceful shutdown.
- [`examples/pubsub/`](examples/pubsub/) — inter-service messaging with topics.
- [`examples/typedreq/`](examples/typedreq/) — typed request-reply via `TypedRequest`/`TypedRespond`.
- [`examples/advanced/`](examples/advanced/) — groups, labels, soft dependencies, `RegisterFunc`,
  `Validator`, `ReadinessChecker`, `HealthChecker`, state-change hooks, health-check hooks,
  `WithStartCondition`, `WaitFor`, `Metrics`, and `Done()`.
- [`examples/nested/`](examples/nested/) — reloading a subsystem at runtime with a
  sub-orchestrator (the `AppOrchestrator` pattern).

## Statistics

Statement coverage and the line breakdown (code / comment / blank) by category, refreshed from
the release tag by [`.github/workflows/stats.yml`](.github/workflows/stats.yml). The numbers
describe the release named in the block, never a moving `main`; the badges above are read from
the generated JSON, so neither needs a hand edit.

<!-- stats:begin --><!-- stats:end -->

## Development

```bash
go test . -race -count=1 -shuffle=on -coverprofile=coverage.out
go tool cover -func=coverage.out | grep total  # must be 100.0%
go test . -run '^$' -bench . -benchmem
go test -fuzz=FuzzTypedSubscribeDecode -fuzztime=30s .
go test -fuzz=FuzzGroupSurface -fuzztime=30s .
go vet ./...
gofmt -w .
```

Tests live only in the root package, so `go test .` (not `./...`) runs the suite; `./...` adds
the `examples/` and `cmd/` builds. CI runs the suite with `-race -count=1 -shuffle=on` and
fails unless coverage is exactly **100.0%** — every new branch needs a test. `golangci-lint`
(v2.13.2, with `gosec`) runs on the package; only `_test.go` is exempt from `errcheck` and
`gosec`. The two fuzz targets run on pull requests for a bounded window and nightly for longer.

## FAQ

**Why is the whole-orchestrator lifecycle single-shot?** The state machine is the
highest-defect-density part of the library, and an orchestrator that has fully stopped has no
meaningful state to resume from. Rather than reset the machine, own the reloadable part in a
sub-orchestrator wrapped by an adapter service and rebuild that sub-orchestrator through the
membership API (`StopService` + `StartService`, or `ReplaceService`) — the `AppOrchestrator`
pattern in [`examples/nested`](examples/nested/). Each replacement is constructed fresh, so the
single-shot lifecycle is never abused.

**How do I restart the orchestrator after `Stop`?** You do not; `Start` returns
`ErrAlreadyStarted` and `Register` returns `ErrOrchestratorStopped`. Use the sub-orchestrator
pattern above, or build a new `Orchestrator`.

**How do I replace a running service without bouncing its dependents?** `ReplaceService` keeps
the name in the graph and swaps only the implementation; dependents stay `Running`.

**Is there a group-level `Unregister`?** Yes: `UnregisterGroup` removes every member, refuses
to break outside hard dependents with `ErrHasDependents`, and accepts `WithCascadeStop` or
`Orphans`.

**Why is `Publish` lossy?** Blocking a publisher on a slow subscriber inverts backpressure and
can deadlock a service graph. Drop-only keeps `Publish` non-blocking; size the consumer with
`SubscribeWithBuffer`.

**How do I get notified of crashes?** Set `WithOnCrash` (or `WithOnStateChange`) and read
`Metrics().Crashes`. A self-heal crash is reported before the restart.

**What does `Done()` mean?** `Stop`/`Run` returned. Not "every goroutine exited". Watch
`Metrics().AbandonedGoroutines` for leaked user code.

## License

MIT
