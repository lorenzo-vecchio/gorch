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
- **Run() convenience** — single call starts, blocks on OS signals, then stops.
- **Dependency ordering** — declare dependencies with `DependsOn`, cycle detection at registration, topological start and reverse-topological stop.
- **Start timeout** — per-service start deadline via `WithStartTimeout`, with a `DefaultStartTimeout` config default.
- **Cron scheduling** — 6-field cron (seconds included) with three concurrency modes: Parallel, Queue, Skip.
- **Pub-sub Messenger** — topic-based messaging between services (Socket.IO rooms style), non-blocking sends, request-reply, and typed messages.
- **Self-healing** — auto-restart crashed services with a factory-provided fresh instance and configurable backoff/retry.
- **Health checks** — `HealthChecker` interface; orchestrator probes services on configurable intervals, auto-restarts unhealthy services.
- **Backoff & retry** — `ExponentialBackoff` and `ConstantBackoff` strategies, max retries, stability-window retry reset.
- **One-shot services** — init/gate tasks that run once before persistent services; `Stop()` is called at shutdown.
- **Lifecycle hooks** — `OnBeforeStart`, `OnAfterStart`, `OnBeforeStop`, `OnAfterStop` (global or per-service overrides).
- **Status introspection** — `Status`, `Statuses`, `Names`, `Count` for runtime observability.
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
- **Metrics** — atomic int64 counters (`Starts`, `Stops`, `Crashes`, `Restarts`, `HealthFails`), exposed via `Metrics()` snapshot.
- **Validator interface** — `Validate() error` called at `Register` for early config checks.
- **WithStartCondition** — Skip a service at runtime via a `func() bool`.
- **Per-service stop timeout** — `WithStopTimeout` controls how long to wait for `Stop()`.
- **Configurable channel buffer** — `SubscribeWithBuffer` for the Messenger.
- **Health check hooks** — `BeforeHealthCheck` / `AfterHealthCheck` for instrumenting probes.
- **Messenger.Drain** — Gracefully close all subscriber channels and clear subscriptions.
- **Done() channel** — Non-blocking shutdown notification; closes when all goroutines finish.

## Concurrency

All methods are safe to call from multiple goroutines unless noted otherwise. The
table below summarizes what may run concurrently with a live `Start`/`Stop`.

| Method group | Concurrent with `Start`/`Stop` |
|--------------|-------------------------------|
| `Register`, `RegisterFunc` | No — call before `Start`. During or after the lifecycle they return `ErrAlreadyStarted`. |
| `Start`, `Stop` | Yes — against each other. Guarded by `sync.Once`; the orchestrator is single-shot, so after a successful `Stop` neither can run again. |
| `Status`, `Statuses`, `Names`, `Count` | Yes — safe to read while services run and during shutdown. |
| `Health`, `IsReady`, `WaitFor` | Yes — each probe/tick takes its own read lock; `IsReady` honors the caller's `ctx`. |
| `Metrics`, `Done` | Yes — atomic counters and a lazily cached channel. |
| `StartGroup`, `StopGroup` | Not synchronized with `Start`/`Stop`; drive one lifecycle per orchestrator. |
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
- **Lifecycle is single-shot.** After a successful `Stop`, neither `Start` nor
  `Register` can be used again; both return `ErrAlreadyStarted`. A failed
  `Start` does not consume the lifecycle and may be retried.
- **Errors are aggregated.** `Start` and `Stop` join every failure with
  `errors.Join`, so a single call reports all causes, not just the first. Use
  `errors.Is`/`errors.As` to inspect them.

Sentinel errors returned by the orchestrator:

| Error | Returned by | Meaning |
|-------|-------------|---------|
| `ErrAlreadyStarted` | `Register`, `Start` | Called after the orchestrator already started (or was stopped). |
| `ErrDuplicateName` | `Register` | Two services share a `WithName`. |
| `ErrDependencyCycle` | `Register`, `Start` | A hard or soft dependency chain loops. |
| `ErrStartAborted` | `Start` | A hard/soft dependency failed or was skipped. |
| `ErrInvalidCron` | `Start` | A `WithCron` spec is invalid. |
| `ErrUnsupportedOption` | `Register` | `WithSelfHeal` combined with `WithCron`/`WithRunOnce`. |
| `ErrStopTimeout` | `Stop` | Services did not stop within the caller's timeout. |

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

Configuration uses functional options. `New()` with no options uses the defaults (Info log level, health checks every 30s with a 5s probe timeout).

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

### Start timeout

Per-service start deadline, with a config-level default.

```go
orch := gorch.New(gorch.WithDefaultStartTimeout(5 * time.Second))
orch.Register(svc, gorch.WithStartTimeout(30 * time.Second)) // per-service override
```

### Cron modes

| Mode | Behavior |
|------|----------|
| `CronParallel` | Fire every tick, overlapping runs allowed. |
| `CronQueue` | Serialize — wait for the previous run to finish. |
| `CronSkip` | Drop ticks that would overlap. |

### Status introspection

```go
status, ok := orch.Status("db")            // ServiceStatus, bool
all := orch.Statuses()                     // map[string]ServiceStatus
names := orch.Names()                      // []string in registration order
count := orch.Count()                      // total registered services
```

`ServiceStatus` values: `StatusRegistered`, `StatusStarting`, `StatusRunning`, `StatusStopping`, `StatusStopped`, `StatusCrashed`, `StatusSucceeded`. Each has a `String()` method. `StatusSucceeded` marks a one-shot service whose `Start` completed without error (a successful gate); dependents are not aborted by it.

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

`Metrics()` returns a snapshot of atomic counters for orchestrator-level events. The user wires these into their own monitoring system — no metrics library dependency.

```go
stats := orch.Metrics()
fmt.Printf("starts=%d stops=%d crashes=%d restarts=%d healthFails=%d\n",
    stats.Starts, stats.Stops, stats.Crashes, stats.Restarts, stats.HealthFails)
```

### Validator

Implement the `Validator` interface to catch config errors at `Register` time (before `Start`).

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

`Drain()` closes all subscriber channels and clears subscriptions. `Done()` returns a channel that closes when all goroutines (services, log-pump, health-check loop) have exited — useful for non-blocking shutdown.

```go
// Gracefully flush pending messages before shutdown.
messenger.Drain()

// Non-blocking wait for full shutdown.
select {
case <-orch.Done():
case <-time.After(10 * time.Second):
}
```

## Compatibility & versioning

gorch follows [Semantic Versioning](https://semver.org/). Until 1.0, minor
releases may contain breaking changes; every one is marked **Breaking** in the
[release notes](https://github.com/lorenzo-vecchio/gorch/releases) and covered by a migration note.

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
go vet ./...
gofmt -w .
```

## License

MIT
