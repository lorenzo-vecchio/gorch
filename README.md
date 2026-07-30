# gorch — Go Orchestrator Library

Manage goroutine lifecycles — start, stop, cron scheduling, pub-sub messaging, dependency ordering, health checks, and self-healing — with a small, composable API.

```go
import "github.com/lorenzo-vecchio/gorch/gorch"
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
- **One-shot services** — init/gate tasks that run once before persistent services and never receive `Stop()`.
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

## Quick start

```go
package main

import (
    "context"
    "time"

    "github.com/lorenzo-vecchio/gorch/gorch"
)

type MyService struct{}

func (s *MyService) Start(ctx context.Context) error {
    <-ctx.Done()
    return nil
}

func (s *MyService) Stop() error { return nil }

func main() {
    orch := gorch.New(gorch.Config{LogLevel: gorch.LogLevelInfo})
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
    Start(ctx context.Context) error
    Stop() error
}
```

`ServiceContext` (the `ctx` passed to `Start`) embeds `context.Context` and carries a `*ServiceLogger` and `*Messenger`.

### Orchestrator

```go
orch := gorch.New(gorch.Config{
    LogLevel:            gorch.LogLevelInfo,
    DefaultStartTimeout: 5 * time.Second,
})
orch.Register(svc, gorch.WithCron("@every 5s", gorch.CronSkip))
orch.Register(svc, gorch.WithSelfHeal(func() gorch.Service { return &MyService{} }))
orch.Start()
orch.Stop(10 * time.Second)
```

### Run() convenience

`Run` starts the orchestrator, blocks until a signal is received (SIGINT by default, configurable via variadic signals), then stops.

```go
// Default: waits for SIGINT.
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
orch := gorch.New(gorch.Config{DefaultStartTimeout: 5 * time.Second})
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

`ServiceStatus` values: `StatusRegistered`, `StatusStarting`, `StatusRunning`, `StatusStopping`, `StatusStopped`, `StatusCrashed`. Each has a `String()` method.

### One-shot / init services

`WithRunOnce` marks a service as a one-shot init task. It runs before persistent services, never receives `Stop()`, and transitions to `StatusStopped` when `Start` returns. If `Start` returns an error, startup aborts.

```go
orch.Register(migrator, gorch.WithRunOnce())
```

### Lifecycle hooks

Global hooks on `Config`, or per-service overrides via `RegisterOption`.

```go
orch := gorch.New(gorch.Config{
    OnBeforeStart: func(name string) error {
        log.Printf("starting %s", name)
        return nil
    },
    OnAfterStop: func(name string, err error) {
        log.Printf("stopped %s, err=%v", name, err)
    },
})

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

orch := gorch.New(gorch.Config{
    HealthInterval:  30 * time.Second,  // how often to probe (0 disables)
    HealthTimeout:   5 * time.Second,   // per-probe deadline
    HealthThreshold: 3,                // consecutive failures before restart
})
```

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

`Request` publishes a message and blocks until a response arrives (or ctx expires). The responding service receives a `Message` with a `ReplyTopic` field and publishes its reply there.

```go
// Requestor:
resp, err := messenger.Request(ctx, payload, "orders.create")

// Responder (inside a service goroutine):
rawCh, _ := messenger.Subscribe("orders.create")
for val := range rawCh {
    msg := val.(gorch.Message)
    // ... process msg.Payload ...
    messenger.Publish(response, msg.ReplyTopic)
}
```

`RequestAsync` returns a response channel immediately without blocking.

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
// 2026-07-27 14:30:05.123 INFO  *main.MyService --- request completed status=200 latency=12ms
```

The built-in log-pump writes to `os.Stderr`. Log level filters entries: `Debug < Info < Warn < Error`.

#### Custom logger

Inject any logger that satisfies the `Logger` interface via `Config.Logger`. `*slog.Logger` from the standard library satisfies this interface directly.

```go
import "log/slog"

orch := gorch.New(gorch.Config{
    Logger: slog.Default(),
})
```

When a custom logger is set, the built-in log-pump is disabled entirely. The service name is prepended as `"service"=<name>` to every log call so the custom logger can include or exclude it as needed. `Config.LogLevel` is ignored — the custom logger manages its own level filtering.

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
if orch.IsReady("api") {
    // route traffic
}
```

### State-change hooks

`OnStateChange` fires on every status transition. `OnCrash` fires specifically on `Running -> Crashed`. Wire these to Prometheus counters, Slack webhooks, or a status page instead of polling.

```go
orch := gorch.New(gorch.Config{
    OnStateChange: func(name string, from, to gorch.ServiceStatus) {
        log.Printf("%s: %s -> %s", name, from, to)
    },
    OnCrash: func(name string, err error) {
        notifications.Send(name + " crashed")
    },
})
```

### WaitFor

Block until a service reaches a target status (or times out). Useful for tests and services that need external coordination.

```go
err := orch.WaitFor("db", gorch.StatusRunning, 10*time.Second)
```

### Typed Request-Reply

`TypedRequest` provides type-safe request-reply without falling back to the untyped `Message` API.

```go
type CreateOrderReq struct {
    ItemID string
    Qty    int
}
type CreateOrderResp struct {
    OrderID string
    Status  string
}

// Requestor:
resp, err := gorch.TypedRequest[CreateOrderReq, CreateOrderResp](
    messenger, ctx, req, "orders.create",
)

// Responder (inside a service goroutine via TypedSubscribe):
ch, _ := gorch.TypedSubscribe[CreateOrderReq](messenger, "orders.create")
for msg := range ch {
    result := processOrder(msg)
    gorch.TypedPublish(messenger, result, "orders.results")
}
```

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
orch := gorch.New(gorch.Config{
    HealthInterval:  30 * time.Second,
    BeforeHealthCheck: func(name string) error {
        metrics.Inc("health_checks_total")
        return nil
    },
    AfterHealthCheck: func(name string, err error) {
        if err != nil {
            metrics.Inc("health_checks_failed")
        }
    },
})
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

## Examples

- [`examples/basic/`](examples/basic/) — Service lifecycle, cron scheduling, graceful shutdown.
- [`examples/pubsub/`](examples/pubsub/) — Inter-service messaging with topics.

## Development

```bash
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out | grep total  # must be 100.0%
go vet ./...
gofmt -w .
```

## License

MIT
