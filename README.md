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
- **Structured logging** — channel-based log-pump; services call `Info/Error/Debug/Warn` on a `ServiceLogger`, no slog dependency.

## Quick start

```go
package main

import (
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

The log-pump writes to `os.Stderr`. Log level filters entries: `Debug < Info < Warn < Error`.

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
