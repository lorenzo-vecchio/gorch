# gorch — Go Orchestrator Library

Manage goroutine lifecycles — start, stop, cron scheduling, and pub-sub messaging — with a small, composable API.

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
- **Cron scheduling** — 6-field cron (seconds included) with three concurrency modes: Parallel, Queue, Skip.
- **Pub-sub Messenger** — topic-based messaging between services (Socket.IO rooms style), non-blocking sends.
- **Self-healing** — auto-restart crashed services with a factory-provided fresh instance.
- **Nestable orchestrators** — a service can create its own gorch for sub-services.
- **Structured logging** — channel-based log-pump; services call `Info/Error/Debug/Warn` on a `ServiceLogger`, no slog dependency.

## Quick start

```go
package main

import (
    "context"
    "os"
    "os/signal"
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

    if err := orch.Start(); err != nil {
        panic(err)
    }

    sig := make(chan os.Signal, 1)
    signal.Notify(sig, os.Interrupt)
    <-sig

    if err := orch.Stop(10 * time.Second); err != nil {
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
orch := gorch.New(gorch.Config{LogLevel: gorch.LogLevelInfo})
orch.Register(svc, gorch.WithCron("@every 5s", gorch.CronSkip))
orch.Register(svc, gorch.WithSelfHeal(func() gorch.Service { return &MyService{} }))
orch.Start()
orch.Stop(10 * time.Second)
```

### Cron modes

| Mode | Behavior |
|------|----------|
| `CronParallel` | Fire every tick, overlapping runs allowed. |
| `CronQueue` | Serialize — wait for the previous run to finish. |
| `CronSkip` | Drop ticks that would overlap. |

### Messenger

```go
ch, unsub := messenger.Subscribe("topic")
messenger.Publish(msg, "topic")  // send to topic subscribers
messenger.Publish(msg)           // broadcast to ALL subscribers
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
