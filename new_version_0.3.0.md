# gorch feature ideas

## 1. `RegisterFunc` — closure-based services

Most services are thin wrappers. A `RegisterFunc` avoids boilerplate structs for simple cases:

```go
orch.RegisterFunc("health-server", func(ctx gorch.ServiceContext) error {
    srv := &http.Server{Addr: ":8080"}
    go func() { <-ctx.Done(); srv.Shutdown(context.Background()) }()
    return srv.ListenAndServe()
}, nil) // optional Stop func
```

A `Stop` func can be nil if the service stops purely via context cancellation.

* * *

## 2. Service groups / namespaces

Being able to operate on subsets of services is powerful, especially in nestable orchestrators:

```go
orch.Register(svc, gorch.WithGroup("infra"))
orch.Register(svc, gorch.WithGroup("app"))

orch.StartGroup("infra")              // start only infra
orch.StopGroup("app", 5*time.Second)  // stop only app layer
orch.StatusesByGroup("infra")         // map filtered by group
```

Groups naturally compose with `DependsOn` (intra-group ordering unchanged; inter-group dependencies possible).

* * *

## 3. Readiness vs. Liveness distinction

Kubernetes got this right — a service can be _running_ but not _ready_. Currently `HealthChecker` conflates both:

```go
type ReadinessChecker interface {
    Ready(ctx context.Context) error
}
```

The orchestrator already probes health; adding a separate readiness probe lets you gate traffic routing (messenger, API gateways) without killing the service. A `IsReady(name string) bool` method on the orchestrator would expose this.

* * *

## 4. State-change hooks / callbacks

Right now hooks are only around start/stop. Adding state-transition hooks enables external monitoring without polling:

```go
type Config struct {
    // ...
    OnStateChange func(name string, from, to ServiceStatus)
    OnCrash       func(name string, err error) // sugar for StatusRunning→StatusCrashed
}
```

These are superior to the log channel for programmatic alerting — you can wire them directly to Prometheus counters, Slack webhooks, or a status page.

* * *

## 5. Optional / soft dependencies

Hard dependencies (`DependsOn`) block startup if a dependency isn't registered. A soft variant says _"start after this if it exists, but don't fail if it doesn't"_:

```go
orch.Register(apiSvc,
    gorch.DependsOn("db"),         // hard: must exist
    gorch.DependsOnSoft("cache"),  // soft: start after if present, ignore if missing
)
```

This is especially useful in nestable orchestrators where the parent may or may not provide certain services.

* * *

## 6. Metadata / labels

Arbitrary key-value tags on services for introspection and filtering:

```go
orch.Register(svc, gorch.WithLabel("tier", "critical"))
orch.Register(svc, gorch.WithLabel("team", "payments"))

// Filter statuses:
critical := orch.StatusesByLabel("tier", "critical")
```

This composes with groups and makes `Statuses()` much more useful in large deployments.

* * *

## 7. `WaitFor` / `AwaitStatus`

Block until a service reaches a target status (or times out):

```go
err := orch.WaitFor("db", gorch.StatusRunning, 10*time.Second)
```

This is useful for integration tests and for services that need to coordinate outside of `DependsOn` (e.g., a service that can start in degraded mode and then upgrade when a dependency becomes healthy).

* * *

## 8. Typed request-reply

`TypedPublish` and `TypedSubscribe` exist, but there's no `TypedRequest`:

```go
resp, err := gorch.TypedRequest[CreateOrderReq, CreateOrderResp](
    messenger, ctx, req, "orders.create",
)
```

Without it, request-reply forces users back to the untyped `Message` API, losing the type safety that `TypedSubscribe` provides.

* * *

## 9. Metrics (opt-in)

Not a full Prometheus dependency, but counters exposed as atomic int64s:

```go
type Metrics struct {
    Starts      atomic.Int64
    Stops       atomic.Int64
    Crashes     atomic.Int64
    Restarts    atomic.Int64
    HealthFails atomic.Int64
}
stats := orch.Metrics() // snapshot
```

This lets users wire up their own metrics without pulling in a metrics library. A `MetricsHandler() http.Handler` would be a nice bonus.

* * *

## 10. Smaller / nice-to-have

| Suggestion | Why |
| --- | --- |
| **Validator interface** — `Validate() error` called at `Register` time | Catch config errors before start (port conflicts, missing env vars) |
| `WithStartCondition` — `func() bool` | Skip a service at runtime without removing its registration |
| **Channel buffer size config** for messenger subscriptions | Prevent slow consumers from blocking publishers |
| `StopTimeout` **per-service** | Currently only global on `orch.Stop()`; a slow service can block the whole shutdown |
| `BeforeHealthCheck` **/** `AfterHealthCheck` **hooks** | Instrument health probing without wrapping every `HealthChecker` |
| `Drain()` **on the messenger** | Gracefully close subscriptions and flush pending messages before shutdown |

* * *

## Design things worth revisiting

*   `Run()` **blocking semantics**: `Run` blocks on OS signals, which is convenient for `main()` but makes the orchestrator hard to embed in tests or other run loops. Consider exposing a `Done() <-chan struct{}` that closes when all services stop, letting the caller decide how to wait.
    
*   **Messenger as standalone**: If `Messenger` is separable, it could be used before the orchestrator starts (e.g., seed a configuration topic). Right now it seems coupled to the orchestrator lifecycle.
    
*   **Go 1.25 requirement**: You're targeting an unreleased Go version. If this is a real constraint tied to new standard library features, call it out in the README. Otherwise, consider 1.22+ for broader adoption (1.22 brought loop variable semantics; 1.21 brought `slices`/`maps`).
    

* * *

## Priority shortlist

The highest-impact items to tackle first:

1.  **Groups & labels** — foundational for operating at scale
    
2.  **State-change hooks** — unlocks external observability
    
3.  **Soft dependencies** — makes nestable orchestrators practical
    
4.  **Readiness checks** — separates "alive" from "ready to serve"