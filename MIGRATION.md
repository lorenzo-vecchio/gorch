# Migration guide

Upgrade steps for breaking changes. Each section is a single mechanical edit.

## v0.5 → v1.0 (unreleased)

### 1. Import path flattened to the module root

The package moved from the `gorch/` subdirectory to the module root, removing
the doubled `gorch/gorch` path.

```go
// before
import "github.com/lorenzo-vecchio/gorch/gorch"

// after
import "github.com/lorenzo-vecchio/gorch"
```

`go get github.com/lorenzo-vecchio/gorch@v1.0.0` is unchanged; only the import
line changes. The package name stays `gorch`.

### 2. `WithHealthChecks` takes options

The probe timeout and failure threshold are no longer two adjacent positional
arguments that could be swapped silently.

```go
// before
orch := gorch.New(
    gorch.WithHealthChecks(30*time.Second, 5*time.Second, 3),
)

// after
orch := gorch.New(
    gorch.WithHealthChecks(30*time.Second,
        gorch.WithProbeTimeout(5*time.Second),
        gorch.WithFailureThreshold(3),
    ),
)
```

Omitting an option keeps its default (5s timeout, threshold 3); omitting the
interval keeps the default 30s. `WithHealthChecksDisabled` is unchanged.

### 3. `WithSelfHeal` combined with `WithCron`/`WithRunOnce` now errors

Neither a cron tick nor a runOnce gate ever consumed the self-heal factory, so
the option was silently ignored. `Register` now rejects the combination:

```go
err := orch.Register(svc,
    gorch.WithCron("@every 5s", gorch.CronSkip),
    gorch.WithSelfHeal(factory), // before: silently ignored
)
// after: err is ErrUnsupportedOption
```

If you relied on the combination, drop `WithSelfHeal`. Cron services keep
running on schedule; wrap the tick body in your own retry if you need it.

### 4. Reply-topic generation handles `crypto/rand` failure

`newUUID` no longer ignores a read error; it falls back to a process-unique
counter. No API change, only safer behavior on the (unlikely) error path.
