# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] — 2026-09-09

### Changed

- **Breaking:** new `StatusSucceeded` service status. A runOnce gate whose `Start`
  completes without error now transitions to `StatusSucceeded` (previously
  `StatusStopped`), so a persistent service that soft-depends on a successful gate
  starts instead of being aborted with `ErrStartAborted`. A crashed or skipped gate
  still aborts its dependents.
- **Breaking:** `IsReady(ctx, name)` now takes a `context.Context`; the
  `ReadinessChecker` probe runs with a caller-supplied deadline instead of blocking
  forever on `context.Background()`.
- Unnamed services log under their auto-assigned `$N` name — matching
  `Status()`/`Names()` — instead of their reflect type.
- `go.mod` now declares `go 1.25` (no toolchain patch pin).

### Fixed

- A no-op `Stop()` before `Start()` no longer poisons the later `Stop`: the real
  shutdown was silently skipped and services were left running.
- A persistent service that fails synchronously within its start-timeout window now
  makes `Start()` return the real error deterministically (and its dependents are
  skipped) instead of silently succeeding. Self-heal services keep their restart
  semantics.
- `CronQueue` serializes ticks with a per-entry mutex instead of a spin-wait loop.

### Docs

- RunOnce `Stop()` contract, single-shot orchestrator lifecycle, typed request-reply
  responder example, and reattached orphaned doc comments.

## [0.4.0] — 2026-09-09

### Changed

- **Breaking:** `New(cfg Config)` replaced with `New(opts ...Option)`. Configure via
  functional options (`WithLogger`, `WithLogLevel`, `WithHealthChecks`, …).
  This fixes two unfixable zero-value bugs: `LogLevelDebug` is now selectable, and
  health checks can now be disabled via `WithHealthChecksDisabled()`.
- **Breaking:** `Service.Start(ctx ServiceContext)` — the interface now takes a
  `ServiceContext` directly instead of `context.Context`, removing the need for the
  unsafe `ctx.(gorch.ServiceContext)` assertion. `ServiceContext` embeds
  `context.Context`, so existing `<-ctx.Done()` bodies keep compiling unchanged.

### Fixed

- Shutdown panic ("send on closed channel") by signaling the log-pump via a dedicated
  `logQuit` channel instead of closing `logCh`.
- Crash semantics: persistent services that return an error (or panic) now transition
  to `StatusCrashed`, and `OnCrash` receives the real error; `Metrics().Crashes`
  increments for real crashes.
- `Start()` idempotence: a second `Start()` returns `ErrAlreadyStarted`, and a failed
  `Start()` no longer bricks the orchestrator (retry is allowed).
- `TypedRequest` double-encoding; added `TypedSubscribeRequest` and `TypedRespond` for
  a complete typed request-reply story with no manual gob.
- `DependsOnSoft` now provides real start ordering (and soft-dep cycles are detected).
- Health checks: each probe gets its own deadline (a slow checker no longer fails later
  probes).
- Self-heal restart data races (per-entry state mutex).
- `RequestAsync` goroutine leak (forwarding goroutine exits on reply or cancel).
- Messenger teardown: `Stop()` drains the messenger and closes subscriber channels;
  subscriptions are safe after `Drain`.
- Cron services now transition to `running`/`stopped` instead of staying `registered`.
- Log-level filtering happens at emit, so a Debug flood cannot starve INFO/ERROR entries.
- `Run()` defaults to SIGINT **and** SIGTERM.

## [0.3.1] — 2026-09-07

### Fixed

- CI coverage gate targets `gorch/` only; gofmt applied to `examples/advanced`.

## [0.3.0] — 2026-09-07

### Added

- `RegisterFunc`, service groups, labels, soft dependencies, readiness checks,
  state-change hooks, `WaitFor`, `TypedRequest`, metrics counters, custom logger
  (`Config.Logger`), and more.

## [0.2.0] — 2026-09-06

### Added

- Dependency ordering, start timeout, `Run()`, status introspection, backoff,
  health checks, error aggregation, one-shot services, lifecycle hooks,
  request-reply, and typed messages.

## [0.1.0] — 2026-09-05

### Added

- Initial release: service lifecycle, cron scheduling, and pub-sub messaging.
