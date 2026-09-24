# Migration guide

gorch follows [Semantic Versioning](https://semver.org/). Until 1.0, minor
releases may contain breaking changes. This guide lists every breaking or
behavioural change from **v0.5.0** onward, oldest first, with the mechanical
step to migrate. The authoritative per-release detail lives in the
[release notes](https://github.com/lorenzo-vecchio/gorch/releases).

## v0.5.0

- **`StatusSucceeded` for `runOnce` gates.** A one-shot `Start` that returns nil
  now ends `StatusSucceeded`, not `StatusStopped`. Update any exhaustive
  `switch` over `ServiceStatus` to handle the new value. A persistent service
  soft-depending on a successful gate now starts; a crashed gate still aborts
  its dependents.
- **`IsReady` takes a context.** `IsReady(name)` becomes
  `IsReady(ctx, name)`, so the `ReadinessChecker` probe honours a caller
  deadline. Pass `context.Background()` for the old behaviour.

## v0.6.0

- **Import path flattened.** Import `github.com/lorenzo-vecchio/gorch`, not
  `github.com/lorenzo-vecchio/gorch/gorch`.
- **`WithSelfHeal` conflicts are rejected.** Combining it with `WithCron` or
  `WithRunOnce` returns `ErrUnsupportedOption` (previously the factory was
  silently ignored). Drop the unused option.
- **`WithHealthChecks` signature changed.**
  `WithHealthChecks(interval, timeout, threshold)` becomes
  `WithHealthChecks(interval, opts...)`; set the timeout and threshold with
  `WithProbeTimeout` and `WithFailureThreshold`.

## v0.7.0

- **`Register` after `Start` is a hot add.** It no longer returns
  `ErrAlreadyStarted`; the service joins as `StatusRegistered` and is started
  with `StartService`. The whole-orchestrator lifecycle stays single-shot:
  after a successful `Stop`, `Start` returns `ErrAlreadyStarted` and `Register`
  returns `ErrOrchestratorStopped`. Replace any "register only before Start"
  assumption with an explicit `StartService` call.
- **Messenger subscriptions are scoped to the instance/tick that created them.**
  They are released when that instance stops or is removed, instead of living
  for the whole orchestrator. Long-lived subscribers that must outlive an
  instance should re-subscribe on each start.
- **New exported API:** `StartService`, `StopService`, `Unregister`,
  `StopOption`, `WithCascadeStop`, and the sentinels `ErrServiceNotFound`,
  `ErrHasDependents`, `ErrOrchestratorStopping`, `ErrOrchestratorStopped`,
  `ErrDependencyNotRunning`, `ErrDependencyNotFound`.

## v0.8.0

- **The membership `timeout` now bounds the whole stop**, including the
  service's `Stop()` and its before/after-stop hooks, not just the wait for an
  instance to exit. `WithStopTimeout` still caps `Stop()` when smaller.
- **A stop now waits for in-flight cron ticks to return** instead of only
  cancelling them.
- **A `ServiceContext.Messenger` view cannot subscribe after its instance or
  tick ended** (or after `Drain`): `Subscribe` returns an already-closed channel
  and `Request`/`RequestAsync`/`TypedRequest` return an error instead of a nil
  reply.

## v0.9.0

- **`StartService` before `Start` returns `ErrOrchestratorNotStarted`** instead
  of panicking on a nil context/scheduler.
- **Reentrancy is an exported sentinel.** `StopService`/`Unregister` (and
  `StartService`/group ops) can now be classified with
  `errors.Is(err, ErrReentrantMembership)`.
- **A hot-added cron entry is staged.** `Register` validates but does not
  schedule it; it only ticks after `StartService`, so it never ticks while
  reporting `StatusRegistered`. A malformed hot-added spec is rejected by
  `Register` with `ErrInvalidCron`.
- **The stop deadline covers before/after-stop hooks.** A hook that outlasts the
  deadline is now reported as `ErrStopTimeout` (see the unreleased change below
  for the finer-grained `ErrHookTimeout`).

## Unreleased (hardening after v0.9.0)

Additive and non-breaking for callers who already handle errors with
`errors.Is`:

- **New sentinel `ErrHookTimeout`**, joined with `ErrStopTimeout` when a
  before-stop hook overran the share of the stop budget reserved for it. The
  service's own `Stop()` is still invoked, so resources are released.
- **New sentinel `ErrNilService`.** `Register(nil, …)` and `RegisterFunc` with a
  nil `startFn` are rejected at registration instead of panicking at `Start`.
- **User callbacks are panic-contained.** A panic in a lifecycle hook,
  `Validator`, start condition, or health/readiness probe is recovered and
  reported as an error (or as unhealthy/unready); it no longer unwinds through
  a public entry point.
