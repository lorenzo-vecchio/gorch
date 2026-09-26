// Package gorch orchestrates the lifecycle of long-running goroutines.
//
// It is a small, dependency-light runtime supervisor for a Go program that
// starts several cooperating services and must start, stop, and supervise them
// in a defined order.
//
// # What it does
//
//   - Starts services in dependency order and stops them in reverse order.
//   - Runs cron-scheduled ticks and one-shot init (runOnce) gates.
//   - Self-heals services that crash, using a factory plus backoff and retry.
//   - Probes health and readiness and can restart unhealthy services.
//   - Carries a topic pub-sub Messenger between services.
//
// # Use case
//
// gorch is for the composition root of a single process: the place where you
// wire a handful of long-lived components (an HTTP server, a worker pool, a
// cache refresher, migrations) and want deterministic startup, supervision, and
// graceful shutdown without adopting a framework.
//
// # What it is not
//
// gorch is not a scheduler for distributed work, a durable queue, a service
// mesh, or a replacement for context. It supervises goroutines inside one
// process and nothing more. It does not persist state across restarts, retry
// with deduplication, or guarantee delivery of messages.
//
// # Contract
//
//   - Membership is dynamic: Register may add a service to a running
//     orchestrator, and StartService, StopService, and Unregister start, stop,
//     and remove services while it runs. Only the whole-orchestrator lifecycle
//     is single-shot: after a successful Stop the orchestrator cannot be
//     restarted.
//   - The wire format is encoding/gob and is part of the public contract; types
//     passed through the typed Messenger helpers must be gob-compatible.
//   - Publish is drop-only: when a subscriber's buffer is full the message is
//     dropped for that subscriber.
//   - No user code (Start, Stop, Validate, probes, or lifecycle hooks) runs
//     while an orchestrator lock is held, so user code may block or call back
//     into membership operations without deadlocking.
//   - A hot-added cron service is staged by Register and only scheduled by
//     StartService, so it never ticks while reporting StatusRegistered.
//   - A panic from a lifecycle hook, Validator, start condition, or probe is
//     recovered and reported as an error (or as unhealthy/unready), so a
//     misbehaving callback never unwinds through a public entry point.
//   - A stop that times out is reported honestly: the entry stays
//     StatusStopping rather than StatusStopped, and the incomplete stop is not
//     counted in Metrics().Stops.
//   - A teardown goroutine abandoned because a deadline won — a before-stop
//     hook or Stop() that did not return in time, or a failed-Start wait that
//     outlived its rollback budget — is logged at Error level naming the
//     service and counted in the monotonic Metrics().AbandonedGoroutines. The
//     goroutine is not registered with Done(), so the counter is the only
//     visibility into a leak user code can cause by ignoring the contract.
//   - Messenger subscriptions are scoped to the instance or cron tick that
//     created them, and their owner is released on every exit path. Teardown
//     drains every live owner id of an entry, so an owner whose goroutine is
//     abandoned — its deferred release skipped — is still released. Owner ids
//     are monotonic and never reused, so a drained id cannot be reminted into a
//     later view.
//   - A self-heal crash is observable before the restart: the entry transitions
//     StatusRunning to StatusCrashed (firing OnCrash and incrementing
//     Metrics().Crashes) and is returned to StatusRunning once the new instance
//     is live.
//   - Health is a live signal: both the periodic loop and Health() probe only
//     services that are StatusRunning. A non-running entry is omitted from the
//     Health() result rather than reported healthy, while a running service that
//     does not implement HealthChecker is reported with a nil error.
//   - A hot add that names a hard dependency which is being removed fails with
//     ErrDependencyRemoving: a retryable "not now" condition distinct from
//     ErrHasDependents, which is only about the target's own hard dependents.
//   - A plain StopService/Unregister is refused with ErrHasDependents when a
//     hard dependent is Running or Starting, and the error names each blocker
//     with its status. Dependents in any other status (Stopping, Registered,
//     Crashed, Stopped, Succeeded) do not block. WithCascadeStop tears the
//     dependents down in reverse topological order; a dependent already Stopping
//     is left to its own in-flight teardown rather than stopped a second time.
//   - The cycle check walks each node once, so a diamond costs O(V+E) rather
//     than one walk per path. The walk is depth-capped: a chain deeper than
//     10000 edges fails registration with
//     ErrDependencyDepthExceeded instead of overflowing the goroutine stack.
//     Registration is expected to build a bounded, startup-sized acyclic graph;
//     the cap guards the unbounded reload loop, not normal use.
//   - A membership op blocked by an in-flight reservation is classified by
//     cause: re-entry from the target's own Start/Stop on the same goroutine is
//     a programming error and returns ErrReentrantMembership, while a collision
//     with another goroutine's reservation is transient and returns the
//     retryable ErrMembershipBusy. Busy(name) is the predicate a caller can poll
//     to observe the reservation instead of racing.
//   - A failed Start rolls back within a bounded budget: services that had
//     started are stopped in reverse topological order (the same order as Stop),
//     so a dependency is never torn down before its dependent regardless of
//     registration order. The per-service stop sequences and the final wait for
//     instance and log-pump goroutines share one failed-start deadline
//     (WithFailedStartTimeout, default 30s), so a service that blocks in Stop()
//     does not hang Start unless the bound is removed with a negative
//     WithFailedStartTimeout. Overrunning the budget reports ErrStopTimeout. The
//     reset is best-effort — a goroutine that
//     ignores cancellation may outlive the failed Start — so the orchestrator is
//     left restartable and Start can be retried.
//   - A ServiceLogger never blocks: it drops an entry when the log channel is
//     full or its pump has stopped, so a torn-down log channel cannot stall a
//     service or a Stop. A service hot-added during a failed Start keeps a
//     logger bound to that Start's torn-down channel, and its entries are
//     dropped until the next Start rebinds every snapshotted entry's logger.
//
// See the README for the concurrency table and the per-method guarantees.
package gorch
