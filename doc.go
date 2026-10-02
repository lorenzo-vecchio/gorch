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
// # Requirements
//
// gorch requires Go 1.25+.
//
// # Restarting
//
// The whole-orchestrator lifecycle is single-shot by design, not by omission:
// after a successful Stop neither Start nor Register can be used again. The
// supported way to reload configuration or rebuild a subsystem in place is to
// own the reloadable part in a sub-orchestrator wrapped by an adapter service —
// the AppOrchestrator pattern shown in examples/nested — and rebuild that
// sub-orchestrator through the membership API (StopService + StartService, or
// ReplaceService) while the outer orchestrator keeps running. Each replacement
// sub-orchestrator is constructed fresh, so its single-shot lifecycle is never
// abused and the state machine is never asked to reset itself. This covers the
// common "reload without a process restart" need without touching the
// highest-defect-density part of the code.
//
// # Contract
//
//   - Membership is dynamic: Register may add a service to a running
//     orchestrator, and StartService, StopService, and Unregister start, stop,
//     and remove services while it runs. Only the whole-orchestrator lifecycle
//     is single-shot: after a successful Stop the orchestrator cannot be
//     restarted. See the Restarting section above for the supported
//     in-process reload pattern.
//   - Done is a shutdown-completed signal: its channel closes once Stop (or
//     Run, which calls Stop) has returned, and not before. It is not a "every
//     goroutine has exited" guarantee — a timed-out Stop still closes it while
//     an abandoned teardown goroutine may run — so it stays open before Stop,
//     including after a failed Start and across a successful retry. A no-op
//     Stop on a never-started orchestrator closes it.
//   - StartService is idempotent on a Running persistent/cron service (a no-op)
//     and never restarts a live instance: replacing one is the explicit
//     StopService + StartService. Its start decision and reservation are claimed
//     atomically, so concurrent calls start exactly one instance and a collision
//     with a reservation held by another goroutine — an in-flight start
//     transaction or teardown — is the transient ErrMembershipBusy (a runOnce
//     entry is the deliberate re-run exception). A membership op re-entered from
//     the target's own Start/Stop on the same goroutine is instead the permanent
//     ErrReentrantMembership; a persistent service owns its Start goroutine for
//     the instance's lifetime, so a self-start from that callback stays a
//     re-entry even after the reservation cleared.
//   - ReplaceService swaps a registered service's implementation without
//     removing its name from the graph, so its hard dependents are neither torn
//     down nor blocked: a concurrent Register/Status/Dependents/IsReady always
//     sees the entry — never a gap — while a dependent stays StatusRunning and
//     reads IsReady false only while the target is not Running. It tears down
//     only the target's instance (running the hooks and Stop() within the
//     caller's timeout), installs the new implementation, resets the
//     per-instance counters, and starts a fresh instance through the same
//     per-kind path as StartService. The entry's registration-time config
//     (dependencies, cron spec/mode, runOnce, group, labels, hooks, timeouts,
//     and self-heal factory) is frozen; to change the factory, Unregister and
//     Register again. Unlike a plain stop, replace never cascades and never
//     refuses on a running dependent: WithCascadeStop is rejected with
//     ErrUnsupportedOption, while Orphans is accepted as a no-op because it is
//     already replace's fixed behaviour. A teardown that does not complete
//     cleanly (a deadline or a Stop() error) aborts the swap, leaving the old
//     implementation in place. The new service runs through Validator.Validate()
//     before the old instance is touched, and the same hard-dependency gate as
//     StartService applies.
//   - Concurrent Start calls are claimed atomically: the caller that wins the
//     claim runs the lifecycle's start and every other caller returns
//     ErrAlreadyStarted immediately without waiting for it, so a nil return
//     always identifies the call that actually ran the start. A failed Start
//     releases the claim, leaving the lifecycle retryable.
//   - The wire format is encoding/gob and is part of the public contract; types
//     passed through the typed Messenger helpers must be gob-compatible.
//   - Publish is drop-only: when a subscriber's buffer is full the message is
//     dropped for that subscriber.
//   - No user code (Start, Stop, Validate, probes, or lifecycle hooks) runs
//     while an orchestrator lock is held, so user code may block or call back
//     into membership operations without deadlocking.
//   - A hot-added cron service is staged by Register and only scheduled by
//     StartService, so it never ticks while reporting StatusRegistered.
//   - A WithCron spec is validated by Register on both the static and the
//     hot-add path, so the same spec is accepted or rejected identically. An
//     empty spec returns ErrInvalidCron instead of silently registering a
//     non-cron service, and a sub-second @every interval is clamped to one
//     second by the underlying parser.
//   - Public entry points return sentinels rather than panicking on hostile but
//     plausible input: StartGroup before Start returns ErrOrchestratorNotStarted
//     exactly like StartService, SubscribeWithBuffer rejects a negative capacity
//     with ErrInvalidBufferSize, and Request, RequestAsync, and TypedRequest
//     reject a nil context with ErrNilContext. An unknown or empty group selects
//     nothing and returns nil (a group is a filter tag, not an entity), and
//     StopGroup before Start is a no-op, consistent with StopService/Unregister.
//   - UnregisterGroup removes every member of a group — cron and runOnce members
//     included — as the group-level analogue of Unregister. A plain call refuses
//     to break a Running or Starting hard dependent outside the group with
//     ErrHasDependents (the typed *HasDependentsError names the member depended
//     on and the outside blockers); WithCascadeStop removes the transitive hard
//     dependents outside the group too, and Orphans instead leaves them Running
//     but degraded. The two options are mutually exclusive and rejected with
//     ErrUnsupportedOption. Removal aggregates stop failures with errors.Join and
//     proceeds even when a member's Stop() fails — every member is removed — and
//     an unknown or empty group is a nil no-op, agreeing with StartGroup/StopGroup.
//   - An Orchestrator zero value is usable: its first public call lazily
//     initialises it with the same defaults as New(), so `var o Orchestrator`
//     behaves like New() and no public entry point panics on an uninitialised
//     registry, Messenger, or shutdown channel.
//   - Introspection reports what is registered, not what is live: Count, Names,
//     and Statuses include an entry from the moment it is registered, so a
//     hot-added, not-yet-started service and a staged cron entry are both
//     present and both read StatusRegistered. CountRunning and RunningNames are
//     the StatusRunning subset ("N of M running"). Dependents and DependenciesOf
//     read the hard-dependency edges in the reverse and forward directions
//     respectively: Dependents is the blocking set a plain stop is refused on,
//     so it excludes soft and non-blocking dependents, while DependenciesOf is
//     the direct hard dependencies. A reservation is not
//     encoded in status: an entry actively starting still reports its prior
//     status until startOneService commits StatusStarting, so poll Busy(name)
//     for an in-flight reservation. Busy is the membership transaction, not a
//     live instance: a running persistent service and a cron entry with a tick
//     in flight are not reserved, so neither makes a stop collide. For a cron
//     entry StatusRunning means the
//     schedule is installed, not that a tick is working: a tick that returns an
//     error or panics only logs and increments Metrics().CronFailures, and
//     IsReady and Health inherit that scheduling-fact reading.
//   - A panic from a lifecycle hook, Validator, start condition, or probe is
//     recovered and reported as an error (or as unhealthy/unready), so a
//     misbehaving callback never unwinds through a public entry point.
//   - A stop that times out is reported honestly: the entry stays
//     StatusStopping rather than StatusStopped, and the incomplete stop is not
//     counted in Metrics().Stops.
//   - StatusSucceeded is permanent: a runOnce gate that succeeded keeps its
//     status when it is later stopped (by Stop, StopService, Unregister, or a
//     cascade). Its Stop() still runs — make it idempotent — but the gate is
//     never demoted to StatusStopped, so its success is not erased and a
//     hard-depending service can still start afterwards: StartService accepts a
//     dependency that is either StatusRunning or a runOnce gate in
//     StatusSucceeded.
//   - A teardown goroutine abandoned because a deadline won — a before-stop
//     hook or Stop() that did not return in time, the whole-Stop final wait for
//     the instance goroutines and the log-pump, or a failed-Start wait that
//     outlived its rollback budget — is logged at Error level naming the
//     service and counted in the monotonic Metrics().AbandonedGoroutines.
//     Done() closes as soon as Stop returns even while such a goroutine still
//     runs, so the counter is the only visibility into a leak user code can
//     cause by ignoring the contract.
//   - Messenger subscriptions are scoped to the instance or cron tick that
//     created them, and their owner is released on every exit path. Both
//     teardown and the failed-Start rollback drain every live owner id of an
//     entry, so an owner whose goroutine is abandoned — its deferred release
//     skipped — is still released. Owner ids are monotonic and never reused, so
//     a drained id cannot be reminted into a later view.
//   - A self-heal crash is observable before the restart: the entry transitions
//     StatusRunning to StatusCrashed (firing OnCrash and incrementing
//     Metrics().Crashes) and is returned to StatusRunning once the new instance
//     is live.
//   - A self-heal restart's best-effort cleanup of the dead instance is
//     bounded. It runs the same before/after-stop hooks and Stop() as a
//     caller-initiated teardown — so a hook that releases a lease is retried on
//     every crash, not only on an explicit stop — but, with no caller to supply
//     a budget, the before-stop hook is capped by the per-service
//     WithStopTimeout (30s when unset). An overrunning hook is abandoned,
//     logged and counted in Metrics().AbandonedGoroutines, and the restart
//     still proceeds. Instance spawning has a single path, so a start and a
//     restart install the same per-instance state.
//   - Metrics() is a snapshot of monotonic lifecycle counters. Each counter has
//     a fixed meaning: Starts counts lifecycle starts (one per instance launched
//     or cron schedule installed, invocations not successes); Stops counts
//     completed instance stops, excluding a self-heal restart's internal cleanup
//     and not counting a timed-out stop until the abandoned instance finally
//     exits; Crashes counts Running -> Crashed transitions; Restarts counts
//     self-heal re-launches; HealthFails counts failed periodic probes (per
//     probe, not per incident, and not on-demand Health() probes); CronFailures
//     counts failed or panicking cron ticks (skips and teardown cancellations
//     excluded); AbandonedGoroutines counts deadline-abandoned teardown
//     goroutines. See the Metrics type for the frozen definitions.
//   - Health is a live signal: both the periodic loop and Health() probe only
//     services that are StatusRunning. A non-running entry is omitted from the
//     Health() result rather than reported healthy, while a running service that
//     does not implement HealthChecker is reported with a nil error.
//   - A hot add that names a hard dependency which is being removed fails with
//     ErrDependencyRemoving: a retryable "not now" condition distinct from
//     ErrHasDependents, which is only about the target's own hard dependents.
//   - A plain StopService/Unregister is refused with ErrHasDependents when a
//     hard dependent is Running or Starting. The typed *HasDependentsError
//     carries the target name and the blocking dependents (the same set
//     Dependents(name) returns, in reverse topological order); DependenciesOf
//     is the forward direction. Dependents in any other status (Stopping,
//     Registered, Crashed, Stopped, Succeeded) do not block, and soft
//     dependencies neither block nor are reported. WithCascadeStop tears the
//     dependents down in reverse topological order; a dependent already Stopping
//     is left to its own in-flight teardown rather than stopped a second time.
//     Orphans is the explicit opposite: an opt-in that stops only the target and
//     leaves running hard dependents degraded. They keep StatusRunning, but
//     IsReady reports them not ready for as long as a hard dependency is neither
//     Running nor a succeeded runOnce gate; the orphan becomes ready again on its
//     own once the dependency is back. Orphans and WithCascadeStop are mutually
//     exclusive and the pair is rejected with ErrUnsupportedOption.
//   - The cycle check walks each node once, so a diamond costs O(V+E) rather
//     than one walk per path. The walk is depth-capped: a chain deeper than
//     10000 edges fails registration with
//     ErrDependencyDepthExceeded instead of overflowing the goroutine stack.
//     Registration is expected to build a bounded, startup-sized acyclic graph;
//     the cap guards the unbounded reload loop, not normal use.
//   - A membership op blocked by an in-flight reservation is classified by
//     cause: re-entry from the target's own Start/Stop on the same goroutine is
//     a programming error and returns ErrReentrantMembership, while a collision
//     with a reservation held by another goroutine (a start transaction or a
//     teardown) is transient and returns the retryable ErrMembershipBusy.
//     Busy(name) is the predicate a caller can poll to observe the reservation
//     instead of racing. The re-entry rule covers the whole callback, so a
//     persistent service self-starting from its Start is a re-entry for the
//     instance's lifetime, and the reservation is the transaction, so a running
//     persistent service or an in-flight cron tick is never busy.
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
//     left restartable and Start can be retried. The rollback still drains every
//     owner id such an abandoned instance registered, so retrying a failing
//     Start does not accumulate owner ids (or their subscriptions).
//   - Unregister discards the removed service's per-instance state — instance
//     and teardown contexts, exit channel, retry and health counters, stability
//     window, liveness and accounting flags, owner ids, and cron accounting —
//     and drops it from both the entries slice and the name index. The reset
//     shares one definition of "fresh" with the failed-Start retry; re-registering
//     the same name creates a brand-new entry that inherits nothing from the
//     previous incarnation. The auto-name sequence is monotonic: an omitted
//     WithName gets the next $N, and a value is never reused, even after its
//     entry is unregistered.
//   - A ServiceLogger never blocks: it drops an entry when the log channel is
//     full or its pump has stopped, so a torn-down log channel cannot stall a
//     service or a Stop. A service hot-added during a failed Start keeps a
//     logger bound to that Start's torn-down channel, and its entries are
//     dropped until the next Start rebinds every snapshotted entry's logger.
//
// See the README for the concurrency table and the per-method guarantees.
package gorch
