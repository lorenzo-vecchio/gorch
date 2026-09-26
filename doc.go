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
//   - A self-heal crash is observable before the restart: the entry transitions
//     StatusRunning to StatusCrashed (firing OnCrash and incrementing
//     Metrics().Crashes) and is returned to StatusRunning once the new instance
//     is live.
//   - A hot add that names a hard dependency which is being removed fails with
//     ErrDependencyRemoving: a retryable "not now" condition distinct from
//     ErrHasDependents, which is only about the target's own running dependents.
//
// See the README for the concurrency table and the per-method guarantees.
package gorch
