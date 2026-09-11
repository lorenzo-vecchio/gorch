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
//   - Lifecycle is single-shot: after a successful Stop the orchestrator cannot
//     be restarted.
//   - The wire format is encoding/gob and is part of the public contract; types
//     passed through the typed Messenger helpers must be gob-compatible.
//   - Publish is drop-only: when a subscriber's buffer is full the message is
//     dropped for that subscriber.
//
// See the README for the concurrency table and the per-method guarantees.
package gorch
