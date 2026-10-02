package gorch

// Metrics is a snapshot of the orchestrator's event counters. Every counter is
// monotonic — it is never reset or decremented — so a snapshot is meaningful
// only relative to an earlier one (take a baseline, then subtract).
//
// The counters are lifecycle events, not per-tick work, and the distinction is
// deliberate: a self-heal restart is a Restart and never a Stop; a cron tick is
// not a Start; and a failed probe is counted per probe, not per incident. These
// definitions are the frozen v1.0 contract.
type Metrics struct {
	// Starts counts the service starts the orchestrator initiated through its
	// lifecycle API (Start, StartService, StartGroup, Run): one per
	// persistent/runOnce instance launched and one per cron schedule installed.
	// It counts invocations, not successes, so a start that immediately fails or
	// times out is still counted. A self-heal re-launch is counted by Restarts,
	// not here, so Starts + Restarts is the total number of instances launched
	// for a persistent service.
	Starts int64
	// Stops counts completed instance stops. A lifecycle-API stop (Stop,
	// StopService, StopGroup, Unregister) and a non-self-heal instance that exits
	// on its own and is committed StatusStopped both count. A caller stop that
	// timed out —
	// ErrStopTimeout/ErrHookTimeout, or a WithStopTimeout cap that fired — is not
	// counted at that moment, matching the entry's unverified StatusStopping; it
	// counts once the abandoned instance finally exits, so one that never does is
	// never counted. The internal Stop() a self-heal restart runs is not a stop —
	// the restart itself is in Restarts. Each instance counts at most once even
	// when the teardown and the instance's own exit race.
	Stops int64
	// Crashes counts Running -> Crashed transitions: an instance that exited with
	// a real error, or whose retry budget was exhausted. A self-heal crash is
	// counted before the restart, so it is observable even when the service comes
	// straight back up. A clean or context.Canceled exit is not a crash.
	Crashes int64
	// Restarts counts self-heal re-launches: a fresh instance spawned after an
	// instance exited on its own — crash, clean return, or a health-threshold
	// cancellation — without a caller-initiated teardown.
	Restarts int64
	// HealthFails counts failed periodic health probes: it increments once per
	// failed probe, not once per service that crossed the failure threshold. A
	// service probed every 30s with a permanently failing checker therefore adds
	// one per check, not one per outage. Probes triggered on demand by Health()
	// are not counted — the counter instruments the supervision loop, not ad-hoc
	// reads.
	HealthFails int64
	// CronFailures counts cron tick invocations that failed: the tick's Start
	// returned a non-nil, non-context.Canceled error, or panicked. A tick that is
	// skipped (CronSkip while the previous invocation is still running) or
	// cancelled by teardown is not a failure. Ticks are not counted as Starts, so
	// this is the counter for per-tick cron activity.
	CronFailures int64
	// AbandonedGoroutines counts teardown goroutines abandoned because a
	// deadline won: a before-stop hook or Stop() that did not return within its
	// budget, or a failed-Start wait that outlived its rollback budget. A
	// non-zero value means user code may be leaked for the process lifetime. The
	// counter is monotonic — it is never decremented if the abandoned goroutine
	// later returns.
	AbandonedGoroutines int64
}

// Metrics returns a snapshot of orchestrator-level event counters. See the
// Metrics type for the definition of each counter; all of them are monotonic,
// so compare two snapshots rather than reading absolute values.
func (o *Orchestrator) Metrics() Metrics {
	o.ensureInit()
	return Metrics{
		Starts:              o.metricsStarts.Load(),
		Stops:               o.metricsStops.Load(),
		Crashes:             o.metricsCrashes.Load(),
		Restarts:            o.metricsRestarts.Load(),
		HealthFails:         o.metricsHealthFails.Load(),
		CronFailures:        o.metricsCronFailures.Load(),
		AbandonedGoroutines: o.metricsAbandoned.Load(),
	}
}

// Done returns a channel that closes once a Stop call has completed — that is,
// once Stop (or Run, which calls Stop on the way out) has returned. It is a
// shutdown-completed signal, not an "all goroutines have exited" signal:
//
//   - The orchestrator must be stopped before the channel closes. A hot add or
//     a restart while running does not close it, and a failed Start — which
//     never calls Stop — leaves it open.
//   - The channel is created once in New and is the same on every call; it is
//     never recreated, so a caller holding it across a failed-Start retry sees
//     it close only at the eventual Stop.
//   - A Stop that times out still closes it, even though an abandoned teardown
//     goroutine may still be running (see Metrics().AbandonedGoroutines). Only
//     Stop's completion is guaranteed, not that every goroutine has exited.
//   - A no-op Stop on an orchestrator that was never started closes it too.
func (o *Orchestrator) Done() <-chan struct{} {
	o.ensureInit()
	return o.shutdownDone
}
