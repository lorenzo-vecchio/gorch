package gorch

import "context"

// Health probes all registered services that implement HealthChecker.
// Returns a map of service name to error (nil = healthy).
// Services that don't implement HealthChecker are reported as nil.
// Thread-safe.
// IsReady reports whether a named service is running, ready to serve, and not
// degraded by a missing hard dependency. The entry must be StatusRunning and
// every hard dependency (DependsOn) must itself be satisfied — Running, or a
// runOnce gate that reached StatusSucceeded — mirroring the edge condition
// StartService enforces before it starts a dependent. The dependency clause is
// what makes an orphaned dependent (kept StatusRunning by Orphans) read as not
// ready while its dependency is gone; it becomes ready again on its own once the
// dependency is back. Soft dependencies are never consulted. The
// ReadinessChecker probe (if any) runs with the given ctx, so callers can bound
// how long they wait (e.g. IsReady(ctx, name) with a deadline context).
// Thread-safe.
func (o *Orchestrator) IsReady(ctx context.Context, name string) bool {
	o.ensureInit()
	o.mu.RLock()
	entry, ok := o.nameIndex[name]
	if !ok {
		o.mu.RUnlock()
		return false
	}
	ready := o.entryReadyLocked(entry)
	o.mu.RUnlock()
	if !ready {
		return false
	}
	rc, ok := entry.getSvc().(ReadinessChecker)
	if !ok {
		return true
	}
	err := callErr(func() error { return rc.Ready(ctx) })
	return err == nil
}

// entryReadyLocked reports whether entry is Running and every one of its hard
// dependencies is satisfied. A dependency is satisfied when it is StatusRunning
// or a runOnce gate that reached StatusSucceeded, the same edge condition
// StartService checks before it starts a dependent; a dependency that was
// unregistered resolves to nil and fails the check. Statuses are read under
// statusMu, so the caller must hold o.mu for the lookup and must not already
// hold statusMu.
func (o *Orchestrator) entryReadyLocked(entry *serviceEntry) bool {
	o.statusMu.RLock()
	defer o.statusMu.RUnlock()
	if entry.status != StatusRunning {
		return false
	}
	for _, dep := range entry.cfg.dependsOn {
		de := o.lookupEntry(dep)
		if de == nil {
			return false
		}
		if s := de.status; s != StatusRunning && s != StatusSucceeded {
			return false
		}
	}
	return true
}
