package gorch

import (
	"context"
	"time"
)

// HealthChecker is implemented by services that can report their own health.
// Health is called periodically by the orchestrator. A non-nil error means
// the service is unhealthy.
type HealthChecker interface {
	Health(ctx context.Context) error
}

// ReadinessChecker is implemented by services that distinguish "running" from
// "ready to serve". Ready returns nil when the service can accept traffic.
type ReadinessChecker interface {
	Ready(ctx context.Context) error
}

// Validator is implemented by services that validate their configuration at
// Register time. Validate is called immediately during Register; a non-nil
// error causes Register to return that error.
type Validator interface {
	Validate() error
}

// ServiceStatus represents the lifecycle state of a registered service.
type ServiceStatus int

const (
	StatusRegistered ServiceStatus = iota
	StatusStarting
	StatusRunning
	StatusStopping
	StatusStopped
	StatusCrashed
	// StatusSucceeded marks a runOnce service whose Start completed without
	// error: it is a successful gate, distinct from StatusStopped so dependents
	// are not aborted by a gate that did its job.
	StatusSucceeded
)

// String returns a human-readable name for the status.
func (s ServiceStatus) String() string {
	switch s {
	case StatusRegistered:
		return "registered"
	case StatusStarting:
		return "starting"
	case StatusRunning:
		return "running"
	case StatusStopping:
		return "stopping"
	case StatusStopped:
		return "stopped"
	case StatusCrashed:
		return "crashed"
	case StatusSucceeded:
		return "succeeded"
	default:
		return "unknown"
	}
}

// Health probes every registered service that is currently StatusRunning and
// returns a map from service name to its probe error (nil = healthy). Services
// that are not running — registered, starting, stopping, stopped, crashed, or
// succeeded — are omitted, because their probe result would not be a live
// health signal. A running service that does not implement HealthChecker is
// reported with a nil error, as is a running service whose probe succeeds.
// Each probe gets a fresh deadline (HealthTimeout) and a panic is recovered and
// returned as an error. Thread-safe.
func (o *Orchestrator) Health() map[string]error {
	o.mu.RLock()
	entries := make([]*serviceEntry, len(o.entries))
	copy(entries, o.entries)
	o.mu.RUnlock()

	result := make(map[string]error, len(entries))
	for _, e := range entries {
		if !o.healthCandidate(e) {
			continue
		}
		_, probeErr := o.probeHealth(e)
		// Re-validate membership after the probe: a concurrent teardown may have
		// removed the entry from the graph while the probe ran, and a removed
		// name must not appear in the result (C5).
		if !o.healthStillCurrent(e) {
			continue
		}
		result[e.name] = probeErr
	}
	return result
}

// healthCandidate reports whether entry is eligible for a health probe: it is
// not being torn down and its status is StatusRunning. Both Health and the
// periodic loop use it so the two paths cannot diverge on which entries they
// probe.
func (o *Orchestrator) healthCandidate(entry *serviceEntry) bool {
	return !entry.removing.Load() && o.statusOf(entry) == StatusRunning
}

// probeHealth runs entry's HealthChecker probe, if it implements one. It
// returns (false, nil) for a service that does not implement HealthChecker and
// (true, err) otherwise, where err is the probe result or a recovered panic.
// Each probe gets a fresh deadline (HealthTimeout) so a slow checker does not
// fail later probes.
func (o *Orchestrator) probeHealth(entry *serviceEntry) (bool, error) {
	hc, ok := entry.getSvc().(HealthChecker)
	if !ok {
		return false, nil
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), o.cfg.HealthTimeout)
	defer cancel()
	return true, callErr(func() error { return hc.Health(probeCtx) })
}

// healthStillCurrent reports whether entry is still the registered entry for its
// name and is not being torn down. Both paths re-check it after a probe so a
// concurrent teardown cannot make them act on a stale snapshot (C5).
func (o *Orchestrator) healthStillCurrent(entry *serviceEntry) bool {
	o.mu.RLock()
	current := o.nameIndex[entry.name]
	o.mu.RUnlock()
	return current == entry && !entry.removing.Load()
}

// RegisterFunc registers a closure-based service.
func (o *Orchestrator) healthCheckLoop(ctx context.Context) {
	defer o.wg.Done()
	defer close(o.healthDone)
	ticker := time.NewTicker(o.cfg.HealthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.runHealthChecks()
		}
	}
}

func (o *Orchestrator) runHealthChecks() {
	o.mu.RLock()
	entries := make([]*serviceEntry, len(o.entries))
	copy(entries, o.entries)
	o.mu.RUnlock()

	for _, e := range entries {
		// Skip an entry the health loop snapshotted just before it was removed,
		// while it is being torn down, or that is not running (C5).
		if !o.healthCandidate(e) {
			continue
		}
		// Hooks fire only for services that actually implement HealthChecker;
		// probeHealth would otherwise report a running non-checker as healthy,
		// but the periodic path must not instrument it.
		if _, ok := e.getSvc().(HealthChecker); !ok {
			continue
		}

		if o.cfg.BeforeHealthCheck != nil {
			if err := callErr(func() error { return o.cfg.BeforeHealthCheck(e.name) }); err != nil {
				e.getLogger().Warn("before-health-check hook failed", "error", err.Error())
			}
		}

		_, healthErr := o.probeHealth(e)
		if o.cfg.AfterHealthCheck != nil {
			callVoid(func() { o.cfg.AfterHealthCheck(e.name, healthErr) })
		}
		// Re-validate membership after the (possibly slow) probe, exactly as
		// Health does: a concurrent Unregister may have removed or replaced this
		// entry, and acting on the stale snapshot would cancel an instance the
		// graph no longer tracks (C5).
		if !o.healthStillCurrent(e) {
			continue
		}
		failures := e.getHealthFailures()
		if healthErr != nil {
			failures++
			o.metricsHealthFails.Add(1)
			e.getLogger().Warn("health check failed", "failures", failures, "error", healthErr.Error())
			if failures >= o.cfg.HealthThreshold && e.cfg.factory != nil {
				e.getLogger().Error("health threshold reached, restarting service", "failures", failures)
				e.setHealthFailures(0)
				// Cancel the service's context → Start returns → handleServiceDone → self-heal.
				if cancelFn := e.getCancel(); cancelFn != nil {
					cancelFn()
				}
			} else {
				e.setHealthFailures(failures)
			}
		} else {
			e.setHealthFailures(0)
		}
	}
}

// ── Internal helpers ──
