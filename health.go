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

func (o *Orchestrator) Health() map[string]error {
	o.mu.RLock()
	entries := make([]*serviceEntry, len(o.entries))
	copy(entries, o.entries)
	o.mu.RUnlock()

	result := make(map[string]error, len(entries))
	for _, e := range entries {
		// An entry being torn down must not be probed (C5).
		if e.removing.Load() {
			continue
		}
		hc, ok := e.getSvc().(HealthChecker)
		var probeErr error
		if ok {
			// Per-probe deadline so a slow checker does not fail later probes.
			probeCtx, cancel := context.WithTimeout(context.Background(), o.cfg.HealthTimeout)
			probeErr = hc.Health(probeCtx)
			cancel()
		}
		// Re-validate membership after the probe: a concurrent teardown may have
		// removed the entry from the graph while the probe ran, and a removed
		// name must not appear in the result (C5).
		o.mu.RLock()
		current := o.nameIndex[e.name]
		o.mu.RUnlock()
		if current != e {
			continue
		}
		result[e.name] = probeErr
	}
	return result
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
		// Skip an entry the health loop snapshotted just before it was removed
		// or while it is being torn down (C5).
		if e.removing.Load() {
			continue
		}
		hc, ok := e.getSvc().(HealthChecker)
		if !ok {
			continue
		}

		o.statusMu.RLock()
		s := e.status
		o.statusMu.RUnlock()
		if s != StatusRunning {
			continue
		}

		if o.cfg.BeforeHealthCheck != nil {
			if err := o.cfg.BeforeHealthCheck(e.name); err != nil {
				e.getLogger().Warn("before-health-check hook failed", "error", err.Error())
			}
		}

		// Each probe gets a fresh per-service deadline so a slow checker does
		// not fail all later probes with an expired context.
		probeCtx, cancel := context.WithTimeout(context.Background(), o.cfg.HealthTimeout)
		healthErr := hc.Health(probeCtx)
		cancel()
		if o.cfg.AfterHealthCheck != nil {
			o.cfg.AfterHealthCheck(e.name, healthErr)
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
