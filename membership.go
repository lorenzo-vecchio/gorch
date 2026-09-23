package gorch

import (
	"errors"
	"fmt"
)

// errReentrantMembership is returned when a membership operation is re-entered
// from a service's own Start (C17). It is a programming error, not a state a
// caller can recover from.
var errReentrantMembership = errors.New("gorch: reentrant membership operation")

// StartService starts (or restarts) a registered service by name.
//
// A persistent service is (re)started in a fresh goroutine; a cron service is
// (re)scheduled on the live scheduler; a runOnce service is re-run once. An
// already-running persistent or cron entry is a no-op.
//
// Every hard dependency (DependsOn) must be StatusRunning first. Returns
// ErrServiceNotFound for an unknown name, ErrDependencyNotFound for a missing
// hard dependency, ErrDependencyNotRunning when a hard dependency is not
// running, and ErrOrchestratorStopping/ErrOrchestratorStopped once
// whole-orchestrator shutdown has begun.
// Thread-safe.
func (o *Orchestrator) StartService(name string) error {
	o.mu.RLock()
	if o.stopping {
		o.mu.RUnlock()
		return ErrOrchestratorStopping
	}
	if o.stopped {
		o.mu.RUnlock()
		return ErrOrchestratorStopped
	}
	entry := o.lookupEntry(name)
	o.mu.RUnlock()
	if entry == nil {
		return fmt.Errorf("%w: %s", ErrServiceNotFound, name)
	}
	if entry.starting.Load() {
		return fmt.Errorf("%w: %s", errReentrantMembership, name)
	}

	// Hard dependencies must exist (they may have been unregistered) and be
	// running before the dependent starts (C12).
	for _, dep := range entry.cfg.dependsOn {
		o.mu.RLock()
		depEntry := o.lookupEntry(dep)
		o.mu.RUnlock()
		if depEntry == nil {
			return fmt.Errorf("%w: %s -> %s", ErrDependencyNotFound, name, dep)
		}
		if s := o.statusOf(depEntry); s != StatusRunning {
			return fmt.Errorf("%w: %s depends on %s (%s)", ErrDependencyNotRunning, name, dep, s)
		}
	}

	// Idempotent for an already-running persistent/cron entry (runOnce is the
	// deliberate exception — it re-runs once).
	if o.statusOf(entry) == StatusRunning {
		return nil
	}

	switch {
	case entry.cfg.cronSpec != "":
		return o.startCronEntry(entry)
	case entry.cfg.runOnce:
		return o.startOneService(entry)
	default:
		// Clear the wait-group latch so the restarted instance's exit decrements
		// the wait group again.
		o.mu.Lock()
		entry.wgDone = false
		o.mu.Unlock()
		return o.startOneService(entry)
	}
}

// statusOf reads an entry's status under the shared status lock.
func (o *Orchestrator) statusOf(entry *serviceEntry) ServiceStatus {
	o.statusMu.RLock()
	defer o.statusMu.RUnlock()
	return entry.status
}

// startCronEntry (re)schedules a cron entry and marks it running. When the entry
// still holds a live cron ID the schedule already exists and only the status is
// reconciled.
func (o *Orchestrator) startCronEntry(entry *serviceEntry) error {
	o.mu.Lock()
	if entry.cronID == 0 {
		if err := o.scheduleEntry(entry); err != nil {
			o.mu.Unlock()
			return err
		}
	}
	o.mu.Unlock()

	o.setStatus(entry, StatusRunning)
	o.metricsStarts.Add(1)
	return nil
}
