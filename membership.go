package gorch

import (
	"errors"
	"fmt"
	"strings"
	"time"
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
	// A stopped entry is looked up before it is unregistered: treat a teardown in
	// progress as already gone so nothing can restart it (C11).
	if entry.removing.Load() {
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

// StopService stops a registered service without removing it. The entry stays
// in Names()/Statuses() and can be started again with StartService.
//
// A running hard dependent blocks the stop with ErrHasDependents unless
// WithCascadeStop() is passed, in which case the target and its transitive hard
// dependents are stopped in reverse topological order. Soft dependencies never
// block and are never cascaded. timeout bounds how long the stop waits for each
// persistent instance's current run to exit, shared across a cascade; a
// non-positive timeout waits indefinitely. It does not bound a service's own
// Stop(), which is controlled by WithStopTimeout. A cron tick is cancelled but
// not awaited. Returns ErrServiceNotFound for an unknown name and
// ErrOrchestratorStopping/ErrOrchestratorStopped once whole-orchestrator
// shutdown has begun. Thread-safe.
func (o *Orchestrator) StopService(name string, timeout time.Duration, opts ...StopOption) error {
	return o.tearDown(name, false, timeout, opts)
}

// Unregister stops a registered service and removes it from the graph: it
// disappears from Names()/Statuses(), and its cron schedule and Messenger
// subscriptions are released. Any pending self-heal restart is cancelled.
// Blocking and cascade semantics match StopService. Thread-safe.
func (o *Orchestrator) Unregister(name string, timeout time.Duration, opts ...StopOption) error {
	return o.tearDown(name, true, timeout, opts)
}

// tearDown implements StopService and Unregister. It computes the hard-dependent
// cascade and marks the affected entries removing under the orchestrator lock,
// then stops them outside it (user code never runs under the lock) against one
// shared deadline, and finally removes them when remove is true.
func (o *Orchestrator) tearDown(name string, remove bool, timeout time.Duration, opts []StopOption) error {
	var cfg stopConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	// Serialize teardown transactions so a concurrent StopGroup/Unregister
	// cannot select the same entries mid-flight (C3, D16).
	o.membershipMu.Lock()
	defer o.membershipMu.Unlock()

	o.mu.Lock()
	if err := o.membershipGateLocked(); err != nil {
		o.mu.Unlock()
		return err
	}
	entry := o.lookupEntry(name)
	if entry == nil {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrServiceNotFound, name)
	}
	if entry.starting.Load() {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", errReentrantMembership, name)
	}

	// ordered holds the target plus its transitive hard dependents, dependents
	// first and the target last (reverse topological order).
	ordered := o.hardDependentsOrderLocked(entry)
	set := ordered
	if !cfg.cascade {
		if blockers := o.activeDependentsLocked(entry, ordered); len(blockers) > 0 {
			o.mu.Unlock()
			return fmt.Errorf("%w: %s is depended on by %s", ErrHasDependents, name, strings.Join(blockers, ", "))
		}
		// Without cascade only the target itself is stopped/removed; a
		// non-running dependent stays registered untouched.
		set = []*serviceEntry{entry}
	}
	// Freeze new hard-dependency edges into the set while it is torn down; a
	// concurrent Register rejects them (C11).
	for _, e := range set {
		e.removing.Store(true)
	}
	o.mu.Unlock()

	// One budget for the whole set (D8): a cascade does not multiply the
	// caller's deadline.
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	var stopErr error
	for _, e := range set {
		if err := o.stopEntry(e, deadline); err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("%s: %w", e.name, err))
		}
	}

	o.mu.Lock()
	if remove {
		for _, e := range set {
			o.removeEntryLocked(e)
		}
	}
	o.mu.Unlock()

	// Release subscriptions only after the contexts were cancelled and the
	// goroutines exited, so no instance can re-subscribe post-drain. The
	// removing flag stays set until the drain finishes: a concurrent
	// StartService (which takes no membership lock) must keep seeing the entry
	// as unavailable, or it could restart and be drained a moment later.
	for _, e := range set {
		o.drainService(e)
	}

	o.mu.Lock()
	for _, e := range set {
		e.removing.Store(false)
	}
	o.mu.Unlock()
	return stopErr
}

// hardDependentsOrderLocked returns target and its transitive hard dependents in
// reverse topological order: every dependent precedes the dependency it reaches
// through DependsOn, and target is last. The caller must hold o.mu.
func (o *Orchestrator) hardDependentsOrderLocked(target *serviceEntry) []*serviceEntry {
	var order []*serviceEntry
	seen := make(map[string]bool)
	var visit func(e *serviceEntry)
	visit = func(e *serviceEntry) {
		if seen[e.name] {
			return
		}
		seen[e.name] = true
		for _, d := range o.entries {
			for _, dep := range d.cfg.dependsOn {
				if dep == e.name {
					visit(d)
				}
			}
		}
		order = append(order, e)
	}
	visit(target)
	return order
}

// activeDependentsLocked returns the names in set, other than target, that are
// Running or Starting: the hard dependents a plain stop would break. The caller
// must hold o.mu.
func (o *Orchestrator) activeDependentsLocked(target *serviceEntry, set []*serviceEntry) []string {
	var blockers []string
	for _, e := range set {
		if e == target {
			continue
		}
		if s := o.statusOf(e); s == StatusRunning || s == StatusStarting {
			blockers = append(blockers, e.name)
		}
	}
	return blockers
}

// stopEntry cancels an entry's per-service context (which also cancels a pending
// self-heal backoff), removes its cron schedule, runs its Stop lifecycle, and
// waits for the current instance to exit up to deadline. A zero deadline means
// no timeout; a non-awaitable entry (runOnce, never started) returns as soon as
// its Stop lifecycle ran.
func (o *Orchestrator) stopEntry(entry *serviceEntry, deadline time.Time) error {
	// Cancel the entry's teardown context first: it aborts the running instance
	// and any pending self-heal backoff (C4). The instance cancel is also
	// cancelled for entries without a teardown (e.g. cron ticks).
	if cancel := entry.getTeardownCancel(); cancel != nil {
		cancel()
	}
	if cancel := entry.getCancel(); cancel != nil {
		cancel()
	}
	if entry.cfg.cronSpec != "" {
		o.removeCronEntry(entry)
	}

	err := o.stopOneService(entry)

	done := entry.getDone()
	if done == nil {
		return err
	}
	if deadline.IsZero() {
		<-done
		return err
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		err = errors.Join(err, ErrStopTimeout)
	}
	return err
}

// removeEntryLocked deletes entry from the registry. The caller must hold o.mu.
func (o *Orchestrator) removeEntryLocked(entry *serviceEntry) {
	delete(o.nameIndex, entry.name)
	filtered := o.entries[:0]
	for _, e := range o.entries {
		if e != entry {
			filtered = append(filtered, e)
		}
	}
	o.entries = filtered
}

// drainService releases every Messenger subscription owned by entry. It is the
// per-service counterpart of the orchestrator-wide Drain and is used when a
// service leaves the graph. Repeated calls are a no-op; other services keep
// their subscriptions and continue to receive Publish. Thread-safe.
func (o *Orchestrator) drainService(entry *serviceEntry) {
	o.messenger.drainOwner(entry.owner)
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
