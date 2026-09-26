package gorch

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// StartService starts (or restarts) a registered service by name.
//
// A persistent service is (re)started in a fresh goroutine; a cron service is
// (re)scheduled on the live scheduler; a runOnce service is re-run once. An
// already-running persistent or cron entry is a no-op.
//
// Every hard dependency (DependsOn) must be StatusRunning first. Returns
// ErrServiceNotFound for an unknown name, ErrDependencyNotFound for a missing
// hard dependency, ErrDependencyNotRunning when a hard dependency is not
// running, ErrOrchestratorNotStarted before the orchestrator is started, and
// ErrOrchestratorStopping/ErrOrchestratorStopped once whole-orchestrator
// shutdown has begun.
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
	// There is no scheduler or service context to start into before Start; a
	// cron entry in particular would otherwise reach a nil scheduler and panic.
	if !o.started {
		o.mu.RUnlock()
		return ErrOrchestratorNotStarted
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
		// A start reservation can be held either by this entry's own Start
		// (same goroutine: a genuine re-entry, a programming error) or by
		// another goroutine's start (a transient collision the caller may
		// retry). Only goroutine identity can tell them apart.
		if entry.lifecycleOwnedBy(curGoroutineID()) {
			return fmt.Errorf("%w: %s", ErrReentrantMembership, name)
		}
		return fmt.Errorf("%w: %s", ErrMembershipBusy, name)
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

	// A persistent instance can be live while its status is momentarily not
	// Running: the self-heal handler reports Crashed before it restarts, and a
	// restart in backoff is not Running either. The live instance is what the
	// idempotence guard must key on, not the status, or a concurrent StartService
	// would start a second instance behind the first and leak the wait group
	// (each instance adds one, but the entry's exit latch releases only one).
	// entry.done is open from the moment an instance starts until its goroutine
	// has fully exited, so it covers both windows. Cron (no instance channel) and
	// runOnce (deliberately re-runnable) keep the status-only guard above.
	if done := entry.getDone(); done != nil {
		select {
		case <-done:
			// No live instance: fall through and (re)start.
		default:
			return nil
		}
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
// in Names()/Statuses(), can be started again with StartService, and its
// Messenger subscriptions are released on stop.
//
// A running hard dependent blocks the stop with ErrHasDependents unless
// WithCascadeStop() is passed, in which case the target and its transitive hard
// dependents are stopped in reverse topological order. Soft dependencies never
// block and are never cascaded. timeout bounds the whole stop — the service's
// own Stop() and the wait for its instance or in-flight cron ticks to exit — and
// is shared across a cascade; a non-positive timeout waits indefinitely. A
// per-service WithStopTimeout still caps Stop() when it is smaller than the
// remaining budget. Returns ErrServiceNotFound for an unknown name and
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

	// Serialize teardown *selection* so a concurrent StopGroup/Unregister cannot
	// pick the same entries mid-flight (C3, D16). The lock is released before any
	// user code runs: StopService/Unregister may be called from a service's own
	// Stop hook, and holding membershipMu across it would self-deadlock.
	o.membershipMu.Lock()
	o.mu.Lock()
	if err := o.membershipGateLocked(); err != nil {
		o.mu.Unlock()
		o.membershipMu.Unlock()
		return err
	}
	entry := o.lookupEntry(name)
	if entry == nil {
		o.mu.Unlock()
		o.membershipMu.Unlock()
		return fmt.Errorf("%w: %s", ErrServiceNotFound, name)
	}

	// ordered holds the target plus its transitive hard dependents, dependents
	// first and the target last (reverse topological order).
	ordered := o.hardDependentsOrderLocked(entry)
	set := ordered
	if !cfg.cascade {
		if blockers := o.activeDependentsLocked(entry, ordered); len(blockers) > 0 {
			o.mu.Unlock()
			o.membershipMu.Unlock()
			return fmt.Errorf("%w: %s is depended on by %s", ErrHasDependents, name, strings.Join(blockers, ", "))
		}
		// Without cascade only the target itself is stopped/removed; a
		// non-running dependent stays registered untouched.
		set = []*serviceEntry{entry}
	}
	// Reject a membership op that re-enters from a selected entry's own
	// Start/Stop on this goroutine (C17): a programming error, never retryable.
	// This pass must come before the reservation check so a self-reentry is
	// classified as reentrancy even though the entry is also reserved.
	goid := curGoroutineID()
	for _, e := range set {
		if e.lifecycleOwnedBy(goid) {
			o.mu.Unlock()
			o.membershipMu.Unlock()
			return fmt.Errorf("%w: %s", ErrReentrantMembership, e.name)
		}
	}
	// Otherwise a selected entry reserved by another goroutine's transaction or
	// StartGroup start is a transient collision: the caller may retry once the
	// reservation clears (observe it with Busy, or poll the status).
	for _, e := range set {
		if e.starting.Load() || e.removing.Load() {
			o.mu.Unlock()
			o.membershipMu.Unlock()
			return fmt.Errorf("%w: %s", ErrMembershipBusy, e.name)
		}
	}
	// Freeze new hard-dependency edges into the set while it is torn down; a
	// concurrent Register rejects them (C11). The defer guarantees the flags are
	// cleared even if a stop below panics: a leaked removing flag would reject
	// every later membership op on the entry (C1.1).
	for _, e := range set {
		e.removing.Store(true)
	}
	o.mu.Unlock()
	o.membershipMu.Unlock()
	defer func() {
		o.mu.Lock()
		for _, e := range set {
			e.removing.Store(false)
		}
		o.mu.Unlock()
	}()

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

// stopEntry cancels an entry's contexts (which also aborts a pending self-heal
// backoff), removes its cron schedule, runs its Stop lifecycle, and waits for the
// current instance and any in-flight cron ticks to exit up to deadline. A zero
// deadline means no timeout; a non-awaitable entry (runOnce, never started)
// returns as soon as its Stop lifecycle ran.
func (o *Orchestrator) stopEntry(entry *serviceEntry, deadline time.Time) error {
	// Cancel the entry's teardown context first: it aborts the running instance
	// and any pending self-heal backoff (C4). Cron entries have no instance
	// cancel; their in-flight ticks are reached through the shared schedule
	// context in cronDrain below.
	if cancel := entry.getTeardownCancel(); cancel != nil {
		cancel()
	}
	if cancel := entry.getCancel(); cancel != nil {
		cancel()
	}

	// A cron entry has no instance channel: cancel every in-flight tick and wait
	// on the drained latch instead of entry.getDone().
	var awaited <-chan struct{}
	if entry.cfg.cronSpec != "" {
		o.removeCronEntry(entry)
		awaited = entry.cronDrain()
	} else {
		awaited = entry.getDone()
	}

	wasActive, completed, err := o.stopOneServiceDeadline(entry, deadline)
	// The instance goroutine exiting is the other half of a verified stop: a
	// sequence that returned while the instance still runs (a service that
	// ignores context cancellation) must not be reported stopped.
	if !o.awaitDone(awaited, deadline) {
		err = errors.Join(err, ErrStopTimeout)
		completed = false
	}
	if completed {
		o.finishStop(entry, wasActive)
	}
	return err
}

// awaitDone blocks until ch closes, at most until deadline. A nil channel has
// nothing to await, and a zero deadline waits indefinitely. It reports whether
// the wait finished before the deadline.
func (o *Orchestrator) awaitDone(ch <-chan struct{}, deadline time.Time) bool {
	if ch == nil {
		return true
	}
	if deadline.IsZero() {
		<-ch
		return true
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ch:
		return true
	case <-timer.C:
		return false
	}
}

// removeEntryLocked deletes entry from the registry and marks it removed. The
// flag is permanent, so a start that selected the entry before deletion cannot
// revive it. The caller must hold o.mu.
func (o *Orchestrator) removeEntryLocked(entry *serviceEntry) {
	entry.removed.Store(true)
	delete(o.nameIndex, entry.name)
	filtered := o.entries[:0]
	for _, e := range o.entries {
		if e != entry {
			filtered = append(filtered, e)
		}
	}
	o.entries = filtered
}

// newOwner allocates a fresh Messenger owner for a running instance or cron
// tick, registers its liveness token immediately (so a drain that lands before
// the view is built still retires it), and records it on the entry. The id is
// drawn from the monotonic ownerSeq and never reused.
//
// The owner is released when its instance or tick exits (handleServiceDone or
// invokeCron) or, if that goroutine is abandoned by a deadline, by drainService
// sweeping the entry's live ids on teardown. The root registration happens
// before the entry records the id, and every release path removes from the entry
// before draining the root, so the root map always holds at least the ids the
// entries claim: the two maps cannot diverge into a stale entry id the root has
// already forgotten.
func (o *Orchestrator) newOwner(entry *serviceEntry) *ownerState {
	id := o.ownerSeq.Add(1)
	st := o.messenger.registerOwner(id)
	entry.addOwner(id)
	return st
}

// releaseOwner drains and forgets one owner of the entry when its instance or
// tick exits. Owner id 0 is the unowned root registry and is never released.
// It is idempotent, so a path that also sweeps the owner (or releases it twice,
// as handleServiceDone's explicit release and its defer do) is safe.
func (o *Orchestrator) releaseOwner(entry *serviceEntry, id uint64) {
	if id == 0 {
		return
	}
	entry.removeOwner(id)
	o.messenger.drainOwner(id)
}

// drainService releases every Messenger subscription owned by entry, across all
// live owner ids (one per running instance or cron tick). It is the per-service
// counterpart of the orchestrator-wide Drain and is used when a service leaves
// the graph or an instance exits. Repeated calls are a no-op; other services
// keep their subscriptions and continue to receive Publish. Thread-safe.
//
// It is also the teardown safety net for an abandoned goroutine: a cron tick or
// instance whose deadline expired may never reach its deferred releaseOwner, so
// takeOwners drains the ids it still owns regardless of whether that goroutine
// runs again.
func (o *Orchestrator) drainService(entry *serviceEntry) {
	for _, id := range entry.takeOwners() {
		o.messenger.drainOwner(id)
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
