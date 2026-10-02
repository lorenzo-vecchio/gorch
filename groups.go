package gorch

import (
	"errors"
	"fmt"
	"time"
)

// StartGroup starts all services in the named group in topological order.
// The shared membership lock serializes group selection against
// StopService/Unregister/StopGroup (C3, D16), but is released before any user
// code runs: a service's Start may call a membership op without self-deadlocking.
// Each selected entry is reserved via its starting flag, so a concurrent teardown
// is rejected rather than stopping an entry about to be started.
//
// Each member is started through the same path StartService uses for its kind: a
// persistent or runOnce member via its Start, a cron member by (re)installing
// its schedule, never by running its Start as a persistent instance.
//
// Returns ErrOrchestratorNotStarted before Start, exactly like StartService:
// without a service context a member's Start would otherwise reach a nil parent
// context. An unknown or empty group selects nothing and returns nil: a group is
// a tag, not a registered entity, so an empty match is a no-op. After Stop it
// returns ErrOrchestratorStopped (or ErrOrchestratorStopping while Stop runs).
// Thread-safe.
func (o *Orchestrator) StartGroup(group string) error {
	o.ensureInit()
	o.membershipMu.Lock()
	o.mu.Lock()
	if err := o.membershipGateLocked(); err != nil {
		o.mu.Unlock()
		o.membershipMu.Unlock()
		return err
	}
	// No service context or scheduler exists before Start: a selected member
	// would reach startOneService and build a context from a nil parent. Gate on
	// the lifecycle flag, as StartService does.
	if !o.started {
		o.mu.Unlock()
		o.membershipMu.Unlock()
		return ErrOrchestratorNotStarted
	}
	// groupEntries keeps every group member so topoSort sees the full
	// dependency graph even when only a subset is actually started.
	groupEntries := make([]*serviceEntry, 0)
	// reserved holds the entries this call will start: not already running,
	// starting or being torn down. Reserving only those keeps a group start
	// idempotent and stops two concurrent StartGroups double-starting an entry.
	reserved := make([]*serviceEntry, 0)
	for _, e := range o.entries {
		if e.cfg.group != group {
			continue
		}
		groupEntries = append(groupEntries, e)
		if e.removing.Load() || e.starting.Load() {
			continue
		}
		if s := o.statusOf(e); s == StatusRunning || s == StatusStarting {
			continue
		}
		// StopGroup runs Stop() without cancelling the entry's context, so a
		// persistent instance can still be exiting while the status already
		// reads StatusStopped. Starting again would spawn a second instance and
		// leak the wait group (the wgDone latch releases only one). Treat a live
		// instance channel as already running, exactly like StartService.
		if done := e.getDone(); done != nil {
			select {
			case <-done:
			default:
				continue
			}
		}
		e.starting.Store(true)
		reserved = append(reserved, e)
	}
	o.mu.Unlock()
	o.membershipMu.Unlock()
	// Release every reservation on the way out, including a panic from a
	// synchronous user Start/hook: started entries already cleared their own.
	defer o.clearStarting(reserved)

	levels, err := o.topoSort(groupEntries)
	if err != nil {
		return err
	}
	startable := make(map[*serviceEntry]bool, len(reserved))
	for _, e := range reserved {
		startable[e] = true
	}
	var started []*serviceEntry
	for _, level := range levels {
		for _, entry := range level {
			if !startable[entry] {
				continue
			}
			if err := o.startGroupEntry(entry); err != nil {
				// Roll back the members already started in earlier levels so a
				// mid-group failure leaves no partially started group.
				o.rollbackGroupStart(started)
				return err
			}
			started = append(started, entry)
		}
	}
	return nil
}

// startGroupEntry starts one group member through the same path StartService
// uses for that member's kind. A cron member is (re)scheduled through
// startCronEntry: startOneService would launch the cron service's Start
// continuously and leave its schedule uninstalled, so a StartGroup after a
// StopService on that member would silently turn a scheduled service into an
// always-on one. Persistent and runOnce members share startOneService, the
// same as Start's own phases.
func (o *Orchestrator) startGroupEntry(entry *serviceEntry) error {
	if entry.cfg.cronSpec != "" {
		return o.startCronEntry(entry)
	}
	return o.startOneService(entry)
}

// rollbackGroupStart stops the members a failed StartGroup already started, in
// reverse start order. It is best-effort: the caller reports the original start
// failure, and a rollback error must not mask it.
func (o *Orchestrator) rollbackGroupStart(started []*serviceEntry) {
	for i := len(started) - 1; i >= 0; i-- {
		e := started[i]
		_ = o.stopEntry(e, time.Time{})
		o.drainService(e)
	}
}

// clearStarting releases the StartGroup reservation on entries whose start never
// ran (or failed); started entries already cleared their own flag.
func (o *Orchestrator) clearStarting(entries []*serviceEntry) {
	for _, e := range entries {
		e.starting.Store(false)
	}
}

// StopGroup stops all non-cron, non-runOnce services in the named group in
// reverse topological order. Errors are aggregated via errors.Join. The shared
// membership lock serializes selection (C3, D16) but is released before the
// services' Stop hooks run, so a hook may call a membership op without
// self-deadlocking. Selected entries are reserved via the removing flag so a
// concurrent group start/stop or teardown sees them mid-operation.
//
// Before Start it is a no-op returning nil, consistent with StopService and
// Unregister: nothing is running to stop. An unknown or empty group likewise
// selects nothing and returns nil. After Stop it returns ErrOrchestratorStopped
// (or ErrOrchestratorStopping while Stop runs). Thread-safe.
func (o *Orchestrator) StopGroup(group string, timeout time.Duration) error {
	o.ensureInit()
	o.membershipMu.Lock()
	o.mu.Lock()
	if err := o.membershipGateLocked(); err != nil {
		o.mu.Unlock()
		o.membershipMu.Unlock()
		return err
	}
	persistent := make([]*serviceEntry, 0)
	for _, e := range o.entries {
		if e.cfg.group != group || e.cfg.cronSpec != "" || e.cfg.runOnce {
			continue
		}
		// Honour a concurrent StartGroup reservation and skip an entry another
		// teardown already owns.
		if e.starting.Load() || e.removing.Load() {
			continue
		}
		e.removing.Store(true)
		persistent = append(persistent, e)
	}
	o.mu.Unlock()
	o.membershipMu.Unlock()
	// Release the reservations once the stops (user code) have run.
	defer func() {
		o.mu.Lock()
		for _, e := range persistent {
			e.removing.Store(false)
		}
		o.mu.Unlock()
	}()

	levels, topoErr := o.topoSortForStop(persistent)
	stopErr := topoErr
	for i := len(levels) - 1; i >= 0; i-- {
		for _, entry := range levels[i] {
			if err := o.stopOneService(entry); err != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("%s: %w", entry.name, err))
			}
		}
	}
	return stopErr
}

// UnregisterGroup stops and removes every service in the named group — the
// group-level analogue of Unregister. Unlike StopGroup it removes the members
// rather than leaving them registered, and it includes cron and runOnce members
// that StopGroup deliberately leaves alone: after it returns no member appears
// in Names(), Statuses(), or Count(), its cron schedule and Messenger
// subscriptions are released, and its per-instance state is discarded exactly as
// Unregister discards one service's.
//
// Members are selected and reserved under the shared membership lock, like
// StopGroup, but a selected entry already reserved by another operation — or by
// this goroutine's own Start/Stop — rejects the whole call instead of being
// skipped: a start collision returns the transient ErrMembershipBusy and a
// same-goroutine re-entry returns the programming error ErrReentrantMembership.
// Removal is destructive, so a silent skip would claim the group gone while a
// member lived on.
//
// A hard dependent outside the group is the cross-group edge. By default the
// operation refuses to break one that is Running or Starting, returning
// ErrHasDependents; the *HasDependentsError names the member it hangs from and
// the blocking outside dependents, the same set Dependents reports for that
// member. WithCascadeStop extends the removal to every transitive hard dependent
// of every member, including entries outside the group — a deliberate expansion
// past the operation's nominal scope, so the removed set may exceed the group.
// Orphans is the opposite opt-in: it removes only the members and leaves the
// outside dependents Running but degraded, exactly as Orphans does for
// Unregister. WithCascadeStop and Orphans are mutually exclusive and rejected
// together with ErrUnsupportedOption. Soft dependencies never block and are
// never cascaded.
//
// The selected entries are stopped in reverse topological order under one shared
// timeout (a non-positive timeout waits indefinitely, and a per-service
// WithStopTimeout still caps Stop()). An entry already StatusStopping is left to
// its own in-flight teardown rather than stopped a second time. Failures are
// aggregated with errors.Join and the removal still proceeds: every member is
// removed even when a member's Stop() errored or timed out, matching Unregister
// and StopGroup, which aggregate and continue — removal cannot be rolled back
// once an entry is gone. An unknown or empty group selects nothing and returns
// nil, consistent with StartGroup and StopGroup. Before Start it removes the
// registered members without starting anything, like Unregister; after Stop it
// returns ErrOrchestratorStopped (or ErrOrchestratorStopping while Stop runs).
// Thread-safe.
func (o *Orchestrator) UnregisterGroup(group string, timeout time.Duration, opts ...StopOption) error {
	o.ensureInit()
	var cfg stopConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	// Orphans and cascade answer the same question oppositely — leave the outside
	// dependents running or tear them down — so the pair is incoherent, exactly as
	// for StopService/Unregister. Reject it before any selection.
	if cfg.cascade && cfg.orphans {
		return fmt.Errorf("%w: WithCascadeStop cannot be combined with Orphans", ErrUnsupportedOption)
	}

	// Serialize selection and reservation with every other membership op; the
	// lock is released before any user code (Stop/hooks) runs.
	o.membershipMu.Lock()
	o.mu.Lock()
	if err := o.membershipGateLocked(); err != nil {
		o.mu.Unlock()
		o.membershipMu.Unlock()
		return err
	}
	members := make([]*serviceEntry, 0)
	memberNames := make(map[string]bool)
	for _, e := range o.entries {
		if e.cfg.group == group {
			members = append(members, e)
			memberNames[e.name] = true
		}
	}
	// An unknown or empty group selects nothing and is a no-op, consistent with
	// StartGroup and StopGroup: a group is a filter tag, not an entity.
	if len(members) == 0 {
		o.mu.Unlock()
		o.membershipMu.Unlock()
		return nil
	}

	// A plain removal refuses to break an active hard dependent outside the
	// group. An in-group dependent is part of the removal set and never blocks, so
	// filter the member names out of each member's active dependents.
	if !cfg.cascade && !cfg.orphans {
		var refusal error
		for _, m := range members {
			active := o.activeDependentsLocked(m, o.hardDependentsOrderLocked(m))
			outside := make([]string, 0, len(active))
			for _, name := range active {
				if !memberNames[name] {
					outside = append(outside, name)
				}
			}
			if len(outside) > 0 {
				refusal = errors.Join(refusal, &HasDependentsError{Name: m.name, Dependents: outside})
			}
		}
		if refusal != nil {
			o.mu.Unlock()
			o.membershipMu.Unlock()
			return refusal
		}
	}

	// The removal set is the members plus, under cascade, their outside
	// dependents. Without cascade only the members are stopped and removed; an
	// inactive outside dependent stays registered and untouched.
	set := append([]*serviceEntry(nil), members...)
	if cfg.cascade {
		set = append(set, o.externalDependentsLocked(members, memberNames)...)
	}

	// Reentrancy is classified before contention, exactly as tearDown does, so a
	// membership op re-entered from a selected entry's own Start/Stop on this
	// goroutine is a programming error rather than a transient collision.
	goid := curGoroutineID()
	for _, e := range set {
		if e.lifecycleOwnedBy(goid) {
			o.mu.Unlock()
			o.membershipMu.Unlock()
			return fmt.Errorf("%w: %s", ErrReentrantMembership, e.name)
		}
	}
	for _, e := range set {
		if e.starting.Load() || e.removing.Load() {
			o.mu.Unlock()
			o.membershipMu.Unlock()
			return fmt.Errorf("%w: %s", ErrMembershipBusy, e.name)
		}
	}
	// Freeze new hard-dependency edges into the set while it is torn down, and
	// release them even if a stop panics: a leaked removal flag would reject every
	// later membership op on the entry.
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

	levels, topoErr := o.topoSortForStop(set)
	stopErr := topoErr
	// One budget for the whole set, like StopGroup and the per-service cascade.
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	for i := len(levels) - 1; i >= 0; i-- {
		for _, e := range levels[i] {
			// An entry already Stopping is mid-teardown (or was left there by a
			// timed-out stop): running its sequence again would fire its hooks and
			// Stop() a second time. Skip it; it is still removed below.
			if o.statusOf(e) == StatusStopping {
				continue
			}
			if err := o.stopEntry(e, deadline); err != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("%s: %w", e.name, err))
			}
		}
	}

	// Remove every selected entry regardless of a stop failure, exactly as
	// Unregister does: the group is gone, and the joined error reports why a
	// member's teardown was unverified. Subscriptions are released only after the
	// contexts were cancelled and the goroutines exited.
	o.mu.Lock()
	for _, e := range set {
		o.removeEntryLocked(e)
	}
	o.mu.Unlock()
	for _, e := range set {
		o.drainService(e)
	}
	return stopErr
}

// StatusesByGroup returns a map of service name to status for all services
// in the named group. Thread-safe.
func (o *Orchestrator) StatusesByGroup(group string) map[string]ServiceStatus {
	o.ensureInit()
	o.mu.RLock()
	entries := make([]*serviceEntry, 0)
	for _, e := range o.entries {
		if e.cfg.group == group {
			entries = append(entries, e)
		}
	}
	o.mu.RUnlock()
	result := make(map[string]ServiceStatus, len(entries))
	o.statusMu.RLock()
	defer o.statusMu.RUnlock()
	for _, e := range entries {
		result[e.name] = e.status
	}
	return result
}

// StatusesByLabel returns a map of service name to status for all services
// matching the given label key-value pair. Thread-safe.
func (o *Orchestrator) StatusesByLabel(key, value string) map[string]ServiceStatus {
	o.ensureInit()
	o.mu.RLock()
	entries := make([]*serviceEntry, 0)
	for _, e := range o.entries {
		if e.cfg.labels != nil && e.cfg.labels[key] == value {
			entries = append(entries, e)
		}
	}
	o.mu.RUnlock()
	result := make(map[string]ServiceStatus, len(entries))
	o.statusMu.RLock()
	defer o.statusMu.RUnlock()
	for _, e := range entries {
		result[e.name] = e.status
	}
	return result
}
