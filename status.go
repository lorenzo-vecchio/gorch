package gorch

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

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

// Status returns the current lifecycle status of a named service.
// ok is false if no service with that name is registered.
//
// Status is the lifecycle fact, not the reservation. While an entry holds an
// in-flight start reservation but startOneService has not yet committed
// StatusStarting, it still reports its previous status (typically
// StatusRegistered). Use Busy(name) to observe that window; it is not encoded in
// ServiceStatus. A cron entry's StatusRunning means its schedule is installed,
// not that a tick is working (see Statuses).
// Thread-safe.
func (o *Orchestrator) Status(name string) (ServiceStatus, bool) {
	o.ensureInit()
	o.mu.RLock()
	entry, ok := o.nameIndex[name]
	o.mu.RUnlock()
	if !ok {
		return 0, false
	}
	o.statusMu.RLock()
	s := entry.status
	o.statusMu.RUnlock()
	return s, true
}

// Statuses returns a map of service name to lifecycle status for all registered
// services, keyed by name.
//
// Registered, not running: a hot-added entry is included from the moment it is
// registered, even before it is started (StatusRegistered), and a staged cron
// entry is included before it is scheduled (also StatusRegistered). For a cron
// entry StatusRunning means the schedule is installed, not that a tick is
// working or healthy; a failing tick only logs and does not change the status,
// so IsReady and Health inherit that scheduling-fact reading. Use CountRunning
// and RunningNames for the "live" subset.
//
// The reservation is deliberately not reported here: an entry reserved for an
// in-flight start still shows its prior lifecycle status until startOneService
// commits StatusStarting, and a teardown shows StatusStopping. Poll Busy(name)
// for the reservation. Thread-safe.
func (o *Orchestrator) Statuses() map[string]ServiceStatus {
	o.ensureInit()
	o.mu.RLock()
	entries := make([]*serviceEntry, len(o.entries))
	copy(entries, o.entries)
	o.mu.RUnlock()

	result := make(map[string]ServiceStatus, len(entries))
	o.statusMu.RLock()
	defer o.statusMu.RUnlock()
	for _, e := range entries {
		result[e.name] = e.status
	}
	return result
}

// Names returns the names of all registered services in registration order,
// whether or not they are started. A hot-added and not-yet-started service and a
// staged cron entry are both included; use RunningNames for the running subset.
// Thread-safe.
func (o *Orchestrator) Names() []string {
	o.ensureInit()
	o.mu.RLock()
	defer o.mu.RUnlock()
	names := make([]string, len(o.entries))
	for i, e := range o.entries {
		names[i] = e.name
	}
	return names
}

// Count returns the total number of registered services, whether or not they are
// started. It is len(Names()), not the number of live services: a hot-added,
// not-yet-started entry and a staged cron entry both count. Use CountRunning for
// the number of StatusRunning services ("N of M running").
// Thread-safe.
func (o *Orchestrator) Count() int {
	o.ensureInit()
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.entries)
}

// CountRunning returns the number of registered services currently in
// StatusRunning. It is the "live" half of Count: a hot-added or staged entry that
// reports StatusRegistered is excluded, and for a cron entry "running" means its
// schedule is installed, not that a tick is working. The count is a snapshot
// taken under the same locks as Statuses, so it is consistent with the
// StatusRunning entries of that map. Thread-safe.
func (o *Orchestrator) CountRunning() int {
	o.ensureInit()
	o.mu.RLock()
	entries := make([]*serviceEntry, len(o.entries))
	copy(entries, o.entries)
	o.mu.RUnlock()

	o.statusMu.RLock()
	defer o.statusMu.RUnlock()
	n := 0
	for _, e := range entries {
		if e.status == StatusRunning {
			n++
		}
	}
	return n
}

// RunningNames returns the names of the registered services currently in
// StatusRunning, in registration order. It is the name list matching
// CountRunning and the StatusRunning subset of Statuses: registered but
// not-started entries and staged cron entries are omitted. Thread-safe.
func (o *Orchestrator) RunningNames() []string {
	o.ensureInit()
	o.mu.RLock()
	entries := make([]*serviceEntry, len(o.entries))
	copy(entries, o.entries)
	o.mu.RUnlock()

	o.statusMu.RLock()
	defer o.statusMu.RUnlock()
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.status == StatusRunning {
			names = append(names, e.name)
		}
	}
	return names
}

// Dependents returns the transitive hard dependents of name that currently
// block a plain StopService/Unregister — those whose status is StatusRunning or
// StatusStarting — in reverse topological order: a dependent precedes the
// dependency it reaches through DependsOn, and the named service itself is
// omitted. It is exactly the set a plain stop is refused on (ErrHasDependents),
// so an empty result means the stop would proceed, while the *HasDependentsError
// carries the same names when it is refused. It is also the set
// WithCascadeStop would tear down, in that order.
//
// The result is status-dependent and excludes the non-blocking hard dependents
// (StatusRegistered, StatusStopping, StatusCrashed, StatusStopped,
// StatusSucceeded), which the graph still holds but a plain stop does not break.
// Soft dependencies are never included: they neither block nor cascade. Use
// DependenciesOf for the opposite direction. Returns ErrServiceNotFound for an
// unknown name. Thread-safe.
func (o *Orchestrator) Dependents(name string) ([]string, error) {
	o.ensureInit()
	o.mu.RLock()
	defer o.mu.RUnlock()
	entry := o.lookupEntry(name)
	if entry == nil {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, name)
	}
	return o.activeDependentsLocked(entry, o.hardDependentsOrderLocked(entry)), nil
}

// DependenciesOf returns the direct hard dependencies of name, in the order its
// DependsOn options declared them. It is the opposite direction of Dependents:
// it answers "what must be Running before name can start". Soft dependencies
// (DependsOnSoft) are not included. A dependency named here may have been
// unregistered since; it stays listed because it is still part of the entry's
// configuration, and StartService reports it as ErrDependencyNotFound until the
// dependent is re-registered or removed. Returns ErrServiceNotFound for an
// unknown name. Thread-safe.
func (o *Orchestrator) DependenciesOf(name string) ([]string, error) {
	o.ensureInit()
	o.mu.RLock()
	defer o.mu.RUnlock()
	entry := o.lookupEntry(name)
	if entry == nil {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, name)
	}
	deps := make([]string, len(entry.cfg.dependsOn))
	copy(deps, entry.cfg.dependsOn)
	return deps, nil
}

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

// WaitFor blocks until the named service reaches target status or timeout expires.
// Polls at 50ms intervals. Returns an error on timeout or if the service is not
// found. A non-positive timeout does not wait: it returns immediately (only
// succeeding if the status already matches).
func (o *Orchestrator) WaitFor(name string, target ServiceStatus, timeout time.Duration) error {
	o.ensureInit()
	deadline := time.After(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, ok := o.Status(name)
		if !ok {
			return fmt.Errorf("gorch: service %s not found", name)
		}
		if status == target {
			return nil
		}
		select {
		case <-deadline:
			return fmt.Errorf("gorch: WaitFor %s -> %s timed out after %v (current: %s)", name, target, timeout, status)
		case <-ticker.C:
		}
	}
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

// ── Topological sort ──

// topoSort groups entries into levels based on their dependsOn chains.
// Services in the same level are independent and can start in parallel.
// entries may be a subset of the registered graph: dependency edges pointing
// outside the set are ignored (there is no ordering to derive against an entry
// this call will not operate on). Returns ErrDependencyCycle if a cycle is
// detected within the set.
func (o *Orchestrator) topoSort(entries []*serviceEntry) ([][]*serviceEntry, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	// Build in-degree map and adjacency list.
	inDegree := make(map[string]int)
	children := make(map[string][]string)
	byName := make(map[string]*serviceEntry)

	for _, e := range entries {
		name := e.name
		byName[name] = e
		if _, ok := inDegree[name]; !ok {
			inDegree[name] = 0
		}
	}
	// Add hard edges, then soft edges. Only an edge whose target is present in
	// this entry set constrains the ordering: entries is a subset of the graph
	// (e.g. Start skips cron/runOnce gates, StopGroup selects one group), and a
	// dependency outside the subset is never visited, so counting it would leave
	// a dangling in-degree and report a phantom ErrDependencyCycle. Hard-dep
	// existence is already enforced by validateRegisterConfigLocked, so ignoring the
	// out-of-subset edge cannot mask a real config error.
	for _, e := range entries {
		name := e.name
		for _, dep := range e.cfg.dependsOn {
			if _, ok := byName[dep]; !ok {
				continue
			}
			children[dep] = append(children[dep], name)
			inDegree[name]++
		}
		for _, dep := range e.cfg.softDependsOn {
			if _, ok := byName[dep]; ok {
				children[dep] = append(children[dep], name)
				inDegree[name]++
			}
		}
	}

	var levels [][]*serviceEntry
	visited := make(map[string]bool)

	for len(visited) < len(entries) {
		// Collect nodes with zero in-degree (not yet visited).
		var level []string
		for name := range byName {
			if visited[name] {
				continue
			}
			if inDegree[name] == 0 {
				level = append(level, name)
			}
		}

		if len(level) == 0 {
			return nil, ErrDependencyCycle
		}

		// Sort for deterministic output.
		sort.Strings(level)

		var levelEntries []*serviceEntry
		for _, name := range level {
			visited[name] = true
			levelEntries = append(levelEntries, byName[name])
			for _, child := range children[name] {
				inDegree[child]--
			}
		}
		levels = append(levels, levelEntries)
	}

	return levels, nil
}

// topoSortForStop orders entries for teardown. A cyclic subset has no valid
// topological order, so it falls back to registration order (the caller iterates
// levels in reverse) and still returns the error: every entry is reached, and
// the caller surfaces why the order could not be honoured instead of silently
// stopping nothing.
func (o *Orchestrator) topoSortForStop(entries []*serviceEntry) ([][]*serviceEntry, error) {
	levels, err := o.topoSort(entries)
	if err != nil {
		return [][]*serviceEntry{entries}, err
	}
	return levels, nil
}

// ── Health check loop ──
