package gorch

import (
	"fmt"
	"time"
)

// Busy reports whether the named service is registered and currently holds an
// in-flight membership reservation: a start transaction is running, a teardown
// is running, or a group operation has reserved it. It is the predicate a caller
// can poll to decide whether StartService/StopService/Unregister/ReplaceService
// would be rejected with the transient ErrMembershipBusy, instead of racing and
// retrying blind. It returns false for an unknown name and for a registered but
// idle entry.
//
// Busy is the observable for the reservation, which Status/Statuses do not
// encode: a reserved entry still reports its prior lifecycle status (typically
// StatusRegistered) until startOneService commits StatusStarting. That window is
// the answer to "is anything in flight?" — Statuses reports what the entry is,
// Busy reports that a membership operation has claimed it.
//
// The reservation is the membership transaction, not a service's own callback.
// Once a persistent service's instance is spawned the transaction is complete,
// the entry is StatusRunning and Busy is false for the rest of the instance's
// lifetime — even while its user Start still runs. A cron entry never holds a
// reservation while its schedule is installed, so a tick in flight does not make
// it busy. In both cases a stop is an ordinary stop, not a reservation
// collision; a membership op re-entered from the entry's own Start/Stop is
// instead the permanent ErrReentrantMembership.
// Thread-safe.
func (o *Orchestrator) Busy(name string) bool {
	o.ensureInit()
	o.mu.RLock()
	entry := o.lookupEntry(name)
	o.mu.RUnlock()
	if entry == nil {
		return false
	}
	return entry.starting.Load() || entry.removing.Load()
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
