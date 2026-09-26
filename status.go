package gorch

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

type Metrics struct {
	Starts      int64
	Stops       int64
	Crashes     int64
	Restarts    int64
	HealthFails int64
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
// Thread-safe.
func (o *Orchestrator) Status(name string) (ServiceStatus, bool) {
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

// Statuses returns a map of service name to status for all registered services.
// Thread-safe.
func (o *Orchestrator) Statuses() map[string]ServiceStatus {
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

// Names returns the names of all registered services in registration order.
// Thread-safe.
func (o *Orchestrator) Names() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	names := make([]string, len(o.entries))
	for i, e := range o.entries {
		names[i] = e.name
	}
	return names
}

// Count returns the total number of registered services.
// Thread-safe.
func (o *Orchestrator) Count() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.entries)
}

// Health probes all registered services that implement HealthChecker.
// Returns a map of service name to error (nil = healthy).
// Services that don't implement HealthChecker are reported as nil.
// Thread-safe.
// IsReady reports whether a named service is running and ready to serve.
// The ReadinessChecker probe (if any) runs with the given ctx, so callers can
// bound how long they wait (e.g. IsReady(ctx, name) with a deadline context).
// Thread-safe.
func (o *Orchestrator) IsReady(ctx context.Context, name string) bool {
	o.mu.RLock()
	entry, ok := o.nameIndex[name]
	o.mu.RUnlock()
	if !ok {
		return false
	}
	o.statusMu.RLock()
	s := entry.status
	o.statusMu.RUnlock()
	if s != StatusRunning {
		return false
	}
	rc, ok := entry.getSvc().(ReadinessChecker)
	if !ok {
		return true
	}
	err := callErr(func() error { return rc.Ready(ctx) })
	return err == nil
}

// StartGroup starts all services in the named group in topological order.
// The shared membership lock serializes group selection against
// StopService/Unregister/StopGroup (C3, D16), but is released before any user
// code runs: a service's Start may call a membership op without self-deadlocking.
// Each selected entry is reserved via its starting flag, so a concurrent teardown
// is rejected rather than stopping an entry about to be started.
func (o *Orchestrator) StartGroup(group string) error {
	o.membershipMu.Lock()
	o.mu.Lock()
	if err := o.membershipGateLocked(); err != nil {
		o.mu.Unlock()
		o.membershipMu.Unlock()
		return err
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
			if err := o.startOneService(entry); err != nil {
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
func (o *Orchestrator) StopGroup(group string, timeout time.Duration) error {
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

// StatusesByGroup returns a map of service name to status for all services
// in the named group. Thread-safe.
func (o *Orchestrator) StatusesByGroup(group string) map[string]ServiceStatus {
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
// Polls at 50ms intervals. Returns an error on timeout or if the service is not found.
func (o *Orchestrator) WaitFor(name string, target ServiceStatus, timeout time.Duration) error {
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

// Metrics returns a snapshot of orchestrator-level event counters.
func (o *Orchestrator) Metrics() Metrics {
	return Metrics{
		Starts:              o.metricsStarts.Load(),
		Stops:               o.metricsStops.Load(),
		Crashes:             o.metricsCrashes.Load(),
		Restarts:            o.metricsRestarts.Load(),
		HealthFails:         o.metricsHealthFails.Load(),
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
