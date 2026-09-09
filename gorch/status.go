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
}

// logEntry is an internal log record sent from ServiceLogger to the log-pump.
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
func (o *Orchestrator) IsReady(name string) bool {
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
	return rc.Ready(context.Background()) == nil
}

// StartGroup starts all services in the named group in topological order.
func (o *Orchestrator) StartGroup(group string) error {
	o.mu.RLock()
	entries := make([]*serviceEntry, 0)
	for _, e := range o.entries {
		if e.cfg.group == group {
			entries = append(entries, e)
		}
	}
	o.mu.RUnlock()
	levels, err := o.topoSort(entries)
	if err != nil {
		return err
	}
	for _, level := range levels {
		for _, entry := range level {
			if err := o.startOneService(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// StopGroup stops all non-cron, non-runOnce services in the named group in
// reverse topological order. Errors are aggregated via errors.Join.
func (o *Orchestrator) StopGroup(group string, timeout time.Duration) error {
	o.mu.RLock()
	persistent := make([]*serviceEntry, 0)
	for _, e := range o.entries {
		if e.cfg.group == group && e.cfg.cronSpec == "" && !e.cfg.runOnce {
			persistent = append(persistent, e)
		}
	}
	o.mu.RUnlock()
	levels, _ := o.topoSort(persistent)
	var stopErr error
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
		Starts:      o.metricsStarts.Load(),
		Stops:       o.metricsStops.Load(),
		Crashes:     o.metricsCrashes.Load(),
		Restarts:    o.metricsRestarts.Load(),
		HealthFails: o.metricsHealthFails.Load(),
	}
}

// Done returns a channel that closes when all managed goroutines (services,
// log-pump, health-check loop) have exited. The orchestrator must be stopped
// (via Stop or Run returning) before the channel closes. The channel is
// created lazily and cached: repeated calls return the same channel.
func (o *Orchestrator) Done() <-chan struct{} {
	return o.doneCh()
}

// ── Topological sort ──

// topoSort groups entries into levels based on their dependsOn chains.
// Services in the same level are independent and can start in parallel.
// Returns ErrDependencyCycle if a cycle is detected.
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
	// Add hard edges, then soft edges (only when the soft dep is present in
	// this entry set — otherwise it is ignored).
	for _, e := range entries {
		name := e.name
		for _, dep := range e.cfg.dependsOn {
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

// ── Health check loop ──
