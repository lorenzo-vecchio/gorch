package gorch

import (
	"fmt"
	"sort"
)

// lookupEntry returns the registered entry with the given name, searching the
// nameIndex first and then the entries slice (for entries registered in the
// same batch). Returns nil if no such entry exists.
func (o *Orchestrator) lookupEntry(name string) *serviceEntry {
	if e, ok := o.nameIndex[name]; ok {
		return e
	}
	for _, e := range o.entries {
		if e.cfg.name == name {
			return e
		}
	}
	return nil
}

// maxDependencyDepth bounds the recursion in dependsOnRecursive. The graph is
// only expected to be this deep under an unbounded registration/reload loop:
// registration enforces an acyclic graph, but membership is dynamic and nothing
// removes entries except Unregister, so a long-running supervisor that reloads
// config grows the graph monotonically. Exceeding the cap returns
// ErrDependencyDepthExceeded instead of overflowing the goroutine stack, which
// is a fatal error no recover can catch.
const maxDependencyDepth = 10000

// dependsOnRecursive checks whether entry transitively depends on target via
// either hard or soft dependency edges. visited makes the walk O(V+E) per call
// by expanding each node at most once, so a diamond is not re-walked once per
// path. depth bounds the recursion: once it exceeds maxDependencyDepth the walk
// aborts with ErrDependencyDepthExceeded rather than overflowing the stack. The
// caller must pass a fresh visited map and depth 0 per top-level check, and must
// hold o.mu (the graph is read via lookupEntry).
func (o *Orchestrator) dependsOnRecursive(entry *serviceEntry, target string, visited map[string]struct{}, depth int) (bool, error) {
	if entry == nil {
		return false, nil
	}
	if depth > maxDependencyDepth {
		return false, fmt.Errorf("%w: exceeds %d edges", ErrDependencyDepthExceeded, maxDependencyDepth)
	}
	if _, seen := visited[entry.name]; seen {
		return false, nil
	}
	visited[entry.name] = struct{}{}
	for _, dep := range entry.cfg.dependsOn {
		if dep == target {
			return true, nil
		}
		found, err := o.dependsOnRecursive(o.lookupEntry(dep), target, visited, depth+1)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	for _, dep := range entry.cfg.softDependsOn {
		if dep == target {
			return true, nil
		}
		found, err := o.dependsOnRecursive(o.lookupEntry(dep), target, visited, depth+1)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}

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
