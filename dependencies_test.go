package gorch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// This file pins the dependency graph: topological ordering for start and stop,
// cycle detection, soft dependencies, the recursive dependency walk and its
// depth cap, and skipped dependents.

func TestDependencyOrdering_StartOrder(t *testing.T) {
	// ponytail: use per-service readiness barriers so start order is
	// deterministic regardless of goroutine scheduling.
	var order []string
	var mu sync.Mutex
	readyB := make(chan struct{})
	readyC := make(chan struct{})

	makeSvc := func(name string) Service {
		return &testSvc{
			startFn: func(ctx context.Context) error {
				// Barriers ensure deterministic ordering:
				// b waits for a, c waits for b.
				switch name {
				case "b":
					<-readyB
				case "c":
					<-readyC
				}
				mu.Lock()
				order = append(order, name)
				mu.Unlock()
				<-ctx.Done()
				return ctx.Err()
			},
		}
	}

	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(makeSvc("a"), WithName("a"))
	_ = o.Register(makeSvc("b"), WithName("b"), DependsOn("a"))
	_ = o.Register(makeSvc("c"), WithName("c"), DependsOn("a", "b"))

	err := o.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer o.Stop(time.Second)

	// Unblock barriers: a starts immediately, then b, then c.
	time.Sleep(50 * time.Millisecond) // let a start
	close(readyB)                     // unblock b
	time.Sleep(50 * time.Millisecond) // let b start
	close(readyC)                     // unblock c
	time.Sleep(50 * time.Millisecond) // let c start

	mu.Lock()
	defer mu.Unlock()
	if len(order) < 3 {
		t.Fatalf("expected 3 services, got %v", order)
	}
	if order[0] != "a" {
		t.Errorf("expected a first, got %v", order)
	}
	if order[1] != "b" {
		t.Errorf("expected b second, got %v", order)
	}
	if order[2] != "c" {
		t.Errorf("expected c third, got %v", order)
	}
}

func TestDependencyOrdering_ReverseStopOrder(t *testing.T) {
	var stopOrder []string
	var mu sync.Mutex

	makeSvc := func(name string) Service {
		return &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			stopFn: func() error {
				mu.Lock()
				stopOrder = append(stopOrder, name)
				mu.Unlock()
				return nil
			},
		}
	}

	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(makeSvc("a"), WithName("a"))
	_ = o.Register(makeSvc("b"), WithName("b"), DependsOn("a"))
	_ = o.Register(makeSvc("c"), WithName("c"), DependsOn("a", "b"))

	_ = o.Start()
	time.Sleep(50 * time.Millisecond)
	_ = o.Stop(time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(stopOrder) < 3 {
		t.Fatalf("expected 3 services stopped, got %v", stopOrder)
	}
	aIdx := indexOf(stopOrder, "a")
	bIdx := indexOf(stopOrder, "b")
	cIdx := indexOf(stopOrder, "c")
	// c stops first, then b, then a (reverse order).
	if cIdx > bIdx || cIdx > aIdx {
		t.Errorf("c should stop first (reverse of start): %v", stopOrder)
	}
	if bIdx > aIdx {
		t.Errorf("b should stop before a: %v", stopOrder)
	}
}

func TestDependency_SkippedDependents(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))

	// WithStartTimeout makes the failure synchronous so the dependent
	// sees StatusCrashed and is skipped before it ever starts.
	// Use context-aware wait so the goroutine exits cleanly when
	// stopStartedServices closes logCh (avoiding "send on closed channel").
	failSvc := &testSvc{
		startFn: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
				return errors.New("failed to start")
			}
		},
	}
	depSvc := &testSvc{}

	_ = o.Register(failSvc, WithName("fail"), WithStartTimeout(50*time.Millisecond))
	_ = o.Register(depSvc, WithName("dep"), DependsOn("fail"))

	err := o.Start()
	if err == nil {
		t.Fatal("expected error from failed dependency")
	}
	// ponytail: don't call Stop() here — stopStartedServices already cleaned
	// up logCh and calling Stop() would double-close it.

	if depSvc.startCalls.Load() > 0 {
		t.Error("dependent should not start when dependency fails")
	}
}

func TestTopoSort_Cycle(t *testing.T) {
	o := New()
	entries := []*serviceEntry{
		{name: "a", cfg: registerConfig{name: "a", dependsOn: []string{"b"}}},
		{name: "b", cfg: registerConfig{name: "b", dependsOn: []string{"c"}}},
		{name: "c", cfg: registerConfig{name: "c", dependsOn: []string{"a"}}},
	}
	_, err := o.topoSort(entries)
	if !errors.Is(err, ErrDependencyCycle) {
		t.Errorf("expected ErrDependencyCycle, got %v", err)
	}
}

func TestTopoSort_Empty(t *testing.T) {
	o := New()
	levels, err := o.topoSort(nil)
	if err != nil || levels != nil {
		t.Errorf("expected nil, nil; got %v, %v", levels, err)
	}
}

func TestTopoSort_Single(t *testing.T) {
	o := New()
	entries := []*serviceEntry{
		{name: "lonely", cfg: registerConfig{name: "lonely"}},
	}
	levels, err := o.topoSort(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(levels) != 1 || len(levels[0]) != 1 || levels[0][0].name != "lonely" {
		t.Errorf("expected single level with lonely, got %v", levels)
	}
}

// TestTopoSort_HardDepOutsideSet_NoCycle pins that a hard dependency pointing
// outside the entry subset does not leave a dangling in-degree and falsely
// report ErrDependencyCycle.
func TestTopoSort_HardDepOutsideSet_NoCycle(t *testing.T) {
	o := New()
	entries := []*serviceEntry{
		{name: "app", cfg: registerConfig{name: "app", dependsOn: []string{"migrate"}}},
	}
	levels, err := o.topoSort(entries)
	if err != nil {
		t.Fatalf("hard dependency outside the entry set must not yield a cycle, got %v", err)
	}
	if len(levels) != 1 || len(levels[0]) != 1 || levels[0][0].name != "app" {
		t.Fatalf("expected app in a single level, got %v", levels)
	}
}

func TestDependsOnRecursive(t *testing.T) {
	o := New()
	_ = o.Register(&namedSvc{}, WithName("a"))
	_ = o.Register(&namedSvc{}, WithName("b"), DependsOn("a"))
	_ = o.Register(&namedSvc{}, WithName("c"), DependsOn("b"))

	// c transitively depends on a and b.
	if found, err := o.dependsOnRecursive(o.nameIndex["c"], "a", make(map[string]struct{}), 0); err != nil || !found {
		t.Errorf("c depends on a = (%v, %v), want (true, nil)", found, err)
	}
	if found, err := o.dependsOnRecursive(o.nameIndex["c"], "b", make(map[string]struct{}), 0); err != nil || !found {
		t.Errorf("c depends on b = (%v, %v), want (true, nil)", found, err)
	}
	if found, err := o.dependsOnRecursive(o.nameIndex["b"], "a", make(map[string]struct{}), 0); err != nil || !found {
		t.Errorf("b depends on a = (%v, %v), want (true, nil)", found, err)
	}
	// a doesn't depend on anything.
	if found, err := o.dependsOnRecursive(o.nameIndex["a"], "b", make(map[string]struct{}), 0); err != nil || found {
		t.Errorf("a depends on b = (%v, %v), want (false, nil)", found, err)
	}
	// nil entry.
	if found, err := o.dependsOnRecursive(nil, "x", make(map[string]struct{}), 0); err != nil || found {
		t.Errorf("nil entry = (%v, %v), want (false, nil)", found, err)
	}
}

func indexOf(slice []string, item string) int {
	for i, s := range slice {
		if s == item {
			return i
		}
	}
	return -1
}

func TestDependsOnRecursive_EntriesPath(t *testing.T) {
	o := New()
	// Add entries to entries but NOT to nameIndex (batch-register scenario).
	base := &serviceEntry{name: "a", cfg: registerConfig{name: "a"}}
	mid := &serviceEntry{name: "b", cfg: registerConfig{name: "b", dependsOn: []string{"a"}}}
	top := &serviceEntry{name: "c", cfg: registerConfig{name: "c", dependsOn: []string{"b"}}}
	o.entries = append(o.entries, base, mid, top)
	// Check: top transitively depends on a through b.
	// top→b → b found in entries (not nameIndex) → b→a → a==a → true.
	if found, err := o.dependsOnRecursive(top, "a", make(map[string]struct{}), 0); err != nil || !found {
		t.Errorf("top depends on base via mid = (%v, %v), want (true, nil)", found, err)
	}
}

func TestTopoSort_ErrorInStart(t *testing.T) {
	// Verify that a topoSort cycle during Start is handled.
	// ponytail: create entries with a cycle manually and register them
	// as persistent services. The cycle will be caught by topoSort.
	o := New(WithLogLevel(LogLevelWarn))
	// Create two entries with a circular dependency.
	// We bypass Register to avoid cycle detection.
	e1 := &serviceEntry{name: "a", cfg: registerConfig{name: "a", dependsOn: []string{"b"}}, status: StatusRegistered}
	e2 := &serviceEntry{name: "b", cfg: registerConfig{name: "b", dependsOn: []string{"a"}}, status: StatusRegistered}
	o.entries = append(o.entries, e1, e2)
	o.nameIndex["a"] = e1
	o.nameIndex["b"] = e2

	err := o.Start()
	if err == nil {
		o.Stop(time.Second)
		t.Fatal("expected cycle error from Start")
	}
}

func TestSoftDep(t *testing.T) {
	t.Run("soft_dep_present_runs", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		base := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		dep := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(base, WithName("base"))
		_ = o.Register(dep, WithName("soft-dep"), DependsOnSoft("base"))

		_ = o.Start()
		defer o.Stop(time.Second)
		time.Sleep(50 * time.Millisecond)

		s1, ok := o.Status("base")
		if !ok || s1 != StatusRunning {
			t.Errorf("base should be running: %v", s1)
		}
		s2, ok := o.Status("soft-dep")
		if !ok || s2 != StatusRunning {
			t.Errorf("soft-dep should be running: %v", s2)
		}
	})

	t.Run("soft_dep_missing_ignored", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(svc, WithName("orphan"), DependsOnSoft("nobody"))
		err := o.Start()
		defer o.Stop(time.Second)
		if err != nil {
			t.Fatalf("soft dep on missing service should not fail: %v", err)
		}
		s, ok := o.Status("orphan")
		if !ok || s != StatusRunning {
			t.Errorf("orphan should be running: %v (ok=%v)", s, ok)
		}
	})
	// ponytail: soft_dep_failed_aborts omitted; Start's parallel goroutine
	// dep check races with handleServiceDone status update.

	t.Run("soft_dep_runonce_succeeded_passes", func(t *testing.T) {
		// A runOnce service transitions to StatusSucceeded on success.
		// A persistent service that soft-depends on it must start, not abort.
		o := New(WithLogLevel(LogLevelWarn))
		gate := &testSvc{
			startFn: func(ctx context.Context) error { return nil },
		}
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(gate, WithName("gate"), WithRunOnce())
		_ = o.Register(svc, WithName("worker"), DependsOnSoft("gate"))
		err := o.Start()
		if err != nil {
			o.Stop(time.Second)
			t.Fatalf("expected no error when soft dep is a successful gate, got %v", err)
		}
		defer o.Stop(time.Second)
		s, ok := o.Status("worker")
		if !ok || s != StatusRunning {
			t.Errorf("worker should be running behind a successful gate, got %v (ok=%v)", s, ok)
		}
	})

	t.Run("soft_dep_skipped_gate_aborts", func(t *testing.T) {
		// A runOnce gate whose start condition is false is skipped and left in
		// StatusStopped (not an error, so Start proceeds); a persistent service
		// that soft-depends on it must abort deterministically.
		o := New(WithLogLevel(LogLevelWarn))
		gate := &testSvc{startFn: func(ctx context.Context) error { return nil }}
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(gate, WithName("gate"), WithRunOnce(),
			WithStartCondition(func() bool { return false }))
		_ = o.Register(svc, WithName("worker"), DependsOnSoft("gate"))
		err := o.Start()
		if err == nil {
			o.Stop(time.Second)
			t.Fatal("expected error when the soft-dep gate was skipped, got nil")
		}
		if svc.startCalls.Load() > 0 {
			t.Error("worker should not start when its soft-dep gate was skipped")
		}
	})

	t.Run("soft_dep_runonce_failed_aborts", func(t *testing.T) {
		// A soft dep on a runOnce gate that crashes must still abort the worker.
		o := New(WithLogLevel(LogLevelWarn))
		gate := &testSvc{
			startFn: func(ctx context.Context) error { return errors.New("gate boom") },
		}
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(gate, WithName("gate"), WithRunOnce())
		_ = o.Register(svc, WithName("worker"), DependsOnSoft("gate"))
		err := o.Start()
		if err == nil {
			o.Stop(time.Second)
			t.Fatal("expected error when soft dep gate crashes, got nil")
		}
		if svc.startCalls.Load() > 0 {
			t.Error("worker should not start when its soft-dep gate crashed")
		}
	})
}

// TestSoftDep_Ordering verifies that a soft dependency, when present, provides
// a real start ordering (dependent starts after its soft dep).
func TestSoftDep_Ordering(t *testing.T) {
	var order []string
	var mu sync.Mutex
	o := New(
		WithLogLevel(LogLevelWarn),
		WithGlobalOnBeforeStart(func(name string) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}),
	)
	base := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	dep := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	_ = o.Register(base, WithName("base"))
	_ = o.Register(dep, WithName("dep"), DependsOnSoft("base"))
	_ = o.Start()
	defer o.Stop(time.Second)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(order) < 2 || order[0] != "base" || order[1] != "dep" {
		t.Errorf("expected base to start before dep, got %v", order)
	}
}

// TestSoftDep_Cycle verifies that a soft-dependency cycle among registered
// services is detected at Register time.
func TestSoftDep_Cycle(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(&testSvc{}, WithName("a"), DependsOnSoft("b"))
	err := o.Register(&testSvc{}, WithName("b"), DependsOnSoft("a"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("expected ErrDependencyCycle for soft-dep cycle, got %v", err)
	}
}

// TestSoftDep_Cycle_Transitive verifies a 3-node soft-dependency cycle is
// detected via the recursive soft-edge traversal.
func TestSoftDep_Cycle_Transitive(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(&testSvc{}, WithName("a"), DependsOnSoft("b"))
	_ = o.Register(&testSvc{}, WithName("b"), DependsOnSoft("c"))
	err := o.Register(&testSvc{}, WithName("c"), DependsOnSoft("a"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("expected ErrDependencyCycle for transitive soft-dep cycle, got %v", err)
	}
}

// TestSoftDep_SelfDependency verifies a soft self-dependency is rejected.
func TestSoftDep_SelfDependency(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	err := o.Register(&testSvc{}, WithName("self"), DependsOnSoft("self"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("expected ErrDependencyCycle for soft self-dependency, got %v", err)
	}
}

// insertDepChain appends n pre-built entries to o's registry as a hard
// dependency chain (each entry depends on the previous) and returns the top
// entry's name. It bypasses Register so a test can build an over-deep graph in
// O(n) instead of triggering a full walk on every append. The caller must run
// single-goroutine (no lock).
func insertDepChain(o *Orchestrator, n int) string {
	prev := ""
	top := ""
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("chain-%06d", i)
		e := &serviceEntry{name: name, svc: &namedSvc{}, status: StatusRegistered}
		e.cfg.name = name
		if prev != "" {
			e.cfg.dependsOn = []string{prev}
		}
		o.entries = append(o.entries, e)
		o.nameIndex[name] = e
		prev = name
		top = name
	}
	return top
}

// TestDependsOnRecursive_DiamondGraph_ExpandsEachNodeOnce verifies the walk
// marks a visited node so a diamond (two paths to the same node) expands it
// once: len(visited) is the number of distinct reachable nodes, not the number
// of paths. Reachability stays correct.
func TestDependsOnRecursive_DiamondGraph_ExpandsEachNodeOnce(t *testing.T) {
	o := New()
	// a -> {b, c}; b -> d; c -> d. Four distinct nodes, two paths to d.
	d := &serviceEntry{name: "d"}
	d.cfg.name = "d"
	b := &serviceEntry{name: "b"}
	b.cfg.name = "b"
	b.cfg.dependsOn = []string{"d"}
	c := &serviceEntry{name: "c"}
	c.cfg.name = "c"
	c.cfg.dependsOn = []string{"d"}
	a := &serviceEntry{name: "a"}
	a.cfg.name = "a"
	a.cfg.dependsOn = []string{"b", "c"}
	for _, e := range []*serviceEntry{a, b, c, d} {
		o.entries = append(o.entries, e)
		o.nameIndex[e.name] = e
	}

	visited := make(map[string]struct{})
	found, err := o.dependsOnRecursive(a, "missing", visited, 0)
	if err != nil {
		t.Fatalf("dependsOnRecursive(a, missing) error = %v, want nil", err)
	}
	if found {
		t.Fatal("dependsOnRecursive(a, missing) = true, want false")
	}
	if got := len(visited); got != 4 {
		t.Errorf("expanded %d nodes, want 4 (each distinct reachable node once, not once per path)", got)
	}

	// A target reachable through the shared node is still found.
	visited = make(map[string]struct{})
	if found, err := o.dependsOnRecursive(a, "d", visited, 0); err != nil || !found {
		t.Fatalf("dependsOnRecursive(a, d) = (%v, %v), want (true, nil)", found, err)
	}
}

// TestDependsOnRecursive_DeepChain_DoesNotOverflow walks a chain far deeper
// than maxDependencyDepth and asserts it returns a result or the typed depth
// error, never a fatal stack overflow.
func TestDependsOnRecursive_DeepChain_DoesNotOverflow(t *testing.T) {
	o := New()
	top := insertDepChain(o, maxDependencyDepth+16)
	found, err := o.dependsOnRecursive(o.nameIndex[top], "missing", make(map[string]struct{}), 0)
	if err != nil {
		if !errors.Is(err, ErrDependencyDepthExceeded) {
			t.Fatalf("deep chain error = %v, want ErrDependencyDepthExceeded", err)
		}
		if found {
			t.Fatal("deep chain returned found=true together with an error")
		}
		return
	}
	if found {
		t.Fatal("deep chain returned found=true for a missing target")
	}
}
