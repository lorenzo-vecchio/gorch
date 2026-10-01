package gorch

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestOrphans_LeavesDependentRunning pins the core of orphan mode: an explicit
// Orphans() lets a stop proceed past a running hard dependent, stopping the
// target while that dependent is deliberately left live (not cascaded, its own
// Stop never called). The default refuses such a stop; Orphans is the opt-in
// exception, so the dependent keeps StatusRunning.
func TestOrphans_LeavesDependentRunning(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	base, dependent := orphanPair(t, o)

	if err := o.StopService("base", time.Second, Orphans()); err != nil {
		t.Fatalf("StopService(base, Orphans()) = %v, want nil", err)
	}
	if s, _ := o.Status("base"); s != StatusStopped {
		t.Errorf("base status = %v, want StatusStopped", s)
	}
	if s, _ := o.Status("dependent"); s != StatusRunning {
		t.Errorf("dependent status = %v, want StatusRunning (left orphaned)", s)
	}
	if got := base.stopCalls.Load(); got != 1 {
		t.Errorf("base Stop() calls = %d, want 1", got)
	}
	if got := dependent.stopCalls.Load(); got != 0 {
		t.Errorf("dependent Stop() calls = %d, want 0 (must not be torn down)", got)
	}
}

// TestOrphans_DependentIsNotReady pins the degraded read: an orphaned dependent
// keeps StatusRunning, so a status check cannot see the break, but IsReady walks
// its hard edges and reports false. It holds both when the dependency is merely
// stopped (still registered) and when it was removed entirely.
func TestOrphans_DependentIsNotReady(t *testing.T) {
	t.Run("stopped dependency", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		orphanPair(t, o)

		if err := o.StopService("base", time.Second, Orphans()); err != nil {
			t.Fatalf("StopService(base, Orphans()) = %v, want nil", err)
		}
		// The status still reads Running: the degradation lives in IsReady.
		if s, _ := o.Status("dependent"); s != StatusRunning {
			t.Fatalf("dependent status = %v, want StatusRunning", s)
		}
		if o.IsReady(context.Background(), "dependent") {
			t.Error("IsReady = true while the hard dependency is stopped, want false")
		}
	})

	t.Run("unregistered dependency", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		orphanPair(t, o)

		if err := o.Unregister("base", time.Second, Orphans()); err != nil {
			t.Fatalf("Unregister(base, Orphans()) = %v, want nil", err)
		}
		if o.IsReady(context.Background(), "dependent") {
			t.Error("IsReady = true while the hard dependency is unregistered, want false")
		}
	})
}

// TestOrphans_DependentBecomesReadyWhenDependencyReturns pins reversibility: the
// degradation is not a one-way trip. Restarting the stopped dependency, and
// re-adding a removed one under the same name, both make the untouched orphan
// ready again without any action on the orphan itself.
func TestOrphans_DependentBecomesReadyWhenDependencyReturns(t *testing.T) {
	t.Run("restarted dependency", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		orphanPair(t, o)

		if err := o.StopService("base", time.Second, Orphans()); err != nil {
			t.Fatalf("StopService(base, Orphans()) = %v, want nil", err)
		}
		if o.IsReady(context.Background(), "dependent") {
			t.Fatal("dependent ready while dependency is stopped")
		}

		startAndWait(t, o, "base")
		if !o.IsReady(context.Background(), "dependent") {
			t.Error("dependent must be ready again once its dependency is running")
		}
	})

	t.Run("re-added dependency", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		orphanPair(t, o)

		if err := o.Unregister("base", time.Second, Orphans()); err != nil {
			t.Fatalf("Unregister(base, Orphans()) = %v, want nil", err)
		}
		if o.IsReady(context.Background(), "dependent") {
			t.Fatal("dependent ready while dependency is unregistered")
		}

		if err := o.Register(&testSvc{}, WithName("base")); err != nil {
			t.Fatalf("re-Register(base): %v", err)
		}
		startAndWait(t, o, "base")
		if !o.IsReady(context.Background(), "dependent") {
			t.Error("dependent must be ready again once a same-named dependency is running")
		}
	})
}

// TestOrphans_DependentCannotRestartWhileDependencyGone pins design point 2: the
// orphan is not silently restartable. StartService checks the hard edges before
// its idempotence guard, so re-starting the still-Running orphan is refused with
// the ordinary dependency sentinel until the dependency is back.
func TestOrphans_DependentCannotRestartWhileDependencyGone(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	orphanPair(t, o)

	if err := o.StopService("base", time.Second, Orphans()); err != nil {
		t.Fatalf("StopService(base, Orphans()) = %v, want nil", err)
	}
	if err := o.StartService("dependent"); !errors.Is(err, ErrDependencyNotRunning) {
		t.Fatalf("StartService(dependent) while dependency stopped = %v, want ErrDependencyNotRunning", err)
	}
}

// TestOrphans_Unregister_LeavesDependentRunning pins the remove variant: an
// orphaned Unregister drops the target from the graph while the dependent stays
// registered and running, and its hard edge now names a service that is gone.
func TestOrphans_Unregister_LeavesDependentRunning(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	orphanPair(t, o)

	if err := o.Unregister("base", time.Second, Orphans()); err != nil {
		t.Fatalf("Unregister(base, Orphans()) = %v, want nil", err)
	}
	if _, ok := o.Status("base"); ok {
		t.Error("base is still registered after Unregister")
	}
	if s, _ := o.Status("dependent"); s != StatusRunning {
		t.Errorf("dependent status = %v, want StatusRunning", s)
	}
	// The forward query still names the removed edge, so the break is visible.
	deps, err := o.DependenciesOf("dependent")
	if err != nil {
		t.Fatalf("DependenciesOf(dependent): %v", err)
	}
	if !equalStrings(deps, []string{"base"}) {
		t.Errorf("DependenciesOf(dependent) = %v, want [base]", deps)
	}
}

// TestOrphans_And_CascadeStop_Rejected pins the incoherent pair: a stop cannot
// both leave the dependents alone and tear them down. The combination is a
// permanent option error, rejected before the target is even resolved, so it
// does not depend on what is registered and the argument order does not matter.
func TestOrphans_And_CascadeStop_Rejected(t *testing.T) {
	o := New()
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	if err := o.Register(&namedSvc{}, WithName("svc")); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		call func() error
	}{
		{"StopService cascade+orphans", func() error {
			return o.StopService("svc", time.Second, WithCascadeStop(), Orphans())
		}},
		{"StopService orphans+cascade", func() error {
			return o.StopService("svc", time.Second, Orphans(), WithCascadeStop())
		}},
		{"Unregister cascade+orphans", func() error {
			return o.Unregister("svc", time.Second, WithCascadeStop(), Orphans())
		}},
		{"unknown name", func() error {
			return o.StopService("ghost", time.Second, Orphans(), WithCascadeStop())
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, ErrUnsupportedOption) {
				t.Fatalf("err = %v, want ErrUnsupportedOption", err)
			}
		})
	}
}

// TestDefault_WithoutOrphans_StillRefuses is the regression guard for the
// default: unless Orphans is passed, a stop/removal with a running hard
// dependent keeps failing with the typed ErrHasDependents and leaves the target
// untouched, exactly as before orphan mode existed.
func TestDefault_WithoutOrphans_StillRefuses(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	orphanPair(t, o)

	err := o.StopService("base", time.Second)
	if !errors.Is(err, ErrHasDependents) {
		t.Fatalf("StopService(base) = %v, want ErrHasDependents", err)
	}
	var depErr *HasDependentsError
	if !errors.As(err, &depErr) {
		t.Fatalf("StopService(base) = %v, want *HasDependentsError", err)
	}
	if depErr.Name != "base" || !equalStrings(depErr.Dependents, []string{"dependent"}) {
		t.Errorf("HasDependentsError = %+v, want Name=base Dependents=[dependent]", depErr)
	}
	if s, _ := o.Status("base"); s != StatusRunning {
		t.Errorf("base status = %v, want StatusRunning (refused stop must not touch it)", s)
	}

	// Unregister refuses on the same guard and leaves both entries in place.
	if err := o.Unregister("base", time.Second); !errors.Is(err, ErrHasDependents) {
		t.Fatalf("Unregister(base) = %v, want ErrHasDependents", err)
	}
	if len(o.Names()) != 2 {
		t.Errorf("Names() = %v, want base and dependent still registered", o.Names())
	}
}

// TestOrphans_SoftDependentUnaffected pins design point 5: soft edges neither
// block a stop nor make a dependent degrades. A soft dependent keeps running and
// keeps reading ready while its soft target is gone, because IsReady consults
// only hard DependsOn edges.
func TestOrphans_SoftDependentUnaffected(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	if err := o.Register(&testSvc{}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	soft := &readySvc{readyFn: func(context.Context) error { return nil }}
	if err := o.Register(soft, WithName("soft"), DependsOnSoft("base")); err != nil {
		t.Fatal(err)
	}
	startAndWait(t, o, "base")
	startAndWait(t, o, "soft")

	if err := o.StopService("base", time.Second, Orphans()); err != nil {
		t.Fatalf("StopService(base, Orphans()) = %v, want nil", err)
	}
	if s, _ := o.Status("soft"); s != StatusRunning {
		t.Errorf("soft status = %v, want StatusRunning", s)
	}
	if !o.IsReady(context.Background(), "soft") {
		t.Error("soft dependent must stay ready: soft edges are not readiness inputs")
	}
}

// TestIsReady_RunOnceGateDependencyCountsAsSatisfied pins the satisfied-gate
// half of the readiness edge: a dependency that is a runOnce gate in
// StatusSucceeded satisfies IsReady, exactly as it satisfies StartService. Were
// this not so, a healthy service behind a completed gate would read as degraded
// forever.
func TestIsReady_RunOnceGateDependencyCountsAsSatisfied(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	gate := &testSvc{startFn: func(context.Context) error { return nil }}
	if err := o.Register(gate, WithName("gate"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("gate"); err != nil {
		t.Fatalf("StartService(gate): %v", err)
	}
	waitForStatus(t, o, "gate", StatusSucceeded)

	if err := o.Register(&testSvc{}, WithName("worker"), DependsOn("gate")); err != nil {
		t.Fatal(err)
	}
	startAndWait(t, o, "worker")

	if !o.IsReady(context.Background(), "worker") {
		t.Error("IsReady = false behind a succeeded runOnce gate, want true")
	}
}

// orphanPair registers and starts a "base" and a hard dependent "dependent"
// (with an always-passing readiness probe) on o, returning both so a test can
// inspect their Stop counts. Both are waited into StatusRunning, so a later stop
// is deterministic.
func orphanPair(t *testing.T, o *Orchestrator) (base *testSvc, dependent *readySvc) {
	t.Helper()
	base = &testSvc{}
	dependent = &readySvc{readyFn: func(context.Context) error { return nil }}
	if err := o.Register(base, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(dependent, WithName("dependent"), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	startAndWait(t, o, "base")
	startAndWait(t, o, "dependent")
	return base, dependent
}

// startAndWait starts a persistent service and blocks until it is Running.
func startAndWait(t *testing.T, o *Orchestrator, name string) {
	t.Helper()
	if err := o.StartService(name); err != nil {
		t.Fatalf("StartService(%s): %v", name, err)
	}
	waitForStatus(t, o, name, StatusRunning)
}
