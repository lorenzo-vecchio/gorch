package gorch

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestUnregisterGroup_RemovesAllMembers pins the core obligation: every kind of
// group member — persistent, cron and runOnce — is stopped and dropped from the
// graph, unlike StopGroup which excludes cron/runOnce and keeps every member
// registered. Count, Names and Statuses must all forget the group, and a cron
// member's schedule must be released.
func TestUnregisterGroup_RemovesAllMembers(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	cronSvc := &fuzzCronSvc{}
	gate := &fuzzGateSvc{}
	if err := o.Register(&testSvc{}, WithName("p"), WithGroup("g")); err != nil {
		t.Fatalf("Register(p) = %v", err)
	}
	if err := o.Register(cronSvc, WithName("cron"), WithGroup("g"), WithCron("@every 1h", CronParallel)); err != nil {
		t.Fatalf("Register(cron) = %v", err)
	}
	if err := o.Register(gate, WithName("gate"), WithGroup("g"), WithRunOnce()); err != nil {
		t.Fatalf("Register(gate) = %v", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	// Start schedules the cron member; the runOnce gate needs an explicit start,
	// like every runOnce member.
	if err := o.StartService("gate"); err != nil {
		t.Fatalf("StartService(gate) = %v", err)
	}
	waitForStatus(t, o, "gate", StatusSucceeded)

	cronEntry := fuzzEntry(t, o, "cron")
	if cronEntry.cronID == 0 {
		t.Fatal("cron member was not scheduled before the removal")
	}
	if got := o.Count(); got != 3 {
		t.Fatalf("Count before removal = %d, want 3", got)
	}

	// A non-positive timeout means "wait indefinitely": useful here because all
	// three kinds stop cooperatively, and it exercises the zero-deadline path.
	if err := o.UnregisterGroup("g", 0); err != nil {
		t.Fatalf("UnregisterGroup(g) = %v, want nil", err)
	}

	if got := o.Count(); got != 0 {
		t.Errorf("Count after removal = %d, want 0", got)
	}
	if names := o.Names(); len(names) != 0 {
		t.Errorf("Names after removal = %v, want empty", names)
	}
	if sts := o.Statuses(); len(sts) != 0 {
		t.Errorf("Statuses after removal = %v, want empty", sts)
	}
	for _, name := range []string{"p", "cron", "gate"} {
		if _, ok := o.Status(name); ok {
			t.Errorf("Status(%s) still registered after UnregisterGroup", name)
		}
	}
	if cronEntry.cronID != 0 {
		t.Errorf("cron member cronID = %d after removal, want 0 (schedule released)", cronEntry.cronID)
	}
}

// TestUnregisterGroup_RefusesExternalDependents pins the cross-group guard: a
// plain removal refuses to break a hard dependent outside the group, returning
// the typed ErrHasDependents that names the member depended on and every active
// outside blocker, and leaves the whole graph untouched.
func TestUnregisterGroup_RefusesExternalDependents(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	if err := o.Register(&testSvc{}, WithName("base"), WithGroup("g1")); err != nil {
		t.Fatalf("Register(base) = %v", err)
	}
	if err := o.Register(&testSvc{}, WithName("api"), WithGroup("g2"), DependsOn("base")); err != nil {
		t.Fatalf("Register(api) = %v", err)
	}
	if err := o.Register(&testSvc{}, WithName("web"), WithGroup("g2"), DependsOn("api")); err != nil {
		t.Fatalf("Register(web) = %v", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}

	err := o.UnregisterGroup("g1", time.Second)
	if !errors.Is(err, ErrHasDependents) {
		t.Fatalf("UnregisterGroup(g1) = %v, want ErrHasDependents", err)
	}
	var depErr *HasDependentsError
	if !errors.As(err, &depErr) {
		t.Fatalf("UnregisterGroup(g1) = %v, want *HasDependentsError", err)
	}
	if depErr.Name != "base" {
		t.Errorf("HasDependentsError.Name = %q, want base", depErr.Name)
	}
	// Reverse topological order: the transitive dependent web precedes api.
	if !equalStrings(depErr.Dependents, []string{"web", "api"}) {
		t.Errorf("HasDependentsError.Dependents = %v, want [web api]", depErr.Dependents)
	}
	// The refusal is a pre-condition failure: nothing was reserved or removed.
	if got := o.Count(); got != 3 {
		t.Errorf("Count after refused removal = %d, want 3 (untouched)", got)
	}
	if s, _ := o.Status("api"); s != StatusRunning {
		t.Errorf("api status = %v, want StatusRunning (untouched)", s)
	}
}

// TestUnregisterGroup_CascadeRemove_RemovesExternalDependents documents the
// deliberate scope expansion: WithCascadeStop removes the group members and every
// transitive hard dependent of any member, even entries in another group, in
// reverse topological order. The diamond (x depends on both members) also pins
// that a shared dependent is visited once.
func TestUnregisterGroup_CascadeRemove_RemovesExternalDependents(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	m1 := &testSvc{}
	m2 := &testSvc{}
	x := &testSvc{}
	if err := o.Register(m1, WithName("m1"), WithGroup("g1")); err != nil {
		t.Fatalf("Register(m1) = %v", err)
	}
	if err := o.Register(m2, WithName("m2"), WithGroup("g1"), DependsOn("m1")); err != nil {
		t.Fatalf("Register(m2) = %v", err)
	}
	if err := o.Register(x, WithName("x"), WithGroup("g2"), DependsOn("m1", "m2")); err != nil {
		t.Fatalf("Register(x) = %v", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}

	if err := o.UnregisterGroup("g1", time.Second, WithCascadeStop()); err != nil {
		t.Fatalf("UnregisterGroup(g1, cascade) = %v, want nil", err)
	}
	if got := o.Count(); got != 0 {
		t.Errorf("Count after cascade removal = %d, want 0 (group and outside dependent gone)", got)
	}
	for name, svc := range map[string]*testSvc{"m1": m1, "m2": m2, "x": x} {
		if n := svc.stopCalls.Load(); n != 1 {
			t.Errorf("%s Stop() calls = %d, want 1", name, n)
		}
	}
}

// TestUnregisterGroup_ReverseTopologicalStop pins the teardown order: a member
// is stopped before the member it depends on, so the shared membershipMu is
// released before the user Stop() code runs and the graph is never torn down
// dependency-first.
func TestUnregisterGroup_ReverseTopologicalStop(t *testing.T) {
	var mu sync.Mutex
	var order []string
	o := New(WithHealthChecksDisabled(), WithGlobalOnAfterStop(func(name string, _ error) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}))
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	if err := o.Register(&testSvc{}, WithName("a"), WithGroup("g")); err != nil {
		t.Fatalf("Register(a) = %v", err)
	}
	if err := o.Register(&testSvc{}, WithName("b"), WithGroup("g"), DependsOn("a")); err != nil {
		t.Fatalf("Register(b) = %v", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}

	if err := o.UnregisterGroup("g", time.Second); err != nil {
		t.Fatalf("UnregisterGroup(g) = %v, want nil", err)
	}
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if !equalStrings(got, []string{"b", "a"}) {
		t.Errorf("stop order = %v, want [b a] (dependent before dependency)", got)
	}
}

// TestUnregisterGroup_PartialFailure_StateIsDocumented pins the failure
// contract: a member whose Stop() errors or times out does not abort the
// removal. The error is aggregated, every member is removed anyway, and the
// group is gone — the same aggregate-and-continue shape as Unregister and
// StopGroup, chosen because a removed entry cannot be rolled back.
func TestUnregisterGroup_PartialFailure_StateIsDocumented(t *testing.T) {
	t.Run("stop error", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		if err := o.Register(&testSvc{}, WithName("good"), WithGroup("g")); err != nil {
			t.Fatalf("Register(good) = %v", err)
		}
		bad := &testSvc{stopFn: func() error { return errors.New("boom") }}
		if err := o.Register(bad, WithName("bad"), WithGroup("g")); err != nil {
			t.Fatalf("Register(bad) = %v", err)
		}
		if err := o.Start(); err != nil {
			t.Fatalf("Start = %v", err)
		}

		err := o.UnregisterGroup("g", time.Second)
		if err == nil {
			t.Fatal("UnregisterGroup = nil, want the aggregated Stop() error")
		}
		if !strings.Contains(err.Error(), "bad") || !strings.Contains(err.Error(), "boom") {
			t.Errorf("aggregated error = %v, want it to name bad and boom", err)
		}
		if got := o.Count(); got != 0 {
			t.Errorf("Count after partial failure = %d, want 0 (removal proceeds)", got)
		}
		if got := bad.stopCalls.Load(); got != 1 {
			t.Errorf("bad Stop() calls = %d, want 1", got)
		}
	})

	t.Run("stop timeout", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		slow := &testSvc{stopFn: func() error { <-release; return nil }}
		if err := o.Register(slow, WithName("slow"), WithGroup("g")); err != nil {
			t.Fatalf("Register(slow) = %v", err)
		}
		if err := o.Start(); err != nil {
			t.Fatalf("Start = %v", err)
		}

		err := o.UnregisterGroup("g", 30*time.Millisecond)
		if !errors.Is(err, ErrStopTimeout) {
			t.Fatalf("UnregisterGroup = %v, want ErrStopTimeout", err)
		}
		if got := o.Count(); got != 0 {
			t.Errorf("Count after timed-out stop = %d, want 0 (removal proceeds)", got)
		}
	})
}

// TestUnregisterGroup_SkipsStoppingMember pins that a member already left
// StatusStopping by an earlier timed-out teardown is not stopped a second time:
// its Stop() already ran once and must not run again, while it is still removed
// from the graph with the rest of the group.
func TestUnregisterGroup_SkipsStoppingMember(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	t.Cleanup(func() {
		close(release)
		_ = o.Stop(2 * time.Second)
	})
	dep := &testSvc{startFn: func(context.Context) error { <-release; return nil }}
	if err := o.Register(dep, WithName("dep"), WithGroup("g")); err != nil {
		t.Fatalf("Register(dep) = %v", err)
	}
	if err := o.Register(&testSvc{}, WithName("other"), WithGroup("g")); err != nil {
		t.Fatalf("Register(other) = %v", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}

	// The instance ignores cancellation, so the stop times out and dep is left
	// StatusStopping with its Stop() already run exactly once.
	if err := o.StopService("dep", 30*time.Millisecond); !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("first stop = %v, want ErrStopTimeout", err)
	}
	if s, _ := o.Status("dep"); s != StatusStopping {
		t.Fatalf("dep status = %v, want StatusStopping", s)
	}
	if n := dep.stopCalls.Load(); n != 1 {
		t.Fatalf("dep Stop() calls before group removal = %d, want 1", n)
	}

	if err := o.UnregisterGroup("g", time.Second); err != nil {
		t.Fatalf("UnregisterGroup(g) = %v, want nil", err)
	}
	if n := dep.stopCalls.Load(); n != 1 {
		t.Errorf("UnregisterGroup re-ran Stop() on a Stopping member: %d calls, want 1", n)
	}
	if got := o.Count(); got != 0 {
		t.Errorf("Count = %d, want 0 (Stopping member still removed)", got)
	}
}

// TestUnregisterGroup_Orphans_LeavesExternalDependentRunning pins the opposite
// opt-in of WithCascadeStop: Orphans removes only the group members and leaves
// the outside hard dependent Running but degraded, its Stop never called and
// IsReady false until the member is back.
func TestUnregisterGroup_Orphans_LeavesExternalDependentRunning(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	base := &testSvc{}
	api := &readySvc{readyFn: func(context.Context) error { return nil }}
	if err := o.Register(base, WithName("base"), WithGroup("g1")); err != nil {
		t.Fatalf("Register(base) = %v", err)
	}
	if err := o.Register(api, WithName("api"), WithGroup("g2"), DependsOn("base")); err != nil {
		t.Fatalf("Register(api) = %v", err)
	}
	startAndWait(t, o, "base")
	startAndWait(t, o, "api")

	if err := o.UnregisterGroup("g1", time.Second, Orphans()); err != nil {
		t.Fatalf("UnregisterGroup(g1, Orphans()) = %v, want nil", err)
	}
	if _, ok := o.Status("base"); ok {
		t.Error("base still registered after an orphan removal")
	}
	if s, _ := o.Status("api"); s != StatusRunning {
		t.Errorf("api status = %v, want StatusRunning (left orphaned)", s)
	}
	if n := api.stopCalls.Load(); n != 0 {
		t.Errorf("api Stop() calls = %d, want 0 (must not be torn down)", n)
	}
	if o.IsReady(context.Background(), "api") {
		t.Error("IsReady(api) = true while its hard dependency is gone, want false")
	}
}

// TestUnregisterGroup_RejectsCascadeWithOrphans pins the incoherent option pair:
// a removal cannot both tear the outside dependents down and leave them alone.
// The rejection is a permanent option error, independent of argument order and
// of whether the group exists.
func TestUnregisterGroup_RejectsCascadeWithOrphans(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	if err := o.Register(&testSvc{}, WithName("svc"), WithGroup("g")); err != nil {
		t.Fatalf("Register = %v", err)
	}

	cases := []struct {
		name string
		call func() error
	}{
		{"cascade+orphans", func() error {
			return o.UnregisterGroup("g", time.Second, WithCascadeStop(), Orphans())
		}},
		{"orphans+cascade", func() error {
			return o.UnregisterGroup("g", time.Second, Orphans(), WithCascadeStop())
		}},
		{"unknown group", func() error {
			return o.UnregisterGroup("ghost", time.Second, WithCascadeStop(), Orphans())
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, ErrUnsupportedOption) {
				t.Fatalf("err = %v, want ErrUnsupportedOption", err)
			}
		})
	}
	if got := o.Count(); got != 1 {
		t.Errorf("Count after rejected removals = %d, want 1 (group untouched)", got)
	}
}

// TestUnregisterGroup_BusyMember pins the destructive-selection rule: unlike
// StopGroup, which skips an entry reserved by another operation, UnregisterGroup
// rejects the whole removal with the transient ErrMembershipBusy so it never
// claims a group gone while a member is in flight. Both a start reservation and
// an in-progress removal are covered.
func TestUnregisterGroup_BusyMember(t *testing.T) {
	t.Run("start reservation", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		o := New(WithHealthChecksDisabled())
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		if err := o.Start(); err != nil {
			t.Fatalf("Start = %v", err)
		}
		// Hot-add the member so Start does not run its blocking before-start
		// hook: only the StartGroup below may enter it.
		if err := o.Register(&testSvc{}, WithName("m"), WithGroup("g"),
			WithOnBeforeStart(func(string) error {
				close(entered)
				<-release
				return nil
			}),
		); err != nil {
			t.Fatalf("Register = %v", err)
		}

		startDone := make(chan error, 1)
		go func() { startDone <- o.StartGroup("g") }()
		<-entered

		if err := o.UnregisterGroup("g", time.Second); !errors.Is(err, ErrMembershipBusy) {
			t.Fatalf("UnregisterGroup beside a start reservation = %v, want ErrMembershipBusy", err)
		}
		if _, ok := o.Status("m"); !ok {
			t.Error("m was removed while its start was reserved")
		}

		close(release)
		if err := <-startDone; err != nil {
			t.Fatalf("StartGroup = %v, want nil", err)
		}
	})

	t.Run("removal reservation", func(t *testing.T) {
		stopEntered := make(chan struct{})
		stopRelease := make(chan struct{})
		o := New(WithHealthChecksDisabled())
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		svc := &testSvc{stopFn: func() error {
			close(stopEntered)
			<-stopRelease
			return nil
		}}
		if err := o.Register(svc, WithName("m"), WithGroup("g")); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := o.Start(); err != nil {
			t.Fatalf("Start = %v", err)
		}

		first := make(chan error, 1)
		go func() { first <- o.UnregisterGroup("g", time.Second) }()
		<-stopEntered

		if err := o.UnregisterGroup("g", time.Second); !errors.Is(err, ErrMembershipBusy) {
			t.Fatalf("UnregisterGroup beside a removal reservation = %v, want ErrMembershipBusy", err)
		}

		close(stopRelease)
		if err := <-first; err != nil {
			t.Fatalf("first UnregisterGroup = %v, want nil", err)
		}
	})
}

// TestUnregisterGroup_ReentrantFromMemberStop pins the reentrancy classification:
// a membership op re-entered from a selected member's own Stop on the same
// goroutine is the programming error ErrReentrantMembership, not the retryable
// ErrMembershipBusy. The inner call must not deadlock or mutate the graph.
func TestUnregisterGroup_ReentrantFromMemberStop(t *testing.T) {
	inner := make(chan error, 1)
	var o *Orchestrator
	svc := &testSvc{stopFn: func() error {
		inner <- o.UnregisterGroup("g", time.Second)
		return nil
	}}
	o = New(WithHealthChecksDisabled())
	o.Register(svc, WithName("m"), WithGroup("g"))
	if err := o.Register(&testSvc{}, WithName("x"), WithGroup("g")); err != nil {
		t.Fatalf("Register(x) = %v", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	if err := o.UnregisterGroup("g", time.Second); err != nil {
		t.Fatalf("outer UnregisterGroup = %v, want nil", err)
	}
	select {
	case err := <-inner:
		if !errors.Is(err, ErrReentrantMembership) {
			t.Fatalf("re-entrant UnregisterGroup = %v, want ErrReentrantMembership", err)
		}
	default:
		t.Fatal("the member's Stop did not run the re-entrant call")
	}
	if got := o.Count(); got != 0 {
		t.Errorf("Count = %d, want 0 (the outer removal completed)", got)
	}
}

// TestGroupOps_AgreeOnUnknownGroup pins the fourth design decision: StartGroup,
// StopGroup and UnregisterGroup all treat an unknown or empty group as a nil
// no-op, and StatusesByGroup returns an empty result. A group is a filter tag,
// not a registered entity, so there is nothing to report and nothing to reject.
func TestGroupOps_AgreeOnUnknownGroup(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	for _, group := range []string{"ghost", ""} {
		if err := o.StartGroup(group); err != nil {
			t.Errorf("StartGroup(%q) = %v, want nil", group, err)
		}
		if err := o.StopGroup(group, time.Second); err != nil {
			t.Errorf("StopGroup(%q) = %v, want nil", group, err)
		}
		if err := o.UnregisterGroup(group, time.Second); err != nil {
			t.Errorf("UnregisterGroup(%q) = %v, want nil", group, err)
		}
		if got := o.StatusesByGroup(group); len(got) != 0 {
			t.Errorf("StatusesByGroup(%q) = %v, want empty", group, got)
		}
	}
}

// TestUnregisterGroup_BeforeStart pins that removal works before Start like
// Unregister: there is no scheduler or service context, but the registered
// members leave the graph just the same.
func TestUnregisterGroup_BeforeStart(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(&testSvc{}, WithName("m"), WithGroup("g")); err != nil {
		t.Fatalf("Register = %v", err)
	}
	if err := o.UnregisterGroup("g", time.Second); err != nil {
		t.Fatalf("UnregisterGroup before Start = %v, want nil", err)
	}
	if got := o.Count(); got != 0 {
		t.Errorf("Count after removal = %d, want 0", got)
	}
}

// TestUnregisterGroup_ShutdownGates pins the lifecycle classification: like the
// other membership ops, UnregisterGroup reports ErrOrchestratorStopping while a
// whole-orchestrator Stop runs and ErrOrchestratorStopped once it has completed,
// before it even looks at the group.
func TestUnregisterGroup_ShutdownGates(t *testing.T) {
	t.Run("during stop", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		svc := &testSvc{stopFn: func() error {
			close(entered)
			<-release
			return nil
		}}
		o := New()
		if err := o.Register(svc, WithName("blocker")); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := o.Start(); err != nil {
			t.Fatalf("Start = %v", err)
		}

		stopDone := make(chan error, 1)
		go func() { stopDone <- o.Stop(5 * time.Second) }()
		<-entered

		if err := o.UnregisterGroup("g", time.Second); !errors.Is(err, ErrOrchestratorStopping) {
			t.Errorf("UnregisterGroup during Stop = %v, want ErrOrchestratorStopping", err)
		}
		close(release)
		if err := <-stopDone; err != nil {
			t.Fatalf("Stop = %v", err)
		}
	})

	t.Run("after stop", func(t *testing.T) {
		o := New()
		if err := o.Start(); err != nil {
			t.Fatalf("Start = %v", err)
		}
		if err := o.Stop(time.Second); err != nil {
			t.Fatalf("Stop = %v", err)
		}
		if err := o.UnregisterGroup("g", time.Second); !errors.Is(err, ErrOrchestratorStopped) {
			t.Errorf("UnregisterGroup after Stop = %v, want ErrOrchestratorStopped", err)
		}
	})
}
