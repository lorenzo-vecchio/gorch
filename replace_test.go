package gorch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// ── ReplaceService ──

// replaceValidatorSvc counts Validate invocations and can be made to fail, so a
// test can pin both that replace validates the new implementation and that a
// rejected replacement leaves the running one untouched.
type replaceValidatorSvc struct {
	testSvc
	validateCalls atomic.Int32
	validateErr   error
}

func (s *replaceValidatorSvc) Validate() error {
	s.validateCalls.Add(1)
	return s.validateErr
}

// replaceSelfSvc attempts a ReplaceService on itself from inside its own Start:
// the same-goroutine re-entry must be classified as a programming error rather
// than deadlocking or being reported busy.
type replaceSelfSvc struct {
	o     *Orchestrator
	errCh chan error
}

func (s *replaceSelfSvc) Start(ctx ServiceContext) error {
	s.errCh <- s.o.ReplaceService("self", &testSvc{}, time.Second)
	<-ctx.Done()
	return ctx.Err()
}

func (s *replaceSelfSvc) Stop() error { return nil }

// waitForStartCalls blocks until svc's Start has been invoked at least want
// times, since a persistent instance starts on its own goroutine.
func waitForStartCalls(t *testing.T, svc *testSvc, want int32) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if svc.startCalls.Load() >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("service Start invoked %d times, want at least %d", svc.startCalls.Load(), want)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestReplaceService_SwapsImplementation pins the happy path: the old
// instance's Stop() runs, the new implementation is started, the name stays
// registered, and the entry ends Running on the replacement. A zero timeout
// exercises the unbounded-teardown branch.
func TestReplaceService_SwapsImplementation(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	old := &testSvc{}
	if err := o.Register(old, WithName("s")); err != nil {
		t.Fatal(err)
	}
	startAndWait(t, o, "s")
	entry := entryNamed(t, o, "s")

	replacement := &testSvc{}
	if err := o.ReplaceService("s", replacement, 0); err != nil {
		t.Fatalf("ReplaceService = %v, want nil", err)
	}
	// The replacement's Start runs in its own goroutine, so wait for it to be
	// invoked before asserting on it.
	waitForStartCalls(t, replacement, 1)
	if got := old.stopCalls.Load(); got != 1 {
		t.Errorf("old Stop() calls = %d, want 1", got)
	}
	if got := old.startCalls.Load(); got != 1 {
		t.Errorf("old Start() calls = %d, want 1 (started once, never re-run)", got)
	}
	if got := replacement.startCalls.Load(); got != 1 {
		t.Errorf("replacement Start() calls = %d, want 1", got)
	}
	if s, ok := o.Status("s"); !ok || s != StatusRunning {
		t.Errorf("Status(s) = (%v, %v), want (StatusRunning, true)", s, ok)
	}
	if entry.getSvc() != Service(replacement) {
		t.Error("entry still holds the old implementation")
	}
	if names := o.Names(); !equalStrings(names, []string{"s"}) {
		t.Errorf("Names() = %v, want [s]", names)
	}
}

// TestReplaceService_SameNameInGraphThroughout pins the atomicity guarantee: the
// name is never removed from the graph, so a concurrent Status/Dependents never
// observes an absent service even while the old instance is mid-teardown.
func TestReplaceService_SameNameInGraphThroughout(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	stopEntered := make(chan struct{})
	stopRelease := make(chan struct{})
	old := &testSvc{stopFn: func() error { close(stopEntered); <-stopRelease; return nil }}
	if err := o.Register(old, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("dep"), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	startAndWait(t, o, "base")
	startAndWait(t, o, "dep")

	done := make(chan error, 1)
	go func() { done <- o.ReplaceService("base", &testSvc{}, time.Second) }()
	<-stopEntered

	// The teardown is deliberately held open: throughout it the graph entry must
	// stay resolvable, never returning a not-found.
	for i := 0; i < 1000; i++ {
		if _, ok := o.Status("base"); !ok {
			t.Fatalf("Status(base) reported absent mid-replace (iteration %d)", i)
		}
		if _, err := o.Dependents("base"); err != nil {
			t.Fatalf("Dependents(base) = %v mid-replace", err)
		}
	}
	if s, _ := o.Status("base"); s == StatusRunning || s == StatusRegistered {
		t.Errorf("base status mid-replace = %v, want a non-live teardown status", s)
	}

	close(stopRelease)
	if err := <-done; err != nil {
		t.Fatalf("ReplaceService = %v", err)
	}
	waitForStatus(t, o, "base", StatusRunning)
}

// TestReplaceService_DependentsNotBounced pins the reason replace exists: a
// running hard dependent is never torn down or restarted. It stays Running
// across the swap, its Stop() is not called, and it is ready again once the new
// instance is up.
func TestReplaceService_DependentsNotBounced(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	old, dependent := orphanPair(t, o)

	if err := o.ReplaceService("base", &testSvc{}, time.Second); err != nil {
		t.Fatalf("ReplaceService = %v", err)
	}
	if s, _ := o.Status("dependent"); s != StatusRunning {
		t.Errorf("dependent status = %v, want StatusRunning (not bounced)", s)
	}
	if got := dependent.stopCalls.Load(); got != 0 {
		t.Errorf("dependent Stop() calls = %d, want 0", got)
	}
	if got := old.stopCalls.Load(); got != 1 {
		t.Errorf("base old Stop() calls = %d, want 1", got)
	}
	if !o.IsReady(context.Background(), "dependent") {
		t.Error("dependent must be ready again once the new base is running")
	}
}

// TestReplaceService_OldInstanceFullyTornDown pins that the old instance is
// verifiably gone before the replacement runs: its goroutine has exited, its
// Stop() ran once, and its Messenger owner is released from both the entry and
// the root map (no leaked id).
func TestReplaceService_OldInstanceFullyTornDown(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	oldExited := make(chan struct{})
	old := &testSvc{startFn: func(ctx context.Context) error {
		<-ctx.Done()
		close(oldExited)
		return ctx.Err()
	}}
	if err := o.Register(old, WithName("s")); err != nil {
		t.Fatal(err)
	}
	startAndWait(t, o, "s")
	entry := entryNamed(t, o, "s")
	if got := rootOwnerCount(o); got != 1 {
		t.Fatalf("baseline root owners = %d, want 1", got)
	}

	if err := o.ReplaceService("s", &testSvc{}, time.Second); err != nil {
		t.Fatalf("ReplaceService = %v", err)
	}
	select {
	case <-oldExited:
	default:
		t.Error("old instance goroutine still running after ReplaceService returned")
	}
	if got := old.stopCalls.Load(); got != 1 {
		t.Errorf("old Stop() calls = %d, want 1", got)
	}
	if got := rootOwnerCount(o); got != 1 {
		t.Errorf("root owners = %d after replace, want 1 (old owner must be released)", got)
	}
	if ids := entryOwnerIDs(entry); len(ids) != 1 {
		t.Errorf("entry owners = %v, want exactly the new instance's id", ids)
	}
}

// TestReplaceService_StateReset pins that the replacement starts with no history:
// the retry and health-failure counters the old instance had accumulated are
// cleared.
func TestReplaceService_StateReset(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	if err := o.Register(&testSvc{}, WithName("s")); err != nil {
		t.Fatal(err)
	}
	startAndWait(t, o, "s")
	entry := entryNamed(t, o, "s")
	entry.setRetryCount(4)
	entry.setHealthFailures(3)

	if err := o.ReplaceService("s", &testSvc{}, time.Second); err != nil {
		t.Fatalf("ReplaceService = %v", err)
	}
	if got := entry.getRetryCount(); got != 0 {
		t.Errorf("retryCount = %d after replace, want 0", got)
	}
	if got := entry.getHealthFailures(); got != 0 {
		t.Errorf("healthFailures = %d after replace, want 0", got)
	}
}

// TestReplaceService_ValidatorRuns pins the validation contract: the new
// implementation's Validate() runs exactly once, and a validation error aborts
// the replacement before the running instance is touched.
func TestReplaceService_ValidatorRuns(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		if err := o.Register(&testSvc{}, WithName("s")); err != nil {
			t.Fatal(err)
		}
		startAndWait(t, o, "s")

		replacement := &replaceValidatorSvc{}
		if err := o.ReplaceService("s", replacement, time.Second); err != nil {
			t.Fatalf("ReplaceService = %v, want nil", err)
		}
		if got := replacement.validateCalls.Load(); got != 1 {
			t.Errorf("Validate calls = %d, want 1", got)
		}
		if s, _ := o.Status("s"); s != StatusRunning {
			t.Errorf("status = %v, want StatusRunning", s)
		}
	})

	t.Run("invalid aborts before teardown", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		old := &testSvc{}
		if err := o.Register(old, WithName("s")); err != nil {
			t.Fatal(err)
		}
		startAndWait(t, o, "s")

		validateErr := errors.New("bad config")
		replacement := &replaceValidatorSvc{validateErr: validateErr}
		err := o.ReplaceService("s", replacement, time.Second)
		if !errors.Is(err, validateErr) {
			t.Fatalf("ReplaceService = %v, want wrapping %v", err, validateErr)
		}
		if got := replacement.validateCalls.Load(); got != 1 {
			t.Errorf("Validate calls = %d, want 1", got)
		}
		if got := old.stopCalls.Load(); got != 0 {
			t.Errorf("old Stop() calls = %d, want 0 (validate must precede teardown)", got)
		}
		if s, _ := o.Status("s"); s != StatusRunning {
			t.Errorf("status = %v, want StatusRunning (untouched on rejected replace)", s)
		}
	})
}

// TestReplaceService_IsReadyFalseDuringSwap_ThenTrue pins the honest readiness
// reading: while the target is mid-swap a dependent is not ready, and it becomes
// ready again without any action on the dependent once the new instance is up.
func TestReplaceService_IsReadyFalseDuringSwap_ThenTrue(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	stopEntered := make(chan struct{})
	stopRelease := make(chan struct{})
	base := &testSvc{stopFn: func() error { close(stopEntered); <-stopRelease; return nil }}
	dependent := &readySvc{readyFn: func(context.Context) error { return nil }}
	if err := o.Register(base, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(dependent, WithName("dependent"), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	startAndWait(t, o, "base")
	startAndWait(t, o, "dependent")
	if !o.IsReady(context.Background(), "dependent") {
		t.Fatal("dependent not ready before the swap")
	}

	done := make(chan error, 1)
	go func() { done <- o.ReplaceService("base", &testSvc{}, time.Second) }()
	<-stopEntered

	if o.IsReady(context.Background(), "dependent") {
		t.Error("IsReady(dependent) = true while base is mid-swap, want honest false")
	}
	if _, ok := o.Status("base"); !ok {
		t.Error("base must stay registered mid-swap")
	}

	close(stopRelease)
	if err := <-done; err != nil {
		t.Fatalf("ReplaceService = %v", err)
	}
	if !o.IsReady(context.Background(), "dependent") {
		t.Error("IsReady(dependent) = false after the new base is running, want true")
	}
}

// TestReplaceService_ErrorSurface pins every rejection an invalid call returns.
// Each is a documented sentinel, never a panic or a silent no-op.
func TestReplaceService_ErrorSurface(t *testing.T) {
	t.Run("nil service", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		if err := o.ReplaceService("s", nil, time.Second); !errors.Is(err, ErrNilService) {
			t.Fatalf("err = %v, want ErrNilService", err)
		}
	})

	t.Run("not started", func(t *testing.T) {
		o := New()
		if err := o.Register(&testSvc{}, WithName("s")); err != nil {
			t.Fatal(err)
		}
		if err := o.ReplaceService("s", &testSvc{}, time.Second); !errors.Is(err, ErrOrchestratorNotStarted) {
			t.Fatalf("err = %v, want ErrOrchestratorNotStarted", err)
		}
	})

	t.Run("unknown name", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		if err := o.ReplaceService("ghost", &testSvc{}, time.Second); !errors.Is(err, ErrServiceNotFound) {
			t.Fatalf("err = %v, want ErrServiceNotFound", err)
		}
	})

	t.Run("after stop", func(t *testing.T) {
		o := startedOrchestrator(t)
		if err := o.Stop(time.Second); err != nil {
			t.Fatal(err)
		}
		if err := o.ReplaceService("s", &testSvc{}, time.Second); !errors.Is(err, ErrOrchestratorStopped) {
			t.Fatalf("err = %v, want ErrOrchestratorStopped", err)
		}
	})

	t.Run("cascade rejected", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		if err := o.Register(&testSvc{}, WithName("s")); err != nil {
			t.Fatal(err)
		}
		startAndWait(t, o, "s")
		if err := o.ReplaceService("s", &testSvc{}, time.Second, WithCascadeStop()); !errors.Is(err, ErrUnsupportedOption) {
			t.Fatalf("err = %v, want ErrUnsupportedOption", err)
		}
	})

	t.Run("orphans accepted", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		if err := o.Register(&testSvc{}, WithName("s")); err != nil {
			t.Fatal(err)
		}
		startAndWait(t, o, "s")
		if err := o.ReplaceService("s", &testSvc{}, time.Second, Orphans()); err != nil {
			t.Fatalf("ReplaceService with Orphans = %v, want nil", err)
		}
	})
}

// TestReplaceService_ReservationCollisions pins the reservation discipline:
// colliding with another goroutine's in-flight teardown or start is the
// transient ErrMembershipBusy, while a same-goroutine re-entry from the
// service's own Start is the permanent ErrReentrantMembership.
func TestReplaceService_ReservationCollisions(t *testing.T) {
	t.Run("teardown in flight", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		stopEntered := make(chan struct{})
		stopRelease := make(chan struct{})
		svc := &testSvc{stopFn: func() error { close(stopEntered); <-stopRelease; return nil }}
		if err := o.Register(svc, WithName("s")); err != nil {
			t.Fatal(err)
		}
		startAndWait(t, o, "s")

		stopDone := make(chan error, 1)
		go func() { stopDone <- o.StopService("s", time.Second) }()
		<-stopEntered

		if err := o.ReplaceService("s", &testSvc{}, time.Second); !errors.Is(err, ErrMembershipBusy) {
			t.Fatalf("err = %v, want ErrMembershipBusy", err)
		}
		close(stopRelease)
		if err := <-stopDone; err != nil {
			t.Fatalf("StopService = %v", err)
		}
	})

	t.Run("start in flight", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		startEntered := make(chan struct{})
		startRelease := make(chan struct{})
		svc := &testSvc{startFn: func(ctx context.Context) error {
			close(startEntered)
			<-startRelease
			return ctx.Err()
		}}
		// The start timeout keeps startOneService blocked (and the entry's
		// reservation held) while its Start runs, so the collision is observable
		// deterministically.
		if err := o.Register(svc, WithName("s"), WithStartTimeout(3*time.Second)); err != nil {
			t.Fatal(err)
		}
		startDone := make(chan error, 1)
		go func() { startDone <- o.StartService("s") }()
		<-startEntered

		if err := o.ReplaceService("s", &testSvc{}, time.Second); !errors.Is(err, ErrMembershipBusy) {
			t.Fatalf("err = %v, want ErrMembershipBusy", err)
		}
		close(startRelease)
		if err := <-startDone; err != nil {
			t.Fatalf("StartService = %v", err)
		}
	})

	t.Run("reentrant from own Start", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		errCh := make(chan error, 1)
		if err := o.Register(&replaceSelfSvc{o: o, errCh: errCh}, WithName("self")); err != nil {
			t.Fatal(err)
		}
		if err := o.StartService("self"); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-errCh:
			if !errors.Is(err, ErrReentrantMembership) {
				t.Fatalf("reentrant ReplaceService = %v, want ErrReentrantMembership", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("service Start never attempted the reentrant ReplaceService")
		}
	})
}

// TestReplaceService_AbortsOnIncompleteTeardown pins the safety rule: a
// replacement is committed only after the old instance is verifiably gone. Both
// a deadline and a Stop() error abort the swap, so the entry keeps the old
// implementation and the new instance is never started.
func TestReplaceService_AbortsOnIncompleteTeardown(t *testing.T) {
	t.Run("stop timeout", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		old := &testSvc{stopFn: func() error { <-release; return nil }}
		if err := o.Register(old, WithName("s")); err != nil {
			t.Fatal(err)
		}
		startAndWait(t, o, "s")
		entry := entryNamed(t, o, "s")

		replacement := &testSvc{}
		err := o.ReplaceService("s", replacement, 50*time.Millisecond)
		if !errors.Is(err, ErrStopTimeout) {
			t.Fatalf("err = %v, want ErrStopTimeout", err)
		}
		if got := replacement.startCalls.Load(); got != 0 {
			t.Errorf("replacement Start() calls = %d, want 0 (swap aborted)", got)
		}
		if entry.getSvc() != Service(old) {
			t.Error("entry implementation changed despite an aborted teardown")
		}
	})

	t.Run("stop error", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })
		stopErr := errors.New("stop failed")
		old := &testSvc{stopFn: func() error { return stopErr }}
		if err := o.Register(old, WithName("s")); err != nil {
			t.Fatal(err)
		}
		startAndWait(t, o, "s")
		entry := entryNamed(t, o, "s")

		replacement := &testSvc{}
		err := o.ReplaceService("s", replacement, time.Second)
		if !errors.Is(err, stopErr) {
			t.Fatalf("err = %v, want wrapping %v", err, stopErr)
		}
		if got := replacement.startCalls.Load(); got != 0 {
			t.Errorf("replacement Start() calls = %d, want 0 (swap aborted)", got)
		}
		if entry.getSvc() != Service(old) {
			t.Error("entry implementation changed despite a Stop() error")
		}
	})
}

// TestReplaceService_RequiresRunningHardDependencies pins that replace gates the
// fresh start on the same hard-dependency edge as StartService: a missing or
// non-running dependency refuses the swap before the target is touched.
func TestReplaceService_RequiresRunningHardDependencies(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	if err := o.Register(&testSvc{}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	dependent := &testSvc{}
	if err := o.Register(dependent, WithName("dep"), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	startAndWait(t, o, "base")
	startAndWait(t, o, "dep")

	t.Run("stopped dependency", func(t *testing.T) {
		if err := o.StopService("base", time.Second, Orphans()); err != nil {
			t.Fatal(err)
		}
		if err := o.ReplaceService("dep", &testSvc{}, time.Second); !errors.Is(err, ErrDependencyNotRunning) {
			t.Fatalf("err = %v, want ErrDependencyNotRunning", err)
		}
		if got := dependent.stopCalls.Load(); got != 0 {
			t.Errorf("dependent Stop() calls = %d, want 0 (refused before teardown)", got)
		}
		startAndWait(t, o, "base")
	})

	t.Run("missing dependency", func(t *testing.T) {
		if err := o.Unregister("base", time.Second, Orphans()); err != nil {
			t.Fatal(err)
		}
		if err := o.ReplaceService("dep", &testSvc{}, time.Second); !errors.Is(err, ErrDependencyNotFound) {
			t.Fatalf("err = %v, want ErrDependencyNotFound", err)
		}
	})
}

// TestReplaceService_CronReschedules pins that a replaced cron entry runs the
// new implementation on subsequent ticks and that the old schedule is gone: the
// old instance's Stop() ran, and only the replacement's Start is invoked.
func TestReplaceService_CronReschedules(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	old := &testSvc{startFn: func(context.Context) error { return nil }}
	if err := o.Register(old, WithName("c"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	startAndWait(t, o, "c")
	entry := entryNamed(t, o, "c")

	o.invokeCron(entry, entry.cronGeneration())
	if got := old.startCalls.Load(); got != 1 {
		t.Fatalf("old tick calls = %d, want 1", got)
	}

	replacement := &testSvc{startFn: func(context.Context) error { return nil }}
	if err := o.ReplaceService("c", replacement, time.Second); err != nil {
		t.Fatalf("ReplaceService = %v", err)
	}
	if got := old.stopCalls.Load(); got != 1 {
		t.Errorf("old Stop() calls = %d, want 1", got)
	}

	o.invokeCron(entry, entry.cronGeneration())
	if got := replacement.startCalls.Load(); got != 1 {
		t.Errorf("replacement tick calls = %d, want 1", got)
	}
	if got := old.startCalls.Load(); got != 1 {
		t.Errorf("old tick calls after replace = %d, want 1", got)
	}
	if s, _ := o.Status("c"); s != StatusRunning {
		t.Errorf("status = %v, want StatusRunning", s)
	}
}

// TestReplaceService_RunOnce pins that a succeeded runOnce gate can be replaced
// and re-run with the new implementation, keeping its StatusSucceeded.
func TestReplaceService_RunOnce(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	old := &testSvc{startFn: func(context.Context) error { return nil }}
	if err := o.Register(old, WithName("g"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("g"); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, o, "g", StatusSucceeded)

	replacement := &testSvc{startFn: func(context.Context) error { return nil }}
	if err := o.ReplaceService("g", replacement, time.Second); err != nil {
		t.Fatalf("ReplaceService = %v", err)
	}
	if got := replacement.startCalls.Load(); got != 1 {
		t.Errorf("replacement Start() calls = %d, want 1", got)
	}
	if got := old.stopCalls.Load(); got != 1 {
		t.Errorf("old Stop() calls = %d, want 1", got)
	}
	if s, _ := o.Status("g"); s != StatusSucceeded {
		t.Errorf("status = %v, want StatusSucceeded", s)
	}
}
