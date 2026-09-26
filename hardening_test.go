package gorch

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// entryNamed fetches a registered entry under the orchestrator lock.
func entryNamed(t *testing.T, o *Orchestrator, name string) *serviceEntry {
	t.Helper()
	o.mu.RLock()
	defer o.mu.RUnlock()
	e := o.nameIndex[name]
	if e == nil {
		t.Fatalf("service %q is not registered", name)
	}
	return e
}

// mustNotPanic fails the test if fn panics, so a public entry point can be
// pinned as panic-free even when handed hostile input.
func mustNotPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", name, r)
		}
	}()
	fn()
}

// TestNoPublicAPI_Panics pins that no public entry point panics when called
// with nil, unknown, or post-shutdown input. It complements the sentinel tests:
// where a value cannot be used, a documented error must come back, never an
// unwind.
func TestNoPublicAPI_Panics(t *testing.T) {
	t.Run("nil_service", func(t *testing.T) {
		o := New()
		if err := o.Register(nil); !errors.Is(err, ErrNilService) {
			t.Fatalf("Register(nil) = %v, want ErrNilService", err)
		}
		if err := o.RegisterFunc("fn", nil, nil); !errors.Is(err, ErrNilService) {
			t.Fatalf("RegisterFunc(nil startFn) = %v, want ErrNilService", err)
		}
		if o.Count() != 0 {
			t.Fatalf("rejected registrations must not enter the graph, got %d", o.Count())
		}
	})

	t.Run("before_start_introspection", func(t *testing.T) {
		o := New()
		mustNotPanic(t, "Names", func() { _ = o.Names() })
		mustNotPanic(t, "Statuses", func() { _ = o.Statuses() })
		mustNotPanic(t, "Count", func() { _ = o.Count() })
		mustNotPanic(t, "Metrics", func() { _ = o.Metrics() })
		mustNotPanic(t, "Done", func() { _ = o.Done() })
		mustNotPanic(t, "Health", func() { _ = o.Health() })
		mustNotPanic(t, "Status", func() { _, _ = o.Status("nope") })
		mustNotPanic(t, "IsReady", func() { _ = o.IsReady(context.Background(), "nope") })
	})

	t.Run("unknown_names", func(t *testing.T) {
		o := New()
		if err := o.StartService("nope"); !errors.Is(err, ErrOrchestratorNotStarted) {
			t.Fatalf("StartService unknown before Start = %v, want ErrOrchestratorNotStarted", err)
		}
		if err := o.StopService("nope", time.Second); !errors.Is(err, ErrServiceNotFound) {
			t.Fatalf("StopService unknown = %v, want ErrServiceNotFound", err)
		}
		if err := o.Unregister("nope", time.Second); !errors.Is(err, ErrServiceNotFound) {
			t.Fatalf("Unregister unknown = %v, want ErrServiceNotFound", err)
		}
		if err := o.WaitFor("nope", StatusRunning, 10*time.Millisecond); err == nil {
			t.Fatal("WaitFor unknown must return an error")
		}
	})

	t.Run("dynamic_bad_cron", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)
		if err := o.Register(&namedSvc{}, WithName("bad"), WithCron("not a spec", CronParallel)); !errors.Is(err, ErrInvalidCron) {
			t.Fatalf("dynamic invalid cron = %v, want ErrInvalidCron", err)
		}
	})

	t.Run("messenger_after_drain", func(t *testing.T) {
		m := newMessenger()
		m.Drain()
		mustNotPanic(t, "Publish", func() { m.Publish("x", "topic") })
		mustNotPanic(t, "Subscribe", func() { _, unsub := m.Subscribe("topic"); unsub() })
		mustNotPanic(t, "Request", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			_, _ = m.Request(ctx, "x", "topic")
		})
	})
}

// TestUserCodePanics_AreContained pins that a panic raised by any caller
// supplied callback is turned into ordinary control flow instead of unwinding
// through a public entry point.
func TestUserCodePanics_AreContained(t *testing.T) {
	t.Run("before_start_hook", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Register(&namedSvc{}, WithName("s"),
			WithOnBeforeStart(func(string) error { panic("hook boom") })); err != nil {
			t.Fatal(err)
		}
		err := o.Start()
		if err == nil || !strings.Contains(err.Error(), "panicked") {
			t.Fatalf("Start = %v, want a recovered panic error", err)
		}
		_ = o.Stop(time.Second)
	})

	t.Run("start_condition", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Register(&namedSvc{}, WithName("s"),
			WithStartCondition(func() bool { panic("condition boom") })); err != nil {
			t.Fatal(err)
		}
		err := o.Start()
		if err == nil || !strings.Contains(err.Error(), "panicked") {
			t.Fatalf("Start = %v, want a recovered panic error", err)
		}
		_ = o.Stop(time.Second)
	})

	t.Run("after_start_hook", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Register(&namedSvc{}, WithName("s"),
			WithOnAfterStart(func(string, error) { panic("after start boom") })); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatalf("Start = %v", err)
		}
		if s, _ := o.Status("s"); s != StatusRunning {
			t.Fatalf("status = %v, want running", s)
		}
		_ = o.Stop(time.Second)
	})

	t.Run("before_and_after_stop_hooks", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Register(&namedSvc{}, WithName("s"),
			WithOnBeforeStop(func(string) error { panic("before stop boom") }),
			WithOnAfterStop(func(string, error) { panic("after stop boom") })); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		err := o.StopService("s", time.Second)
		if err == nil || !strings.Contains(err.Error(), "panicked") {
			t.Fatalf("StopService = %v, want a recovered panic error", err)
		}
		if s, _ := o.Status("s"); s != StatusStopped {
			t.Fatalf("status = %v, want stopped", s)
		}
		_ = o.Stop(time.Second)
	})

	t.Run("state_change_and_crash_callbacks", func(t *testing.T) {
		o := New(
			WithHealthChecksDisabled(),
			WithOnStateChange(func(string, ServiceStatus, ServiceStatus) { panic("state boom") }),
			WithOnCrash(func(string, error) { panic("crash boom") }),
		)
		if err := o.Register(&errSvc{err: errors.New("boom")}, WithName("s")); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		waitForStatus(t, o, "s", StatusCrashed)
		_ = o.Stop(time.Second)
	})

	t.Run("health_and_readiness_probes", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		h := &healthSvc{
			testSvc: testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
			healthFn: func(context.Context) error {
				panic("health boom")
			},
		}
		r := &readySvc{
			testSvc: testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
			readyFn: func(context.Context) error {
				panic("ready boom")
			},
		}
		if err := o.Register(h, WithName("h")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(r, WithName("r")); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		health := o.Health()
		if health["h"] == nil || !strings.Contains(health["h"].Error(), "panicked") {
			t.Fatalf("Health[h] = %v, want recovered panic error", health["h"])
		}
		if o.IsReady(context.Background(), "r") {
			t.Fatal("IsReady must be false when Ready panics")
		}
		o.runHealthChecks() // must not panic
		_ = o.Stop(time.Second)
	})

	t.Run("validator", func(t *testing.T) {
		o := New()
		if err := o.Register(&validSvc{}, WithName("ok")); err != nil {
			t.Fatal(err)
		}
		err := o.Register(&panicValidateSvc{}, WithName("v"))
		if err == nil || !strings.Contains(err.Error(), "panicked") {
			t.Fatalf("Register with panicking Validate = %v, want recovered panic error", err)
		}
	})
}

// panicValidateSvc panics from Validate.
type panicValidateSvc struct{ namedSvc }

func (s *panicValidateSvc) Validate() error { panic("validate boom") }

// TestHooksUnderConcurrentMembership_Race exercises every lifecycle hook under
// the race detector while another goroutine churns membership, so the
// "no user code under an orchestrator lock" invariant is tested, not merely
// documented: a hook that calls back into the orchestrator must not deadlock.
func TestHooksUnderConcurrentMembership_Race(t *testing.T) {
	var o *Orchestrator
	o = New(
		WithHealthChecksDisabled(),
		WithGlobalOnBeforeStart(func(string) error { _ = o.Names(); return nil }),
		WithGlobalOnAfterStart(func(string, error) { _ = o.Statuses() }),
		WithGlobalOnBeforeStop(func(string) error { _ = o.Count(); return nil }),
		WithGlobalOnAfterStop(func(string, error) { _ = o.Metrics() }),
		WithOnStateChange(func(string, ServiceStatus, ServiceStatus) { _ = o.Names() }),
	)
	for _, name := range []string{"a", "b", "c"} {
		if err := o.Register(&namedSvc{}, WithName(name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			_ = o.StopService("b", 200*time.Millisecond)
			_ = o.StartService("b")
		}
	}()
	for i := 0; i < 30; i++ {
		_ = o.Statuses()
		_ = o.Health()
	}
	wg.Wait()
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestFailedStart_ReleasesReservations_AndRetrySucceeds pins the lifecycle of
// the start reservation: a Start that aborts partway must release every
// reservation it took, so the orchestrator is not bricked and a retry can
// succeed. This is the regression the reservation mechanism could reintroduce.
func TestFailedStart_ReleasesReservations_AndRetrySucceeds(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	var failing = true
	hook := func(string) error {
		if failing {
			return errors.New("nope")
		}
		return nil
	}
	if err := o.Register(&namedSvc{}, WithName("a"), WithOnBeforeStart(hook)); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("b"), DependsOn("a")); err != nil {
		t.Fatal(err)
	}

	if err := o.Start(); err == nil {
		t.Fatal("first Start must fail")
	}
	for _, name := range []string{"a", "b"} {
		if entryNamed(t, o, name).starting.Load() {
			t.Fatalf("entry %s stayed reserved after a failed Start", name)
		}
		if s, _ := o.Status(name); s != StatusRegistered {
			t.Fatalf("entry %s status = %v after failed Start, want registered", name, s)
		}
	}

	failing = false
	if err := o.Start(); err != nil {
		t.Fatalf("retry Start: %v", err)
	}
	waitForStatus(t, o, "a", StatusRunning)
	waitForStatus(t, o, "b", StatusRunning)
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestStop_BusyReservation_IsRetryable pins that a membership op rejected only
// because the entry was momentarily reserved by *another goroutine* is reported
// as the transient ErrMembershipBusy (never the programming-error
// ErrReentrantMembership) and succeeds once the reservation is released.
func TestStop_BusyReservation_IsRetryable(t *testing.T) {
	t.Run("white-box reservation", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)
		if err := o.Register(&namedSvc{}, WithName("r")); err != nil {
			t.Fatal(err)
		}
		entry := entryNamed(t, o, "r")

		entry.starting.Store(true)
		err := o.StopService("r", time.Second)
		if !errors.Is(err, ErrMembershipBusy) {
			t.Fatalf("StopService on a reserved entry = %v, want ErrMembershipBusy", err)
		}
		if errors.Is(err, ErrReentrantMembership) {
			t.Fatalf("StopService on a reserved entry = %v, must not be ErrReentrantMembership", err)
		}
		entry.starting.Store(false)

		if err := o.StartService("r"); err != nil {
			t.Fatalf("StartService after reservation release: %v", err)
		}
		if err := o.StopService("r", time.Second); err != nil {
			t.Fatalf("StopService retry after reservation release: %v", err)
		}
	})

	t.Run("real in-flight Start", func(t *testing.T) {
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)

		started := make(chan struct{})
		release := make(chan struct{})
		svc := &testSvc{startFn: func(ctx context.Context) error {
			close(started)
			<-release
			return nil
		}}
		// Hot-add as runOnce: StartService then runs Start synchronously on the
		// calling goroutine, so the entry stays reserved until release closes
		// while a StopService from this goroutine is a reservation collision.
		if err := o.Register(svc, WithName("s"), WithRunOnce()); err != nil {
			t.Fatal(err)
		}
		startErr := make(chan error, 1)
		go func() { startErr <- o.StartService("s") }()
		<-started

		err := o.StopService("s", time.Second)
		if !errors.Is(err, ErrMembershipBusy) {
			t.Fatalf("StopService during an in-flight Start = %v, want ErrMembershipBusy", err)
		}
		if errors.Is(err, ErrReentrantMembership) {
			t.Fatalf("StopService during an in-flight Start = %v, must not be ErrReentrantMembership", err)
		}

		close(release)
		if err := <-startErr; err != nil {
			t.Fatalf("StartService: %v", err)
		}
		if err := o.StopService("s", time.Second); err != nil {
			t.Fatalf("StopService retry after reservation cleared: %v", err)
		}
	})
}

// TestMembershipOp_FromOwnStart_IsProgrammingError pins the split: a membership
// op invoked from a service's own Start on the same goroutine is genuine
// reentrancy — a programming error — and must never be reported as the
// retryable ErrMembershipBusy.
func TestMembershipOp_FromOwnStart_IsProgrammingError(t *testing.T) {
	run := func(t *testing.T, op func(o *Orchestrator) error) error {
		t.Helper()
		o := New()
		var innerErr error
		done := make(chan struct{})
		svc := &testSvc{startFn: func(ctx context.Context) error {
			innerErr = op(o)
			close(done)
			return nil
		}}
		if err := o.Register(svc, WithName("self"), WithRunOnce()); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer o.Stop(time.Second)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("membership op from own Start deadlocked")
		}
		return innerErr
	}

	t.Run("StopService self", func(t *testing.T) {
		err := run(t, func(o *Orchestrator) error { return o.StopService("self", time.Second) })
		if !errors.Is(err, ErrReentrantMembership) {
			t.Fatalf("StopService from own Start = %v, want ErrReentrantMembership", err)
		}
		if errors.Is(err, ErrMembershipBusy) {
			t.Fatalf("StopService from own Start = %v, must not be ErrMembershipBusy", err)
		}
	})

	t.Run("StartService self", func(t *testing.T) {
		err := run(t, func(o *Orchestrator) error { return o.StartService("self") })
		if !errors.Is(err, ErrReentrantMembership) {
			t.Fatalf("StartService from own Start = %v, want ErrReentrantMembership", err)
		}
		if errors.Is(err, ErrMembershipBusy) {
			t.Fatalf("StartService from own Start = %v, must not be ErrMembershipBusy", err)
		}
	})
}

// TestMembershipOp_FromOwnStop_IsProgrammingError covers the stopGoid branch: a
// membership op invoked from a service's own Stop on the same goroutine is
// reentrancy, must return ErrReentrantMembership rather than the transient
// sentinel, and must not deadlock.
func TestMembershipOp_FromOwnStop_IsProgrammingError(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	var innerErr error
	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		stopFn: func() error {
			innerErr = o.StopService("self", time.Second)
			return nil
		},
	}
	if err := o.Register(svc, WithName("self")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	outer := make(chan error, 1)
	go func() { outer <- o.StopService("self", time.Second) }()
	select {
	case err := <-outer:
		if err != nil {
			t.Fatalf("StopService: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("membership op re-entered from Stop deadlocked")
	}
	if !errors.Is(innerErr, ErrReentrantMembership) {
		t.Fatalf("StopService from own Stop = %v, want ErrReentrantMembership", innerErr)
	}
	if errors.Is(innerErr, ErrMembershipBusy) {
		t.Fatalf("StopService from own Stop = %v, must not be ErrMembershipBusy", innerErr)
	}
}

// TestBusy pins the observable predicate for the retryable collision: it is true
// only for a registered entry holding an in-flight reservation, and false for an
// idle or unknown entry.
func TestBusy(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)
	if err := o.Register(&namedSvc{}, WithName("r")); err != nil {
		t.Fatal(err)
	}

	if o.Busy("r") {
		t.Error("idle registered entry must not report busy")
	}
	if o.Busy("unknown") {
		t.Error("unknown name must not report busy")
	}

	entry := entryNamed(t, o, "r")
	entry.starting.Store(true)
	if !o.Busy("r") {
		t.Error("entry with a start reservation must report busy")
	}
	entry.starting.Store(false)

	entry.removing.Store(true)
	if !o.Busy("r") {
		t.Error("entry with a teardown reservation must report busy")
	}
	entry.removing.Store(false)

	if o.Busy("r") {
		t.Error("entry must report idle again after its reservation clears")
	}
}

// TestCascade_StartingDependentBlocks pins the dependency guard's boundary: a
// hard dependent that is Starting (a reserved, explicit state) blocks a plain
// stop exactly like a Running one, and cascade overrides it.
func TestCascade_StartingDependentBlocks(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("dep"), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	o.setStatus(entryNamed(t, o, "dep"), StatusStarting)
	if err := o.StopService("base", time.Second); !errors.Is(err, ErrHasDependents) {
		t.Fatalf("plain stop with a Starting dependent = %v, want ErrHasDependents", err)
	}
	if err := o.StopService("base", time.Second, WithCascadeStop()); err != nil {
		t.Fatalf("cascade stop = %v", err)
	}
	if s, _ := o.Status("dep"); s != StatusStopped {
		t.Fatalf("cascaded dependent status = %v, want stopped", s)
	}
}

// TestUnregister_DestroysAllPerServiceState pins that Unregister removes every
// trace of the entry from the live graph and releases its per-instance
// resources, so a later re-add cannot inherit or leak them.
func TestUnregister_DestroysAllPerServiceState(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	subscribed := make(chan struct{})
	svc := &testSvc{startFn: func(ctx context.Context) error {
		sc := ctx.(ServiceContext)
		_, unsub := sc.Messenger.Subscribe("topic")
		defer unsub()
		close(subscribed)
		<-ctx.Done()
		return ctx.Err()
	}}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("c"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	<-subscribed
	entry := entryNamed(t, o, "s")
	if entry.currentOwner() == 0 {
		t.Fatal("running instance has no Messenger owner")
	}
	cronEntry := entryNamed(t, o, "c")
	if cronEntry.cronID == 0 {
		t.Fatal("cron entry was not scheduled")
	}

	if err := o.Unregister("s", time.Second); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if _, ok := o.Status("s"); ok {
		t.Error("unregistered service still reports a status")
	}
	if !entry.removed.Load() {
		t.Error("removed flag was not set")
	}
	if entry.currentOwner() != 0 {
		t.Error("Messenger owners were not drained")
	}
	o.mu.RLock()
	_, stillIndexed := o.nameIndex["s"]
	o.mu.RUnlock()
	if stillIndexed {
		t.Error("nameIndex still holds the unregistered entry")
	}

	if err := o.Unregister("c", time.Second); err != nil {
		t.Fatalf("Unregister cron: %v", err)
	}
	if cronEntry.cronID != 0 {
		t.Error("cron schedule was not released")
	}
	if o.Count() != 0 {
		t.Errorf("Count = %d after Unregister, want 0", o.Count())
	}
	_ = o.Stop(time.Second)
}

// TestReAdd_SameName_StartsClean pins that re-registering a name after
// Unregister creates a genuinely fresh entry: no retry counter, health-failure
// count, status, or cron state carries over.
func TestReAdd_SameName_StartsClean(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	old := &crashSignalSvc{sig: make(chan struct{})}
	if err := o.Register(old, WithName("s"),
		WithSelfHeal(func() Service { return &crashSignalSvc{sig: make(chan struct{})} }),
		WithBackoff(ConstantBackoff{Delay: 5 * time.Millisecond})); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	<-old.sig
	oldEntry := entryNamed(t, o, "s")
	deadline := time.After(2 * time.Second)
	for oldEntry.getRetryCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("retry counter never advanced")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	if err := o.Unregister("s", time.Second); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if err := o.Register(&namedSvc{}, WithName("s")); err != nil {
		t.Fatalf("re-add: %v", err)
	}
	fresh := entryNamed(t, o, "s")
	if fresh == oldEntry {
		t.Fatal("re-add reused the removed entry")
	}
	if fresh.getRetryCount() != 0 {
		t.Errorf("fresh retryCount = %d, want 0", fresh.getRetryCount())
	}
	if fresh.getHealthFailures() != 0 {
		t.Errorf("fresh healthFailures = %d, want 0", fresh.getHealthFailures())
	}
	if fresh.cronID != 0 {
		t.Error("fresh entry inherited a cron schedule")
	}
	if fresh.currentOwner() != 0 {
		t.Error("fresh entry inherited Messenger owners")
	}
	if s, _ := o.Status("s"); s != StatusRegistered {
		t.Errorf("fresh status = %v, want registered", s)
	}
	if err := o.StartService("s"); err != nil {
		t.Fatalf("StartService on the re-added entry: %v", err)
	}
	if s, _ := o.Status("s"); s != StatusRunning {
		t.Fatalf("status after StartService = %v, want running", s)
	}
	if fresh.getRetryCount() != 0 || fresh.getHealthFailures() != 0 {
		t.Error("fresh entry accumulated stale retry/health state after start")
	}
	_ = o.Stop(time.Second)
}

// TestHookTimeout_DoesNotStarveServiceStop pins that a before-stop hook which
// never returns still leaves the service's own Stop() a budget, so its
// resources are released rather than abandoned.
func TestHookTimeout_DoesNotStarveServiceStop(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	var once sync.Once
	svc := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(svc, WithName("s"), WithOnBeforeStop(func(string) error {
		once.Do(func() { <-release })
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		_ = o.Stop(time.Second)
	})

	const budget = 200 * time.Millisecond
	start := time.Now()
	err := o.StopService("s", budget)
	if !errors.Is(err, ErrHookTimeout) {
		t.Fatalf("StopService = %v, want ErrHookTimeout", err)
	}
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("StopService = %v, want it to also match ErrStopTimeout", err)
	}
	if svc.stopCalls.Load() == 0 {
		t.Fatal("the service's Stop() was starved by the blocking hook")
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("StopService took %v; the hook sub-budget did not bound it", elapsed)
	}
}

// TestFailedStart_RollbackIsBounded pins that the failed-Start rollback is
// bounded: a service already running when a later level fails whose Stop()
// blocks forever must not hang Start. Start returns within a generous bound and
// the error matches ErrStopTimeout.
func TestFailedStart_RollbackIsBounded(t *testing.T) {
	o := New(WithHealthChecksDisabled(), WithFailedStartTimeout(300*time.Millisecond))

	stopEntered := make(chan struct{})
	stopRelease := make(chan struct{})
	startRelease := make(chan struct{})
	t.Cleanup(func() {
		close(stopRelease)
		close(startRelease)
	})

	a := &testSvc{
		// Ignore cancellation: the instance outlives the rollback on purpose.
		startFn: func(context.Context) error { <-startRelease; return errors.New("a exited") },
		stopFn: func() error {
			close(stopEntered)
			<-stopRelease
			return nil
		},
	}
	if err := o.Register(a, WithName("a")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&testSvc{}, WithName("b"), DependsOn("a"),
		WithOnBeforeStart(func(string) error { return errors.New("b cannot start") })); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- o.Start() }()

	// Prove the rollback actually reached the blocking Stop() before bounding
	// Start, so the assertion is not merely about the earlier failure.
	select {
	case <-stopEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("rollback never reached the blocking Stop()")
	}

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start hung instead of returning a bounded rollback error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Start took %v, want a bounded rollback", elapsed)
	}
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("Start error = %v, want ErrStopTimeout from the bounded rollback", err)
	}
}

// TestFailedStart_RollbackHooksBounded pins that an overrunning before-stop hook
// during the failed-Start rollback is bounded too: Start returns within the
// budget and the error matches both ErrHookTimeout and ErrStopTimeout.
func TestFailedStart_RollbackHooksBounded(t *testing.T) {
	o := New(WithHealthChecksDisabled(), WithFailedStartTimeout(300*time.Millisecond))

	hookEntered := make(chan struct{})
	hookRelease := make(chan struct{})
	t.Cleanup(func() { close(hookRelease) })

	a := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(a, WithName("a"), WithOnBeforeStop(func(string) error {
		close(hookEntered)
		<-hookRelease
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&testSvc{}, WithName("b"), DependsOn("a"),
		WithOnBeforeStart(func(string) error { return errors.New("b cannot start") })); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- o.Start() }()

	select {
	case <-hookEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("rollback never ran the blocking before-stop hook")
	}

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start hung instead of bounding the overrunning hook")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Start took %v, want a bounded rollback", elapsed)
	}
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("Start error = %v, want ErrStopTimeout", err)
	}
	if !errors.Is(err, ErrHookTimeout) {
		t.Fatalf("Start error = %v, want ErrHookTimeout for the overrunning hook", err)
	}
}

// stopOrderRecorder records the order in which services' Stop() methods run.
type stopOrderRecorder struct {
	mu    sync.Mutex
	order []string
}

func (r *stopOrderRecorder) record(name string) {
	r.mu.Lock()
	r.order = append(r.order, name)
	r.mu.Unlock()
}

func (r *stopOrderRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

// onlyNames returns the entries of order restricted to names, preserving order.
func onlyNames(order []string, names ...string) []string {
	want := make(map[string]bool, len(names))
	for _, n := range names {
		want[n] = true
	}
	var out []string
	for _, n := range order {
		if want[n] {
			out = append(out, n)
		}
	}
	return out
}

// TestFailedStart_RollbackOrderIsReverseTopological pins that the failed-Start
// rollback unwinds in reverse topological order, not reverse registration
// order. The graph registers the dependent before its dependency — via a soft
// dependency, since validateRegisterConfigLocked rejects a hard dependency that is not
// yet registered — so registration order is a, b, c while topological order is
// b, a, c. c (hard-dependent on a) fails to start, so the rollback must stop a
// before b; reverse registration would stop b first.
func TestFailedStart_RollbackOrderIsReverseTopological(t *testing.T) {
	rec := &stopOrderRecorder{}
	svc := func(name string) Service {
		return &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			stopFn:  func() error { rec.record(name); return nil },
		}
	}

	o := New(WithHealthChecksDisabled())
	if err := o.Register(svc("a"), WithName("a"), DependsOnSoft("b")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(svc("b"), WithName("b")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(svc("c"), WithName("c"), DependsOn("a"),
		WithOnBeforeStart(func(string) error { return errors.New("c cannot start") })); err != nil {
		t.Fatal(err)
	}

	if err := o.Start(); err == nil {
		t.Fatal("Start should have failed on c")
	}

	order := rec.snapshot()
	aIdx, bIdx := indexOf(order, "a"), indexOf(order, "b")
	if aIdx == -1 || bIdx == -1 {
		t.Fatalf("rollback stop order = %v, want both a and b stopped", order)
	}
	if aIdx > bIdx {
		t.Errorf("rollback stop order = %v, want dependent a stopped before dependency b", order)
	}
}

// TestFailedStart_RollbackOrder_MatchesOrchestratorStop pins that the rollback
// and a normal orchestrator Stop unwind the same graph in the same order. On
// the rollback path c never started, so only a and b are stopped; comparing the
// a/b subsequences of the two orders isolates the ordering invariant.
func TestFailedStart_RollbackOrder_MatchesOrchestratorStop(t *testing.T) {
	registerGraph := func(o *Orchestrator, rec *stopOrderRecorder, failC bool) error {
		svc := func(name string) Service {
			return &testSvc{
				startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
				stopFn:  func() error { rec.record(name); return nil },
			}
		}
		if err := o.Register(svc("a"), WithName("a"), DependsOnSoft("b")); err != nil {
			return err
		}
		if err := o.Register(svc("b"), WithName("b")); err != nil {
			return err
		}
		opts := []RegisterOption{WithName("c"), DependsOn("a")}
		if failC {
			opts = append(opts, WithOnBeforeStart(func(string) error { return errors.New("c cannot start") }))
		}
		return o.Register(svc("c"), opts...)
	}

	rollbackRec := &stopOrderRecorder{}
	rollback := New(WithHealthChecksDisabled())
	if err := registerGraph(rollback, rollbackRec, true); err != nil {
		t.Fatal(err)
	}
	if err := rollback.Start(); err == nil {
		t.Fatal("Start should have failed on c")
	}

	stopRec := &stopOrderRecorder{}
	stopped := New(WithHealthChecksDisabled())
	if err := registerGraph(stopped, stopRec, false); err != nil {
		t.Fatal(err)
	}
	if err := stopped.Start(); err != nil {
		t.Fatal(err)
	}
	if err := stopped.Stop(time.Second); err != nil {
		t.Fatal(err)
	}

	rollbackAB := onlyNames(rollbackRec.snapshot(), "a", "b")
	stopAB := onlyNames(stopRec.snapshot(), "a", "b")
	if len(rollbackAB) != 2 || len(stopAB) != 2 {
		t.Fatalf("a/b stop order = rollback %v, Stop %v; want both stopped on each",
			rollbackRec.snapshot(), stopRec.snapshot())
	}
	if strings.Join(rollbackAB, ",") != strings.Join(stopAB, ",") {
		t.Errorf("a/b stop order differs: rollback %v, Stop %v", rollbackAB, stopAB)
	}
}

// TestFailedStart_Rollback_NoCascadeOfDependencyStopErrors pins that a correct
// reverse-topological rollback does not manufacture teardown errors. b's Stop()
// fails if it runs before its dependent a has stopped; under the old
// reverse-registration order b stopped first and that error was joined into the
// rollback, burying the real start failure. With reverse-topological order a
// stops first, so the only error is the original start failure.
func TestFailedStart_Rollback_NoCascadeOfDependencyStopErrors(t *testing.T) {
	var mu sync.Mutex
	aStopped := false
	orderErr := errors.New("b stopped before its dependent a")
	startErr := errors.New("c cannot start")

	o := New(WithHealthChecksDisabled())
	a := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn: func() error {
			mu.Lock()
			aStopped = true
			mu.Unlock()
			return nil
		},
	}
	b := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn: func() error {
			mu.Lock()
			defer mu.Unlock()
			if !aStopped {
				return orderErr
			}
			return nil
		},
	}
	if err := o.Register(a, WithName("a"), DependsOnSoft("b")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(b, WithName("b")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&testSvc{}, WithName("c"), DependsOn("a"),
		WithOnBeforeStart(func(string) error { return startErr })); err != nil {
		t.Fatal(err)
	}

	err := o.Start()
	if err == nil {
		t.Fatal("Start should have failed on c")
	}
	if !errors.Is(err, startErr) {
		t.Errorf("Start error = %v, want the original start failure", err)
	}
	if errors.Is(err, orderErr) {
		t.Errorf("rollback error = %v, must not contain the dependency teardown cascade", err)
	}
}

// TestFailedStart_ThenRetry_Succeeds pins that an ordinary failed Start with a
// non-blocking rollback leaves the orchestrator genuinely reusable.
func TestFailedStart_ThenRetry_Succeeds(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	failing := true
	hook := func(string) error {
		if failing {
			return errors.New("gate closed")
		}
		return nil
	}
	if err := o.Register(&testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
		WithName("a"), WithOnBeforeStart(hook)); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
		WithName("b"), DependsOn("a")); err != nil {
		t.Fatal(err)
	}

	if err := o.Start(); err == nil {
		_ = o.Stop(time.Second)
		t.Fatal("first Start must fail")
	}
	failing = false
	if err := o.Start(); err != nil {
		t.Fatalf("retry Start: %v", err)
	}
	defer func() { _ = o.Stop(time.Second) }()
	waitForStatus(t, o, "a", StatusRunning)
	waitForStatus(t, o, "b", StatusRunning)
}

// TestFailedStart_ConcurrentHotAdd_DoesNotHangReset pins that a service
// hot-added and started through StartService while a Start is failing — and so
// feeding the orchestrator-wide wait group — does not block the reset, and that
// the orchestrator is still reusable afterwards.
func TestFailedStart_ConcurrentHotAdd_DoesNotHangReset(t *testing.T) {
	o := New(WithHealthChecksDisabled())

	bHookEntered := make(chan struct{})
	hotAddDone := make(chan struct{})
	var hookOnce sync.Once
	failFirst := true

	a := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(a, WithName("a")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&testSvc{}, WithName("b"), DependsOn("a"),
		WithOnBeforeStart(func(string) error {
			if !failFirst {
				return nil
			}
			hookOnce.Do(func() { close(bHookEntered) })
			<-hotAddDone
			return errors.New("b cannot start")
		})); err != nil {
		t.Fatal(err)
	}

	startDone := make(chan error, 1)
	go func() { startDone <- o.Start() }()

	// Wait until b's hook is holding the failing Start, then hot-add and start a
	// service that contributes to o.wg before the failure cleanup runs.
	select {
	case <-bHookEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("b's before-start hook never ran")
	}
	hot := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(hot, WithName("hot")); err != nil {
		t.Fatalf("hot Register: %v", err)
	}
	if err := o.StartService("hot"); err != nil {
		t.Fatalf("hot StartService: %v", err)
	}
	close(hotAddDone)

	select {
	case err := <-startDone:
		if err == nil {
			t.Fatal("first Start must fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start hung after a concurrent hot add")
	}

	// The hot service honoured cancellation and has wound down; drop it and
	// retry to prove the orchestrator was left reusable.
	if err := o.Unregister("hot", time.Second); err != nil {
		t.Fatalf("Unregister hot: %v", err)
	}
	failFirst = false
	restartDone := make(chan error, 1)
	go func() { restartDone <- o.Start() }()
	select {
	case err := <-restartDone:
		if err != nil {
			t.Fatalf("retry Start: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retry Start hung")
	}
	defer func() { _ = o.Stop(time.Second) }()
	waitForStatus(t, o, "a", StatusRunning)
}

// startWithHotAddFailure drives a Start that fails at b's before-start gate,
// hot-adds and starts "hot" via StartService while the gate holds the Start
// open, and returns the channel of hot's ServiceContexts (one per instance) plus
// a function that runs the retry Start (its gate now passes). The first Start is
// asserted to fail; the caller consumes the first instance's context and, after
// calling retry, the retry instance's context.
func startWithHotAddFailure(t *testing.T, o *Orchestrator) (<-chan ServiceContext, func() error) {
	t.Helper()

	gateEntered := make(chan struct{})
	hotReady := make(chan struct{})
	var once sync.Once
	failFirst := true

	if err := o.Register(&testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
		WithName("a")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&testSvc{}, WithName("b"), DependsOn("a"),
		WithOnBeforeStart(func(string) error {
			if failFirst {
				once.Do(func() { close(gateEntered) })
				<-hotReady
				return errors.New("b cannot start")
			}
			return nil
		})); err != nil {
		t.Fatal(err)
	}

	// Launch the failing Start first: "hot" must be registered only once that
	// Start is in flight, so that commitDynamicLocked binds its logger to the
	// failing Start's log channel and the failing Start's snapshot excludes it.
	startDone := make(chan error, 1)
	go func() { startDone <- o.Start() }()
	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("b's before-start gate never ran")
	}

	contexts := make(chan ServiceContext, 4)
	if err := o.RegisterFunc("hot", func(sc ServiceContext) error {
		contexts <- sc
		<-sc.Done()
		return sc.Err()
	}, nil); err != nil {
		t.Fatal(err)
	}
	// Hot-add and start while the failing Start is in flight: commitDynamicLocked
	// binds the entry's logger to this Start's log channel.
	if err := o.StartService("hot"); err != nil {
		t.Fatalf("hot StartService: %v", err)
	}
	close(hotReady)

	select {
	case err := <-startDone:
		if err == nil {
			t.Fatal("first Start must fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first Start hung")
	}

	return contexts, func() error {
		failFirst = false
		return o.Start()
	}
}

// captureStderrWhile runs fn with os.Stderr redirected to a pipe and returns
// everything written to it. fn must stop the log-pump before returning (e.g. by
// calling Stop) so no goroutine writes to os.Stderr after it is restored.
func captureStderrWhile(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	old := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = old }()
	fn()
	_ = w.Close()
	out, _ := io.ReadAll(r)
	_ = r.Close()
	return string(out)
}

// TestFailedStart_HotAddedEntryLoggerStillWorks pins the verified behaviour of a
// service hot-added and started during a failing Start. The issue suspected a
// silent hang once the default logger's 256-slot channel filled after the
// failed Start's pump exited. It does not hang: ServiceLogger.emit uses a
// non-blocking send, so the entries are dropped. The next successful Start
// rebinds the survivor's logger, after which its output reaches the retry's live
// log-pump. A custom Logger is unaffected at every step.
func TestFailedStart_HotAddedEntryLoggerStillWorks(t *testing.T) {
	t.Run("default_logger_drops_then_rebinds_on_retry", func(t *testing.T) {
		o := New(WithHealthChecksDisabled(), WithLogLevel(LogLevelInfo))
		contexts, retry := startWithHotAddFailure(t, o)

		var first ServiceContext
		select {
		case first = <-contexts:
		case <-time.After(2 * time.Second):
			t.Fatal("hot never started")
		}

		// The failed Start's pump is gone. 400 entries (> the 256 slot buffer)
		// must all be dropped without blocking the caller.
		drained := make(chan struct{})
		go func() {
			for i := 0; i < 400; i++ {
				first.Logger.Info("survivor line", "i", i)
			}
			close(drained)
		}()
		select {
		case <-drained:
		case <-time.After(2 * time.Second):
			t.Fatal("survivor logger blocked after the failed Start's log-pump exited")
		}

		// Retry succeeds; Start rebinds every snapshotted entry, the survivor
		// included, so its output reaches the retry's log-pump.
		retryOut := captureStderrWhile(t, func() {
			if err := retry(); err != nil {
				t.Fatalf("retry Start: %v", err)
			}
			waitForStatus(t, o, "hot", StatusRunning)

			var second ServiceContext
			select {
			case second = <-contexts:
			case <-time.After(2 * time.Second):
				t.Fatal("hot did not restart on the retry")
			}
			if second.Logger == first.Logger {
				t.Error("retry did not rebind the survivor's logger")
			}
			for i := 0; i < 8; i++ {
				second.Logger.Info("retry reaches the live logger", "i", i)
			}

			// Tear the retry down so the pump exits and the pipe can be read.
			// Issue #26's stale-wgDone leak for a started survivor makes this
			// whole Stop report ErrStopTimeout, so its error is not asserted:
			// this test is scoped to the logger concern.
			_ = o.Stop(200 * time.Millisecond)
			select {
			case <-o.logPumpDone:
			case <-time.After(2 * time.Second):
				t.Error("retry log-pump did not exit")
			}
		})
		if !strings.Contains(retryOut, "retry reaches the live logger") {
			t.Fatalf("survivor output did not reach the retry logger; captured %q", retryOut)
		}
	})

	t.Run("custom_logger_is_unaffected", func(t *testing.T) {
		tl := &testLogger{}
		o := New(WithHealthChecksDisabled(), WithLogger(tl))
		contexts, retry := startWithHotAddFailure(t, o)

		var first ServiceContext
		select {
		case first = <-contexts:
		case <-time.After(2 * time.Second):
			t.Fatal("hot never started")
		}
		for i := 0; i < 400; i++ {
			first.Logger.Info("custom survivor", "i", i)
		}
		if got := len(tl.callsMatching("custom survivor")); got != 400 {
			t.Fatalf("custom logger recorded %d survivor lines, want 400", got)
		}

		if err := retry(); err != nil {
			t.Fatalf("retry Start: %v", err)
		}
		waitForStatus(t, o, "hot", StatusRunning)
		select {
		case second := <-contexts:
			second.Logger.Info("custom retry")
		case <-time.After(2 * time.Second):
			t.Fatal("hot did not restart on the retry")
		}
		if got := len(tl.callsMatching("custom retry")); got != 1 {
			t.Fatalf("custom logger recorded %d retry lines, want 1", got)
		}

		// Cleanup; see the default-logger subtest for why the whole Stop's error
		// is not asserted.
		_ = o.Stop(200 * time.Millisecond)
	})
}

// TestFailedStart_ThenRetry_HotAddedEntryIsUsable pins that the survivor is a
// full participant in the retried Start, not merely a log destination: it
// reaches StatusRunning, it is rebound to the retry's logger, and its own Stop
// lifecycle completes cleanly (Stop() runs, StopService returns nil, status
// returns to Stopped). Issue #26's stale-wgDone leak for a *started* survivor
// makes a whole-orchestrator Stop report ErrStopTimeout here, so that path is
// deliberately not asserted and the test is scoped to the survivor itself.
func TestFailedStart_ThenRetry_HotAddedEntryIsUsable(t *testing.T) {
	o := New(WithHealthChecksDisabled(), WithLogLevel(LogLevelInfo))
	contexts, retry := startWithHotAddFailure(t, o)

	var first ServiceContext
	select {
	case first = <-contexts:
	case <-time.After(2 * time.Second):
		t.Fatal("hot never started")
	}

	if err := retry(); err != nil {
		t.Fatalf("retry Start: %v", err)
	}
	waitForStatus(t, o, "hot", StatusRunning)

	var second ServiceContext
	select {
	case second = <-contexts:
	case <-time.After(2 * time.Second):
		t.Fatal("hot did not restart on the retry")
	}
	if second.Logger == nil {
		t.Fatal("retry ServiceContext carries no logger")
	}
	if second.Logger == first.Logger {
		t.Error("retry did not rebind the survivor's logger")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- o.StopService("hot", time.Second) }()
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("StopService on the survivor = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StopService on the survivor hung")
	}
	if s, _ := o.Status("hot"); s != StatusStopped {
		t.Fatalf("survivor status after StopService = %v, want StatusStopped", s)
	}

	// Bound the teardown of the remaining services; the error is expected to be
	// ErrStopTimeout because of the issue #26 leak, so it is not asserted.
	_ = o.Stop(200 * time.Millisecond)
}

// TestFailedStartTimeout_DefaultAndOverride pins the configurable rollback
// budget: New defaults it to 30s, WithFailedStartTimeout overrides it, and a
// negative value removes the bound (a zero deadline).
func TestFailedStartTimeout_DefaultAndOverride(t *testing.T) {
	t.Run("default is 30s", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if got := o.cfg.failedStartTimeout; got != 30*time.Second {
			t.Fatalf("failedStartTimeout = %v, want 30s", got)
		}
		if o.failedStartDeadline().IsZero() {
			t.Fatal("default failed-start deadline must be set")
		}
	})
	t.Run("override", func(t *testing.T) {
		o := New(WithHealthChecksDisabled(), WithFailedStartTimeout(2*time.Second))
		if got := o.cfg.failedStartTimeout; got != 2*time.Second {
			t.Fatalf("failedStartTimeout = %v, want 2s", got)
		}
		if o.failedStartDeadline().IsZero() {
			t.Fatal("overridden failed-start deadline must be set")
		}
	})
	t.Run("negative disables the bound", func(t *testing.T) {
		o := New(WithHealthChecksDisabled(), WithFailedStartTimeout(-1))
		if !o.failedStartDeadline().IsZero() {
			t.Fatal("a negative failed-start timeout must produce no deadline")
		}
	})
}

// TestResetAfterStartFailure_BoundsWaits pins that both waits in the reset are
// bounded by the deadline and surface ErrStopTimeout: the instance wait group
// and the nil-safe log-pump wait. It is a white-box check of the timeout
// branches, which an integration test cannot reach deterministically.
func TestResetAfterStartFailure_BoundsWaits(t *testing.T) {
	t.Run("instance wait", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		o.wg.Add(1) // an instance that never exits
		err := o.resetAfterStartFailure(nil, time.Now().Add(100*time.Millisecond))
		if !errors.Is(err, ErrStopTimeout) {
			t.Fatalf("reset with a live instance = %v, want ErrStopTimeout", err)
		}
		if got := o.Metrics().AbandonedGoroutines; got != 1 {
			t.Fatalf("AbandonedGoroutines = %d, want 1 for the abandoned instance wait", got)
		}
	})
	t.Run("log pump wait", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		o.logPumpDone = make(chan struct{}) // never closed
		err := o.resetAfterStartFailure(nil, time.Now().Add(100*time.Millisecond))
		if !errors.Is(err, ErrStopTimeout) {
			t.Fatalf("reset with a stuck log-pump = %v, want ErrStopTimeout", err)
		}
		if got := o.Metrics().AbandonedGoroutines; got != 1 {
			t.Fatalf("AbandonedGoroutines = %d, want 1 for the abandoned log-pump wait", got)
		}
	})
}

// hasErrorForService reports whether the recording logger captured an Error
// entry whose key-value args name the given service.
func hasErrorForService(tl *testLogger, service string) bool {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	for _, c := range tl.calls {
		if c.level != "ERROR" {
			continue
		}
		for i := 0; i+1 < len(c.args); i += 2 {
			if c.args[i] == "service" && c.args[i+1] == service {
				return true
			}
		}
	}
	return false
}

// TestHookOverrun_LeaksExactlyOneGoroutine pins the abandoned-goroutine
// accounting for an overrunning before-stop hook: a bounded StopService returns
// ErrHookTimeout and the counter moves by exactly one, as a delta (not an
// absolute), so an earlier abandonment elsewhere cannot mask the leak. The hook
// stays blocked until cleanup; the service's Stop() returns promptly, so no Stop
// goroutine is leaked by this test.
func TestHookOverrun_LeaksExactlyOneGoroutine(t *testing.T) {
	o := New(WithHealthChecksDisabled())

	hookEntered := make(chan struct{})
	hookRelease := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { close(hookRelease) })

	if err := o.Register(
		&testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
		WithName("s"),
		WithOnBeforeStop(func(string) error {
			once.Do(func() { close(hookEntered) })
			<-hookRelease
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	before := o.Metrics().AbandonedGoroutines
	done := make(chan error, 1)
	go func() { done <- o.StopService("s", 200*time.Millisecond) }()

	select {
	case <-hookEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("before-stop hook never ran")
	}

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("StopService hung instead of bounding the overrunning hook")
	}
	if !errors.Is(err, ErrHookTimeout) {
		t.Fatalf("StopService = %v, want ErrHookTimeout", err)
	}
	if got := o.Metrics().AbandonedGoroutines - before; got != 1 {
		t.Fatalf("abandoned goroutines delta = %d, want exactly 1", got)
	}
}

// TestStopTimeout_LeakedGoroutineIsLogged pins that an abandoned teardown
// goroutine is logged at Error level naming the service, for both the
// before-stop hook and the service's own Stop().
func TestStopTimeout_LeakedGoroutineIsLogged(t *testing.T) {
	t.Run("before-stop hook", func(t *testing.T) {
		tl := &testLogger{}
		o := New(WithHealthChecksDisabled(), WithLogger(tl))

		hookEntered := make(chan struct{})
		hookRelease := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { close(hookRelease) })

		if err := o.Register(
			&testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
			WithName("s"),
			WithOnBeforeStop(func(string) error {
				once.Do(func() { close(hookEntered) })
				<-hookRelease
				return nil
			}),
		); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}

		done := make(chan error, 1)
		go func() { done <- o.StopService("s", 200*time.Millisecond) }()

		select {
		case <-hookEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("before-stop hook never ran")
		}
		select {
		case err := <-done:
			if !errors.Is(err, ErrHookTimeout) {
				t.Fatalf("StopService = %v, want ErrHookTimeout", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("StopService hung instead of bounding the overrunning hook")
		}
		if !hasErrorForService(tl, "s") {
			t.Fatal("abandoned before-stop hook was not logged at Error level for service s")
		}
	})

	t.Run("service Stop", func(t *testing.T) {
		tl := &testLogger{}
		o := New(WithHealthChecksDisabled(), WithLogger(tl))

		stopEntered := make(chan struct{})
		stopRelease := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { close(stopRelease) })

		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			stopFn: func() error {
				once.Do(func() { close(stopEntered) })
				<-stopRelease
				return nil
			},
		}
		if err := o.Register(svc, WithName("s"), WithStopTimeout(50*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}

		done := make(chan error, 1)
		go func() { done <- o.StopService("s", 5*time.Second) }()

		select {
		case <-stopEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("service Stop() never ran")
		}
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("StopService = nil, want the per-service timeout surfaced")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("StopService hung instead of surfacing the per-service cap")
		}
		if !hasErrorForService(tl, "s") {
			t.Fatal("abandoned Stop() was not logged at Error level for service s")
		}
	})
}

// TestStopOverrun_PerServiceTimeoutCountsAbandoned pins that the service's own
// Stop() abandoned by a per-service WithStopTimeout increments the counter by
// exactly one.
func TestStopOverrun_PerServiceTimeoutCountsAbandoned(t *testing.T) {
	o := New(WithHealthChecksDisabled())

	stopEntered := make(chan struct{})
	stopRelease := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { close(stopRelease) })

	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn: func() error {
			once.Do(func() { close(stopEntered) })
			<-stopRelease
			return nil
		},
	}
	if err := o.Register(svc, WithName("s"), WithStopTimeout(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	before := o.Metrics().AbandonedGoroutines
	done := make(chan error, 1)
	go func() { done <- o.StopService("s", 5*time.Second) }()

	select {
	case <-stopEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("service Stop() never ran")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("StopService hung instead of surfacing the per-service cap")
	}
	if got := o.Metrics().AbandonedGoroutines - before; got != 1 {
		t.Fatalf("abandoned goroutines delta = %d, want exactly 1", got)
	}
}

// TestAbandonedGoroutines_HappyPathStaysZero pins that a normal stop with no
// overrun leaves the counter at zero, so a non-zero value always means a
// genuine abandonment rather than incidental accounting.
func TestAbandonedGoroutines_HappyPathStaysZero(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(
		&testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
		WithName("s"),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	if err := o.StopService("s", time.Second); err != nil {
		t.Fatalf("StopService = %v, want nil", err)
	}
	if got := o.Metrics().AbandonedGoroutines; got != 0 {
		t.Fatalf("AbandonedGoroutines = %d on the happy path, want 0", got)
	}
	_ = o.Stop(time.Second)
}
