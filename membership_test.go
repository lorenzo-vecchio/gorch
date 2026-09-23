package gorch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// ── Dynamic Register (hot add) ──

func TestRegisterDynamic_Persistent(t *testing.T) {
	o := New()
	defer o.Stop(1 * time.Second)
	if err := o.Register(&namedSvc{name: "base"}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	svc := &testSvc{}
	if err := o.Register(svc, WithName("late")); err != nil {
		t.Fatalf("hot add: %v", err)
	}
	if s, ok := o.Status("late"); !ok || s != StatusRegistered {
		t.Errorf("status = %v, want StatusRegistered", s)
	}
	if got := svc.startCalls.Load(); got != 0 {
		t.Errorf("hot Register must not start the service, Start ran %d times", got)
	}
	if len(o.Names()) != 2 {
		t.Errorf("Names() = %v, want 2 entries", o.Names())
	}
}

func TestRegisterDynamic_AutoName(t *testing.T) {
	o := New()
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(1 * time.Second)

	if err := o.Register(&namedSvc{}); err != nil {
		t.Fatalf("hot add with auto-name: %v", err)
	}
	names := o.Names()
	if len(names) != 1 || names[0] != "$1" {
		t.Errorf("Names() = %v, want [$1]", names)
	}
}

func TestRegisterDynamic_Errors(t *testing.T) {
	newRunning := func(t *testing.T) *Orchestrator {
		t.Helper()
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { o.Stop(1 * time.Second) })
		return o
	}

	t.Run("duplicate", func(t *testing.T) {
		o := newRunning(t)
		if err := o.Register(&namedSvc{}, WithName("dup")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("dup")); !errors.Is(err, ErrDuplicateName) {
			t.Fatalf("got %v, want ErrDuplicateName", err)
		}
	})

	t.Run("self dependency", func(t *testing.T) {
		o := newRunning(t)
		if err := o.Register(&namedSvc{}, WithName("self"), DependsOn("self")); !errors.Is(err, ErrDependencyCycle) {
			t.Fatalf("got %v, want ErrDependencyCycle", err)
		}
	})

	t.Run("unknown hard dependency", func(t *testing.T) {
		o := newRunning(t)
		if err := o.Register(&namedSvc{}, WithName("orphan"), DependsOn("ghost")); !errors.Is(err, ErrDependencyNotFound) {
			t.Fatalf("got %v, want ErrDependencyNotFound", err)
		}
	})

	t.Run("missing soft dependency tolerated", func(t *testing.T) {
		o := newRunning(t)
		if err := o.Register(&namedSvc{}, WithName("soft"), DependsOnSoft("ghost")); err != nil {
			t.Fatalf("got %v, want nil", err)
		}
	})

	t.Run("unsupported self-heal combination", func(t *testing.T) {
		o := newRunning(t)
		err := o.Register(&namedSvc{}, WithName("sh"),
			WithCron("* * * * * *", CronParallel),
			WithSelfHeal(func() Service { return &namedSvc{} }))
		if !errors.Is(err, ErrUnsupportedOption) {
			t.Fatalf("got %v, want ErrUnsupportedOption", err)
		}
	})

	t.Run("validator rejects", func(t *testing.T) {
		o := newRunning(t)
		err := o.Register(&validSvc{validateErr: errors.New("nope")}, WithName("v"))
		if err == nil {
			t.Fatal("expected validation error")
		}
		if _, ok := o.Status("v"); ok {
			t.Error("rejected validator must not be registered")
		}
	})

	t.Run("duplicate introduced during validation", func(t *testing.T) {
		o := newRunning(t)
		v := &registeringValidator{o: o, inner: &namedSvc{}, name: "raced"}
		if err := o.Register(v, WithName("raced")); !errors.Is(err, ErrDuplicateName) {
			t.Fatalf("got %v, want ErrDuplicateName", err)
		}
	})
}

func TestRegisterDynamic_CustomLogger(t *testing.T) {
	tl := &testLogger{}
	o := New(WithLogger(tl))
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(1 * time.Second)

	if err := o.Register(&namedSvc{}, WithName("logged")); err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	entry := o.nameIndex["logged"]
	o.mu.Unlock()
	if entry.getLogger() == nil {
		t.Fatal("hot-added entry has no logger")
	}
	entry.getLogger().Info("hot", "k", "v")
	if got := len(tl.callsMatching("hot")); got != 1 {
		t.Errorf("log calls = %d, want 1", got)
	}
}

func TestRegisterDynamic_Cron(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	cron := &testSvc{startFn: func(ctx context.Context) error {
		calls.Add(1)
		return nil
	}}
	if err := o.Register(cron, WithName("c"), WithCron("* * * * * *", CronParallel)); err != nil {
		t.Fatalf("hot cron: %v", err)
	}
	// Register does not auto-start: the cron entry is scheduled but the service
	// stays StatusRegistered until StartService (D1).
	if s, _ := o.Status("c"); s != StatusRegistered {
		t.Errorf("hot cron status = %v, want StatusRegistered", s)
	}

	o.mu.Lock()
	entry := o.nameIndex["c"]
	o.mu.Unlock()
	if entry.cronID == 0 {
		t.Error("hot-added cron entry has no cron ID")
	}

	deadline := time.After(3 * time.Second)
	for calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("hot-added cron service never ticked")
		case <-time.After(20 * time.Millisecond):
		}
	}

	if err := o.StartService("c"); err != nil {
		t.Fatalf("StartService cron: %v", err)
	}
	if s, _ := o.Status("c"); s != StatusRunning {
		t.Errorf("status after StartService = %v, want StatusRunning", s)
	}
	// Already running: StartService is a no-op.
	if err := o.StartService("c"); err != nil {
		t.Fatalf("StartService on running cron: %v", err)
	}
}

func TestRegisterDynamic_InvalidCron(t *testing.T) {
	o := New()
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(1 * time.Second)

	err := o.Register(&namedSvc{}, WithName("bad"), WithCron("invalid", CronParallel))
	if !errors.Is(err, ErrInvalidCron) {
		t.Fatalf("got %v, want ErrInvalidCron", err)
	}
	if _, ok := o.Status("bad"); ok {
		t.Error("rejected cron service must not be registered")
	}
}

func TestRegisterDynamic_ShutdownGates(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	svc := &testSvc{stopFn: func() error {
		close(entered)
		<-release
		return nil
	}}
	o := New()
	if err := o.Register(svc, WithName("blocker")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- o.Stop(5 * time.Second) }()
	<-entered // Stop is under way: the stopping flag is set.

	if err := o.Register(&namedSvc{}, WithName("during")); !errors.Is(err, ErrOrchestratorStopping) {
		t.Errorf("Register during Stop = %v, want ErrOrchestratorStopping", err)
	}
	if err := o.StartService("blocker"); !errors.Is(err, ErrOrchestratorStopping) {
		t.Errorf("StartService during Stop = %v, want ErrOrchestratorStopping", err)
	}

	close(release)
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if err := o.Register(&namedSvc{}, WithName("after")); !errors.Is(err, ErrOrchestratorStopped) {
		t.Errorf("Register after Stop = %v, want ErrOrchestratorStopped", err)
	}
	if err := o.StartService("blocker"); !errors.Is(err, ErrOrchestratorStopped) {
		t.Errorf("StartService after Stop = %v, want ErrOrchestratorStopped", err)
	}
}

// registeringValidator registers another service from inside Validate, racing a
// duplicate name against the outer registration's commit step.
type registeringValidator struct {
	testSvc
	o     *Orchestrator
	inner Service
	name  string
}

func (r *registeringValidator) Validate() error {
	_ = r.o.Register(r.inner, WithName(r.name))
	return nil
}

// ── StartService ──

func TestStartService(t *testing.T) {
	t.Run("unknown name", func(t *testing.T) {
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(1 * time.Second)
		if err := o.StartService("nope"); !errors.Is(err, ErrServiceNotFound) {
			t.Fatalf("got %v, want ErrServiceNotFound", err)
		}
	})

	t.Run("starts registered persistent", func(t *testing.T) {
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(1 * time.Second)

		started := make(chan struct{}, 4)
		svc := &testSvc{startFn: func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}}
		if err := o.Register(svc, WithName("late")); err != nil {
			t.Fatal(err)
		}
		if err := o.StartService("late"); err != nil {
			t.Fatalf("StartService: %v", err)
		}
		if s, _ := o.Status("late"); s != StatusRunning {
			t.Errorf("status = %v, want StatusRunning", s)
		}
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("service Start was not invoked")
		}
		if got := svc.startCalls.Load(); got != 1 {
			t.Errorf("Start ran %d times, want 1", got)
		}
	})

	t.Run("idempotent when running", func(t *testing.T) {
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(1 * time.Second)

		started := make(chan struct{}, 4)
		svc := &testSvc{startFn: func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}}
		if err := o.Register(svc, WithName("late")); err != nil {
			t.Fatal(err)
		}
		if err := o.StartService("late"); err != nil {
			t.Fatal(err)
		}
		<-started
		if err := o.StartService("late"); err != nil {
			t.Fatalf("second StartService: %v", err)
		}
		if got := svc.startCalls.Load(); got != 1 {
			t.Errorf("Start ran %d times, want 1", got)
		}
	})

	t.Run("dependency must be running", func(t *testing.T) {
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(1 * time.Second)

		if err := o.Register(&namedSvc{}, WithName("dep")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("child"), DependsOn("dep")); err != nil {
			t.Fatal(err)
		}
		err := o.StartService("child")
		if !errors.Is(err, ErrDependencyNotRunning) {
			t.Fatalf("got %v, want ErrDependencyNotRunning", err)
		}
	})

	t.Run("starts dependency then dependent", func(t *testing.T) {
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(1 * time.Second)

		if err := o.Register(&namedSvc{}, WithName("dep")); err != nil {
			t.Fatal(err)
		}
		if err := o.StartService("dep"); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("child"), DependsOn("dep")); err != nil {
			t.Fatal(err)
		}
		if err := o.StartService("child"); err != nil {
			t.Fatalf("StartService child: %v", err)
		}
		if s, _ := o.Status("child"); s != StatusRunning {
			t.Errorf("child status = %v, want StatusRunning", s)
		}
	})

	t.Run("missing hard dependency entry", func(t *testing.T) {
		o := New()
		if err := o.Register(&namedSvc{}, WithName("dep")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("child"), DependsOn("dep")); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(1 * time.Second)

		// Simulate a Phase-3 Unregister of "dep" leaving a dangling edge.
		o.mu.Lock()
		filtered := o.entries[:0]
		for _, e := range o.entries {
			if e.name != "dep" {
				filtered = append(filtered, e)
			}
		}
		o.entries = filtered
		delete(o.nameIndex, "dep")
		o.mu.Unlock()

		if err := o.StartService("child"); !errors.Is(err, ErrDependencyNotFound) {
			t.Fatalf("got %v, want ErrDependencyNotFound", err)
		}
	})

	t.Run("runOnce re-runs once", func(t *testing.T) {
		o := New()
		var calls atomic.Int32
		svc := &testSvc{startFn: func(ctx context.Context) error {
			calls.Add(1)
			return nil
		}}
		if err := o.Register(svc, WithName("once"), WithRunOnce()); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(1 * time.Second)

		if got := calls.Load(); got != 1 {
			t.Fatalf("initial runs = %d, want 1", got)
		}
		if s, _ := o.Status("once"); s != StatusSucceeded {
			t.Fatalf("status = %v, want StatusSucceeded", s)
		}
		if err := o.StartService("once"); err != nil {
			t.Fatalf("StartService: %v", err)
		}
		if got := calls.Load(); got != 2 {
			t.Errorf("runs after StartService = %d, want 2", got)
		}
		if s, _ := o.Status("once"); s != StatusSucceeded {
			t.Errorf("status = %v, want StatusSucceeded", s)
		}
	})

	t.Run("cron reschedules after stop", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(1 * time.Second)

		if err := o.Register(&testSvc{}, WithName("c"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
			t.Fatal(err)
		}
		o.mu.Lock()
		entry := o.nameIndex["c"]
		entry.cronID = 0 // simulate a Phase-3 StopService having removed it
		o.mu.Unlock()
		o.setStatus(entry, StatusStopped)

		if err := o.StartService("c"); err != nil {
			t.Fatalf("StartService cron: %v", err)
		}
		o.mu.Lock()
		id := entry.cronID
		o.mu.Unlock()
		if id == 0 {
			t.Error("cron entry was not rescheduled")
		}
		if s, _ := o.Status("c"); s != StatusRunning {
			t.Errorf("status = %v, want StatusRunning", s)
		}

		// Scheduled but not running: only the status is reconciled.
		o.setStatus(entry, StatusStarting)
		if err := o.StartService("c"); err != nil {
			t.Fatalf("StartService scheduled cron: %v", err)
		}
		if s, _ := o.Status("c"); s != StatusRunning {
			t.Errorf("status = %v, want StatusRunning", s)
		}
	})

	t.Run("cron reschedule failure", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(1 * time.Second)

		if err := o.Register(&testSvc{}, WithName("c"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
			t.Fatal(err)
		}
		o.mu.Lock()
		entry := o.nameIndex["c"]
		entry.cronID = 0
		entry.cfg.cronSpec = "invalid"
		o.mu.Unlock()
		o.setStatus(entry, StatusStopped)

		if err := o.StartService("c"); !errors.Is(err, ErrInvalidCron) {
			t.Fatalf("got %v, want ErrInvalidCron", err)
		}
	})

	t.Run("reentrant start rejected", func(t *testing.T) {
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(1 * time.Second)

		if err := o.Register(&namedSvc{}, WithName("r")); err != nil {
			t.Fatal(err)
		}
		o.mu.Lock()
		entry := o.nameIndex["r"]
		o.mu.Unlock()

		entry.starting.Store(true)
		defer entry.starting.Store(false)
		if err := o.StartService("r"); !errors.Is(err, errReentrantMembership) {
			t.Fatalf("got %v, want reentrant membership error", err)
		}
	})
}

// TestStartService_ReentrantFromOwnStart verifies that a service starting
// itself from its own Start is rejected, not deadlocked.
func TestStartService_ReentrantFromOwnStart(t *testing.T) {
	o := New()
	var innerErr error
	svc := &testSvc{startFn: func(ctx context.Context) error {
		innerErr = o.StartService("gate")
		return nil
	}}
	if err := o.Register(svc, WithName("gate"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer o.Stop(1 * time.Second)

	if !errors.Is(innerErr, errReentrantMembership) {
		t.Fatalf("nested StartService = %v, want reentrant membership error", innerErr)
	}
}

// ── Per-service Messenger ownership ──

// TestMessenger_ZeroValueUsable guards the root/view indirection: a bare
// Messenger value must keep working as before (lazily initializing its
// registry), so ownership plumbing is invisible to callers.
func TestMessenger_ZeroValueUsable(t *testing.T) {
	var m Messenger
	ch, unsub := m.Subscribe("t")
	defer unsub()
	m.Publish("v", "t")
	select {
	case got := <-ch:
		if got != "v" {
			t.Errorf("zero-value Messenger got %v, want v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("zero-value Messenger should deliver after Subscribe")
	}
}

func TestMessenger_DrainOwner_Isolation(t *testing.T) {
	m := newMessenger()
	a := m.scoped(1)
	b := m.scoped(2)

	chA, _ := a.Subscribe("topic")
	chB, _ := b.Subscribe("topic")

	m.drainOwner(1)

	if _, ok := <-chA; ok {
		t.Error("drained owner's channel should be closed")
	}
	m.Publish("still-there", "topic")
	select {
	case v := <-chB:
		if v != "still-there" {
			t.Errorf("owner b got %v, want still-there", v)
		}
	case <-time.After(time.Second):
		t.Fatal("owner b should still receive Publish after owner a drained")
	}
	if _, ok := m.subs["topic"][1]; ok {
		t.Error("drained owner 1 must be absent from the registry")
	}
}

func TestMessenger_DrainOwner_IdempotentAndMissing(t *testing.T) {
	m := newMessenger()
	ch, _ := m.scoped(7).Subscribe("t")

	m.drainOwner(7)
	m.drainOwner(7) // no panic, no double close
	if _, ok := <-ch; ok {
		t.Error("channel should be closed after drainOwner")
	}
	m.drainOwner(99) // owner that never subscribed is a no-op
}

func TestMessenger_DrainOwner_ThenGlobalDrain(t *testing.T) {
	m := newMessenger()
	chA, _ := m.scoped(1).Subscribe("t")
	chB, _ := m.scoped(2).Subscribe("t")

	m.drainOwner(1)
	m.Drain()

	if _, ok := <-chA; ok {
		t.Error("scoped-drained channel should be closed")
	}
	if _, ok := <-chB; ok {
		t.Error("global Drain should close the remaining channel")
	}
	if m.subs != nil {
		t.Error("global Drain must empty the registry")
	}
}

func TestMessenger_DrainOwner_RemovesEmptyTopic(t *testing.T) {
	m := newMessenger()
	_, _ = m.scoped(3).Subscribe("only")
	_, _ = m.scoped(4).Subscribe("other")

	m.drainOwner(3)

	if _, ok := m.subs["only"]; ok {
		t.Error("empty topic must be pruned after drainOwner")
	}
	if _, ok := m.subs["other"]; !ok {
		t.Error("other topics must survive drainOwner")
	}
}

func TestMessenger_DrainOwner_ResubscribeFreshOwner(t *testing.T) {
	m := newMessenger()
	oldCh, _ := m.scoped(1).Subscribe("t")
	m.drainOwner(1)

	newCh, _ := m.scoped(2).Subscribe("t")
	m.Publish("v", "t")

	select {
	case got := <-newCh:
		if got != "v" {
			t.Errorf("new owner got %v, want v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("new owner should receive after re-subscribe")
	}
	if _, ok := <-oldCh; ok {
		t.Error("old owner's channel must stay closed")
	}
}

// ownerSubSvc subscribes through its ServiceContext.Messenger and exposes both
// subscription readiness and delivered messages, so tests can tell which
// instance owns which channel.
type ownerSubSvc struct {
	ready chan struct{}
	got   chan any
}

func (s *ownerSubSvc) Start(ctx ServiceContext) error {
	ch, _ := ctx.Messenger.Subscribe("owned")
	close(s.ready)
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				return nil
			}
			select {
			case s.got <- v:
			default:
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *ownerSubSvc) Stop() error { return nil }

// ownerPubSvc captures the scoped Messenger it is handed so a test can publish
// through a service's own view.
type ownerPubSvc struct {
	ready     chan struct{}
	messenger *Messenger
}

func (s *ownerPubSvc) Start(ctx ServiceContext) error {
	s.messenger = ctx.Messenger
	close(s.ready)
	<-ctx.Done()
	return ctx.Err()
}

func (s *ownerPubSvc) Stop() error { return nil }

// TestScopedPublish_ReachesOtherOwners pins Publish to the shared root registry:
// a service publishing through its scoped view must reach a subscription owned
// by a different service.
func TestScopedPublish_ReachesOtherOwners(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)

	sub := &ownerSubSvc{ready: make(chan struct{}), got: make(chan any, 8)}
	pub := &ownerPubSvc{ready: make(chan struct{})}
	if err := o.Register(sub, WithName("sub")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(pub, WithName("pub")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	<-sub.ready
	<-pub.ready

	pub.messenger.Publish("cross-owner", "owned")

	select {
	case got := <-sub.got:
		if got != "cross-owner" {
			t.Errorf("subscriber got %v, want cross-owner", got)
		}
	case <-time.After(time.Second):
		t.Fatal("a scoped view's Publish must reach another owner's subscription")
	}
}

// TestTypedMessaging_SharedAcrossScopedViews pins RegisterType and TypedPublish
// to the shared root: a type registered via one scoped view must be publishable
// via another and reach a subscriber on a third.
func TestTypedMessaging_SharedAcrossScopedViews(t *testing.T) {
	m := newMessenger()
	registrar := m.scoped(1)
	subscriber := m.scoped(2)
	publisher := m.scoped(3)

	if err := RegisterType[string](registrar); err != nil {
		t.Fatal(err)
	}
	ch, unsub := TypedSubscribe[string](subscriber, "typed")
	defer unsub()

	TypedPublish(publisher, "hello", "typed")

	select {
	case got := <-ch:
		if got != "hello" {
			t.Errorf("scoped typed delivery got %q, want hello", got)
		}
	case <-time.After(time.Second):
		t.Fatal("typed message registered on one scoped view must reach a subscriber on another")
	}
}

func TestDrainService_ScopedToOwner(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)

	svcA := &ownerSubSvc{ready: make(chan struct{}), got: make(chan any, 8)}
	svcB := &ownerSubSvc{ready: make(chan struct{}), got: make(chan any, 8)}
	if err := o.Register(svcA, WithName("a")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(svcB, WithName("b")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	<-svcA.ready
	<-svcB.ready

	o.mu.Lock()
	entryA := o.nameIndex["a"]
	entryB := o.nameIndex["b"]
	o.mu.Unlock()
	if entryA.owner == 0 || entryB.owner == 0 || entryA.owner == entryB.owner {
		t.Fatalf("owner ids must be nonzero and distinct: a=%d b=%d", entryA.owner, entryB.owner)
	}

	o.drainService(entryA)

	o.messenger.Publish("ping", "owned")
	select {
	case <-svcB.got:
	case <-time.After(time.Second):
		t.Fatal("service b should still receive after service a is drained")
	}
	select {
	case <-svcA.got:
		t.Error("drained service a must not receive")
	case <-time.After(50 * time.Millisecond):
	}

	o.drainService(entryA) // idempotent
}

func TestDrainService_ResubscribeSameNameFreshOwner(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)

	svc1 := &ownerSubSvc{ready: make(chan struct{}), got: make(chan any, 8)}
	if err := o.Register(svc1, WithName("svc")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	<-svc1.ready

	o.mu.Lock()
	oldEntry := o.nameIndex["svc"]
	oldOwner := oldEntry.owner
	o.mu.Unlock()
	o.drainService(oldEntry)

	// Simulate a Phase-3 removal, then re-add under the same name.
	o.mu.Lock()
	filtered := o.entries[:0]
	for _, e := range o.entries {
		if e != oldEntry {
			filtered = append(filtered, e)
		}
	}
	o.entries = filtered
	delete(o.nameIndex, "svc")
	o.mu.Unlock()

	svc2 := &ownerSubSvc{ready: make(chan struct{}), got: make(chan any, 8)}
	if err := o.Register(svc2, WithName("svc")); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("svc"); err != nil {
		t.Fatalf("StartService re-added: %v", err)
	}
	<-svc2.ready

	o.mu.Lock()
	newOwner := o.nameIndex["svc"].owner
	o.mu.Unlock()
	if newOwner == oldOwner {
		t.Fatalf("re-added service reused owner id %d", newOwner)
	}

	o.messenger.Publish("fresh", "owned")
	select {
	case <-svc2.got:
	case <-time.After(time.Second):
		t.Fatal("new instance should receive")
	}
	select {
	case <-svc1.got:
		t.Error("removed instance must not receive after re-add")
	case <-time.After(50 * time.Millisecond):
	}
}
