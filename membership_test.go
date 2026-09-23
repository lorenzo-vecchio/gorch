package gorch

import (
	"context"
	"errors"
	"strings"
	"sync"
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

// ── StopService / Unregister / cascade ──

// blockingSvc ignores context cancellation and returns only when released, to
// exercise the shared stop-timeout budget.
type blockingSvc struct {
	release chan struct{}
	stopErr error
}

func (s *blockingSvc) Start(ctx ServiceContext) error {
	<-s.release
	return nil
}

func (s *blockingSvc) Stop() error { return s.stopErr }

// crashSignalSvc crashes immediately and signals its first Start so a test can
// wait until a self-heal backoff is pending.
type crashSignalSvc struct {
	once sync.Once
	sig  chan struct{}
}

func (s *crashSignalSvc) Start(ctx ServiceContext) error {
	s.once.Do(func() { close(s.sig) })
	return errors.New("crash")
}

func (s *crashSignalSvc) Stop() error { return nil }

func TestStopService_UnknownName(t *testing.T) {
	o := New()
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(1 * time.Second)

	if err := o.StopService("nope", time.Second); !errors.Is(err, ErrServiceNotFound) {
		t.Errorf("StopService = %v, want ErrServiceNotFound", err)
	}
	if err := o.Unregister("nope", time.Second); !errors.Is(err, ErrServiceNotFound) {
		t.Errorf("Unregister = %v, want ErrServiceNotFound", err)
	}
}

func TestStopService_BlockedByHardDependent(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)
	if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("dep"), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	err := o.StopService("base", time.Second)
	if !errors.Is(err, ErrHasDependents) {
		t.Fatalf("got %v, want ErrHasDependents", err)
	}
	if !strings.Contains(err.Error(), "dep") {
		t.Errorf("error should name the blocker: %v", err)
	}
	if s, _ := o.Status("base"); s != StatusRunning {
		t.Errorf("base status = %v, want Running (untouched)", s)
	}
}

func TestStopService_Cascade(t *testing.T) {
	var mu sync.Mutex
	var stopOrder []string
	o := New(WithHealthChecksDisabled(), WithGlobalOnAfterStop(func(name string, err error) {
		mu.Lock()
		stopOrder = append(stopOrder, name)
		mu.Unlock()
	}))
	defer o.Stop(1 * time.Second)

	// Diamond: B and C depend on A, D depends on both.
	if err := o.Register(&namedSvc{}, WithName("A")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("B"), DependsOn("A")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("C"), DependsOn("A")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("D"), DependsOn("B", "C")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	if err := o.StopService("A", time.Second, WithCascadeStop()); err != nil {
		t.Fatalf("cascade StopService: %v", err)
	}
	for _, name := range []string{"A", "B", "C", "D"} {
		if s, ok := o.Status(name); !ok || s != StatusStopped {
			t.Errorf("%s status = %v (ok=%v), want Stopped and still registered", name, s, ok)
		}
	}
	if len(o.Names()) != 4 {
		t.Errorf("cascade stop must keep entries registered, Names()=%v", o.Names())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(stopOrder) != 4 || stopOrder[0] != "D" || stopOrder[len(stopOrder)-1] != "A" {
		t.Errorf("stop order = %v, want D first and A last", stopOrder)
	}
}

func TestUnregister_Cascade(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)
	if err := o.Register(&namedSvc{}, WithName("A")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("B"), DependsOn("A")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("C"), DependsOn("B")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	if err := o.Unregister("A", time.Second, WithCascadeStop()); err != nil {
		t.Fatalf("cascade Unregister: %v", err)
	}
	if len(o.Names()) != 0 {
		t.Errorf("Names() = %v, want empty", o.Names())
	}
	if len(o.Statuses()) != 0 {
		t.Errorf("Statuses() = %v, want empty", o.Statuses())
	}
	if o.Count() != 0 {
		t.Errorf("Count() = %d, want 0", o.Count())
	}
}

func TestStopService_SoftDependentNotBlocked(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)
	if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	soft := &namedSvc{}
	if err := o.Register(soft, WithName("soft"), DependsOnSoft("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	if err := o.StopService("base", time.Second); err != nil {
		t.Fatalf("soft dependent must not block: %v", err)
	}
	if s, _ := o.Status("base"); s != StatusStopped {
		t.Errorf("base status = %v, want Stopped", s)
	}
	if s, _ := o.Status("soft"); s != StatusRunning {
		t.Errorf("soft dependent status = %v, want Running (never cascaded)", s)
	}
}

func TestStopService_NonRunningDependentAllowed(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)
	if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	// Start base, then hot-add a dependent and leave it registered (not running).
	if err := o.Register(&namedSvc{}, WithName("dep"), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.StopService("base", time.Second); err != nil {
		t.Fatalf("non-running dependent must not block: %v", err)
	}
	if s, _ := o.Status("dep"); s != StatusRegistered {
		t.Errorf("dep status = %v, want Registered", s)
	}
}

func TestStopService_ThenStartService(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)
	if err := o.Register(&namedSvc{}, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	if err := o.StopService("s", time.Second); err != nil {
		t.Fatalf("StopService: %v", err)
	}
	if s, ok := o.Status("s"); !ok || s != StatusStopped {
		t.Fatalf("status = %v (ok=%v), want Stopped and registered", s, ok)
	}
	if err := o.StartService("s"); err != nil {
		t.Fatalf("StartService after stop: %v", err)
	}
	if s, _ := o.Status("s"); s != StatusRunning {
		t.Errorf("status = %v, want Running", s)
	}
}

func TestStopService_ZeroTimeoutWaits(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)
	if err := o.Register(&namedSvc{}, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	if err := o.StopService("s", 0); err != nil {
		t.Fatalf("zero-timeout StopService: %v", err)
	}
	if s, _ := o.Status("s"); s != StatusStopped {
		t.Errorf("status = %v, want Stopped", s)
	}
}

func TestStopService_Timeout(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	svc := &blockingSvc{release: make(chan struct{})}
	if err := o.Register(svc, WithName("blocker")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	err := o.StopService("blocker", 50*time.Millisecond)
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("got %v, want ErrStopTimeout", err)
	}
	close(svc.release)
	if err := o.Stop(2 * time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestUnregister_JoinsStopError(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)
	svc := &testSvc{stopFn: func() error { return errors.New("stop exploded") }}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	err := o.Unregister("s", time.Second)
	if err == nil || !strings.Contains(err.Error(), "stop exploded") {
		t.Fatalf("Unregister error = %v, want joined stop error", err)
	}
	if _, ok := o.Status("s"); ok {
		t.Error("failed Unregister must still remove the entry")
	}
}

func TestUnregister_CancelsPendingSelfHeal(t *testing.T) {
	var factoryCalls atomic.Int32
	sig := make(chan struct{})
	first := &crashSignalSvc{sig: sig}
	o := New(WithHealthChecksDisabled())
	factory := func() Service {
		factoryCalls.Add(1)
		return &crashSignalSvc{sig: sig}
	}
	if err := o.Register(first, WithName("heal"),
		WithSelfHeal(factory),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Second})); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-sig:
	case <-time.After(time.Second):
		t.Fatal("service never started")
	}
	time.Sleep(20 * time.Millisecond) // let handleServiceDone enter the backoff

	if err := o.Unregister("heal", time.Second); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if factoryCalls.Load() != 0 {
		t.Errorf("factory called %d times; a pending backoff must be cancelled", factoryCalls.Load())
	}
	if _, ok := o.Status("heal"); ok {
		t.Error("entry should be gone")
	}
	if err := o.Stop(2 * time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-o.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() should close after a clean shutdown")
	}
}

func TestStopService_CronRemovesAndReschedules(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&testSvc{}, WithName("c"), WithCron("*/1 * * * * *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("c"); err != nil {
		t.Fatal(err)
	}

	o.mu.Lock()
	id := o.nameIndex["c"].cronID
	o.mu.Unlock()
	if id == 0 {
		t.Fatal("expected a live cron schedule")
	}

	if err := o.StopService("c", time.Second); err != nil {
		t.Fatalf("StopService cron: %v", err)
	}
	if s, _ := o.Status("c"); s != StatusStopped {
		t.Errorf("status = %v, want Stopped", s)
	}
	o.mu.Lock()
	id = o.nameIndex["c"].cronID
	o.mu.Unlock()
	if id != 0 {
		t.Error("cron schedule must be removed on StopService")
	}

	// Idempotent teardown of an entry with no live schedule.
	if err := o.StopService("c", time.Second); err != nil {
		t.Fatalf("second StopService: %v", err)
	}

	// StartService re-schedules it.
	if err := o.StartService("c"); err != nil {
		t.Fatalf("StartService cron: %v", err)
	}
	o.mu.Lock()
	id = o.nameIndex["c"].cronID
	o.mu.Unlock()
	if id == 0 {
		t.Error("StartService must re-schedule a stopped cron entry")
	}
	if s, _ := o.Status("c"); s != StatusRunning {
		t.Errorf("status = %v, want Running", s)
	}
}

func TestUnregister_ReleasesSubscriptions(t *testing.T) {
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

	if err := o.Unregister("a", time.Second); err != nil {
		t.Fatalf("Unregister: %v", err)
	}

	o.messenger.Publish("ping", "owned")
	select {
	case <-svcB.got:
	case <-time.After(time.Second):
		t.Fatal("surviving service must still receive after removal")
	}
	select {
	case <-svcA.got:
		t.Error("removed service must not receive after its subscriptions are drained")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestTearDown_Reentrant covers the reentrancy guard: a membership op issued
// while the target is inside its own Start is rejected, not deadlocked.
func TestTearDown_Reentrant(t *testing.T) {
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
	if err := o.StopService("r", time.Second); !errors.Is(err, errReentrantMembership) {
		t.Errorf("StopService = %v, want reentrant membership error", err)
	}
	if err := o.Unregister("r", time.Second); !errors.Is(err, errReentrantMembership) {
		t.Errorf("Unregister = %v, want reentrant membership error", err)
	}
}

func TestStartService_RemovingRejected(t *testing.T) {
	o := New()
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(1 * time.Second)
	if err := o.Register(&namedSvc{}, WithName("x")); err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	entry := o.nameIndex["x"]
	o.mu.Unlock()

	entry.removing.Store(true)
	defer entry.removing.Store(false)
	if err := o.StartService("x"); !errors.Is(err, ErrServiceNotFound) {
		t.Errorf("StartService on removing entry = %v, want ErrServiceNotFound", err)
	}
}

func TestRegister_DependencyRemovingRejected(t *testing.T) {
	o := New()
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(1 * time.Second)
	if err := o.Register(&namedSvc{}, WithName("dep")); err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	entry := o.nameIndex["dep"]
	o.mu.Unlock()

	entry.removing.Store(true)
	defer entry.removing.Store(false)
	err := o.Register(&namedSvc{}, WithName("child"), DependsOn("dep"))
	if !errors.Is(err, ErrHasDependents) {
		t.Errorf("Register onto a removing dependency = %v, want ErrHasDependents", err)
	}
}

func TestHealthChecks_SkipRemoving(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)
	h := &healthSvc{testSvc: testSvc{startFn: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}}
	if err := o.Register(h, WithName("h")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	entry := o.nameIndex["h"]
	o.mu.Unlock()

	entry.removing.Store(true)
	defer entry.removing.Store(false)
	if _, ok := o.Health()["h"]; ok {
		t.Error("Health must skip an entry flagged removing")
	}
	before := h.healthCalls.Load()
	o.runHealthChecks()
	if h.healthCalls.Load() != before {
		t.Error("runHealthChecks must skip an entry flagged removing")
	}
}

func TestStopService_ShutdownGates(t *testing.T) {
	t.Run("during_stop", func(t *testing.T) {
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
		<-entered

		if err := o.StopService("blocker", time.Second); !errors.Is(err, ErrOrchestratorStopping) {
			t.Errorf("StopService during Stop = %v, want ErrOrchestratorStopping", err)
		}
		if err := o.Unregister("blocker", time.Second); !errors.Is(err, ErrOrchestratorStopping) {
			t.Errorf("Unregister during Stop = %v, want ErrOrchestratorStopping", err)
		}
		close(release)
		if err := <-stopDone; err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})

	t.Run("after_stop", func(t *testing.T) {
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		if err := o.Stop(time.Second); err != nil {
			t.Fatal(err)
		}
		if err := o.StopService("anything", time.Second); !errors.Is(err, ErrOrchestratorStopped) {
			t.Errorf("StopService after Stop = %v, want ErrOrchestratorStopped", err)
		}
		if err := o.Unregister("anything", time.Second); !errors.Is(err, ErrOrchestratorStopped) {
			t.Errorf("Unregister after Stop = %v, want ErrOrchestratorStopped", err)
		}
	})
}

func TestGroupOps_ShutdownGates(t *testing.T) {
	t.Run("during_stop", func(t *testing.T) {
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
		<-entered

		if err := o.StartGroup("g"); !errors.Is(err, ErrOrchestratorStopping) {
			t.Errorf("StartGroup during Stop = %v, want ErrOrchestratorStopping", err)
		}
		if err := o.StopGroup("g", time.Second); !errors.Is(err, ErrOrchestratorStopping) {
			t.Errorf("StopGroup during Stop = %v, want ErrOrchestratorStopping", err)
		}
		close(release)
		if err := <-stopDone; err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})

	t.Run("after_stop", func(t *testing.T) {
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		if err := o.Stop(time.Second); err != nil {
			t.Fatal(err)
		}
		if err := o.StartGroup("g"); !errors.Is(err, ErrOrchestratorStopped) {
			t.Errorf("StartGroup after Stop = %v, want ErrOrchestratorStopped", err)
		}
		if err := o.StopGroup("g", time.Second); !errors.Is(err, ErrOrchestratorStopped) {
			t.Errorf("StopGroup after Stop = %v, want ErrOrchestratorStopped", err)
		}
	})
}

// TestMembershipTransitions runs one deterministic add → start → stop/remove →
// restart sequence end to end; the spec reruns it with -count=50 to shake out
// flakes in the shared membership lock.
func TestMembershipTransitions(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)

	for _, svc := range []struct {
		name string
		deps []string
	}{{"a", nil}, {"b", []string{"a"}}, {"c", []string{"b"}}} {
		opts := []RegisterOption{WithName(svc.name)}
		if len(svc.deps) > 0 {
			opts = append(opts, DependsOn(svc.deps...))
		}
		if err := o.Register(&namedSvc{}, opts...); err != nil {
			t.Fatalf("Register %s: %v", svc.name, err)
		}
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	// A running hard dependent blocks a plain stop.
	if err := o.StopService("a", time.Second); !errors.Is(err, ErrHasDependents) {
		t.Fatalf("StopService a = %v, want ErrHasDependents", err)
	}
	// Cascade stops the whole chain in place.
	if err := o.StopService("a", time.Second, WithCascadeStop()); err != nil {
		t.Fatalf("cascade stop: %v", err)
	}
	if o.Count() != 3 {
		t.Fatalf("cascade stop must keep entries registered, Count()=%d", o.Count())
	}
	// Restart the chain in dependency order.
	for _, name := range []string{"a", "b", "c"} {
		if err := o.StartService(name); err != nil {
			t.Fatalf("StartService %s: %v", name, err)
		}
	}
	// Cascade removal clears the graph.
	if err := o.Unregister("a", time.Second, WithCascadeStop()); err != nil {
		t.Fatalf("cascade unregister: %v", err)
	}
	if o.Count() != 0 {
		t.Fatalf("Count() = %d after cascade unregister, want 0", o.Count())
	}
	// A removed name can be hot re-added and started.
	if err := o.Register(&namedSvc{}, WithName("a")); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if err := o.StartService("a"); err != nil {
		t.Fatalf("restart re-added: %v", err)
	}
}

// TestGroupOps_SerializedWithMembership covers the group-sync matrix row:
// StartGroup/StopGroup and membership ops share the membership lock, so
// concurrent calls are serialized instead of racing on entry selection. Run
// under -race, this asserts no interleaving tears an entry mid-operation.
func TestGroupOps_SerializedWithMembership(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(2 * time.Second)
	for _, spec := range []struct {
		name string
		deps []string
	}{{"a", nil}, {"b", []string{"a"}}, {"c", []string{"b"}}} {
		opts := []RegisterOption{WithName(spec.name), WithGroup("g")}
		if len(spec.deps) > 0 {
			opts = append(opts, DependsOn(spec.deps...))
		}
		if err := o.Register(&namedSvc{}, opts...); err != nil {
			t.Fatalf("Register %s: %v", spec.name, err)
		}
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	run := func(fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				t.Errorf("serialized op failed: %v", err)
			}
		}()
	}
	for i := 0; i < 20; i++ {
		run(func() error { return o.StartGroup("g") })
		run(func() error { return o.StopGroup("g", time.Second) })
		run(func() error { return o.StopService("b", time.Second, WithCascadeStop()) })
	}
	wg.Wait()

	if o.Count() != 3 {
		t.Fatalf("Count() = %d, want 3 (no op removes entries)", o.Count())
	}
	for _, name := range []string{"a", "b", "c"} {
		s, ok := o.Status(name)
		if !ok {
			t.Fatalf("%s missing from the graph after serialized ops", name)
		}
		if s != StatusRunning && s != StatusStopped {
			t.Errorf("%s status = %v, want a settled Running/Stopped", name, s)
		}
	}
}

// cronBlockSvc blocks inside its cron tick until the tick context is cancelled,
// signalling both the start and the return so a test can prove StopService
// cancels an in-flight tick.
type cronBlockSvc struct {
	started  chan struct{}
	returned chan struct{}
	startOne sync.Once
	retOne   sync.Once
}

func (s *cronBlockSvc) Start(ctx ServiceContext) error {
	s.startOne.Do(func() { close(s.started) })
	<-ctx.Done()
	s.retOne.Do(func() { close(s.returned) })
	return ctx.Err()
}

func (s *cronBlockSvc) Stop() error { return nil }

// TestStopService_CancelsInFlightCronTick pins the changed behavior that
// invokeCron installs a per-tick cancel func: a tick blocked in Start must
// observe cancellation once StopService returns.
func TestStopService_CancelsInFlightCronTick(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(1 * time.Second)
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	svc := &cronBlockSvc{started: make(chan struct{}), returned: make(chan struct{})}
	if err := o.Register(svc, WithName("c"), WithCron("* * * * * *", CronSkip)); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("c"); err != nil {
		t.Fatal(err)
	}

	select {
	case <-svc.started:
	case <-time.After(2 * time.Second):
		t.Fatal("cron tick never started")
	}

	if err := o.StopService("c", time.Second); err != nil {
		t.Fatalf("StopService: %v", err)
	}
	select {
	case <-svc.returned:
	case <-time.After(time.Second):
		t.Fatal("in-flight cron tick was not cancelled by StopService")
	}
}

// TestStopService_CascadeSharesOneBudget pins D8/C16: a cascade over several
// blocking dependents must share the caller's single timeout budget rather than
// multiplying it per entry.
func TestStopService_CascadeSharesOneBudget(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	base := &blockingSvc{release: make(chan struct{})}
	d1 := &blockingSvc{release: make(chan struct{})}
	d2 := &blockingSvc{release: make(chan struct{})}
	if err := o.Register(base, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(d1, WithName("d1"), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(d2, WithName("d2"), DependsOn("d1")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	const budget = 200 * time.Millisecond
	start := time.Now()
	err := o.StopService("base", budget, WithCascadeStop())
	elapsed := time.Since(start)
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("StopService = %v, want ErrStopTimeout", err)
	}
	if elapsed > 2*budget {
		t.Errorf("cascade took %v for 3 blocking entries; the budget must be shared, not per-entry", elapsed)
	}

	close(base.release)
	close(d1.release)
	close(d2.release)
	if err := o.Stop(2 * time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
