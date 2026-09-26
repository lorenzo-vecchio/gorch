package gorch

import (
	"context"
	"errors"
	"fmt"
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
	// Register neither starts nor schedules the service: it stays
	// StatusRegistered with no live schedule until StartService (D1).
	if s, _ := o.Status("c"); s != StatusRegistered {
		t.Errorf("hot cron status = %v, want StatusRegistered", s)
	}

	o.mu.Lock()
	entry := o.nameIndex["c"]
	o.mu.Unlock()
	if entry.cronID != 0 {
		t.Error("hot-added cron entry must not be scheduled before StartService")
	}

	// Give the scheduler a full second: with no live schedule nothing ticks.
	time.Sleep(1100 * time.Millisecond)
	if got := calls.Load(); got != 0 {
		t.Fatalf("unscheduled hot cron ticked %d times, want 0", got)
	}

	if err := o.StartService("c"); err != nil {
		t.Fatalf("StartService cron: %v", err)
	}
	if s, _ := o.Status("c"); s != StatusRunning {
		t.Errorf("status after StartService = %v, want StatusRunning", s)
	}
	o.mu.Lock()
	if entry.cronID == 0 {
		t.Error("StartService must schedule the hot-added cron entry")
	}
	o.mu.Unlock()

	deadline := time.After(3 * time.Second)
	for calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("started cron service never ticked")
		case <-time.After(20 * time.Millisecond):
		}
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
		if err := o.StartService("r"); !errors.Is(err, ErrReentrantMembership) {
			t.Fatalf("got %v, want reentrant membership error", err)
		}
	})
}

// TestStartService_NotStarted pins that starting a service before the
// orchestrator has started returns a sentinel instead of panicking on a nil
// context or scheduler (a cron entry would otherwise reach a nil scheduler).
func TestStartService_NotStarted(t *testing.T) {
	o := New()
	if err := o.Register(&namedSvc{}, WithName("n")); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("n"); !errors.Is(err, ErrOrchestratorNotStarted) {
		t.Fatalf("persistent StartService before Start = %v, want ErrOrchestratorNotStarted", err)
	}
	if err := o.Register(&testSvc{}, WithName("c"), WithCron("* * * * * *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("c"); !errors.Is(err, ErrOrchestratorNotStarted) {
		t.Fatalf("cron StartService before Start = %v, want ErrOrchestratorNotStarted", err)
	}
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

	if !errors.Is(innerErr, ErrReentrantMembership) {
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
	ownerA := entryA.currentOwner()
	ownerB := entryB.currentOwner()
	if ownerA == 0 || ownerB == 0 || ownerA == ownerB {
		t.Fatalf("owner ids must be nonzero and distinct: a=%d b=%d", ownerA, ownerB)
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
	oldOwner := oldEntry.currentOwner()
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
	newOwner := o.nameIndex["svc"].currentOwner()
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
	if err := o.StopService("r", time.Second); !errors.Is(err, ErrReentrantMembership) {
		t.Errorf("StopService = %v, want reentrant membership error", err)
	}
	if err := o.Unregister("r", time.Second); !errors.Is(err, ErrReentrantMembership) {
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
// StartGroup/StopGroup and membership ops serialize their *selection*, so
// concurrent calls never interleave on entry selection. Now that user code runs
// outside the lock, a call that loses the race against an in-flight group
// start or teardown is rejected with ErrReentrantMembership rather than
// blocking; run under -race, this asserts no interleaving tears an entry
// mid-operation.
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
			if err := fn(); err != nil && !errors.Is(err, ErrReentrantMembership) {
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

// ── HSM-5 hardening: Start/Register races and teardown reentrancy ──

// TestStart_ConcurrentRegister exercises the matrix row "Register during Start":
// a Register loop runs while Start starts a blocking base graph. Start must
// snapshot the graph under o.mu so the concurrent appends are race-free, and
// every accepted entry must be either started by that Start or left registered
// for a later StartService (D1).
func TestStart_ConcurrentRegister(t *testing.T) {
	o := New(WithHealthChecksDisabled())

	blocking := func() *testSvc {
		return &testSvc{startFn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}}
	}
	// A slow before-start hook widens the window in which Start is live so the
	// Register loop overlaps it deterministically.
	slowHook := func(string) error { time.Sleep(30 * time.Millisecond); return nil }
	for _, name := range []string{"a", "b", "c"} {
		if err := o.Register(blocking(), WithName(name), WithOnBeforeStart(slowHook)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	regErr := make(chan error, 1)
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := o.Register(blocking(), WithName(fmt.Sprintf("hot-%d", i))); err != nil {
				regErr <- err
				return
			}
		}
	}()

	if err := o.Start(); err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("Start: %v", err)
	}
	close(stop)
	wg.Wait()
	select {
	case err := <-regErr:
		t.Fatalf("concurrent Register: %v", err)
	default:
	}

	for _, name := range o.Names() {
		s, ok := o.Status(name)
		if !ok {
			t.Fatalf("service %s missing from Status after Start", name)
		}
		if !strings.HasPrefix(name, "hot-") {
			if s != StatusRunning {
				t.Errorf("base %s status = %v, want StatusRunning", name, s)
			}
			continue
		}
		if s != StatusRunning && s != StatusRegistered {
			t.Errorf("hot %s status = %v, want Running (in Start's snapshot) or Registered (hot add)", name, s)
		}
	}
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestMembership_ReentrantFromStart covers the matrix row "Reentrant op from
// Start": a membership op issued from a service's own Start must return rather
// than self-deadlock, for both a direct Start and one driven by StartGroup.
func TestMembership_ReentrantFromStart(t *testing.T) {
	t.Run("direct self StopService", func(t *testing.T) {
		o := New()
		var innerErr error
		done := make(chan struct{})
		svc := &testSvc{startFn: func(ctx context.Context) error {
			innerErr = o.StopService("self", time.Second)
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
			t.Fatal("reentrant StopService from Start deadlocked")
		}
		if !errors.Is(innerErr, ErrReentrantMembership) {
			t.Errorf("self StopService = %v, want ErrReentrantMembership", innerErr)
		}
	})

	t.Run("direct other Unregister", func(t *testing.T) {
		o := New()
		other := &testSvc{startFn: func(ctx context.Context) error { return nil }}
		if err := o.Register(other, WithName("other"), WithRunOnce()); err != nil {
			t.Fatal(err)
		}
		var innerErr error
		done := make(chan struct{})
		svc := &testSvc{startFn: func(ctx context.Context) error {
			innerErr = o.Unregister("other", time.Second)
			close(done)
			return nil
		}}
		if err := o.Register(svc, WithName("driver"), WithRunOnce()); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer o.Stop(time.Second)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("reentrant Unregister from Start deadlocked")
		}
		if innerErr != nil {
			t.Errorf("Unregister(other) = %v, want nil", innerErr)
		}
		if _, ok := o.Status("other"); ok {
			t.Error("other should be removed")
		}
	})

	t.Run("StartGroup self StopService", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)

		var innerErr error
		done := make(chan struct{})
		svc := &testSvc{startFn: func(ctx context.Context) error {
			innerErr = o.StopService("g-self", time.Second)
			close(done)
			return nil
		}}
		if err := o.Register(svc, WithName("g-self"), WithRunOnce(), WithGroup("g")); err != nil {
			t.Fatal(err)
		}
		if err := o.StartGroup("g"); err != nil {
			t.Fatalf("StartGroup: %v", err)
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("reentrant StopService inside StartGroup deadlocked")
		}
		if !errors.Is(innerErr, ErrReentrantMembership) {
			t.Errorf("self StopService = %v, want ErrReentrantMembership", innerErr)
		}
	})

	t.Run("StartGroup other Unregister", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)

		if err := o.Register(&namedSvc{}, WithName("other")); err != nil {
			t.Fatal(err)
		}
		var innerErr error
		done := make(chan struct{})
		svc := &testSvc{startFn: func(ctx context.Context) error {
			innerErr = o.Unregister("other", time.Second)
			close(done)
			return nil
		}}
		if err := o.Register(svc, WithName("g-driver"), WithRunOnce(), WithGroup("g")); err != nil {
			t.Fatal(err)
		}
		if err := o.StartGroup("g"); err != nil {
			t.Fatalf("StartGroup: %v", err)
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("reentrant Unregister inside StartGroup deadlocked")
		}
		if innerErr != nil {
			t.Errorf("Unregister(other) = %v, want nil", innerErr)
		}
		if _, ok := o.Status("other"); ok {
			t.Error("other should be removed")
		}
	})
}

// TestSelfHeal_MaxRetriesVsTeardown drives the crash race deterministically: a
// self-heal service reaches maxRetries exactly as StopService cancels the
// instance, and the teardown must win so the entry ends StatusStopped, not
// StatusCrashed.
func TestSelfHeal_MaxRetriesVsTeardown(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	var n atomic.Int32
	secondStarted := make(chan struct{})
	factory := func() Service {
		if n.Add(1) == 1 {
			return &testSvc{startFn: func(ctx context.Context) error {
				return errors.New("boom")
			}}
		}
		return &testSvc{startFn: func(ctx context.Context) error {
			close(secondStarted)
			<-ctx.Done()
			return ctx.Err()
		}}
	}
	if err := o.Register(factory(), WithName("heal"),
		WithSelfHeal(factory),
		WithMaxRetries(1),
		WithBackoff(ConstantBackoff{Delay: time.Millisecond}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer o.Stop(time.Second)

	select {
	case <-secondStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("self-heal never reached the second instance")
	}

	if err := o.StopService("heal", time.Second); err != nil {
		t.Fatalf("StopService: %v", err)
	}
	if s, _ := o.Status("heal"); s != StatusStopped {
		t.Errorf("final status = %v, want StatusStopped (teardown wins the crash race)", s)
	}
}

// TestStartOneService_RemovedRejected pins that a start selected before an
// Unregister removed the entry (e.g. a StartGroup reservation) never revives it.
func TestStartOneService_RemovedRejected(t *testing.T) {
	o := New()
	entry := &serviceEntry{name: "gone", svc: &namedSvc{}, cfg: registerConfig{name: "gone"}}
	entry.removed.Store(true)
	if err := o.startOneService(entry); !errors.Is(err, ErrServiceNotFound) {
		t.Errorf("startOneService on a removed entry = %v, want ErrServiceNotFound", err)
	}
}

// TestServiceEntry_TeardownActive covers both teardown signals: the removing
// flag and a cancelled per-entry teardown context.
func TestServiceEntry_TeardownActive(t *testing.T) {
	e := &serviceEntry{}
	if e.teardownActive() {
		t.Error("fresh entry must not report a teardown")
	}
	e.removing.Store(true)
	if !e.teardownActive() {
		t.Error("removing entry must report a teardown")
	}
	e.removing.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.setTeardown(ctx, cancel)
	if e.teardownActive() {
		t.Error("live teardown context must not report a teardown")
	}
	cancel()
	if !e.teardownActive() {
		t.Error("cancelled teardown context must report a teardown")
	}
}

// TestStopGroup_HonorsStartGroupReservation covers the StopGroup regression: it
// released membershipMu before stopping and could stop an entry an in-flight
// StartGroup had reserved. While the group start is blocked in a synchronous
// service Start, StopGroup must skip that entry rather than stop it.
func TestStopGroup_HonorsStartGroupReservation(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	entered := make(chan struct{})
	release := make(chan struct{})
	svc := &testSvc{startFn: func(ctx context.Context) error {
		close(entered)
		<-release
		return nil
	}}
	if err := o.Register(svc, WithName("g1"), WithGroup("g"), WithStartTimeout(3*time.Second)); err != nil {
		t.Fatal(err)
	}

	startDone := make(chan error, 1)
	go func() { startDone <- o.StartGroup("g") }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("group service never entered Start")
	}

	if err := o.StopGroup("g", time.Second); err != nil {
		t.Fatalf("StopGroup: %v", err)
	}
	if got := svc.stopCalls.Load(); got != 0 {
		t.Errorf("StopGroup stopped a reserved entry: Stop calls = %d, want 0", got)
	}

	close(release)
	if err := <-startDone; err != nil {
		t.Fatalf("StartGroup: %v", err)
	}
}

// TestStopService_NonCanceledExitIsStopped covers a non-self-heal service that
// returns a real error just as StopService cancels it: the teardown wins, so it
// ends StatusStopped, does not fire OnCrash, and does not count as a crash.
func TestStopService_NonCanceledExitIsStopped(t *testing.T) {
	var crashCalled atomic.Bool
	o := New(
		WithHealthChecksDisabled(),
		WithOnCrash(func(string, error) { crashCalled.Store(true) }),
	)
	started := make(chan struct{})
	svc := &testSvc{startFn: func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return errors.New("shutdown failed")
	}}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer o.Stop(time.Second)
	<-started

	if err := o.StopService("s", time.Second); err != nil {
		t.Fatalf("StopService: %v", err)
	}
	if s, _ := o.Status("s"); s != StatusStopped {
		t.Errorf("status = %v, want StatusStopped", s)
	}
	if crashCalled.Load() {
		t.Error("OnCrash fired for a teardown-owned exit")
	}
	if got := o.Metrics().Crashes; got != 0 {
		t.Errorf("Crashes = %d, want 0", got)
	}
}

// overlapCronSvc runs every tick concurrently and records how many started,
// are live, and have exited, so a test can prove teardown cancels every one of
// them and that a re-scheduled generation accepts ticks again.
type overlapCronSvc struct {
	calls   atomic.Int32
	running atomic.Int32
	exited  atomic.Int32
	once    sync.Once
	entered chan struct{}
	mu      sync.Mutex
	chans   []<-chan any
}

func (s *overlapCronSvc) Start(ctx ServiceContext) error {
	ch, _ := ctx.Messenger.Subscribe("t")
	s.mu.Lock()
	s.chans = append(s.chans, ch)
	s.mu.Unlock()
	s.calls.Add(1)
	s.running.Add(1)
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	s.running.Add(-1)
	s.exited.Add(1)
	return ctx.Err()
}

func (s *overlapCronSvc) Stop() error { return nil }

func (s *overlapCronSvc) channel(i int) <-chan any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chans[i]
}

// TestServiceEntry_CronTracking covers the cron tracking helpers directly: a
// fresh entry refuses a tick, startCronSchedule installs a context, a stale
// generation is refused, and cronDrain cancels and waits for in-flight ticks.
func TestServiceEntry_CronTracking(t *testing.T) {
	var e serviceEntry
	if _, ok := e.cronBegin(0); ok {
		t.Error("cronBegin before any schedule must be refused")
	}
	gen := e.startCronSchedule(context.Background())
	if gen != e.cronGeneration() {
		t.Error("startCronSchedule must return the current generation")
	}
	ctx, ok := e.cronBegin(gen)
	if !ok || ctx == nil {
		t.Fatal("cronBegin after a schedule must return the shared context")
	}
	if _, ok := e.cronBegin(gen + 1); ok {
		t.Error("cronBegin for a stale generation must be refused")
	}
	e.cronEnd(gen - 1) // stale end must not touch the current accounting
	drained := e.cronDrain()
	if drained == nil {
		t.Fatal("cronDrain with an in-flight tick must return a latch")
	}
	select {
	case <-drained:
		t.Fatal("drain latch closed before cronEnd")
	default:
	}
	if _, ok := e.cronBegin(gen); ok {
		t.Error("cronBegin while draining must be refused")
	}
	e.cronEnd(gen)
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("drain latch did not close after cronEnd")
	}
	if e.cronDrain() != nil {
		t.Error("a fully drained schedule must return a nil latch")
	}

	// A scheduler tick with no live schedule is refused before running user code.
	New().invokeCron(&serviceEntry{}, 0)
}

// TestServiceEntry_ReleaseTeardownIf covers the identity guard: an exit handler
// must not cancel a teardown a concurrent restart has installed.
func TestServiceEntry_ReleaseTeardownIf(t *testing.T) {
	e := &serviceEntry{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.setTeardown(ctx, cancel)

	other, otherCancel := context.WithCancel(context.Background())
	defer otherCancel()
	e.releaseTeardownIf(other) // not ours: must leave it in place
	if e.getTeardown() == nil {
		t.Fatal("releaseTeardownIf cleared a teardown it does not own")
	}
	e.releaseTeardownIf(ctx)
	if e.getTeardown() != nil {
		t.Error("releaseTeardownIf did not clear its own teardown")
	}
	if ctx.Err() == nil {
		t.Error("releaseTeardownIf did not cancel its own teardown")
	}
}

// TestStopService_CancelsAllParallelCronTicks covers the "Parallel ticks
// cancelled" matrix row: with overlapping CronParallel ticks, StopService must
// cancel and await every one, not only the newest.
func TestStopService_CancelsAllParallelCronTicks(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(time.Second)
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	svc := &overlapCronSvc{entered: make(chan struct{})}
	if err := o.Register(svc, WithName("p"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("p"); err != nil {
		t.Fatal(err)
	}
	o.mu.RLock()
	entry := o.nameIndex["p"]
	o.mu.RUnlock()

	go o.invokeCron(entry, entry.cronGeneration())
	go o.invokeCron(entry, entry.cronGeneration())
	select {
	case <-svc.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("cron tick never started")
	}
	deadline := time.After(time.Second)
	for svc.running.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("second parallel tick did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	if err := o.StopService("p", time.Second); err != nil {
		t.Fatalf("StopService: %v", err)
	}
	if got := svc.exited.Load(); got != 2 {
		t.Errorf("exited ticks = %d, want 2 (all cancelled and awaited)", got)
	}
	if owner := entry.currentOwner(); owner != 0 {
		t.Errorf("after teardown the entry must own no Messenger owner, got %d", owner)
	}
	for i := 0; i < 2; i++ {
		select {
		case _, ok := <-svc.channel(i):
			if ok {
				t.Errorf("tick %d subscription must be drained by teardown", i)
			}
		default:
			t.Errorf("tick %d subscription channel must be closed", i)
		}
	}
}

// TestStopService_ReschedulesCronAfterDrain covers the "Re-schedule after stop"
// matrix row: after a stop that drained a running tick, StartService must install
// a fresh generation that accepts ticks again.
func TestStopService_ReschedulesCronAfterDrain(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(time.Second)
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	svc := &overlapCronSvc{entered: make(chan struct{})}
	if err := o.Register(svc, WithName("p"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("p"); err != nil {
		t.Fatal(err)
	}
	o.mu.RLock()
	entry := o.nameIndex["p"]
	o.mu.RUnlock()

	go o.invokeCron(entry, entry.cronGeneration())
	select {
	case <-svc.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("cron tick never started")
	}
	if err := o.StopService("p", time.Second); err != nil {
		t.Fatalf("StopService: %v", err)
	}
	if got := svc.exited.Load(); got != 1 {
		t.Fatalf("exited ticks = %d, want 1", got)
	}

	if err := o.StartService("p"); err != nil {
		t.Fatalf("StartService: %v", err)
	}
	go o.invokeCron(entry, entry.cronGeneration())
	deadline := time.After(time.Second)
	for svc.calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("a re-scheduled cron entry never accepted a tick")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestStopService_DeadlineBoundsStop covers the "Blocking Stop bounded" matrix
// row: with no WithStopTimeout, the StopService deadline must cap a blocking
// Stop() so the call returns promptly with ErrStopTimeout.
func TestStopService_DeadlineBoundsStop(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn:  func() error { <-release; return nil },
	}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		_ = o.Stop(time.Second)
	}()

	const budget = 100 * time.Millisecond
	start := time.Now()
	err := o.StopService("s", budget)
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("StopService = %v, want ErrStopTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > 2*budget {
		t.Errorf("StopService took %v; the deadline did not bound Stop()", elapsed)
	}
}

// TestStopTimeout_StatusIsNotStopped pins that a stop whose caller deadline
// expires while Stop() is still blocking must not claim StatusStopped: the
// service may still be alive holding resources.
func TestStopTimeout_StatusIsNotStopped(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-release; return nil },
		stopFn:  func() error { <-release; return nil },
	}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		_ = o.Stop(time.Second)
	}()
	time.Sleep(20 * time.Millisecond)

	err := o.StopService("s", 50*time.Millisecond)
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("StopService = %v, want ErrStopTimeout", err)
	}
	if s, _ := o.Status("s"); s == StatusStopped {
		t.Fatalf("Status = %s after a timed-out stop; want not stopped (service may still be alive)", s)
	}
}

// TestStopTimeout_DoesNotCountStop pins that a stop whose caller deadline
// expired is not counted in the public Stops metric: the stop did not complete.
func TestStopTimeout_DoesNotCountStop(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-release; return nil },
		stopFn:  func() error { <-release; return nil },
	}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		_ = o.Stop(time.Second)
	}()
	time.Sleep(20 * time.Millisecond)

	before := o.Metrics().Stops
	err := o.StopService("s", 50*time.Millisecond)
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("StopService = %v, want ErrStopTimeout", err)
	}
	if got := o.Metrics().Stops; got != before {
		t.Fatalf("Stops = %d after a timed-out stop, want %d (unchanged)", got, before)
	}
}

// TestStop_HookOverrun_StatusHonest pins that a before-stop hook that overruns
// its share of the budget leaves the teardown unverified: the stop error reports
// ErrHookTimeout and the entry is not claimed Stopped, even though the service's
// own Stop() ran with the remainder.
func TestStop_HookOverrun_StatusHonest(t *testing.T) {
	release := make(chan struct{})
	o := New(
		WithHealthChecksDisabled(),
		WithGlobalOnBeforeStop(func(string) error { <-release; return nil }),
	)
	svc := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		_ = o.Stop(time.Second)
	}()
	time.Sleep(20 * time.Millisecond)

	before := o.Metrics().Stops
	err := o.StopService("s", time.Second)
	if !errors.Is(err, ErrHookTimeout) {
		t.Fatalf("StopService = %v, want ErrHookTimeout", err)
	}
	if s, _ := o.Status("s"); s == StatusStopped {
		t.Fatalf("Status = %s after an overrunning before-stop hook; want not stopped", s)
	}
	if got := o.Metrics().Stops; got != before {
		t.Fatalf("Stops = %d after an unverified hook overrun, want %d (unchanged)", got, before)
	}
}

// TestStopTimeout_PerServiceCap_StatusNotStopped pins that a Stop() capped away
// by WithStopTimeout (the orchestrator gave up waiting but Stop() is still
// running) is not reported as a completed stop.
func TestStopTimeout_PerServiceCap_StatusNotStopped(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn:  func() error { <-release; return nil },
	}
	if err := o.Register(svc, WithName("s"), WithStopTimeout(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		_ = o.Stop(time.Second)
	}()
	time.Sleep(20 * time.Millisecond)

	before := o.Metrics().Stops
	err := o.StopService("s", 5*time.Second)
	if err == nil {
		t.Fatal("expected the per-service stop cap to fire")
	}
	if errors.Is(err, ErrStopTimeout) {
		t.Errorf("per-service cap error = %v, must not be the caller's ErrStopTimeout", err)
	}
	if s, _ := o.Status("s"); s == StatusStopped {
		t.Fatalf("Status = %s after the per-service cap fired; want not stopped", s)
	}
	if got := o.Metrics().Stops; got != before {
		t.Fatalf("Stops = %d after a capped stop, want %d (unchanged)", got, before)
	}
}

// TestStopService_InstanceExitedBeforeStopReturns_StatusStopping pins the
// teardown-ownership rule: while StopService is still blocked in Stop(), an
// instance that already exited on context cancellation must not by itself flip
// the status to Stopped. The stop path owns the terminal status.
func TestStopService_InstanceExitedBeforeStopReturns_StatusStopping(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	exited := make(chan struct{})
	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			<-ctx.Done()
			close(exited)
			return ctx.Err()
		},
		stopFn: func() error { <-release; return nil },
	}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseAll()
		_ = o.Stop(time.Second)
	}()
	time.Sleep(20 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- o.StopService("s", 5*time.Second) }()

	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("service Start did not exit on context cancellation")
	}
	if s, _ := o.Status("s"); s == StatusStopped {
		t.Fatalf("Status = %s while Stop() is still blocked; want Stopping", s)
	}
	releaseAll() // let Stop() return; from here on the stop may commit
	if err := <-done; err != nil {
		t.Fatalf("StopService = %v, want nil once Stop() returned", err)
	}
	if s, _ := o.Status("s"); s != StatusStopped {
		t.Fatalf("Status = %s after a completed stop, want Stopped", s)
	}
}

// TestStopOneService_PastDeadline covers the boundary where the caller deadline
// has already elapsed: the hook+Stop sequence is bounded to a sliver instead of
// running unbounded, so StopService reports ErrStopTimeout promptly.
func TestStopOneService_PastDeadline(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	entry := &serviceEntry{
		name:   "s",
		svc:    &testSvc{stopFn: func() error { time.Sleep(50 * time.Millisecond); return nil }},
		cfg:    registerConfig{name: "s"},
		status: StatusRunning,
	}
	_, _, err := o.stopOneServiceDeadline(entry, time.Now().Add(-time.Second))
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("past-deadline stop = %v, want ErrStopTimeout", err)
	}
}

// TestStopService_PerServiceCapWins covers the "Per-service cap wins" matrix row:
// WithStopTimeout smaller than the caller's deadline still decides when the stop
// returns.
func TestStopService_PerServiceCapWins(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn:  func() error { <-release; return nil },
	}
	if err := o.Register(svc, WithName("s"), WithStopTimeout(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		_ = o.Stop(time.Second)
	}()

	start := time.Now()
	err := o.StopService("s", 5*time.Second)
	if err == nil {
		t.Fatal("expected a stop timeout from the per-service cap")
	}
	if errors.Is(err, ErrStopTimeout) {
		t.Errorf("per-service cap error = %v, must not be the caller's ErrStopTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("StopService took %v; the per-service cap did not apply", elapsed)
	}
}

// TestHandleServiceDone_ReleasesTeardown covers the "Normal exit releases
// teardown" matrix row: a non-self-heal instance that exits clears its per-entry
// teardown context instead of leaking it across cycles.
func TestHandleServiceDone_ReleasesTeardown(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	svc := &testSvc{startFn: func(ctx context.Context) error { return nil }}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	deadline := time.After(2 * time.Second)
	for {
		o.mu.RLock()
		entry := o.nameIndex["s"]
		o.mu.RUnlock()
		if entry.getTeardown() == nil {
			return
		}
		select {
		case <-deadline:
			t.Fatal("teardown context must be released after a normal exit")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestMessenger_PostDrainSubscribeRefused covers the retired-view guard: a
// scoped view whose owner was drained cannot resurrect subscriptions, and the
// channel it receives is already closed.
func TestMessenger_PostDrainSubscribeRefused(t *testing.T) {
	m := newMessenger()
	view := m.scoped(11)
	first, unsub := view.Subscribe("t")
	if unsub == nil {
		t.Fatal("Subscribe must return an unsubscribe func")
	}
	if first == nil {
		t.Fatal("Subscribe must return a channel")
	}
	m.drainOwner(11)

	ch, noop := view.Subscribe("t")
	noop() // no-op unsubscribe must be safe
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("a post-drain Subscribe must return a closed channel")
		}
	default:
		t.Error("a post-drain Subscribe channel must already be closed")
	}
	// Publishing must not reach (or panic on) the refused channel.
	m.Publish("late", "t")
}

// TestServiceEntry_CurrentOwnerEmpty covers the empty case of currentOwner.
func TestServiceEntry_CurrentOwnerEmpty(t *testing.T) {
	if got := (&serviceEntry{}).currentOwner(); got != 0 {
		t.Errorf("currentOwner of an idle entry = %d, want 0", got)
	}
}

// subTrackingSvc records every channel it subscribes to, so a test can prove an
// instance's subscriptions are released when it exits.
type subTrackingSvc struct {
	once  sync.Once
	ready chan struct{}
	mu    sync.Mutex
	chans []<-chan any
}

func (s *subTrackingSvc) Start(ctx ServiceContext) error {
	ch, _ := ctx.Messenger.Subscribe("t")
	s.mu.Lock()
	s.chans = append(s.chans, ch)
	s.mu.Unlock()
	s.once.Do(func() { close(s.ready) })
	<-ctx.Done()
	return ctx.Err()
}

func (s *subTrackingSvc) Stop() error { return nil }

func (s *subTrackingSvc) channel(i int) <-chan any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chans[i]
}

func (s *subTrackingSvc) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.chans)
}

// TestStopService_RestartDrainsInstanceSubs covers the "Restart drains old subs"
// matrix row: stopping an instance releases only its subscriptions, and a restart
// subscribes again under a fresh owner that still receives.
func TestStopService_RestartDrainsInstanceSubs(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(time.Second)
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	svc := &subTrackingSvc{ready: make(chan struct{})}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("s"); err != nil {
		t.Fatal(err)
	}
	<-svc.ready

	if err := o.StopService("s", time.Second); err != nil {
		t.Fatalf("StopService: %v", err)
	}
	select {
	case _, ok := <-svc.channel(0):
		if ok {
			t.Error("the stopped instance's subscription must be closed")
		}
	default:
		t.Error("the stopped instance's subscription channel must be closed immediately")
	}

	if err := o.StartService("s"); err != nil {
		t.Fatalf("StartService: %v", err)
	}
	deadline := time.After(2 * time.Second)
	for svc.count() < 2 {
		select {
		case <-deadline:
			t.Fatal("restart did not subscribe")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	o.messenger.Publish("hello", "t")
	select {
	case _, ok := <-svc.channel(1):
		if !ok {
			t.Error("the restarted instance's subscription must be live")
		}
	case <-time.After(time.Second):
		t.Error("the restarted instance must receive after restart")
	}
}

// cronSubSvc subscribes on every tick and returns immediately.
type cronSubSvc struct {
	once       sync.Once
	subscribed chan struct{}
	mu         sync.Mutex
	chans      []<-chan any
}

func (s *cronSubSvc) Start(ctx ServiceContext) error {
	ch, _ := ctx.Messenger.Subscribe("t")
	s.mu.Lock()
	s.chans = append(s.chans, ch)
	s.mu.Unlock()
	s.once.Do(func() { close(s.subscribed) })
	return nil
}

func (s *cronSubSvc) Stop() error { return nil }

func (s *cronSubSvc) channel(i int) <-chan any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chans[i]
}

// TestInvokeCron_ReleasesTickOwner covers the "Tick releases subs" matrix row: a
// finished tick leaves no live owner behind.
func TestInvokeCron_ReleasesTickOwner(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(time.Second)
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	svc := &cronSubSvc{subscribed: make(chan struct{})}
	if err := o.Register(svc, WithName("c"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("c"); err != nil {
		t.Fatal(err)
	}
	o.mu.RLock()
	entry := o.nameIndex["c"]
	o.mu.RUnlock()

	o.invokeCron(entry, entry.cronGeneration()) // synchronous; the tick returns

	<-svc.subscribed
	if owner := entry.currentOwner(); owner != 0 {
		t.Errorf("a finished tick must leave no live owner, got %d", owner)
	}
	select {
	case _, ok := <-svc.channel(0):
		if ok {
			t.Error("a finished tick's subscription must be drained")
		}
	default:
		t.Error("a finished tick's subscription channel must be closed")
	}
}

// subOnceSvc subscribes then returns immediately, so its instance ends without
// a teardown.
type subOnceSvc struct {
	once  sync.Once
	ready chan struct{}
	mu    sync.Mutex
	ch    <-chan any
}

func (s *subOnceSvc) Start(ctx ServiceContext) error {
	ch, _ := ctx.Messenger.Subscribe("t")
	s.mu.Lock()
	s.ch = ch
	s.mu.Unlock()
	s.once.Do(func() { close(s.ready) })
	return nil
}

func (s *subOnceSvc) Stop() error { return nil }

func (s *subOnceSvc) channel() <-chan any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ch
}

// TestRunOnce_ReleasesSubscription covers the runOnce release: a one-shot
// service's subscriptions are gone once its Start returns.
func TestRunOnce_ReleasesSubscription(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(time.Second)
	svc := &subOnceSvc{ready: make(chan struct{})}
	if err := o.Register(svc, WithName("r"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	<-svc.ready

	o.mu.RLock()
	entry := o.nameIndex["r"]
	o.mu.RUnlock()
	if owner := entry.currentOwner(); owner != 0 {
		t.Errorf("runOnce must release its owner, got %d", owner)
	}
	select {
	case _, ok := <-svc.channel():
		if ok {
			t.Error("a runOnce subscription must be released")
		}
	default:
		t.Error("a runOnce subscription channel must be closed")
	}
}

// TestHandleServiceDone_ReleasesInstanceSubs covers the natural-exit release: a
// non-self-heal instance that returns on its own has its subscriptions drained.
func TestHandleServiceDone_ReleasesInstanceSubs(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(time.Second)
	svc := &subOnceSvc{ready: make(chan struct{})}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	<-svc.ready

	deadline := time.After(2 * time.Second)
	for {
		o.mu.RLock()
		entry := o.nameIndex["s"]
		o.mu.RUnlock()
		if entry.currentOwner() == 0 {
			// Owner bookkeeping and the Messenger drain happen back to back, so
			// the subscription channel may close a moment after the owner count
			// drops to zero. Wait for the close rather than racing it.
			select {
			case _, ok := <-svc.channel():
				if ok {
					t.Error("a naturally exited instance's subscription must be drained")
				}
				return
			case <-deadline:
				t.Error("a naturally exited instance's channel must be closed")
				return
			}
		}
		select {
		case <-deadline:
			t.Fatal("subscription was not released after a natural exit")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// healSubSvc crashes on its first Start and blocks on the second, recording each
// instance's subscription channel.
type healSubSvc struct {
	mu       sync.Mutex
	chans    []<-chan any
	second   chan struct{}
	secondOn sync.Once
}

func (s *healSubSvc) Start(ctx ServiceContext) error {
	ch, _ := ctx.Messenger.Subscribe("t")
	s.mu.Lock()
	s.chans = append(s.chans, ch)
	n := len(s.chans)
	s.mu.Unlock()
	if n == 1 {
		return errors.New("boom")
	}
	if n == 2 {
		s.secondOn.Do(func() { close(s.second) })
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *healSubSvc) Stop() error { return nil }

func (s *healSubSvc) channel(i int) <-chan any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chans[i]
}

// TestSelfHeal_RestartDrainsCrashedSubs covers the self-heal restart drain: the
// crashed instance's subscription is released and the restarted instance owns a
// fresh, live owner.
func TestSelfHeal_RestartDrainsCrashedSubs(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(time.Second)
	svc := &healSubSvc{second: make(chan struct{})}
	if err := o.Register(svc, WithName("h"),
		WithSelfHeal(func() Service { return svc }),
		WithMaxRetries(1),
		WithBackoff(ConstantBackoff{Delay: time.Millisecond}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-svc.second:
	case <-time.After(2 * time.Second):
		t.Fatal("self-heal never restarted the service")
	}

	select {
	case _, ok := <-svc.channel(0):
		if ok {
			t.Error("the crashed instance's subscription must be drained on restart")
		}
	default:
		t.Error("the crashed instance's subscription channel must be closed")
	}
	o.mu.RLock()
	entry := o.nameIndex["h"]
	o.mu.RUnlock()
	if entry.currentOwner() == 0 {
		t.Error("the restarted instance must own a live owner")
	}
}

// TestStartService_SelfHealBackoff_IsNoOp pins that StartService does not start
// a second instance behind a live self-heal service whose status is Crashed
// during a pending restart. The idempotence guard must key on the live instance
// (entry.done), not on the transient status, or it leaks the wait group: each
// duplicate adds one, but the entry's exit latch releases only one.
func TestStartService_SelfHealBackoff_IsNoOp(t *testing.T) {
	crashErr := errors.New("boom")
	crashSeen := make(chan struct{}, 1)
	o := New(
		WithLogLevel(LogLevelWarn),
		WithHealthChecksDisabled(),
		WithOnCrash(func(name string, err error) { crashSeen <- struct{}{} }),
	)
	initial := &testSvc{startFn: func(ctx context.Context) error { return crashErr }}
	if err := o.Register(initial, WithName("healer"),
		WithSelfHeal(func() Service { return &namedSvc{} }),
		WithBackoff(ConstantBackoff{Delay: 2 * time.Second}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer o.Stop(time.Second)

	// The crash has been observed, so the status is Crashed and a long backoff
	// is pending; the instance goroutine is still live (entry.done is open).
	<-crashSeen
	startsBefore := o.Metrics().Starts

	if err := o.StartService("healer"); err != nil {
		t.Fatalf("StartService during self-heal backoff = %v, want nil (no-op)", err)
	}
	if got := initial.startCalls.Load(); got != 1 {
		t.Errorf("StartService started a second instance: startCalls = %d, want 1", got)
	}
	if got := o.Metrics().Starts; got != startsBefore {
		t.Errorf("StartService during backoff changed Starts: %d -> %d", startsBefore, got)
	}
}

// TestMessenger_RequestOnDrainedOwner covers the refusal path: Request,
// RequestAsync and TypedRequest must report an error, never a nil reply.
func TestMessenger_RequestOnDrainedOwner(t *testing.T) {
	m := newMessenger()
	view := m.scoped(42)
	m.drainOwner(42)

	if _, err := view.Request(context.Background(), "x", "t"); err == nil {
		t.Error("Request on a drained owner must fail")
	}
	if _, err := view.RequestAsync(context.Background(), "x", "t"); err == nil {
		t.Error("RequestAsync on a drained owner must fail")
	}
	if _, err := TypedRequest[string, string](view, context.Background(), "x", "t"); err == nil {
		t.Error("TypedRequest on a drained owner must fail")
	}
}

// TestMessenger_GlobalDrainRetiresOwners covers the Drain/owner symmetry: an
// existing scoped view cannot resubscribe after a global Drain, while a fresh
// view (lazy re-init) still can.
func TestMessenger_GlobalDrainRetiresOwners(t *testing.T) {
	m := newMessenger()
	view := m.scoped(5)
	m.Drain()
	select {
	case _, ok := <-mustSubscribe(t, view, "t"):
		if ok {
			t.Error("a pre-Drain view must not resubscribe after a global Drain")
		}
	default:
		t.Error("a pre-Drain view's post-Drain channel must be closed")
	}

	fresh := m.scoped(5)
	ch, unsub := fresh.Subscribe("t")
	defer unsub()
	m.Publish("v", "t")
	select {
	case got := <-ch:
		if got != "v" {
			t.Errorf("fresh view got %v, want v", got)
		}
	case <-time.After(time.Second):
		t.Error("a fresh view must subscribe after a global Drain")
	}
}

func mustSubscribe(t *testing.T, m *Messenger, topic string) <-chan any {
	t.Helper()
	ch, _ := m.Subscribe(topic)
	return ch
}
