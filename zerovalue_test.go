package gorch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// The zero-value Orchestrator must be usable, not merely panic-free: the first
// public call lazily initialises it with exactly New()'s defaults (issue #87),
// so `var o Orchestrator` is a valid way to build one. These tests pin the
// defaults, the full lifecycle, the Messenger wiring, the concurrent-first-call
// path, and the fact that New is never re-initialised by a later public call.

// TestZeroValueOrchestrator_DefaultsMatchNew pins that lazy initialisation wires
// the same runtime state and defaults as New(), rather than just avoiding the
// panic.
func TestZeroValueOrchestrator_DefaultsMatchNew(t *testing.T) {
	var o Orchestrator
	// Any public call must be enough to initialise the receiver.
	_ = o.Count()

	if o.messenger == nil {
		t.Fatal("zero-value Orchestrator has a nil Messenger after its first call")
	}
	if o.nameIndex == nil {
		t.Fatal("zero-value Orchestrator has a nil name index after its first call")
	}
	if o.shutdownDone == nil {
		t.Fatal("zero-value Orchestrator has a nil shutdown channel after its first call")
	}
	if o.cfg.LogLevel != LogLevelInfo {
		t.Errorf("zero-value LogLevel = %v, want LogLevelInfo", o.cfg.LogLevel)
	}
	if o.cfg.HealthInterval != 30*time.Second {
		t.Errorf("zero-value HealthInterval = %v, want 30s", o.cfg.HealthInterval)
	}
	if o.cfg.HealthTimeout != 5*time.Second {
		t.Errorf("zero-value HealthTimeout = %v, want 5s", o.cfg.HealthTimeout)
	}
	if o.cfg.HealthThreshold != 3 {
		t.Errorf("zero-value HealthThreshold = %d, want 3", o.cfg.HealthThreshold)
	}
	if o.cfg.healthDisabled {
		t.Error("zero-value health checks must be enabled by default")
	}
	if o.cfg.failedStartTimeout != 30*time.Second {
		t.Errorf("zero-value failedStartTimeout = %v, want 30s", o.cfg.failedStartTimeout)
	}
	if o.cfg.DefaultStartTimeout != 0 {
		t.Errorf("zero-value DefaultStartTimeout = %v, want 0", o.cfg.DefaultStartTimeout)
	}
	// Done must be stable across calls: it is created once, not per call.
	first := o.Done()
	second := o.Done()
	if first != second {
		t.Fatal("Done must return the same channel on every call")
	}
}

// TestEnsureInit_NewOrchestratorStateIsPreserved pins the load-bearing property
// that New marks the initializer done: a public call after construction must not
// re-run initialization and discard the registered graph.
func TestEnsureInit_NewOrchestratorStateIsPreserved(t *testing.T) {
	o := New()
	if err := o.Register(&namedSvc{}, WithName("svc")); err != nil {
		t.Fatalf("Register = %v", err)
	}
	// A getter would trigger a second initialize if New had not consumed the
	// once, wiping nameIndex and entries.
	_ = o.Names()
	if got := o.Count(); got != 1 {
		t.Fatalf("Count after a getter = %d, want 1 (registry must survive lazy init)", got)
	}
	if s, ok := o.Status("svc"); !ok || s != StatusRegistered {
		t.Fatalf("Status(svc) = %v, %v, want StatusRegistered", s, ok)
	}
}

// TestZeroValueOrchestrator_RegisterAndStop is the exact misuse from issue #87:
// Register wrote to a nil map and Stop closed a nil channel. Both must succeed
// on a zero value.
func TestZeroValueOrchestrator_RegisterAndStop(t *testing.T) {
	var o Orchestrator
	if err := o.Register(&namedSvc{}, WithName("svc")); err != nil {
		t.Fatalf("Register on zero value = %v, want nil", err)
	}
	if got := o.Count(); got != 1 {
		t.Fatalf("Count after Register on zero value = %d, want 1", got)
	}

	// A never-started Stop is the no-op path that must close Done on a zero value
	// instead of closing a nil channel.
	var o2 Orchestrator
	if err := o2.Stop(0); err != nil {
		t.Fatalf("Stop on zero value = %v, want nil", err)
	}
	select {
	case <-o2.Done():
	default:
		t.Fatal("Stop on a zero-value Orchestrator must close Done")
	}
}

// TestZeroValueOrchestrator_FullLifecycle drives register/start/stop to
// completion on a zero-value receiver and asserts the observable state, so the
// lazy init is proven to produce a genuinely working orchestrator.
func TestZeroValueOrchestrator_FullLifecycle(t *testing.T) {
	var o Orchestrator
	started := make(chan struct{})
	stopped := make(chan struct{})
	if err := o.RegisterFunc("svc",
		func(ctx ServiceContext) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
		func() error {
			close(stopped)
			return nil
		},
	); err != nil {
		t.Fatalf("RegisterFunc on zero value = %v", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start on zero value = %v", err)
	}
	// Handshake on the service's own Start, not on a status, so the assertion is
	// deterministic under load.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("service Start never ran on a zero-value Orchestrator")
	}
	if s, ok := o.Status("svc"); !ok || s != StatusRunning {
		t.Fatalf("Status(svc) = %v, %v, want StatusRunning", s, ok)
	}
	if got := o.CountRunning(); got != 1 {
		t.Fatalf("CountRunning = %d, want 1", got)
	}
	if got := o.Metrics().Starts; got != 1 {
		t.Fatalf("Metrics().Starts = %d, want 1", got)
	}
	if err := o.StartService("svc"); err != nil {
		t.Fatalf("StartService on a running service = %v, want nil no-op", err)
	}
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop on zero value = %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("service Stop was not called")
	}
	select {
	case <-o.Done():
	default:
		t.Fatal("Done was not closed after Stop")
	}
	if s, ok := o.Status("svc"); !ok || s != StatusStopped {
		t.Fatalf("Status(svc) after Stop = %v, %v, want StatusStopped", s, ok)
	}
}

// TestZeroValueOrchestrator_MessengerIsWired proves the lazily created Messenger
// is real: without it, the ServiceContext view construction panics on a nil
// receiver. The service subscribes and publishes to itself.
func TestZeroValueOrchestrator_MessengerIsWired(t *testing.T) {
	var o Orchestrator
	got := make(chan any, 1)
	if err := o.RegisterFunc("echo", func(ctx ServiceContext) error {
		ch, unsub := ctx.Messenger.Subscribe("t")
		defer unsub()
		ctx.Messenger.Publish("pong", "t")
		select {
		case msg := <-ch:
			got <- msg
		case <-ctx.Done():
		}
		<-ctx.Done()
		return ctx.Err()
	}, nil); err != nil {
		t.Fatalf("RegisterFunc = %v", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	select {
	case msg := <-got:
		if msg != "pong" {
			t.Fatalf("delivered %v, want pong", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("zero-value Orchestrator Messenger did not deliver")
	}
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop = %v", err)
	}
}

// TestZeroValueOrchestrator_RunStartError covers Run's lazy init without relying
// on signals: a persistent service with a start timeout whose Start returns an
// error fails Start synchronously, so Run returns before it installs a signal
// handler and the path is deterministic.
func TestZeroValueOrchestrator_RunStartError(t *testing.T) {
	var o Orchestrator
	sentinel := errors.New("boom")
	if err := o.RegisterFunc("svc",
		func(ctx ServiceContext) error { return sentinel },
		nil,
		WithStartTimeout(time.Second),
	); err != nil {
		t.Fatalf("RegisterFunc = %v", err)
	}
	if err := o.Run(time.Second); !errors.Is(err, sentinel) {
		t.Fatalf("Run on a zero value with a failing Start = %v, want %v", err, sentinel)
	}
}

// TestZeroValueOrchestrator_ConcurrentFirstCalls races many first calls against
// one zero value. sync.Once must make the lazy initializer run exactly once and
// race-free; run under -race this fails on any double or torn initialisation.
func TestZeroValueOrchestrator_ConcurrentFirstCalls(t *testing.T) {
	var o Orchestrator
	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			switch i % 8 {
			case 0:
				_ = o.Names()
			case 1:
				_ = o.Metrics()
			case 2:
				_, _ = o.Status("x")
			case 3:
				_ = o.CountRunning()
			case 4:
				_ = o.Done()
			case 5:
				_ = o.Health()
			case 6:
				_ = o.Statuses()
			case 7:
				_ = o.Count()
			}
		}(i)
	}
	wg.Wait()

	// After the storm the receiver must be fully usable, not half-initialised.
	if err := o.RegisterFunc("svc", func(ctx ServiceContext) error { <-ctx.Done(); return ctx.Err() }, nil); err != nil {
		t.Fatalf("RegisterFunc after concurrent init = %v", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start after concurrent init = %v", err)
	}
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop after concurrent init = %v", err)
	}
}

// TestZeroValueOrchestrator_IsolatedGetters calls every read-only getter on a
// fresh zero value to show none of them depends on a prior mutating call having
// initialised the receiver.
func TestZeroValueOrchestrator_IsolatedGetters(t *testing.T) {
	var o Orchestrator
	if names := o.Names(); len(names) != 0 {
		t.Fatalf("Names = %v, want empty", names)
	}
	if got := o.Statuses(); len(got) != 0 {
		t.Fatalf("Statuses = %v, want empty", got)
	}
	if got := o.Count(); got != 0 {
		t.Fatalf("Count = %d, want 0", got)
	}
	if got := o.CountRunning(); got != 0 {
		t.Fatalf("CountRunning = %d, want 0", got)
	}
	if names := o.RunningNames(); len(names) != 0 {
		t.Fatalf("RunningNames = %v, want empty", names)
	}
	if _, ok := o.Status("nope"); ok {
		t.Fatal("Status(unknown) ok = true, want false")
	}
	if o.Busy("nope") {
		t.Fatal("Busy(unknown) = true, want false")
	}
	if o.IsReady(context.Background(), "nope") {
		t.Fatal("IsReady(unknown) = true, want false")
	}
	if got := o.Metrics(); got != (Metrics{}) {
		t.Fatalf("Metrics = %+v, want zero value", got)
	}
	if got := o.Health(); len(got) != 0 {
		t.Fatalf("Health = %v, want empty", got)
	}
	if got := o.StatusesByGroup("g"); len(got) != 0 {
		t.Fatalf("StatusesByGroup = %v, want empty", got)
	}
	if got := o.StatusesByLabel("k", "v"); len(got) != 0 {
		t.Fatalf("StatusesByLabel = %v, want empty", got)
	}
	if _, err := o.Dependents("nope"); !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("Dependents(unknown) = %v, want ErrServiceNotFound", err)
	}
	if _, err := o.DependenciesOf("nope"); !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("DependenciesOf(unknown) = %v, want ErrServiceNotFound", err)
	}
}
