package gorch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestGate_SucceededThenStopped_StatusIsDocumented pins the resolution of the
// gate-status question: a runOnce gate that reached StatusSucceeded keeps that
// status when it is later stopped. "Succeeded" is a permanent fact about the
// gate, not a transient label, so a teardown must not demote it to
// StatusStopped — doing so would erase the very distinction the status exists
// for and read as a failed dependency to a dependent started afterwards. The
// gate's Stop() is still called (it must be idempotent).
func TestGate_SucceededThenStopped_StatusIsDocumented(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	gate := &testSvc{startFn: func(context.Context) error { return nil }}
	if err := o.Register(gate, WithName("gate"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	if s, _ := o.Status("gate"); s != StatusSucceeded {
		t.Fatalf("fresh gate status = %v, want StatusSucceeded", s)
	}

	if err := o.StopService("gate", time.Second); err != nil {
		t.Fatalf("StopService(gate): %v", err)
	}
	if s, _ := o.Status("gate"); s != StatusSucceeded {
		t.Fatalf("gate status after StopService = %v, want StatusSucceeded", s)
	}
	if got := gate.stopCalls.Load(); got != 1 {
		t.Errorf("gate Stop() calls = %d, want 1 (Stop still runs on a succeeded gate)", got)
	}

	// Whole-orchestrator Stop must not demote it either.
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if s, _ := o.Status("gate"); s != StatusSucceeded {
		t.Errorf("gate status after Stop = %v, want StatusSucceeded", s)
	}
}

// TestGate_Stopped_DoesNotAbortDependentStart pins the consequence that made the
// status worth preserving: a hard dependent of a succeeded gate can still start
// after the gate was stopped. StartService accepts a dependency that is either
// StatusRunning or a runOnce gate in StatusSucceeded, so stopping the gate does
// not retroactively turn it into a failed dependency.
func TestGate_Stopped_DoesNotAbortDependentStart(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	gate := &testSvc{startFn: func(context.Context) error { return nil }}
	if err := o.Register(gate, WithName("gate"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	// Stop the gate before the dependent exists: a plain stop, not a cascade.
	if err := o.StopService("gate", time.Second); err != nil {
		t.Fatalf("StopService(gate): %v", err)
	}
	if s, _ := o.Status("gate"); s != StatusSucceeded {
		t.Fatalf("gate status after StopService = %v, want StatusSucceeded", s)
	}

	// Hot-add a hard dependent of the now-stopped gate.
	workerStarted := make(chan struct{})
	worker := &testSvc{startFn: func(ctx context.Context) error {
		close(workerStarted)
		<-ctx.Done()
		return ctx.Err()
	}}
	if err := o.Register(worker, WithName("worker"), DependsOn("gate")); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("worker"); err != nil {
		t.Fatalf("StartService(worker) behind a succeeded gate = %v, want nil", err)
	}
	select {
	case <-workerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("worker was never started behind a succeeded gate")
	}
	if s, _ := o.Status("worker"); s != StatusRunning {
		t.Errorf("worker status = %v, want StatusRunning", s)
	}

	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestGate_NotSucceeded_BlocksDependentStart pins the other side of the rule: a
// runOnce gate that was skipped (start condition false) never reached
// StatusSucceeded, so it is as much a failed dependency as a stopped persistent
// service and StartService refuses the dependent with ErrDependencyNotRunning.
func TestGate_NotSucceeded_BlocksDependentStart(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	gate := &testSvc{startFn: func(context.Context) error { return nil }}
	if err := o.Register(gate, WithName("gate"), WithRunOnce(),
		WithStartCondition(func() bool { return false })); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	if s, _ := o.Status("gate"); s != StatusStopped {
		t.Fatalf("skipped gate status = %v, want StatusStopped", s)
	}

	worker := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(worker, WithName("worker"), DependsOn("gate")); err != nil {
		t.Fatal(err)
	}
	err := o.StartService("worker")
	if !errors.Is(err, ErrDependencyNotRunning) {
		t.Fatalf("StartService(worker) behind a skipped gate = %v, want ErrDependencyNotRunning", err)
	}
	if got := worker.startCalls.Load(); got != 0 {
		t.Errorf("worker Start() calls = %d, want 0 (refused by the gate)", got)
	}

	_ = o.Stop(time.Second)
}

// TestRunOnce_StopIsIdempotent pins the documented contract: Stop() is called
// when a runOnce gate is stopped, and it must be safe to call more than once.
// Repeated StopService calls each run the (idempotent) Stop() and never demote
// the gate.
func TestRunOnce_StopIsIdempotent(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	var stops int
	gate := &testSvc{
		startFn: func(context.Context) error { return nil },
		stopFn:  func() error { stops++; return nil },
	}
	if err := o.Register(gate, WithName("gate"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if err := o.StopService("gate", time.Second); err != nil {
			t.Fatalf("StopService #%d: %v", i+1, err)
		}
	}
	if stops != 2 {
		t.Errorf("Stop() calls = %d, want 2 (one per StopService)", stops)
	}
	if s, _ := o.Status("gate"); s != StatusSucceeded {
		t.Fatalf("gate status after repeated StopService = %v, want StatusSucceeded", s)
	}

	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if s, _ := o.Status("gate"); s != StatusSucceeded {
		t.Errorf("gate status after Stop = %v, want StatusSucceeded", s)
	}
	if stops != 3 {
		t.Errorf("Stop() calls after shutdown = %d, want 3 (shutdown stops the gate once more)", stops)
	}
}

// TestOrchestratorStop_GateStatus pins that a whole-orchestrator Stop over a
// graph containing a succeeded gate reports the gate as documented:
// StatusSucceeded, not StatusStopped.
func TestOrchestratorStop_GateStatus(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	gate := &testSvc{startFn: func(context.Context) error { return nil }}
	main := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(gate, WithName("gate"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(main, WithName("main")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, o, "main", StatusRunning)
	if s, _ := o.Status("gate"); s != StatusSucceeded {
		t.Fatalf("gate status before Stop = %v, want StatusSucceeded", s)
	}

	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if s, _ := o.Status("gate"); s != StatusSucceeded {
		t.Errorf("gate status after Stop = %v, want StatusSucceeded", s)
	}
	if s, _ := o.Status("main"); s != StatusStopped {
		t.Errorf("persistent service status after Stop = %v, want StatusStopped", s)
	}
	if got := gate.stopCalls.Load(); got != 1 {
		t.Errorf("gate Stop() calls = %d, want 1", got)
	}
}

// TestGate_CascadeStop_PreservesSuccess pins that the cascade path — the shared
// teardown machinery WithCascadeStop and the group ops funnel through — also
// leaves a succeeded gate Succeeded while tearing its dependent down.
func TestGate_CascadeStop_PreservesSuccess(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	gate := &testSvc{startFn: func(context.Context) error { return nil }}
	worker := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(gate, WithName("gate"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(worker, WithName("worker"), DependsOn("gate")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, o, "worker", StatusRunning)

	// A plain stop of the gate would break the running dependent.
	if err := o.StopService("gate", time.Second); !errors.Is(err, ErrHasDependents) {
		t.Fatalf("plain StopService(gate) = %v, want ErrHasDependents", err)
	}
	if err := o.StopService("gate", time.Second, WithCascadeStop()); err != nil {
		t.Fatalf("cascade StopService(gate): %v", err)
	}
	if s, _ := o.Status("gate"); s != StatusSucceeded {
		t.Errorf("gate status after cascade = %v, want StatusSucceeded", s)
	}
	if s, _ := o.Status("worker"); s != StatusStopped {
		t.Errorf("worker status after cascade = %v, want StatusStopped", s)
	}

	_ = o.Stop(time.Second)
}

// TestGate_SucceededGateStop_ReportsNoStateChange pins that stopping a succeeded
// gate fires no OnStateChange transition: the status does not move, so there is
// no event and no spurious succeeded -> stopping -> succeeded flap for
// observers.
func TestGate_SucceededGateStop_ReportsNoStateChange(t *testing.T) {
	var mu sync.Mutex
	var transitions []ServiceStatus
	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnStateChange(func(_ string, _, to ServiceStatus) {
			mu.Lock()
			transitions = append(transitions, to)
			mu.Unlock()
		}),
	)
	gate := &testSvc{startFn: func(context.Context) error { return nil }}
	if err := o.Register(gate, WithName("gate"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	before := len(transitions)
	mu.Unlock()

	if err := o.StopService("gate", time.Second); err != nil {
		t.Fatalf("StopService(gate): %v", err)
	}
	mu.Lock()
	added := append([]ServiceStatus(nil), transitions[before:]...)
	mu.Unlock()
	if len(added) != 0 {
		t.Errorf("stopping a succeeded gate fired transitions %v, want none", added)
	}

	_ = o.Stop(time.Second)
}
