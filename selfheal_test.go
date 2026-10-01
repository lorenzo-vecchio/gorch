package gorch

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// selfHealFactory builds fresh recordingService incarnations for a self-heal
// test. Incarnations 1 through crashes fail Start with crashErr; every later
// incarnation blocks until its context is cancelled, so a test can observe the
// restart without racing an immediate re-crash.
type selfHealFactory struct {
	crashErr error
	crashes  int
	created  atomic.Int32
}

// new produces the next incarnation.
func (f *selfHealFactory) new() Service {
	id := int(f.created.Add(1))
	s := newRecordingService()
	s.startFn = func(_ int, ctx ServiceContext) error {
		if id <= f.crashes {
			return f.crashErr
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return s
}

// count returns how many incarnations the factory has produced.
func (f *selfHealFactory) count() int { return int(f.created.Load()) }

// gatedBackoff parks the self-heal handler inside its backoff wait until the
// test releases it. It opens a deterministic window in which the crashed
// instance's exit channel is still open and the entry's status is not Running —
// exactly the window the liveness guard must cover against a concurrent
// StartService.
type gatedBackoff struct {
	entered chan struct{}
	release chan struct{}
}

func (b *gatedBackoff) Next(int) time.Duration {
	b.entered <- struct{}{}
	<-b.release
	return 0
}

// TestSelfHeal_RestartMetrics pins the metric split for a restart: a fresh
// instance launched by self-heal is a Restart, never a Start, and its internal
// cleanup is not a Stop. The crash is counted once, in Crashes.
func TestSelfHeal_RestartMetrics(t *testing.T) {
	crashErr := errors.New("self-heal boom")
	f := &selfHealFactory{crashErr: crashErr, crashes: 1}

	o := New(WithHealthChecksDisabled())
	if err := o.Register(f.new(), WithName("svc"),
		WithSelfHeal(f.new),
		WithBackoff(ConstantBackoff{Delay: time.Millisecond}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	defer o.Stop(2 * time.Second)

	waitForCondition(t, 3*time.Second, func() bool {
		return o.Metrics().Restarts == 1
	}, "the self-heal restart to complete")

	if got := o.Metrics().Starts; got != 1 {
		t.Errorf("Starts = %d, want 1: a restart is counted by Restarts, not Starts", got)
	}
	if got := o.Metrics().Restarts; got != 1 {
		t.Errorf("Restarts = %d, want 1", got)
	}
	if got := o.Metrics().Crashes; got != 1 {
		t.Errorf("Crashes = %d, want 1", got)
	}
	if got := o.Metrics().Stops; got != 0 {
		t.Errorf("Stops = %d, want 0: a restart's cleanup is not a stop", got)
	}
	if got, _ := o.Status("svc"); got != StatusRunning {
		t.Errorf("status = %v, want StatusRunning once the restarted instance is live", got)
	}
}

// TestSelfHeal_StopHooksOnRestart pins the decided hook semantics: the
// before-stop and after-stop hooks run on every self-heal restart, around the
// dead instance's Stop(), exactly as they do on a caller-initiated stop. Each
// restart adds one invocation of each; an explicit Stop then adds one more.
func TestSelfHeal_StopHooksOnRestart(t *testing.T) {
	crashErr := errors.New("boom")
	f := &selfHealFactory{crashErr: crashErr, crashes: 1}
	var before, after atomic.Int32

	o := New(WithHealthChecksDisabled())
	if err := o.Register(f.new(), WithName("svc"),
		WithSelfHeal(f.new),
		WithBackoff(ConstantBackoff{Delay: time.Millisecond}),
		WithOnBeforeStop(func(string) error { before.Add(1); return nil }),
		WithOnAfterStop(func(string, error) { after.Add(1) }),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}

	waitForCondition(t, 3*time.Second, func() bool {
		return o.Metrics().Restarts == 1
	}, "the self-heal restart to complete")

	if got := before.Load(); got != 1 {
		t.Errorf("before-stop hook calls after the restart = %d, want 1", got)
	}
	if got := after.Load(); got != 1 {
		t.Errorf("after-stop hook calls after the restart = %d, want 1", got)
	}

	if err := o.Stop(2 * time.Second); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if got := before.Load(); got != 2 {
		t.Errorf("before-stop hook calls after Stop = %d, want 2", got)
	}
	if got := after.Load(); got != 2 {
		t.Errorf("after-stop hook calls after Stop = %d, want 2", got)
	}
}

// TestSelfHeal_BlockingStopHook_IsBounded reproduces the defect #56 reported: a
// before-stop hook that never returns must not hang the restart. The restart's
// cleanup gives the hook half the per-service WithStopTimeout, abandons it on
// overrun, logs it and counts it in AbandonedGoroutines, then proceeds.
func TestSelfHeal_BlockingStopHook_IsBounded(t *testing.T) {
	crashErr := errors.New("boom")
	f := &selfHealFactory{crashErr: crashErr, crashes: 1}
	hook := newBlockingHook()

	o := New(WithHealthChecksDisabled())
	if err := o.Register(f.new(), WithName("svc"),
		WithSelfHeal(f.new),
		WithBackoff(ConstantBackoff{Delay: 0}),
		WithStopTimeout(80*time.Millisecond),
		WithOnBeforeStop(hook.beforeStop),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	// Release the hook before stopping so the final teardown does not overrun
	// too, then stop cleanly.
	defer func() {
		hook.releaseHook()
		_ = o.Stop(2 * time.Second)
	}()

	select {
	case <-hook.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the crashed instance's before-stop hook never ran")
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return o.Metrics().Restarts == 1
	}, "the restart to proceed past the overrunning hook")

	if got := o.Metrics().AbandonedGoroutines; got < 1 {
		t.Errorf("AbandonedGoroutines = %d, want >= 1: the overrunning hook must be abandoned", got)
	}
	if got, _ := o.Status("svc"); got != StatusRunning {
		t.Errorf("status = %v, want StatusRunning: the restart must proceed past the hook", got)
	}
}

// TestSelfHeal_BlockingStopHook_RestartCompletes is the completion half of the
// #56 regression: after an overrunning before-stop hook, a fresh live instance
// exists and the restart is counted.
func TestSelfHeal_BlockingStopHook_RestartCompletes(t *testing.T) {
	crashErr := errors.New("boom")
	f := &selfHealFactory{crashErr: crashErr, crashes: 1}
	hook := newBlockingHook()

	o := New(WithHealthChecksDisabled())
	if err := o.Register(f.new(), WithName("svc"),
		WithSelfHeal(f.new),
		WithBackoff(ConstantBackoff{Delay: 0}),
		WithStopTimeout(80*time.Millisecond),
		WithOnBeforeStop(hook.beforeStop),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	defer func() {
		hook.releaseHook()
		_ = o.Stop(2 * time.Second)
	}()

	select {
	case <-hook.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the crashed instance's before-stop hook never ran")
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return f.count() == 2 && o.Metrics().Restarts == 1
	}, "a fresh instance to be spawned despite the overrunning hook")

	if got, _ := o.Status("svc"); got != StatusRunning {
		t.Errorf("status = %v, want StatusRunning", got)
	}
	// The live instance is the second incarnation; the first was cleaned up.
	if svc := mustEntry(t, o, "svc").getSvc().(*recordingService); svc.wasStopped() {
		t.Error("the live restarted instance was already stopped")
	}
}

// TestSelfHeal_StopHookBudget_SharedWithRestart pins that the restart's cleanup
// budget is the service's own WithStopTimeout — the same documented value a
// caller stop honours — with defaultRestartStopTimeout as the fallback. The
// before-stop hook gets half of it, mirroring stopOneServiceDeadline's split.
func TestSelfHeal_StopHookBudget_SharedWithRestart(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	const timeout = 400 * time.Millisecond
	if err := o.Register(newRecordingService(), WithName("svc"), WithStopTimeout(timeout)); err != nil {
		t.Fatal(err)
	}

	got := o.restartStopHookDeadline(mustEntry(t, o, "svc"))
	if d := time.Until(got); d < timeout/2-time.Second || d > timeout/2+time.Second {
		t.Errorf("restart hook deadline = now + %v, want the per-service stop share %v", d, timeout/2)
	}

	// No WithStopTimeout: the documented default applies.
	got = o.restartStopHookDeadline(&serviceEntry{name: "plain"})
	if d := time.Until(got); d < defaultRestartStopTimeout/2-time.Second || d > defaultRestartStopTimeout/2+time.Second {
		t.Errorf("default restart hook deadline = now + %v, want %v", d, defaultRestartStopTimeout/2)
	}
}

// TestSelfHeal_LivenessGuard_Window covers the crash/backoff window in which the
// entry's status is not Running but its exit channel is still open. A
// concurrent StartService must be a no-op there — the done channel, not the
// status, is what proves no live instance — so it cannot spawn a second
// instance that would leak the wait group.
func TestSelfHeal_LivenessGuard_Window(t *testing.T) {
	crashErr := errors.New("boom")
	f := &selfHealFactory{crashErr: crashErr, crashes: 1}
	backoff := &gatedBackoff{entered: make(chan struct{}, 1), release: make(chan struct{})}

	o := New(WithHealthChecksDisabled())
	if err := o.Register(f.new(), WithName("svc"),
		WithSelfHeal(f.new),
		WithBackoff(backoff),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	defer o.Stop(2 * time.Second)

	select {
	case <-backoff.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("self-heal never reached the backoff window")
	}
	if got, _ := o.Status("svc"); got != StatusCrashed {
		t.Fatalf("status in the window = %v, want StatusCrashed", got)
	}

	// The crashed instance's done channel is still open while the handler is
	// parked, so StartService must conclude "no live instance" and fall through
	// to the idempotence guard rather than spawn.
	if err := o.StartService("svc"); err != nil {
		t.Fatalf("StartService in the restart window = %v, want nil no-op", err)
	}
	if got := f.count(); got != 1 {
		t.Fatalf("factory calls after StartService = %d, want 1 (no second spawn)", got)
	}

	// Release the window: exactly one restart instance is spawned.
	close(backoff.release)
	waitForCondition(t, 2*time.Second, func() bool {
		return o.Metrics().Restarts == 1
	}, "the parked restart to proceed")
	if got := f.count(); got != 2 {
		t.Errorf("factory calls after the restart = %d, want 2", got)
	}
}

// TestSelfHeal_RepeatedRestarts_NoLeakNoOrphan drives N consecutive restarts and
// checks the resource invariants: the per-instance state returns to baseline,
// the entry holds no live instance after Stop, Stop returns without a timeout,
// and no instance is orphaned by the consolidation of the spawn path.
func TestSelfHeal_RepeatedRestarts_NoLeakNoOrphan(t *testing.T) {
	const restarts = 5
	crashErr := errors.New("boom")
	f := &selfHealFactory{crashErr: crashErr, crashes: restarts}

	o := New(WithHealthChecksDisabled())
	base := captureLeakBaseline(o)
	if err := o.Register(f.new(), WithName("svc"),
		WithSelfHeal(f.new),
		WithBackoff(ConstantBackoff{Delay: 0}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}

	waitForCondition(t, 3*time.Second, func() bool {
		return o.Metrics().Restarts == restarts
	}, "all consecutive restarts to complete")
	if got := f.count(); got != restarts+1 {
		t.Fatalf("factory calls = %d, want exactly %d (no orphaned instance)", got, restarts+1)
	}

	if err := o.Stop(3 * time.Second); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}
	if done := mustEntry(t, o, "svc").getDone(); done != nil {
		select {
		case <-done:
		default:
			t.Error("the entry still holds a live instance after Stop")
		}
	}
	assertNoLeak(t, o, base)
}
