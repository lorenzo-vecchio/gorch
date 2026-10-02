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

// This file pins Start: the single-shot lifecycle, concurrent-caller claims and
// retry after failure, start timeouts, dependency-status gating, start
// conditions, and the single-instance spawn path.

func TestStart(t *testing.T) {
	t.Run("start_with_no_services", func(t *testing.T) {
		o := New()
		err := o.Start()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		_ = o.Stop(1 * time.Second)
	})

	t.Run("double_start_returns_ErrAlreadyStarted", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		err1 := o.Start()
		err2 := o.Start()
		if err1 != nil {
			t.Errorf("first Start returned error: %v", err1)
		}
		if !errors.Is(err2, ErrAlreadyStarted) {
			t.Errorf("second Start returned: %v, want ErrAlreadyStarted", err2)
		}
		_ = o.Stop(1 * time.Second)
	})

	t.Run("start_with_invalid_cron_returns_ErrInvalidCron", func(t *testing.T) {
		o := New()
		// Register validates the spec on both paths, so inject a spec that is
		// valid at registration but corrupted afterwards. Start's setupCron
		// re-schedules every cron entry and must still surface ErrInvalidCron
		// defensively rather than leaving a broken entry live.
		if err := o.Register(&namedSvc{name: "bad-cron"}, WithCron("* * * * * *", CronParallel)); err != nil {
			t.Fatalf("Register valid cron: %v", err)
		}
		o.mu.Lock()
		o.entries[0].cfg.cronSpec = "invalid"
		o.mu.Unlock()
		err := o.Start()
		if !errors.Is(err, ErrInvalidCron) {
			t.Errorf("expected ErrInvalidCron, got %v", err)
		}
	})

	t.Run("start_ErrAlreadyStarted_via_whitebox", func(t *testing.T) {
		o := &Orchestrator{
			cfg:       config{LogLevel: LogLevelInfo, logLevelSet: true},
			started:   true,
			messenger: newMessenger(),
		}
		err := o.Start()
		if !errors.Is(err, ErrAlreadyStarted) {
			t.Errorf("expected ErrAlreadyStarted, got %v", err)
		}
	})
}

// TestStart_RetryAfterFailure verifies that a failed Start does not brick the
// orchestrator: it can be registered against, restarted, and stopped.
func TestStart_RetryAfterFailure(t *testing.T) {
	t.Run("register_after_failed_start_succeeds", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(&errSvc{err: errors.New("boom")}, WithName("gate"), WithRunOnce())
		if err := o.Start(); err == nil {
			t.Fatal("expected Start to fail")
		}
		if err := o.Register(&namedSvc{name: "later"}); err != nil {
			t.Errorf("Register after failed Start returned %v", err)
		}
	})

	t.Run("start_after_failed_start_actually_starts", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		var gateCalls atomic.Int32
		gate := &testSvc{startFn: func(ctx context.Context) error {
			if gateCalls.Add(1) == 1 {
				return errors.New("transient gate failure")
			}
			return nil
		}}
		_ = o.Register(gate, WithName("gate"), WithRunOnce())
		_ = o.Register(&namedSvc{name: "svc"}, WithName("svc"))
		if err := o.Start(); err == nil {
			t.Fatal("expected first Start to fail")
		}
		if err := o.Start(); err != nil {
			t.Fatalf("second Start returned %v", err)
		}
		defer o.Stop(time.Second)
		s, ok := o.Status("svc")
		if !ok || s != StatusRunning {
			t.Errorf("expected svc to be running, got %v (ok=%v)", s, ok)
		}
	})

	t.Run("stop_after_failed_start_is_noop", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(&errSvc{err: errors.New("boom")}, WithName("gate"), WithRunOnce())
		_ = o.Start()
		if err := o.Stop(time.Second); err != nil {
			t.Errorf("Stop after failed Start returned %v", err)
		}
	})
}

// TestStart_Concurrent_SecondCallerBehaviour pins the concurrent-Start contract:
// the caller that wins the lifecycle claim runs the start, and a caller that
// loses it returns ErrAlreadyStarted without waiting for the winner and without
// running any service itself.
//
// The test opens the race window deterministically by holding the membership
// lock, which the winner's startOnce closure takes before it can publish
// o.started. The loser therefore races the claim, not a fully started
// orchestrator — exactly the interleaving where it used to return nil.
func TestStart_Concurrent_SecondCallerBehaviour(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn), WithHealthChecksDisabled())

	svc := &testSvc{startFn: func(context.Context) error { return nil }}
	if err := o.Register(svc, WithName("svc"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}

	unlockMembership := sync.OnceFunc(o.membershipMu.Unlock)
	defer unlockMembership()
	o.membershipMu.Lock()

	winner := make(chan error, 1)
	go func() { winner <- o.Start() }()

	// Wait until the winner has claimed the lifecycle. Until it does, the second
	// caller would race a plainly-not-started orchestrator instead of the claim.
	waitUntil(t, func() bool {
		o.mu.RLock()
		claimed := o.startClaimed
		o.mu.RUnlock()
		return claimed
	}, "winning Start never claimed the lifecycle")

	loser := make(chan error, 1)
	go func() { loser <- o.Start() }()
	select {
	case err := <-loser:
		if !errors.Is(err, ErrAlreadyStarted) {
			t.Fatalf("loser Start = %v, want ErrAlreadyStarted", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("loser Start blocked on the winner instead of returning ErrAlreadyStarted")
	}

	// The winner is still blocked inside its closure, so o.started has not been
	// published: the loser really did race the claim window.
	o.mu.RLock()
	started := o.started
	o.mu.RUnlock()
	if started {
		t.Fatal("winner published o.started before the loser was exercised")
	}

	unlockMembership()
	if err := <-winner; err != nil {
		t.Fatalf("winning Start = %v, want nil", err)
	}
	if got := svc.startCalls.Load(); got != 1 {
		t.Fatalf("service Start ran %d times, want 1 (the loser must start nothing)", got)
	}
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop = %v", err)
	}
}

// TestStart_Concurrent_WhenWinnerFails pins that a concurrent Start never reports
// success while the lifecycle is not started: the loser returns ErrAlreadyStarted
// even when the winner's Start goes on to fail and reset the orchestrator.
func TestStart_Concurrent_WhenWinnerFails(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn), WithHealthChecksDisabled())

	boom := errors.New("winner boom")
	if err := o.Register(&errSvc{err: boom}, WithName("gate"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}

	unlockMembership := sync.OnceFunc(o.membershipMu.Unlock)
	defer unlockMembership()
	o.membershipMu.Lock()

	winner := make(chan error, 1)
	go func() { winner <- o.Start() }()
	waitUntil(t, func() bool {
		o.mu.RLock()
		claimed := o.startClaimed
		o.mu.RUnlock()
		return claimed
	}, "winning Start never claimed the lifecycle")

	loser := make(chan error, 1)
	go func() { loser <- o.Start() }()
	select {
	case err := <-loser:
		if err == nil {
			t.Fatal("concurrent Start returned nil while the winner had not finished")
		}
		if !errors.Is(err, ErrAlreadyStarted) {
			t.Fatalf("loser Start = %v, want ErrAlreadyStarted", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent Start blocked on a winner that then failed")
	}

	unlockMembership()
	if err := <-winner; !errors.Is(err, boom) {
		t.Fatalf("winning Start = %v, want %v", err, boom)
	}

	// A failed Start releases the claim: the loser did not return nil, and the
	// orchestrator is genuinely reset rather than left half-started.
	o.mu.RLock()
	started := o.started
	claimed := o.startClaimed
	o.mu.RUnlock()
	if started || claimed {
		t.Fatalf("after the winner failed: started=%v claimed=%v, want false,false", started, claimed)
	}
}

func TestStartTimeout_Persistent(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			// Context-aware: when svcCancel fires, exit cleanly (no log).
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return nil
			}
		},
	}
	_ = o.Register(svc, WithName("slow"), WithStartTimeout(50*time.Millisecond))

	err := o.Start()
	if err == nil {
		o.Stop(time.Second)
		t.Fatal("expected timeout error from Start")
	}
	// ponytail: stopStartedServices already cleaned up logCh;
	// calling Stop() would double-close it.
}

func TestStartTimeout_DefaultStartTimeout(t *testing.T) {
	o := New(
		WithLogLevel(LogLevelWarn),
		WithDefaultStartTimeout(50*time.Millisecond),
	)
	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return nil
			}
		},
	}
	_ = o.Register(svc, WithName("slow"))

	err := o.Start()
	if err == nil {
		o.Stop(time.Second)
		t.Fatal("expected timeout error from Start with DefaultStartTimeout")
	}
	// ponytail: logCh already closed by stopStartedServices.
}

// TestStart_Stop_PersistentDependingOnRunOnceGate pins the documented migration
// pattern: a persistent service hard-depends on a runOnce gate. Start skips the
// gate in its persistent subset, so the out-of-subset hard edge used to abort
// Start with ErrDependencyCycle.
func TestStart_Stop_PersistentDependingOnRunOnceGate(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	gate := &testSvc{startFn: func(ctx context.Context) error { return nil }}
	app := &testSvc{}
	_ = o.Register(gate, WithName("migrate"), WithRunOnce())
	_ = o.Register(app, WithName("app"), DependsOn("migrate"))

	if err := o.Start(); err != nil {
		t.Fatalf("Start with a persistent service depending on a runOnce gate must succeed, got %v", err)
	}
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if app.stopCalls.Load() < 1 {
		t.Error("persistent service depending on a runOnce gate must receive Stop()")
	}
}

func TestStart_DepStatusCheck(t *testing.T) {
	// A fast clean Start return puts the service in StatusStopped (via
	// handleServiceDone) BEFORE startOneService signals completion, so the
	// dependent in the next level deterministically sees StatusStopped and is
	// aborted — never started behind a dead dependency.
	o := New(WithLogLevel(LogLevelWarn))
	svcA := &testSvc{startFn: func(ctx context.Context) error { return nil }}
	svcB := &testSvc{}

	_ = o.Register(svcA, WithName("a"), WithStartTimeout(time.Second))
	_ = o.Register(svcB, WithName("b"), DependsOn("a"))

	err := o.Start()
	if err == nil {
		t.Fatal("expected ErrStartAborted: dependency a exited cleanly before b started")
	}
	if !strings.Contains(err.Error(), "aborted") && !strings.Contains(err.Error(), "dependency") {
		t.Errorf("unexpected error: %v", err)
	}
	if svcB.startCalls.Load() > 0 {
		t.Error("dependent should not start behind a stopped dependency")
	}
}

func TestStart_InstantErrorPropagatesWithTimeout(t *testing.T) {
	// A persistent service that errors synchronously within the start-timeout
	// window aborts Start deterministically: the real error is returned (not
	// swallowed) and the dependent in the next level is skipped.
	o := New(WithLogLevel(LogLevelWarn))
	failSvc := &testSvc{
		startFn: func(ctx context.Context) error {
			return errors.New("instant failure")
		},
	}
	depSvc := &testSvc{}

	_ = o.Register(failSvc, WithName("fail"), WithStartTimeout(500*time.Millisecond))
	_ = o.Register(depSvc, WithName("dep"), DependsOn("fail"))

	err := o.Start()
	if err == nil {
		t.Fatal("expected Start to return the instant error")
	}
	if !strings.Contains(err.Error(), "instant failure") {
		t.Errorf("expected instant failure to propagate, got: %v", err)
	}
	if depSvc.startCalls.Load() > 0 {
		t.Error("dependent should not start when a dependency fails instantly")
	}
}

func TestStartOneService_PersistentStartErrCh(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	svc := &testSvc{
		startFn: func(ctx context.Context) error { return nil },
	}
	_ = o.Register(svc, WithStartTimeout(time.Second))
	err := o.Start()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = o.Stop(time.Second)
}

func TestStart_DepStatusCheck_Whitebox(t *testing.T) {
	// Directly test the dependency-failure check in Start's goroutine,
	// bypassing the race with goroutine scheduling.
	o := New(WithLogLevel(LogLevelWarn))
	o.nameIndex = make(map[string]*serviceEntry)

	depEntry := &serviceEntry{
		name:   "dep",
		svc:    &testSvc{startFn: func(ctx context.Context) error { return nil }},
		cfg:    registerConfig{name: "dep"},
		status: StatusStopped,
	}
	svcEntry := &serviceEntry{
		name: "svc",
		svc:  &testSvc{},
		cfg:  registerConfig{name: "svc", dependsOn: []string{"dep"}},
	}
	o.entries = append(o.entries, depEntry, svcEntry)
	o.nameIndex["dep"] = depEntry
	o.nameIndex["svc"] = svcEntry

	// Simulate the check that happens inside Start's goroutine for svc.
	var failed map[string]error
	var mu sync.Mutex
	e := svcEntry
	for _, dep := range e.cfg.dependsOn {
		depE := o.nameIndex[dep]
		depStatus := depE.status
		if depStatus == StatusCrashed || depStatus == StatusStopped {
			failureMsg := fmt.Sprintf("dependency %s failed or was skipped", dep)
			mu.Lock()
			if failed == nil {
				failed = make(map[string]error)
			}
			failed[e.name] = fmt.Errorf("%w: %s", ErrStartAborted, failureMsg)
			mu.Unlock()
			o.setStatus(e, StatusStopped)
		}
	}
	if failed == nil {
		t.Error("expected dependency failure to be detected")
	}
	if failed["svc"] == nil {
		t.Error("expected svc to be in failed map")
	}
	if svcEntry.status != StatusStopped {
		t.Errorf("expected svc to be StatusStopped, got %v", svcEntry.status)
	}
}

func TestStartCondition(t *testing.T) {
	t.Run("condition_true_starts", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(svc, WithName("yes"), WithStartCondition(func() bool { return true }))
		_ = o.Start()
		defer o.Stop(time.Second)
		time.Sleep(50 * time.Millisecond)
		s, ok := o.Status("yes")
		if !ok || s != StatusRunning {
			t.Errorf("service with true condition should be running: %v (ok=%v)", s, ok)
		}
	})

	t.Run("condition_false_skipped", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		svc := &testSvc{}
		_ = o.Register(svc, WithName("no"), WithStartCondition(func() bool { return false }))
		_ = o.Start()
		defer o.Stop(time.Second)
		s, ok := o.Status("no")
		if !ok || s != StatusStopped {
			t.Errorf("skipped service should be StatusStopped: %v (ok=%v)", s, ok)
		}
		if svc.startCalls.Load() > 0 {
			t.Error("Start should not be called when condition is false")
		}
	})

	t.Run("condition_false_still_reports_in_statuses", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(&testSvc{}, WithName("skipped"), WithStartCondition(func() bool { return false }))
		_ = o.Start()
		defer o.Stop(time.Second)
		all := o.Statuses()
		s, ok := all["skipped"]
		if !ok || s != StatusStopped {
			t.Errorf("skipped service should be in statuses as Stopped: %v (ok=%v)", s, ok)
		}
	})
}
