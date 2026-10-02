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

// This file pins Stop: whole-orchestrator teardown, the per-service stop
// sequence and its hooks, panic recovery, error aggregation, deadline bounding,
// stop accounting, and that cron/runOnce entries stop exactly once.

// TestStop_Concurrent_IsIdempotent pins the documented Stop-vs-Stop behaviour:
// concurrent Stop calls are a no-op for the losers. The first runs the shutdown;
// the others return nil once it completes and never run a second teardown.
func TestStop_Concurrent_IsIdempotent(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn), WithHealthChecksDisabled())

	stopEntered := make(chan struct{})
	stopRelease := make(chan struct{})
	var once sync.Once
	svc := &testSvc{stopFn: func() error {
		once.Do(func() { close(stopEntered) })
		<-stopRelease
		return nil
	}}
	if err := o.Register(svc, WithName("svc")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	first := make(chan error, 1)
	go func() { first <- o.Stop(time.Second) }()
	waitRecv(t, stopEntered)

	second := make(chan error, 1)
	go func() { second <- o.Stop(time.Second) }()
	close(stopRelease)

	for i, ch := range []<-chan error{first, second} {
		if err := <-ch; err != nil {
			t.Fatalf("Stop caller %d = %v, want nil", i+1, err)
		}
	}
	if got := svc.stopCalls.Load(); got != 1 {
		t.Fatalf("service Stop ran %d times, want 1 (the loser must not re-run teardown)", got)
	}
}

func TestStop(t *testing.T) {
	t.Run("stop_on_never_started_is_noop", func(t *testing.T) {
		o := New()
		err := o.Stop(100 * time.Millisecond)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("stop_after_start_with_no_services", func(t *testing.T) {
		o := New()
		_ = o.Start()
		err := o.Stop(1 * time.Second)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("double_stop_is_idempotent", func(t *testing.T) {
		o := New()
		_ = o.Start()
		err1 := o.Stop(1 * time.Second)
		err2 := o.Stop(1 * time.Second)
		if err1 != nil {
			t.Errorf("first Stop returned error: %v", err1)
		}
		if err2 != nil {
			t.Errorf("second Stop returned error: %v", err2)
		}
	})

	t.Run("stop_before_start_does_not_poison_later_stop", func(t *testing.T) {
		o := New()
		svc := &testSvc{}
		_ = o.Register(svc)
		// First Stop is a no-op and must not consume stopOnce.
		if err := o.Stop(1 * time.Second); err != nil {
			t.Errorf("no-op Stop returned error: %v", err)
		}
		if err := o.Start(); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		if err := o.Stop(1 * time.Second); err != nil {
			t.Errorf("later Stop returned error: %v", err)
		}
		if svc.stopCalls.Load() != 1 {
			t.Errorf("expected exactly 1 svc Stop call after later Stop, got %d", svc.stopCalls.Load())
		}
		select {
		case <-o.Done():
		case <-time.After(1 * time.Second):
			t.Errorf("expected Done() to be closed after later Stop")
		}
	})

	t.Run("stop_calls_svc_stop", func(t *testing.T) {
		o := New()
		svc := &testSvc{}
		_ = o.Register(svc)
		_ = o.Start()
		_ = o.Stop(1 * time.Second)
		if svc.stopCalls.Load() < 1 {
			t.Errorf("expected Stop to be called at least once, got %d", svc.stopCalls.Load())
		}
	})

	t.Run("stop_timeout_returns_ErrStopTimeout", func(t *testing.T) {
		o := New()
		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				never := make(chan struct{})
				<-never
				return nil
			},
		}
		_ = o.Register(svc)
		_ = o.Start()
		err := o.Stop(200 * time.Millisecond)
		if !errors.Is(err, ErrStopTimeout) {
			t.Errorf("expected ErrStopTimeout, got %v", err)
		}
	})

	t.Run("stop_cleans_up_messenger_subs", func(t *testing.T) {
		o := New()
		_ = o.Start()
		ch, _ := o.messenger.Subscribe("test")
		_ = o.Stop(1 * time.Second)
		o.messenger.Publish("msg")
		select {
		case <-ch:
		default:
		}
	})
}

// TestStop_DeadlineBoundsBlockingHook pins that the whole-orchestrator Stop
// deadline also bounds a blocking before-stop hook, not just a service that
// ignores cancellation.
func TestStop_DeadlineBoundsBlockingHook(t *testing.T) {
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

	const budget = 100 * time.Millisecond
	start := time.Now()
	err := o.Stop(budget)
	close(release)
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("Stop = %v, want ErrStopTimeout (blocking hook was not bounded)", err)
	}
	if elapsed := time.Since(start); elapsed > 2*budget {
		t.Errorf("Stop took %v; the deadline did not bound the stop hook", elapsed)
	}
}

// TestStop_WholeOrchestratorTimeout_StatusNotStopped pins that when the
// whole-orchestrator Stop deadline expires while a service ignores context
// cancellation, its entry is left StatusStopping rather than falsely claiming
// it stopped.
func TestStop_WholeOrchestratorTimeout_StatusNotStopped(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	svc := &testSvc{startFn: func(ctx context.Context) error { <-release; return nil }}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer close(release)

	if err := o.Stop(50 * time.Millisecond); !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("Stop = %v, want ErrStopTimeout", err)
	}
	if s, _ := o.Status("s"); s == StatusStopped {
		t.Fatalf("Status = %s after Stop timed out; want not stopped (instance is still live)", s)
	}
}

// TestStop_ZeroTimeoutWaitsAndCommits pins the documented "non-positive timeout
// waits indefinitely" contract: Stop(0) waits for every instance to exit and
// commits the terminal Stopped status without an ErrStopTimeout.
func TestStop_ZeroTimeoutWaitsAndCommits(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	svc := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	if err := o.Stop(0); err != nil {
		t.Fatalf("Stop(0) = %v, want nil (waits indefinitely)", err)
	}
	if s, _ := o.Status("s"); s != StatusStopped {
		t.Fatalf("Status = %s after Stop(0), want Stopped", s)
	}
}

func TestStop_ErrorAggregation(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	stopErr1 := errors.New("stop failed alpha")
	stopErr2 := errors.New("stop failed beta")

	svcA := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn:  func() error { return stopErr1 },
	}
	svcB := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn:  func() error { return stopErr2 },
	}
	_ = o.Register(svcA, WithName("alpha"))
	_ = o.Register(svcB, WithName("beta"))
	_ = o.Start()
	time.Sleep(50 * time.Millisecond)

	err := o.Stop(time.Second)
	if err == nil {
		t.Fatal("expected error from Stop")
	}
	// errors.Join wraps the errors, so errors.Is should find each.
	if !errors.Is(err, stopErr1) {
		t.Errorf("expected to find stopErr1: %v", err)
	}
	if !errors.Is(err, stopErr2) {
		t.Errorf("expected to find stopErr2: %v", err)
	}
}

func TestStopMultipleErrors(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.started = true
	o.cronSched = nil
	o.nameIndex = make(map[string]*serviceEntry)

	stopErr := errors.New("stop boom")
	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn:  func() error { return stopErr },
	}
	entry := &serviceEntry{
		name:   "bad",
		svc:    svc,
		cfg:    registerConfig{name: "bad"},
		status: StatusRunning,
	}
	o.entries = append(o.entries, entry)
	o.nameIndex["bad"] = entry
	o.wg.Add(1)

	// handleServiceDone for non-self-heal decrements wg.
	sc := ServiceContext{Context: o.ctx}
	o.handleServiceDone(entry, sc, nil, 0)

	// wg should be done after handleServiceDone.
	o.wg.Wait()
}

func TestStopOneService_HookErrors(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))

	hookErr := errors.New("before-stop hook error")
	stopErr := errors.New("stop error")

	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn:  func() error { return stopErr },
	}

	entry := &serviceEntry{
		name: "test",
		svc:  svc,
		cfg: registerConfig{
			name:         "test",
			onBeforeStop: func(name string) error { return hookErr },
			onAfterStop:  func(name string, err error) {},
		},
		status: StatusRunning,
	}

	err := o.stopOneService(entry)
	if err == nil {
		t.Fatal("expected error from stopOneService")
	}
	if !errors.Is(err, hookErr) {
		t.Errorf("expected to find hookErr: %v", err)
	}
	if !errors.Is(err, stopErr) {
		t.Errorf("expected to find stopErr: %v", err)
	}
}

func TestStopOneService_GlobalHooks(t *testing.T) {
	var events []string
	o := New(
		WithLogLevel(LogLevelWarn),
		WithGlobalOnBeforeStop(func(name string) error {
			events = append(events, "gb:"+name)
			return nil
		}),
		WithGlobalOnAfterStop(func(name string, err error) {
			events = append(events, "ga:"+name)
		}),
	)

	svc := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	entry := &serviceEntry{
		name:   "test",
		svc:    svc,
		cfg:    registerConfig{name: "test"},
		status: StatusRunning,
	}

	o.stopOneService(entry)

	if len(events) < 2 {
		t.Fatalf("expected at least 2 hook events, got %v", events)
	}
	if events[0] != "gb:test" || events[1] != "ga:test" {
		t.Errorf("expected [gb:test ga:test], got %v", events)
	}
}

func TestStopOneService_StopPanic(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	entry := &serviceEntry{
		name:   "panicky",
		svc:    &stopPanicSvc{},
		cfg:    registerConfig{name: "panicky"},
		status: StatusRunning,
	}
	err := o.stopOneService(entry)
	if err == nil {
		t.Fatal("expected error from panicking Stop")
	}
	if !strings.Contains(err.Error(), "stop panicked") {
		t.Errorf("expected 'stop panicked' in error, got %v", err)
	}
}

// TestStop_FailsIfTopoSortFails pins that a topoSort failure is surfaced rather
// than swallowed, while every service is still torn down. Register rejects
// cycles, so a cyclic pair is injected straight into the registry (the same
// white-box technique TestTopoSort_ErrorInStart uses) to hand Stop a subset it
// cannot order.
func TestStop_FailsIfTopoSortFails(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(&testSvc{}, WithName("a"))
	if err := o.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	c := &testSvc{}
	d := &testSvc{}
	o.mu.Lock()
	o.entries = append(o.entries,
		&serviceEntry{name: "c", svc: c, cfg: registerConfig{name: "c", dependsOn: []string{"d"}}, status: StatusRegistered},
		&serviceEntry{name: "d", svc: d, cfg: registerConfig{name: "d", dependsOn: []string{"c"}}, status: StatusRegistered},
	)
	o.mu.Unlock()

	err := o.Stop(time.Second)
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("Stop must surface a topoSort failure, got %v", err)
	}
	if c.stopCalls.Load() < 1 || d.stopCalls.Load() < 1 {
		t.Errorf("a cyclic subset must still be torn down: c=%d d=%d", c.stopCalls.Load(), d.stopCalls.Load())
	}
}

func TestStopStartedServices_UsedDuringStartupFailure(t *testing.T) {
	// One-shot error triggers stopStartedServices.
	o := New(WithLogLevel(LogLevelWarn))
	oneShot := &testSvc{
		startFn: func(ctx context.Context) error {
			return errors.New("init fail")
		},
	}
	persistent := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}
	_ = o.Register(oneShot, WithName("init"), WithRunOnce())
	_ = o.Register(persistent, WithName("svc"))

	err := o.Start()
	if err == nil {
		o.Stop(time.Second)
		t.Fatal("expected error")
	}
	// stopStartedServices should have cleaned up — persistent never started.
	if persistent.startCalls.Load() > 0 {
		t.Error("persistent should not have started")
	}
}

// TestStopStartedServices_StopsRunningService deterministically exercises the
// safeStop branch of stopStartedServices: a running level-0 service is stopped
// when a later level fails to start.
func TestStopStartedServices_StopsRunningService(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	a := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	_ = o.Register(a, WithName("a"))
	_ = o.Register(&testSvc{}, WithName("b"), DependsOn("a"),
		WithOnBeforeStart(func(name string) error { return errors.New("b cannot start") }))

	err := o.Start()
	if err == nil {
		o.Stop(time.Second)
		t.Fatal("expected Start to fail")
	}
	if a.stopCalls.Load() < 1 {
		t.Error("running service 'a' should be stopped during startup failure")
	}
}

func TestStop_StopsCronAndRunOnce(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	cronSvc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}
	oneShot := &testSvc{
		startFn: func(ctx context.Context) error { return nil },
	}
	_ = o.Register(cronSvc, WithName("cron"), WithCron("* * * * * *", CronParallel))
	_ = o.Register(oneShot, WithName("init"), WithRunOnce())
	_ = o.Start()
	time.Sleep(50 * time.Millisecond)
	_ = o.Stop(time.Second)

	// Cron Stop should be called at least once (from stopOneService in Stop).
	if cronSvc.stopCalls.Load() < 1 {
		t.Errorf("expected cron service Stop to be called, got %d", cronSvc.stopCalls.Load())
	}
	// One-shot Stop is also called during normal shutdown.
	if oneShot.stopCalls.Load() < 1 {
		t.Errorf("expected one-shot Stop to be called during normal shutdown, got %d", oneShot.stopCalls.Load())
	}
}

// TestStop_EntrySliceReadIsSynchronised drives a concurrent mutation of the
// entry registry while Stop is reading it, and must not trip the race detector
// under -race: Stop's teardown passes read a snapshot taken under o.mu. The
// writer models a membership op that replaces o.entries under the lock. A start
// barrier puts both goroutines in flight together without an ordering edge
// between their accesses, which is exactly what a data race needs; a Stop that
// read o.entries unsynchronised would report one.
func TestStop_EntrySliceReadIsSynchronised(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	// Non-persistent entries give the second teardown pass (the one that used to
	// read o.entries unsynchronised) real work to do over a wide window.
	for i := 0; i < 128; i++ {
		svc := &testSvc{startFn: func(ctx context.Context) error { return nil }}
		if err := o.Register(svc, WithName(fmt.Sprintf("once-%d", i)), WithRunOnce()); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	stopDone := make(chan error, 1)
	go func() {
		<-start
		stopDone <- o.Stop(5 * time.Second)
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		// Replace the registry slice under the lock, the way a membership op
		// would. The contents are unchanged; only the slice header is rewritten,
		// which is precisely the unsynchronised read Stop used to perform.
		for i := 0; i < 500; i++ {
			o.mu.Lock()
			next := make([]*serviceEntry, len(o.entries))
			copy(next, o.entries)
			o.entries = next
			o.mu.Unlock()
		}
	}()

	close(start)
	wg.Wait()
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestStop_RunOnceAndCronEntriesAreStoppedExactlyOnce pins that Stop's two
// teardown passes are disjoint: the reverse-topological pass handles persistent
// entries, the remaining pass handles cron-only and runOnce entries, and their
// predicates are complements, so every service's Stop() runs exactly once.
func TestStop_RunOnceAndCronEntriesAreStoppedExactlyOnce(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	persistent := &testSvc{}
	once := &testSvc{startFn: func(ctx context.Context) error { return nil }}
	cronSvc := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(persistent, WithName("persistent")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(once, WithName("once"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(cronSvc, WithName("cron"), WithCron("* * * * * *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	if err := o.Stop(2 * time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	for _, tc := range []struct {
		name string
		svc  *testSvc
	}{
		{"persistent", persistent},
		{"once", once},
		{"cron", cronSvc},
	} {
		if got := tc.svc.stopCalls.Load(); got != 1 {
			t.Errorf("%s Stop() called %d times, want exactly 1", tc.name, got)
		}
	}
}

// TestStop_ConcurrentDoubleStop_NoDoubleHooks runs two overlapping Stop calls on
// the same entry and asserts the teardown runs once: the before/after-stop hooks
// and the service's Stop() must not fire twice. The stopOnce latch makes one
// caller the owner while the other waits and then no-ops.
func TestStop_ConcurrentDoubleStop_NoDoubleHooks(t *testing.T) {
	var beforeStop, afterStop atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	svc := &testSvc{stopFn: func() error {
		close(entered)
		<-release
		return nil
	}}
	o := New(WithHealthChecksDisabled(),
		WithGlobalOnBeforeStop(func(string) error { beforeStop.Add(1); return nil }),
		WithGlobalOnAfterStop(func(string, error) { afterStop.Add(1) }),
	)
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	ready := make(chan struct{}, 2)
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			results <- o.Stop(2 * time.Second)
		}()
	}
	<-ready
	<-ready
	close(start)
	<-entered // One Stop owns the teardown and is now blocked inside svc.Stop.
	close(release)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("Stop: %v", err)
		}
	}

	if got := svc.stopCalls.Load(); got != 1 {
		t.Errorf("service Stop() called %d times, want exactly 1", got)
	}
	if got := beforeStop.Load(); got != 1 {
		t.Errorf("before-stop hook ran %d times, want exactly 1", got)
	}
	if got := afterStop.Load(); got != 1 {
		t.Errorf("after-stop hook ran %d times, want exactly 1", got)
	}
}

func TestStopStartedServices_LogQuitSignal(t *testing.T) {
	// stopStartedServices signals the log-pump via logQuit instead of closing
	// logCh, so a late log send can never panic on a closed channel.
	o := New(WithLogLevel(LogLevelWarn))
	if err := o.Register(&namedSvc{}, WithCron("* * * * * *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	// Register validates the spec, so corrupt it afterwards to force Start to
	// fail at setupCron (Start fails → logQuit closed → logPumpDone closed →
	// logCh left open).
	o.mu.Lock()
	o.entries[0].cfg.cronSpec = "invalid"
	o.mu.Unlock()
	_ = o.Start() // will fail with ErrInvalidCron

	// logCh must remain open so sends never panic.
	select {
	case o.logCh <- logEntry{}:
	default:
	}
}

func TestStop_CronAndRunOnceStopErrors(t *testing.T) {
	// Cover the error aggregation path in Stop for cron/runOnce entries.
	o := New(WithLogLevel(LogLevelWarn))
	stopErr := errors.New("stop oops")
	cronSvc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn:  func() error { return stopErr },
	}
	_ = o.Register(cronSvc, WithName("cron"), WithCron("* * * * * *", CronParallel))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { return nil },
	}, WithName("init"), WithRunOnce())
	_ = o.Start()
	time.Sleep(50 * time.Millisecond)
	err := o.Stop(time.Second)
	if err == nil {
		t.Error("expected error from Stop due to cron stop error")
	}
}

func TestStopTimeout(t *testing.T) {
	t.Run("per_service_stop_timeout", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			stopFn: func() error {
				time.Sleep(500 * time.Millisecond)
				return nil
			},
		}
		_ = o.Register(svc, WithName("slow-stop"), WithStopTimeout(50*time.Millisecond))
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		err := o.Stop(time.Second)
		if err == nil {
			t.Error("expected stop timeout error")
		}
	})

	t.Run("stop_timeout_does_not_block_other_services", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		slowSvc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			stopFn: func() error {
				time.Sleep(2 * time.Second)
				return nil
			},
		}
		fastSvc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(slowSvc, WithName("slow"), WithStopTimeout(50*time.Millisecond))
		_ = o.Register(fastSvc, WithName("fast"))
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)

		start := time.Now()
		err := o.Stop(time.Second)
		elapsed := time.Since(start)
		if err == nil {
			t.Error("expected stop timeout error from slow service")
		}
		// Fast service should be stopped.
		if fastSvc.stopCalls.Load() < 1 {
			t.Error("fast service should be stopped even though slow timed out")
		}
		// Overall stop should not wait for slow's full 2s sleep.
		if elapsed > 1500*time.Millisecond {
			t.Errorf("overall stop took too long (%v) — slow timeout didn't apply", elapsed)
		}
	})
}
