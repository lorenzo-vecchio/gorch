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

// This file pins self-heal and crash semantics: the status sequences for a
// crash, a clean exit and a cancelled backoff, every branch of handleServiceDone,
// and the observable crash/restart counters. waitForStatus/assertStatusSequence
// synchronise the ordering rather than sleeping.

func TestSelfHeal(t *testing.T) {
	t.Run("restarts_after_error", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		var factoryCalls atomic.Int32

		factory := func() Service {
			factoryCalls.Add(1)
			return &testSvc{
				startFn: func(ctx context.Context) error { return nil },
			}
		}
		_ = o.Register(&errSvc{err: errors.New("boom")}, WithSelfHeal(factory))
		_ = o.Start()

		deadline := time.After(3 * time.Second)
		for factoryCalls.Load() < 1 {
			select {
			case <-deadline:
				t.Fatal("timed out waiting for factory call")
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
		_ = o.Stop(500 * time.Millisecond)
	})

	t.Run("restarts_after_panic", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		var factoryCalls atomic.Int32

		factory := func() Service {
			factoryCalls.Add(1)
			return &testSvc{
				startFn: func(ctx context.Context) error { return nil },
			}
		}
		_ = o.Register(&panicSvc{msg: "bang"}, WithSelfHeal(factory))
		_ = o.Start()

		deadline := time.After(3 * time.Second)
		for factoryCalls.Load() < 1 {
			select {
			case <-deadline:
				t.Fatal("timed out waiting for factory call after panic")
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
		_ = o.Stop(500 * time.Millisecond)
	})

	t.Run("self_heal_1s_backoff", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		var factoryCalls atomic.Int32

		factory := func() Service {
			factoryCalls.Add(1)
			return &testSvc{
				startFn: func(ctx context.Context) error { return nil },
			}
		}
		_ = o.Register(&errSvc{err: errors.New("fail")}, WithSelfHeal(factory))
		t0 := time.Now()
		_ = o.Start()

		deadline := time.After(3 * time.Second)
		for factoryCalls.Load() < 1 {
			select {
			case <-deadline:
				t.Fatal("timed out waiting for factory call")
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
		elapsed := time.Since(t0)
		_ = o.Stop(500 * time.Millisecond)

		if elapsed < time.Second {
			t.Errorf("expected at least 1s backoff, but factory called after %v", elapsed)
		}
	})

	t.Run("handleServiceDone_no_factory_calls_wgDone", func(t *testing.T) {
		o := New()
		entry := &serviceEntry{svc: &namedSvc{name: "x"}, cfg: registerConfig{}}
		o.wg.Add(1)
		sc := ServiceContext{Context: context.Background()}
		o.handleServiceDone(entry, sc, nil, 0)

		done := make(chan struct{})
		go func() { o.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("wg.Wait did not complete — wg.Done was not called")
		}
	})

	t.Run("handleServiceDone_double_call_no_double_wgDone", func(t *testing.T) {
		o := New()
		entry := &serviceEntry{svc: &namedSvc{name: "x"}, cfg: registerConfig{}}
		o.wg.Add(1)
		sc := ServiceContext{Context: context.Background()}
		o.handleServiceDone(entry, sc, nil, 0)
		o.handleServiceDone(entry, sc, nil, 0)

		done := make(chan struct{})
		go func() { o.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("double handleServiceDone caused wg imbalance")
		}
	})

	t.Run("handleServiceDone_ctx_cancelled_no_restart", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		ctx, cancel := context.WithCancel(context.Background())
		o.ctx = ctx
		cancel() // cancel immediately

		var factoryCalls atomic.Int32
		factory := func() Service {
			factoryCalls.Add(1)
			return &testSvc{}
		}
		entry := &serviceEntry{
			svc:    &errSvc{err: errors.New("boom")},
			cfg:    registerConfig{name: "x", factory: factory},
			name:   "x",
			status: StatusRunning,
		}
		o.wg.Add(1)
		sc := ServiceContext{Context: ctx}
		o.handleServiceDone(entry, sc, nil, 0)

		if factoryCalls.Load() != 0 {
			t.Error("factory should not be called when context is cancelled")
		}
	})
}

func TestSelfHeal_MaxRetries(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	var factoryCalls atomic.Int32

	factory := func() Service {
		factoryCalls.Add(1)
		return &errSvc{err: errors.New("always fail")}
	}
	_ = o.Register(&errSvc{err: errors.New("init fail")},
		WithSelfHeal(factory),
		WithMaxRetries(3),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Millisecond}),
	)
	_ = o.Start()

	// Wait for retries to be exhausted.
	time.Sleep(300 * time.Millisecond)
	_ = o.Stop(500 * time.Millisecond)

	calls := factoryCalls.Load()
	if calls > 3 {
		t.Errorf("expected at most 3 factory calls, got %d", calls)
	}
	if calls < 1 {
		t.Errorf("expected at least 1 factory call, got %d", calls)
	}
}

func TestSelfHeal_ResetAfter(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	var factoryCalls atomic.Int32

	factory := func() Service {
		factoryCalls.Add(1)
		return &testSvc{
			startFn: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(150 * time.Millisecond):
					return errors.New("crash after stable")
				}
			},
		}
	}
	_ = o.Register(&errSvc{err: errors.New("init crash")},
		WithSelfHeal(factory),
		WithMaxRetries(5),
		WithResetAfter(50*time.Millisecond),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Millisecond}),
	)
	_ = o.Start()

	time.Sleep(800 * time.Millisecond)
	_ = o.Stop(500 * time.Millisecond)

	calls := factoryCalls.Load()
	if calls < 2 {
		t.Errorf("expected at least 2 factory calls (resetAfter should keep counter low), got %d", calls)
	}
}

func TestSelfHeal_CustomBackoff(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	var factoryCalls atomic.Int32

	factory := func() Service {
		factoryCalls.Add(1)
		return &testSvc{
			startFn: func(ctx context.Context) error { return nil },
		}
	}
	_ = o.Register(&errSvc{err: errors.New("crash")},
		WithSelfHeal(factory),
		WithBackoff(ExponentialBackoff{Initial: 50 * time.Millisecond, Max: time.Second, Factor: 2.0}),
	)

	t0 := time.Now()
	_ = o.Start()

	deadline := time.After(2 * time.Second)
	for factoryCalls.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("factory not called")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
	elapsed := time.Since(t0)
	_ = o.Stop(500 * time.Millisecond)

	// First retry should have ~50ms delay (vs default 1s).
	if elapsed > 500*time.Millisecond {
		t.Errorf("expected custom backoff (~50ms), but took %v (likely used default 1s)", elapsed)
	}
}

// TestSelfHeal_ConcurrentHealthProbe exercises the per-entry state mutex: a
// self-healing service crashes repeatedly while Health() and the health-check
// loop read its state concurrently. Run with -race.
func TestSelfHeal_ConcurrentHealthProbe(t *testing.T) {
	o := New(
		WithLogLevel(LogLevelWarn),
		WithHealthChecks(5*time.Millisecond, WithProbeTimeout(100*time.Millisecond), WithFailureThreshold(3)),
	)

	mk := func() Service {
		return &healthSvc{
			testSvc: testSvc{
				startFn: func(ctx context.Context) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(time.Millisecond):
						return errors.New("crash quickly")
					}
				},
			},
			healthFn: func(ctx context.Context) error { return errors.New("unhealthy") },
		}
	}
	_ = o.Register(mk(), WithName("flaky"),
		WithSelfHeal(mk),
		WithBackoff(ConstantBackoff{Delay: time.Millisecond}),
	)

	_ = o.Start()
	defer o.Stop(time.Second)

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				o.Health()
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(stop)
}

func TestHandleServiceDone_CancelledDuringBackoff(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	ctx, cancel := context.WithCancel(context.Background())
	o.ctx = ctx

	var factoryCalls atomic.Int32
	factory := func() Service {
		factoryCalls.Add(1)
		return &testSvc{}
	}

	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		svc:    &errSvc{err: errors.New("boom")},
		cfg:    registerConfig{name: "x", factory: factory, backoff: ConstantBackoff{Delay: 500 * time.Millisecond}},
		name:   "x",
		status: StatusRunning,
		logger: newServiceLogger("x", logCh, nil, LogLevelDebug),
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}

	go o.handleServiceDone(entry, sc, nil, 0)

	time.Sleep(100 * time.Millisecond)
	cancel()

	// Wait for cleanup.
	done := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait timed out")
	}

	if factoryCalls.Load() != 0 {
		t.Error("factory should not be called when cancelled during backoff")
	}
}

func TestHandleServiceDone_FactoryContextCancelled(t *testing.T) {
	// Cover the factory != nil + ctx cancelled path (gorch.go:996).
	o := New(WithLogLevel(LogLevelWarn))
	ctx, cancel := context.WithCancel(context.Background())
	o.ctx = ctx
	cancel() // cancelled immediately

	var factoryCalls atomic.Int32
	factory := func() Service {
		factoryCalls.Add(1)
		return &testSvc{}
	}
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		svc:    &errSvc{err: errors.New("boom")},
		cfg:    registerConfig{name: "x", factory: factory},
		name:   "x",
		status: StatusRunning,
		logger: newServiceLogger("x", logCh, nil, LogLevelDebug),
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}
	o.handleServiceDone(entry, sc, nil, 0)

	if factoryCalls.Load() != 0 {
		t.Error("factory should not be called when context is cancelled")
	}
	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait did not complete")
	}
}

func TestHandleServiceDone_SelfHealMaxRetriesReached(t *testing.T) {
	// Ensure the maxRetries block sets StatusCrashed and forwards the real error.
	var crashName string
	var crashErr error
	var crashCalls atomic.Int32
	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnCrash(func(name string, err error) {
			crashName = name
			crashErr = err
			crashCalls.Add(1)
		}),
	)
	ctx := context.Background()
	o.ctx = ctx

	var factoryCalls atomic.Int32
	factory := func() Service {
		factoryCalls.Add(1)
		return &errSvc{err: errors.New("always")}
	}
	logCh := make(chan logEntry, 1)
	exitErr := errors.New("boom")
	entry := &serviceEntry{
		svc:        &errSvc{err: exitErr},
		cfg:        registerConfig{name: "x", factory: factory, maxRetries: 1},
		name:       "x",
		status:     StatusRunning,
		logger:     newServiceLogger("x", logCh, nil, LogLevelDebug),
		retryCount: 1, // already at max
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}
	o.handleServiceDone(entry, sc, exitErr, 0)

	if factoryCalls.Load() != 0 {
		t.Error("factory should not be called when maxRetries reached")
	}
	// Status should be Crashed, not Stopped.
	if entry.status != StatusCrashed {
		t.Errorf("expected StatusCrashed, got %v", entry.status)
	}
	if crashName != "x" {
		t.Errorf("expected OnCrash for 'x', got %q", crashName)
	}
	if !errors.Is(crashErr, exitErr) {
		t.Errorf("expected OnCrash to receive real error %v, got %v", exitErr, crashErr)
	}
	// The terminal crash must be reported exactly once: the pre-restart report
	// and the maxRetries branch must not both count it.
	if crashCalls.Load() != 1 {
		t.Errorf("OnCrash fired %d times, want exactly 1", crashCalls.Load())
	}
	if got := o.Metrics().Crashes; got != 1 {
		t.Errorf("Crashes = %d, want exactly 1", got)
	}
	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait did not complete")
	}
}

// TestHandleServiceDone_SelfHealMaxRetries_TeardownContextWins covers the
// teardown-owned terminal exit on the self-heal path: a cancelled per-entry
// teardown context (without the removing flag) wins over the crash report, so
// the entry ends StatusStopped and nothing is counted as a crash.
func TestHandleServiceDone_SelfHealMaxRetries_TeardownContextWins(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	o.ctx = context.Background()

	entry := &serviceEntry{
		svc:        &errSvc{err: errors.New("boom")},
		cfg:        registerConfig{name: "x", factory: func() Service { return &errSvc{err: errors.New("boom")} }, maxRetries: 1},
		name:       "x",
		status:     StatusRunning,
		logger:     newServiceLogger("x", make(chan logEntry, 1), nil, LogLevelDebug),
		retryCount: 1, // already at max
	}
	teardown, cancel := context.WithCancel(context.Background())
	cancel() // the teardown context is dead, but removing is not set
	entry.setTeardown(teardown, cancel)

	o.wg.Add(1)
	o.handleServiceDone(entry, ServiceContext{Context: o.ctx}, errors.New("boom"), 0)

	if s := o.statusOf(entry); s != StatusStopped {
		t.Errorf("status = %v, want StatusStopped (teardown wins the crash race)", s)
	}
	if got := o.Metrics().Crashes; got != 0 {
		t.Errorf("Crashes = %d, want 0: a teardown-owned exit is not a crash", got)
	}
	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait did not complete")
	}
}

func TestHandleServiceDone_WgDoneTrue_CtxCancelled(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	ctx, cancel := context.WithCancel(context.Background())
	o.ctx = ctx
	cancel()

	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		svc:    &errSvc{err: errors.New("boom")},
		cfg:    registerConfig{name: "x", factory: func() Service { return &testSvc{} }},
		name:   "x",
		status: StatusRunning,
		logger: newServiceLogger("x", logCh, nil, LogLevelDebug),
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}
	o.handleServiceDone(entry, sc, nil, 0)
	o.handleServiceDone(entry, sc, nil, 0)

	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait did not complete")
	}
}

func TestHandleServiceDone_WgDoneTrue_MaxRetries(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	ctx := context.Background()
	o.ctx = ctx

	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		svc:        &errSvc{err: errors.New("boom")},
		cfg:        registerConfig{name: "x", factory: func() Service { return &testSvc{} }, maxRetries: 1},
		name:       "x",
		status:     StatusRunning,
		logger:     newServiceLogger("x", logCh, nil, LogLevelDebug),
		retryCount: 1,
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}
	o.handleServiceDone(entry, sc, nil, 0)
	o.handleServiceDone(entry, sc, nil, 0)

	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait did not complete")
	}
}

func TestHandleServiceDone_WgDoneTrue_BackoffCancelled(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	ctx, cancel := context.WithCancel(context.Background())
	o.ctx = ctx

	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		svc:    &errSvc{err: errors.New("boom")},
		cfg:    registerConfig{name: "x", factory: func() Service { return &testSvc{} }, backoff: ConstantBackoff{Delay: 500 * time.Millisecond}},
		name:   "x",
		status: StatusRunning,
		logger: newServiceLogger("x", logCh, nil, LogLevelDebug),
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}

	go o.handleServiceDone(entry, sc, nil, 0)

	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	// Second call: wgDone is already true from the goroutine → else branch.
	o.handleServiceDone(entry, sc, nil, 0)

	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait did not complete")
	}
}

// waitForStatus polls until a service reaches the target status or times out.
func waitForStatus(t *testing.T, o *Orchestrator, name string, target ServiceStatus) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		s, _ := o.Status(name)
		if s == target {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("service %s never reached %s", name, target)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestCrashSemantics_PersistentError(t *testing.T) {
	crashCh := make(chan struct {
		name string
		err  error
	}, 1)
	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnCrash(func(name string, err error) {
			crashCh <- struct {
				name string
				err  error
			}{name, err}
		}),
	)
	_ = o.Register(&errSvc{err: errors.New("persistent boom")}, WithName("p"))
	_ = o.Start()
	defer o.Stop(time.Second)

	// Receiving implies the status already transitioned to Crashed (OnCrash
	// fires only on that transition) and gives a happens-before edge.
	crash := <-crashCh

	if crash.name != "p" {
		t.Errorf("expected OnCrash for 'p', got %q", crash.name)
	}
	if !strings.Contains(crash.err.Error(), "persistent boom") {
		t.Errorf("expected real error in OnCrash, got %v", crash.err)
	}
	if o.Metrics().Crashes < 1 {
		t.Error("expected Crashes metric to increment")
	}
}

func TestCrashSemantics_Panic(t *testing.T) {
	crashCh := make(chan error, 1)
	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnCrash(func(name string, err error) { crashCh <- err }),
	)
	_ = o.Register(&panicSvc{msg: "kaboom"}, WithName("p"))
	_ = o.Start()
	defer o.Stop(time.Second)

	crashErr := <-crashCh

	if crashErr == nil {
		t.Fatal("expected OnCrash error from panic")
	}
	if !strings.Contains(crashErr.Error(), "kaboom") {
		t.Errorf("expected panic value in OnCrash error, got %v", crashErr)
	}
}

func TestCrashSemantics_CleanExit(t *testing.T) {
	var crashCalls atomic.Int32
	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnCrash(func(name string, err error) { crashCalls.Add(1) }),
	)
	_ = o.Register(&testSvc{startFn: func(ctx context.Context) error { return nil }}, WithName("clean"))
	_ = o.Start()
	defer o.Stop(time.Second)

	waitForStatus(t, o, "clean", StatusStopped)

	if crashCalls.Load() != 0 {
		t.Errorf("clean exit should not fire OnCrash, got %d calls", crashCalls.Load())
	}
	if o.Metrics().Crashes != 0 {
		t.Errorf("clean exit should not increment Crashes, got %d", o.Metrics().Crashes)
	}
}

// assertStatusSequence fails when got does not equal want element by element.
func assertStatusSequence(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("status sequence = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("status sequence = %v, want %v", got, want)
		}
	}
}

// TestSelfHeal_CrashIsObserved pins the crash-observability contract for a
// self-healing service: a crash produces a Running→Crashed transition, an
// OnCrash call carrying the real error, and exactly one Crashes metric
// increment, even though the service is immediately restarted.
func TestSelfHeal_CrashIsObserved(t *testing.T) {
	crashErr := errors.New("self-heal boom")
	crashCh := make(chan error, 1)
	var transitions []string
	var mu sync.Mutex
	secondStarted := make(chan struct{})

	o := New(
		WithLogLevel(LogLevelWarn),
		WithHealthChecksDisabled(),
		WithOnCrash(func(name string, err error) { crashCh <- err }),
		WithOnStateChange(func(name string, from, to ServiceStatus) {
			mu.Lock()
			transitions = append(transitions, fmt.Sprintf("%s->%s", from, to))
			mu.Unlock()
		}),
	)
	factory := func() Service {
		return &testSvc{startFn: func(ctx context.Context) error {
			close(secondStarted)
			<-ctx.Done()
			return ctx.Err()
		}}
	}
	if err := o.Register(&testSvc{startFn: func(ctx context.Context) error { return crashErr }},
		WithName("healer"),
		WithSelfHeal(factory),
		WithBackoff(ConstantBackoff{Delay: time.Millisecond}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer o.Stop(time.Second)

	if got := <-crashCh; !errors.Is(got, crashErr) {
		t.Errorf("OnCrash err = %v, want the real exit error %v", got, crashErr)
	}
	if got := o.Metrics().Crashes; got != 1 {
		t.Errorf("Crashes = %d, want exactly 1", got)
	}

	select {
	case <-secondStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("self-heal never restarted the service")
	}
	if got, _ := o.Status("healer"); got != StatusRunning {
		t.Errorf("status after restart = %v, want StatusRunning while the new instance is live", got)
	}

	mu.Lock()
	defer mu.Unlock()
	seen := false
	for _, tr := range transitions {
		if tr == "running->crashed" {
			seen = true
		}
	}
	if !seen {
		t.Errorf("no running->crashed transition observed; got %v", transitions)
	}
}

// TestSelfHeal_CrashThenRestart_StatusSequence pins the exact status sequence a
// self-heal crash and restart produces, so the crash is never silent and the
// restart re-establishes Running.
func TestSelfHeal_CrashThenRestart_StatusSequence(t *testing.T) {
	crashErr := errors.New("boom")
	var transitions []string
	var mu sync.Mutex
	secondStarted := make(chan struct{})

	o := New(
		WithLogLevel(LogLevelWarn),
		WithHealthChecksDisabled(),
		WithOnStateChange(func(name string, from, to ServiceStatus) {
			mu.Lock()
			transitions = append(transitions, fmt.Sprintf("%s->%s", from, to))
			mu.Unlock()
		}),
	)
	factory := func() Service {
		return &testSvc{startFn: func(ctx context.Context) error {
			close(secondStarted)
			<-ctx.Done()
			return ctx.Err()
		}}
	}
	if err := o.Register(&testSvc{startFn: func(ctx context.Context) error { return crashErr }},
		WithName("healer"),
		WithSelfHeal(factory),
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
		t.Fatal("self-heal never restarted the service")
	}

	mu.Lock()
	got := append([]string(nil), transitions...)
	mu.Unlock()
	assertStatusSequence(t, got, []string{
		"registered->starting",
		"starting->running",
		"running->crashed",
		"crashed->running",
	})
}

// TestSelfHeal_CleanExit_StatusSequence pins that a cleanly-exiting self-heal
// service is reported Stopped during the backoff and Running after the restart,
// without being mislabelled a crash.
func TestSelfHeal_CleanExit_StatusSequence(t *testing.T) {
	var transitions []string
	var mu sync.Mutex
	var crashCalls atomic.Int32
	secondStarted := make(chan struct{})

	o := New(
		WithLogLevel(LogLevelWarn),
		WithHealthChecksDisabled(),
		WithOnCrash(func(name string, err error) { crashCalls.Add(1) }),
		WithOnStateChange(func(name string, from, to ServiceStatus) {
			mu.Lock()
			transitions = append(transitions, fmt.Sprintf("%s->%s", from, to))
			mu.Unlock()
		}),
	)
	factory := func() Service {
		return &testSvc{startFn: func(ctx context.Context) error {
			close(secondStarted)
			<-ctx.Done()
			return ctx.Err()
		}}
	}
	if err := o.Register(&testSvc{startFn: func(ctx context.Context) error { return nil }},
		WithName("clean"),
		WithSelfHeal(factory),
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
		t.Fatal("self-heal never restarted the cleanly-exited service")
	}

	if crashCalls.Load() != 0 {
		t.Errorf("OnCrash fired %d times for a clean exit, want 0", crashCalls.Load())
	}
	if got := o.Metrics().Crashes; got != 0 {
		t.Errorf("Crashes = %d for a clean exit, want 0", got)
	}

	mu.Lock()
	got := append([]string(nil), transitions...)
	mu.Unlock()
	assertStatusSequence(t, got, []string{
		"registered->starting",
		"starting->running",
		"running->stopped",
		"stopped->running",
	})
}

func TestHandleServiceDone_WgDoneTrue_BackoffCancelledV2(t *testing.T) {
	// The backoff ctx-cancelled else branch only fires when:
	// 1. The first handleServiceDone enters the backoff delay
	// 2. ctx is cancelled, triggering the backoff ctx-cancelled block
	// 3. In that block, wgDone is already true → else branch
	// Approach: manually set wgDone=true, have factory and non-cancelled context,
	// then cancel ctx during backoff.
	o := New(WithLogLevel(LogLevelWarn))
	ctx, cancel := context.WithCancel(context.Background())
	o.ctx = ctx

	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		svc:        &errSvc{err: errors.New("boom")},
		cfg:        registerConfig{name: "x", factory: func() Service { return &testSvc{} }, backoff: ConstantBackoff{Delay: 200 * time.Millisecond}},
		name:       "x",
		status:     StatusRunning,
		logger:     newServiceLogger("x", logCh, nil, LogLevelDebug),
		wgDone:     true, // already done
		retryCount: 1,
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}

	// Start handleServiceDone asynchronously — it will enter the backoff delay.
	go o.handleServiceDone(entry, sc, nil, 0)

	// Cancel during backoff, triggering the ctx.Done case in the backoff select.
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Give goroutine time to process and exit.
	time.Sleep(100 * time.Millisecond)

	// The goroutine should have hit the backoff ctx-cancelled else branch
	// because wgDone was already true.
	// Verify: wg should be done (wg.Done NOT called because wgDone was already true).
	// We need to call wg.Done ourselves since wgDone was already true and
	// the first Add(1) was never balanced.
	// Actually, wgDone=true means the goroutine won't call wg.Done.
	// So we need to call it manually to balance the Add(1).
	o.wg.Done()

	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait did not complete")
	}
}
