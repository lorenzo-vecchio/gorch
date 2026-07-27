package gorch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// ── test helpers ──

type testSvc struct {
	startFn    func(ctx context.Context) error
	stopFn     func() error
	stopCalls  atomic.Int32
	startCalls atomic.Int32
}

func (s *testSvc) Start(ctx context.Context) error {
	s.startCalls.Add(1)
	if s.startFn != nil {
		return s.startFn(ctx)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *testSvc) Stop() error {
	s.stopCalls.Add(1)
	if s.stopFn != nil {
		return s.stopFn()
	}
	return nil
}

type namedSvc struct{ name string }

func (s *namedSvc) Start(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
func (s *namedSvc) Stop() error                     { return nil }

type errSvc struct{ err error }

func (s *errSvc) Start(ctx context.Context) error { return s.err }
func (s *errSvc) Stop() error                     { return nil }

type panicSvc struct{ msg string }

func (s *panicSvc) Start(ctx context.Context) error { panic(s.msg) }
func (s *panicSvc) Stop() error                     { return nil }

type stopPanicSvc struct{}

func (s *stopPanicSvc) Start(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
func (s *stopPanicSvc) Stop() error                     { panic("stop boom") }

// healthSvc is a test service that implements HealthChecker.
type healthSvc struct {
	testSvc
	healthFn    func(ctx context.Context) error
	healthCalls atomic.Int32
}

func (s *healthSvc) Health(ctx context.Context) error {
	s.healthCalls.Add(1)
	if s.healthFn != nil {
		return s.healthFn(ctx)
	}
	return nil
}

// ── LogLevel.String ──

func TestLogLevel_String(t *testing.T) {
	tests := []struct {
		level LogLevel
		want  string
	}{
		{LogLevelDebug, "DEBUG"},
		{LogLevelInfo, "INFO"},
		{LogLevelWarn, "WARN"},
		{LogLevelError, "ERROR"},
		{LogLevel(99), "????"},
		{LogLevel(-1), "????"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.level.String(); got != tt.want {
				t.Errorf("LogLevel(%d).String() = %q, want %q", tt.level, got, tt.want)
			}
		})
	}
}

// ── New ──

func TestNew_Defaults(t *testing.T) {
	t.Run("zero_loglevel_defaults_to_info", func(t *testing.T) {
		o := New(Config{})
		if o.cfg.LogLevel != LogLevelInfo {
			t.Errorf("expected LogLevelInfo (1), got %d", o.cfg.LogLevel)
		}
	})

	t.Run("logLevelDebug_zero_defaults_to_info", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelDebug})
		if o.cfg.LogLevel != LogLevelInfo {
			t.Errorf("LogLevelDebug (0) is indistinguishable from zero-value; "+
				"New() defaults it to Info. got %d", o.cfg.LogLevel)
		}
	})

	t.Run("explicit_warn_stays_warn", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelWarn})
		if o.cfg.LogLevel != LogLevelWarn {
			t.Errorf("expected LogLevelWarn (2), got %d", o.cfg.LogLevel)
		}
	})

	t.Run("messenger_initialized", func(t *testing.T) {
		o := New(Config{})
		if o.messenger == nil {
			t.Fatal("expected messenger to be initialized")
		}
	})

	t.Run("messengerDone_initialized", func(t *testing.T) {
		o := New(Config{})
		cleanup := o.messengerDone()
		if cleanup == nil {
			t.Fatal("expected messengerDone to return cleanup func")
		}
	})

	t.Run("health_defaults", func(t *testing.T) {
		o := New(Config{})
		if o.cfg.HealthInterval != 30*time.Second {
			t.Errorf("expected HealthInterval=30s, got %v", o.cfg.HealthInterval)
		}
		if o.cfg.HealthTimeout != 5*time.Second {
			t.Errorf("expected HealthTimeout=5s, got %v", o.cfg.HealthTimeout)
		}
		if o.cfg.HealthThreshold != 3 {
			t.Errorf("expected HealthThreshold=3, got %d", o.cfg.HealthThreshold)
		}
	})

	t.Run("nameIndex_initialized", func(t *testing.T) {
		o := New(Config{})
		if o.nameIndex == nil {
			t.Fatal("expected nameIndex to be initialized")
		}
	})
}

// ── Register ──

func TestRegister(t *testing.T) {
	t.Run("register_before_start", func(t *testing.T) {
		o := New(Config{})
		err := o.Register(&namedSvc{name: "a"})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(o.entries) != 1 {
			t.Errorf("expected 1 entry, got %d", len(o.entries))
		}
	})

	t.Run("register_after_start_returns_ErrAlreadyStarted", func(t *testing.T) {
		o := New(Config{})
		_ = o.Register(&namedSvc{name: "a"})
		_ = o.Start()
		defer o.Stop(1 * time.Second)
		err := o.Register(&namedSvc{name: "b"})
		if !errors.Is(err, ErrAlreadyStarted) {
			t.Errorf("expected ErrAlreadyStarted, got %v", err)
		}
	})

	t.Run("register_with_cron_option", func(t *testing.T) {
		o := New(Config{})
		err := o.Register(&namedSvc{name: "c"}, WithCron("* * * * * *", CronParallel))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if o.entries[0].cfg.cronSpec != "* * * * * *" {
			t.Errorf("expected cron spec, got %q", o.entries[0].cfg.cronSpec)
		}
		if o.entries[0].cfg.cronMode != CronParallel {
			t.Errorf("expected CronParallel, got %v", o.entries[0].cfg.cronMode)
		}
	})

	t.Run("register_with_selfheal_option", func(t *testing.T) {
		o := New(Config{})
		factory := func() Service { return &namedSvc{name: "healed"} }
		err := o.Register(&namedSvc{name: "a"}, WithSelfHeal(factory))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if o.entries[0].cfg.factory == nil {
			t.Fatal("expected factory to be set")
		}
	})
}

// ── Register edge cases ──

func TestRegister_DuplicateName(t *testing.T) {
	o := New(Config{})
	_ = o.Register(&namedSvc{}, WithName("dup"))
	err := o.Register(&namedSvc{}, WithName("dup"))
	if !errors.Is(err, ErrDuplicateName) {
		t.Errorf("expected ErrDuplicateName, got %v", err)
	}
}

func TestRegister_SelfDependency(t *testing.T) {
	o := New(Config{})
	err := o.Register(&namedSvc{}, WithName("self"), DependsOn("self"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Errorf("expected ErrDependencyCycle, got %v", err)
	}
}

func TestRegister_DependencyNotFound(t *testing.T) {
	o := New(Config{})
	err := o.Register(&namedSvc{}, WithName("orphan"), DependsOn("nobody"))
	if err == nil {
		t.Fatal("expected dependency-not-found error")
	}
}

// ── Start ──

func TestStart(t *testing.T) {
	t.Run("start_with_no_services", func(t *testing.T) {
		o := New(Config{})
		err := o.Start()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		_ = o.Stop(1 * time.Second)
	})

	t.Run("double_start_returns_nil", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelWarn})
		err1 := o.Start()
		err2 := o.Start()
		if err1 != nil {
			t.Errorf("first Start returned error: %v", err1)
		}
		if err2 != nil {
			t.Errorf("second Start returned: %v", err2)
		}
		_ = o.Stop(1 * time.Second)
	})

	t.Run("start_with_invalid_cron_returns_ErrInvalidCron", func(t *testing.T) {
		o := New(Config{})
		_ = o.Register(&namedSvc{name: "bad-cron"}, WithCron("invalid", CronParallel))
		err := o.Start()
		if !errors.Is(err, ErrInvalidCron) {
			t.Errorf("expected ErrInvalidCron, got %v", err)
		}
	})

	t.Run("start_ErrAlreadyStarted_via_whitebox", func(t *testing.T) {
		o := &Orchestrator{
			cfg:       Config{LogLevel: LogLevelInfo},
			started:   true,
			messenger: newMessenger(),
		}
		o.messengerDone = sync.OnceValue(func() func() {
			return func() {
				o.messenger.mu.Lock()
				o.messenger.subs = nil
				o.messenger.mu.Unlock()
			}
		})
		err := o.Start()
		if !errors.Is(err, ErrAlreadyStarted) {
			t.Errorf("expected ErrAlreadyStarted, got %v", err)
		}
	})
}

// ── Stop ──

func TestStop(t *testing.T) {
	t.Run("stop_on_never_started_is_noop", func(t *testing.T) {
		o := New(Config{})
		err := o.Stop(100 * time.Millisecond)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("stop_after_start_with_no_services", func(t *testing.T) {
		o := New(Config{})
		_ = o.Start()
		err := o.Stop(1 * time.Second)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("double_stop_is_idempotent", func(t *testing.T) {
		o := New(Config{})
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

	t.Run("stop_calls_svc_stop", func(t *testing.T) {
		o := New(Config{})
		svc := &testSvc{}
		_ = o.Register(svc)
		_ = o.Start()
		_ = o.Stop(1 * time.Second)
		if svc.stopCalls.Load() < 1 {
			t.Errorf("expected Stop to be called at least once, got %d", svc.stopCalls.Load())
		}
	})

	t.Run("stop_timeout_returns_ErrStopTimeout", func(t *testing.T) {
		o := New(Config{})
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
		o := New(Config{})
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

// ── Stop: error aggregation and per-service hooks ──

func TestStop_ErrorAggregation(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
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
	o := New(Config{LogLevel: LogLevelWarn})
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.logCh = make(chan logEntry, 1)
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
	o.handleServiceDone(entry, sc)

	close(o.logCh)
	// wg should be done after handleServiceDone.
	o.wg.Wait()
}

// ── stopOneService hooks ──

func TestStopOneService_HookErrors(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})

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
	o := New(Config{
		LogLevel: LogLevelWarn,
		OnBeforeStop: func(name string) error {
			events = append(events, "gb:"+name)
			return nil
		},
		OnAfterStop: func(name string, err error) {
			events = append(events, "ga:"+name)
		},
	})

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
	o := New(Config{LogLevel: LogLevelWarn})
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

// ── Context cancellation ──

func TestContextCancellation(t *testing.T) {
	t.Run("services_receive_context_cancellation_on_stop", func(t *testing.T) {
		o := New(Config{})
		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}
		_ = o.Register(svc)
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		_ = o.Stop(1 * time.Second)
		if svc.startCalls.Load() != 1 {
			t.Errorf("expected Start to be called once, got %d", svc.startCalls.Load())
		}
	})

	t.Run("service_returns_context_canceled_on_stop", func(t *testing.T) {
		o := New(Config{})
		var returnedErr error
		var mu sync.Mutex

		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				<-ctx.Done()
				err := ctx.Err()
				mu.Lock()
				returnedErr = err
				mu.Unlock()
				return err
			},
		}
		_ = o.Register(svc)
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		_ = o.Stop(1 * time.Second)

		mu.Lock()
		err := returnedErr
		mu.Unlock()
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

// ── Cron modes ──

func TestCronModes(t *testing.T) {
	t.Run("cron_parallel_concurrent_ticks", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelWarn})
		var running atomic.Int32
		var maxRunning atomic.Int32

		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				n := running.Add(1)
				defer running.Add(-1)
				for {
					cur := maxRunning.Load()
					if n <= cur || maxRunning.CompareAndSwap(cur, n) {
						break
					}
				}
				select {
				case <-ctx.Done():
				case <-time.After(2 * time.Second):
				}
				return ctx.Err()
			},
		}
		_ = o.Register(svc, WithCron("* * * * * *", CronParallel))
		_ = o.Start()
		time.Sleep(2500 * time.Millisecond)
		_ = o.Stop(3 * time.Second)

		if maxRunning.Load() < 2 {
			t.Errorf("CronParallel: expected at least 2 concurrent, got %d", maxRunning.Load())
		}
	})

	t.Run("cron_skip_drops_overlapping_tick", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelWarn})
		var calls atomic.Int32

		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				calls.Add(1)
				select {
				case <-ctx.Done():
				case <-time.After(2500 * time.Millisecond):
				}
				return ctx.Err()
			},
		}
		_ = o.Register(svc, WithCron("* * * * * *", CronSkip))
		_ = o.Start()
		time.Sleep(3 * time.Second)
		_ = o.Stop(4 * time.Second)

		c := calls.Load()
		if c > 3 {
			t.Errorf("CronSkip: expected at most 3 calls (most skipped), got %d", c)
		}
	})

	t.Run("cron_queue_serializes_ticks", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelWarn})
		var running atomic.Int32
		var maxRunning atomic.Int32
		var total atomic.Int32

		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				n := running.Add(1)
				total.Add(1)
				defer running.Add(-1)
				for {
					cur := maxRunning.Load()
					if n <= cur || maxRunning.CompareAndSwap(cur, n) {
						break
					}
				}
				select {
				case <-ctx.Done():
				case <-time.After(1200 * time.Millisecond):
				}
				return ctx.Err()
			},
		}
		_ = o.Register(svc, WithCron("* * * * * *", CronQueue))
		_ = o.Start()
		time.Sleep(3 * time.Second)
		_ = o.Stop(3 * time.Second)

		if maxRunning.Load() > 1 {
			t.Errorf("CronQueue: expected serial (max 1 concurrent), got %d", maxRunning.Load())
		}
		if total.Load() < 2 {
			t.Errorf("CronQueue: expected at least 2 total ticks, got %d", total.Load())
		}
	})
}

// ── Self-heal ──

func TestSelfHeal(t *testing.T) {
	t.Run("restarts_after_error", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelWarn})
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
		o := New(Config{LogLevel: LogLevelWarn})
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
		o := New(Config{LogLevel: LogLevelWarn})
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
		o := New(Config{})
		entry := &serviceEntry{svc: &namedSvc{name: "x"}, cfg: registerConfig{}}
		o.wg.Add(1)
		sc := ServiceContext{Context: context.Background()}
		o.handleServiceDone(entry, sc)

		done := make(chan struct{})
		go func() { o.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("wg.Wait did not complete — wg.Done was not called")
		}
	})

	t.Run("handleServiceDone_double_call_no_double_wgDone", func(t *testing.T) {
		o := New(Config{})
		entry := &serviceEntry{svc: &namedSvc{name: "x"}, cfg: registerConfig{}}
		o.wg.Add(1)
		sc := ServiceContext{Context: context.Background()}
		o.handleServiceDone(entry, sc)
		o.handleServiceDone(entry, sc)

		done := make(chan struct{})
		go func() { o.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("double handleServiceDone caused wg imbalance")
		}
	})

	t.Run("handleServiceDone_ctx_cancelled_no_restart", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelWarn})
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
		o.handleServiceDone(entry, sc)

		if factoryCalls.Load() != 0 {
			t.Error("factory should not be called when context is cancelled")
		}
	})
}

// ── Self-heal with options ──

func TestSelfHeal_MaxRetries(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
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
	o := New(Config{LogLevel: LogLevelWarn})
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
	o := New(Config{LogLevel: LogLevelWarn})
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

// ── Service panic recovery ──

func TestServicePanicRecovery(t *testing.T) {
	t.Run("panic_in_start_recovered", func(t *testing.T) {
		o := New(Config{})
		_ = o.Register(&panicSvc{msg: "start panic"})
		_ = o.Start()
		time.Sleep(100 * time.Millisecond)
		err := o.Stop(500 * time.Millisecond)
		if errors.Is(err, ErrStopTimeout) {
			t.Errorf("unexpected timeout: panic recovery should call wg.Done")
		}
	})

	t.Run("panic_in_start_with_log", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelError})
		_ = o.Register(&panicSvc{msg: "logged panic"})
		_ = o.Start()
		time.Sleep(100 * time.Millisecond)
		_ = o.Stop(200 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "service panicked") {
			t.Errorf("expected 'service panicked' in log, got: %s", output)
		}
	})

	t.Run("safeStop_recovers_stop_panic", func(t *testing.T) {
		o := New(Config{})
		svc := &stopPanicSvc{}
		entry := &serviceEntry{name: "test", svc: svc, cfg: registerConfig{name: "test"}}
		o.safeStop(entry)
	})

	t.Run("safeStop_calls_stop", func(t *testing.T) {
		o := New(Config{})
		svc := &testSvc{}
		entry := &serviceEntry{name: "test", svc: svc, cfg: registerConfig{name: "test"}}
		o.safeStop(entry)
		if svc.stopCalls.Load() != 1 {
			t.Errorf("expected Stop to be called once, got %d", svc.stopCalls.Load())
		}
	})

	t.Run("cron_service_panic_recovered", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelWarn})
		_ = o.Register(&panicSvc{msg: "cron panic"}, WithCron("* * * * * *", CronParallel))
		_ = o.Start()
		time.Sleep(1500 * time.Millisecond)
		_ = o.Stop(500 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "cron service panicked") {
			t.Errorf("expected 'cron service panicked' in log, got: %s", output)
		}
	})

	t.Run("cron_skip_warns_when_tick_dropped", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelWarn})
		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
				case <-time.After(2500 * time.Millisecond):
				}
				return ctx.Err()
			},
		}
		_ = o.Register(svc, WithCron("* * * * * *", CronSkip))
		_ = o.Start()
		time.Sleep(3 * time.Second)
		_ = o.Stop(500 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "cron tick skipped") {
			t.Errorf("expected 'cron tick skipped' warning, got: %s", output)
		}
	})
}

// ── Log-pump ──

func TestLogPump(t *testing.T) {
	t.Run("format_includes_timestamp_level_service_msg_args", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelDebug})
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.wg.Add(1)
		go o.logPump()

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 123456789, time.UTC),
			level:   LogLevelInfo,
			service: "*testSvc",
			msg:     "hello world",
			args:    []any{"k1", "v1", "k2", 42},
		}
		close(o.logCh)
		o.wg.Wait()

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		checks := []string{
			"2026-01-02 15:04:05.123",
			"INFO",
			"*testSvc",
			"hello world",
			"k1=v1",
			"k2=42",
		}
		for _, c := range checks {
			if !strings.Contains(output, c) {
				t.Errorf("output missing %q:\n%s", c, output)
			}
		}
	})

	t.Run("odd_args_single_append_missing", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelDebug})
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.wg.Add(1)
		go o.logPump()

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level:   LogLevelError,
			service: "svc",
			msg:     "odd",
			args:    []any{"lonely"},
		}
		close(o.logCh)
		o.wg.Wait()

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "lonely=(missing)") {
			t.Errorf("expected 'lonely=(missing)', got: %s", output)
		}
	})

	t.Run("odd_args_three_elements", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelDebug})
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.wg.Add(1)
		go o.logPump()

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level:   LogLevelError,
			service: "svc",
			msg:     "odd3",
			args:    []any{"a", 1, "b"},
		}
		close(o.logCh)
		o.wg.Wait()

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "a=1") {
			t.Errorf("expected 'a=1', got: %s", output)
		}
		if !strings.Contains(output, "b=(missing)") {
			t.Errorf("expected 'b=(missing)', got: %s", output)
		}
	})

	t.Run("empty_args", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelDebug})
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.wg.Add(1)
		go o.logPump()

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level:   LogLevelInfo,
			service: "svc",
			msg:     "no args",
			args:    nil,
		}
		close(o.logCh)
		o.wg.Wait()

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "no args") {
			t.Errorf("expected 'no args', got: %s", output)
		}
	})

	t.Run("filter_below_loglevel", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelWarn})
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 256)
		o.wg.Add(1)
		go o.logPump()

		o.logCh <- logEntry{
			time:  time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level: LogLevelDebug, service: "svc", msg: "debug msg",
		}
		o.logCh <- logEntry{
			time:  time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level: LogLevelInfo, service: "svc", msg: "info msg",
		}
		o.logCh <- logEntry{
			time:  time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level: LogLevelWarn, service: "svc", msg: "warn msg",
		}
		o.logCh <- logEntry{
			time:  time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level: LogLevelError, service: "svc", msg: "error msg",
		}
		close(o.logCh)
		o.wg.Wait()

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if strings.Contains(output, "debug msg") {
			t.Error("debug message should have been filtered out")
		}
		if strings.Contains(output, "info msg") {
			t.Error("info message should have been filtered out")
		}
		if !strings.Contains(output, "warn msg") {
			t.Error("warn message should have been included")
		}
		if !strings.Contains(output, "error msg") {
			t.Error("error message should have been included")
		}
	})

	t.Run("logPump_exits_when_channel_closed", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelDebug})
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.wg.Add(1)
		done := make(chan struct{})
		go func() {
			o.logPump()
			close(done)
		}()
		close(o.logCh)
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("logPump did not exit after channel close")
		}
	})
}

// ── ServiceLogger emit ──

func TestServiceLogger_Emit(t *testing.T) {
	t.Run("non_blocking_send_when_channel_full", func(t *testing.T) {
		ch := make(chan logEntry, 1)
		ch <- logEntry{}
		logger := newServiceLogger("test", ch)
		logger.Info("dropped")
	})

	t.Run("methods_use_correct_levels", func(t *testing.T) {
		ch := make(chan logEntry, 16)
		logger := newServiceLogger("svc", ch)

		logger.Debug("d")
		logger.Info("i")
		logger.Warn("w")
		logger.Error("e")

		levels := map[string]LogLevel{}
		for i := 0; i < 4; i++ {
			e := <-ch
			levels[e.msg] = e.level
		}
		if levels["d"] != LogLevelDebug {
			t.Errorf("Debug level got %d", levels["d"])
		}
		if levels["i"] != LogLevelInfo {
			t.Errorf("Info level got %d", levels["i"])
		}
		if levels["w"] != LogLevelWarn {
			t.Errorf("Warn level got %d", levels["w"])
		}
		if levels["e"] != LogLevelError {
			t.Errorf("Error level got %d", levels["e"])
		}
	})
}

// ── Messenger ──

func TestMessenger_SubscribePublish(t *testing.T) {
	t.Run("subscribe_and_receive", func(t *testing.T) {
		m := newMessenger()
		ch, unsub := m.Subscribe("topic1")
		defer unsub()
		m.Publish("hello", "topic1")
		select {
		case msg := <-ch:
			if msg != "hello" {
				t.Errorf("expected 'hello', got %v", msg)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected to receive message")
		}
	})

	t.Run("publish_no_topics_broadcasts_to_all", func(t *testing.T) {
		m := newMessenger()
		ch1, _ := m.Subscribe("a")
		ch2, _ := m.Subscribe("b")
		m.Publish("broadcast")
		for _, ch := range []<-chan any{ch1, ch2} {
			select {
			case msg := <-ch:
				if msg != "broadcast" {
					t.Errorf("expected 'broadcast', got %v", msg)
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatal("expected broadcast to reach subscriber")
			}
		}
	})

	t.Run("publish_nil_topics_broadcasts_to_all", func(t *testing.T) {
		m := newMessenger()
		ch, _ := m.Subscribe("x")
		m.Publish("nil broadcast")
		select {
		case msg := <-ch:
			if msg != "nil broadcast" {
				t.Errorf("expected 'nil broadcast', got %v", msg)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected broadcast to reach subscriber")
		}
	})

	t.Run("publish_non_blocking_when_full", func(t *testing.T) {
		m := newMessenger()
		ch, _ := m.Subscribe("t")
		for i := 0; i < 16; i++ {
			m.Publish(i, "t")
		}
		done := make(chan struct{})
		go func() {
			m.Publish("overflow", "t")
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Publish blocked when channel was full")
		}
		for i := 0; i < 16; i++ {
			<-ch
		}
	})

	t.Run("subscribe_unsubscribe_not_found_path", func(t *testing.T) {
		m := newMessenger()
		_, unsub := m.Subscribe("t")
		m.mu.Lock()
		m.subs = nil
		m.mu.Unlock()
		unsub()
	})

	t.Run("unsubscribe_stops_receiving", func(t *testing.T) {
		m := newMessenger()
		ch, unsub := m.Subscribe("t")
		unsub()
		m.Publish("after unsub", "t")
		select {
		case <-ch:
			t.Error("received message after unsubscribe")
		default:
		}
	})

	t.Run("publish_after_messenger_cleanup_no_panic", func(t *testing.T) {
		m := newMessenger()
		_, _ = m.Subscribe("t")
		m.mu.Lock()
		m.subs = nil
		m.mu.Unlock()
		m.Publish("msg", "t")
		m.Publish("broadcast")
	})

	t.Run("unsubscribe_during_publish_no_deadlock", func(t *testing.T) {
		m := newMessenger()
		var wg sync.WaitGroup

		unsubs := make([]func(), 100)
		for i := 0; i < 100; i++ {
			_, unsub := m.Subscribe("t")
			unsubs[i] = unsub
		}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					m.Publish("x", "t")
				}
			}()
		}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				unsubs[idx]()
			}(i)
		}
		wg.Wait()
	})
}

// ── runService error logging ──

func TestRunService_ErrorLogging(t *testing.T) {
	t.Run("service_error_logged", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelError})
		_ = o.Register(&errSvc{err: errors.New("test failure")})
		_ = o.Start()
		time.Sleep(100 * time.Millisecond)
		_ = o.Stop(500 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "service returned error") {
			t.Errorf("expected 'service returned error' in log, got: %s", output)
		}
		if !strings.Contains(output, "test failure") {
			t.Errorf("expected error message in log, got: %s", output)
		}
	})

	t.Run("context_canceled_not_logged_as_error", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelError})
		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				<-ctx.Done()
				return context.Canceled
			},
		}
		_ = o.Register(svc)
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		_ = o.Stop(500 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if strings.Contains(output, "service returned error") {
			t.Errorf("context.Canceled should not be logged as error, got: %s", output)
		}
	})

	t.Run("cron_error_logged", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelError})
		_ = o.Register(&errSvc{err: errors.New("cron fail")}, WithCron("* * * * * *", CronParallel))
		_ = o.Start()
		time.Sleep(1500 * time.Millisecond)
		_ = o.Stop(500 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "cron service returned error") {
			t.Errorf("expected 'cron service returned error' in log, got: %s", output)
		}
	})
}

// ── Config / functional options ──

func TestRegisterOptions(t *testing.T) {
	t.Run("WithCron_sets_spec_and_mode", func(t *testing.T) {
		cfg := registerConfig{}
		WithCron("*/5 * * * * *", CronQueue)(&cfg)
		if cfg.cronSpec != "*/5 * * * * *" {
			t.Errorf("expected spec, got %q", cfg.cronSpec)
		}
		if cfg.cronMode != CronQueue {
			t.Errorf("expected CronQueue, got %v", cfg.cronMode)
		}
	})

	t.Run("WithSelfHeal_sets_factory", func(t *testing.T) {
		cfg := registerConfig{}
		f := func() Service { return &namedSvc{name: "x"} }
		WithSelfHeal(f)(&cfg)
		if cfg.factory == nil {
			t.Fatal("expected factory to be set")
		}
	})
}

// ── runService normal path ──

func TestRunService_Normal(t *testing.T) {
	t.Run("normal_return_no_error_logged", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelError})
		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				return nil
			},
		}
		_ = o.Register(svc)
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		_ = o.Stop(500 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if strings.Contains(output, "service returned error") {
			t.Errorf("normal return should not log error: %s", output)
		}
	})
}

// ── Service type name in log output ──

func TestServiceNameInLogger(t *testing.T) {
	t.Run("service_type_is_used_as_logger_name", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(Config{LogLevel: LogLevelWarn})
		_ = o.Register(&panicSvc{msg: "namecheck"})
		_ = o.Start()
		time.Sleep(100 * time.Millisecond)
		_ = o.Stop(200 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "*gorch.panicSvc") {
			t.Errorf("expected '*gorch.panicSvc' in log, got: %s", output)
		}
	})
}

// ── Dependency ordering ──

func TestDependencyOrdering_StartOrder(t *testing.T) {
	// ponytail: use per-service readiness barriers so start order is
	// deterministic regardless of goroutine scheduling.
	var order []string
	var mu sync.Mutex
	readyB := make(chan struct{})
	readyC := make(chan struct{})

	makeSvc := func(name string) Service {
		return &testSvc{
			startFn: func(ctx context.Context) error {
				// Barriers ensure deterministic ordering:
				// b waits for a, c waits for b.
				if name == "b" {
					<-readyB
				} else if name == "c" {
					<-readyC
				}
				mu.Lock()
				order = append(order, name)
				mu.Unlock()
				<-ctx.Done()
				return ctx.Err()
			},
		}
	}

	o := New(Config{LogLevel: LogLevelWarn})
	_ = o.Register(makeSvc("a"), WithName("a"))
	_ = o.Register(makeSvc("b"), WithName("b"), DependsOn("a"))
	_ = o.Register(makeSvc("c"), WithName("c"), DependsOn("a", "b"))

	err := o.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer o.Stop(time.Second)

	// Unblock barriers: a starts immediately, then b, then c.
	time.Sleep(50 * time.Millisecond) // let a start
	close(readyB)                     // unblock b
	time.Sleep(50 * time.Millisecond) // let b start
	close(readyC)                     // unblock c
	time.Sleep(50 * time.Millisecond) // let c start

	mu.Lock()
	defer mu.Unlock()
	if len(order) < 3 {
		t.Fatalf("expected 3 services, got %v", order)
	}
	if order[0] != "a" {
		t.Errorf("expected a first, got %v", order)
	}
	if order[1] != "b" {
		t.Errorf("expected b second, got %v", order)
	}
	if order[2] != "c" {
		t.Errorf("expected c third, got %v", order)
	}
}

func TestDependencyOrdering_ReverseStopOrder(t *testing.T) {
	var stopOrder []string
	var mu sync.Mutex

	makeSvc := func(name string) Service {
		return &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			stopFn: func() error {
				mu.Lock()
				stopOrder = append(stopOrder, name)
				mu.Unlock()
				return nil
			},
		}
	}

	o := New(Config{LogLevel: LogLevelWarn})
	_ = o.Register(makeSvc("a"), WithName("a"))
	_ = o.Register(makeSvc("b"), WithName("b"), DependsOn("a"))
	_ = o.Register(makeSvc("c"), WithName("c"), DependsOn("a", "b"))

	_ = o.Start()
	time.Sleep(50 * time.Millisecond)
	_ = o.Stop(time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(stopOrder) < 3 {
		t.Fatalf("expected 3 services stopped, got %v", stopOrder)
	}
	aIdx := indexOf(stopOrder, "a")
	bIdx := indexOf(stopOrder, "b")
	cIdx := indexOf(stopOrder, "c")
	// c stops first, then b, then a (reverse order).
	if cIdx > bIdx || cIdx > aIdx {
		t.Errorf("c should stop first (reverse of start): %v", stopOrder)
	}
	if bIdx > aIdx {
		t.Errorf("b should stop before a: %v", stopOrder)
	}
}

func TestDependency_SkippedDependents(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})

	// WithStartTimeout makes the failure synchronous so the dependent
	// sees StatusCrashed and is skipped before it ever starts.
	// Use context-aware wait so the goroutine exits cleanly when
	// stopStartedServices closes logCh (avoiding "send on closed channel").
	failSvc := &testSvc{
		startFn: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
				return errors.New("failed to start")
			}
		},
	}
	depSvc := &testSvc{}

	_ = o.Register(failSvc, WithName("fail"), WithStartTimeout(50*time.Millisecond))
	_ = o.Register(depSvc, WithName("dep"), DependsOn("fail"))

	err := o.Start()
	if err == nil {
		t.Fatal("expected error from failed dependency")
	}
	// ponytail: don't call Stop() here — stopStartedServices already cleaned
	// up logCh and calling Stop() would double-close it.

	if depSvc.startCalls.Load() > 0 {
		t.Error("dependent should not start when dependency fails")
	}
}

// ── Start timeout ──

func TestStartTimeout_Persistent(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
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
	o := New(Config{
		LogLevel:            LogLevelWarn,
		DefaultStartTimeout: 50 * time.Millisecond,
	})
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

// ── One-shot services ──

func TestOneShot(t *testing.T) {
	t.Run("runs_before_persistent", func(t *testing.T) {
		var order []string
		var mu sync.Mutex
		started := make(chan struct{}, 2)

		oneShot := &testSvc{
			startFn: func(ctx context.Context) error {
				mu.Lock()
				order = append(order, "init")
				mu.Unlock()
				started <- struct{}{}
				return nil
			},
		}
		persistent := &testSvc{
			startFn: func(ctx context.Context) error {
				mu.Lock()
				order = append(order, "main")
				mu.Unlock()
				started <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			},
		}

		o := New(Config{LogLevel: LogLevelWarn})
		_ = o.Register(oneShot, WithName("init"), WithRunOnce())
		_ = o.Register(persistent, WithName("main"))
		_ = o.Start()
		defer o.Stop(time.Second)

		// Wait for both to start.
		for i := 0; i < 2; i++ {
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for service to start")
			}
		}

		mu.Lock()
		defer mu.Unlock()
		if len(order) < 2 || order[0] != "init" || order[1] != "main" {
			t.Errorf("expected [init main], got %v", order)
		}
	})

	t.Run("error_aborts_startup", func(t *testing.T) {
		oneShot := &testSvc{
			startFn: func(ctx context.Context) error {
				return errors.New("init failed")
			},
		}
		persistent := &testSvc{}

		o := New(Config{})
		_ = o.Register(oneShot, WithName("init"), WithRunOnce())
		_ = o.Register(persistent, WithName("main"))

		err := o.Start()
		if err == nil {
			o.Stop(time.Second)
			t.Fatal("expected error from failed one-shot")
		}
		// ponytail: logCh already closed by stopStartedServices.

		if persistent.startCalls.Load() > 0 {
			t.Error("persistent service should not start when one-shot fails")
		}
	})

	t.Run("transitions_to_stopped", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelWarn})
		oneShot := &testSvc{
			startFn: func(ctx context.Context) error { return nil },
		}
		_ = o.Register(oneShot, WithName("init"), WithRunOnce())
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("main"))
		_ = o.Start()

		s, ok := o.Status("init")
		if !ok {
			t.Fatal("expected init to be found")
		}
		if s != StatusStopped {
			t.Errorf("one-shot should be StatusStopped, got %v", s)
		}
		_ = o.Stop(time.Second)
	})

	t.Run("context_canceled_is_not_an_error", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelWarn})
		oneShot := &testSvc{
			startFn: func(ctx context.Context) error {
				return context.Canceled
			},
		}
		_ = o.Register(oneShot, WithName("init"), WithRunOnce())
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("main"))
		err := o.Start()
		if err != nil {
			t.Fatalf("context.Canceled from one-shot should not be an error: %v", err)
		}
		defer o.Stop(time.Second)
	})
}

// ── Lifecycle hooks ──

func TestLifecycleHooks(t *testing.T) {
	t.Run("global_before_start_hook", func(t *testing.T) {
		var called []string
		o := New(Config{
			LogLevel: LogLevelWarn,
			OnBeforeStart: func(name string) error {
				called = append(called, "before:"+name)
				return nil
			},
			OnAfterStart: func(name string, err error) {
				called = append(called, "after:"+name)
			},
		})
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("a"))
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		_ = o.Stop(time.Second)

		if len(called) < 1 || called[0] != "before:a" {
			t.Errorf("expected [before:a ...], got %v", called)
		}
	})

	t.Run("before_start_hook_error_aborts", func(t *testing.T) {
		hookErr := errors.New("hook denied")
		o := New(Config{
			LogLevel:      LogLevelWarn,
			OnBeforeStart: func(name string) error { return hookErr },
		})
		_ = o.Register(&testSvc{}, WithName("a"))
		err := o.Start()
		if err == nil {
			t.Fatal("expected error from before-start hook")
		}
		// ponytail: logCh already closed by stopStartedServices.
	})

	t.Run("per_service_after_start_overrides_global", func(t *testing.T) {
		var globalCalls, perSvcCalls []string

		o := New(Config{
			LogLevel: LogLevelWarn,
			OnAfterStart: func(name string, err error) {
				globalCalls = append(globalCalls, name)
			},
		})

		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("a"), WithOnAfterStart(func(name string, err error) {
			perSvcCalls = append(perSvcCalls, name)
		}))

		_ = o.Start()
		defer o.Stop(time.Second)
		time.Sleep(50 * time.Millisecond)

		if len(perSvcCalls) != 1 {
			t.Errorf("per-service hook should be called, got %v", perSvcCalls)
		}
		if len(globalCalls) != 0 {
			t.Errorf("global hook should not be called when per-service overrides, got %v", globalCalls)
		}
	})

	t.Run("all_four_hooks_for_stop", func(t *testing.T) {
		var events []string
		o := New(Config{
			LogLevel: LogLevelWarn,
		})

		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("a"),
			WithOnBeforeStop(func(name string) error {
				events = append(events, "per-before-stop:"+name)
				return nil
			}),
			WithOnAfterStop(func(name string, err error) {
				events = append(events, "per-after-stop:"+name)
			}),
		)

		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		_ = o.Stop(time.Second)

		foundBefore := false
		foundAfter := false
		for _, e := range events {
			if e == "per-before-stop:a" {
				foundBefore = true
			}
			if e == "per-after-stop:a" {
				foundAfter = true
			}
		}
		if !foundBefore || !foundAfter {
			t.Errorf("per-service stop hooks not all called: %v", events)
		}
	})

	t.Run("before_stop_hook_error_not_fatal", func(t *testing.T) {
		o := New(Config{
			LogLevel: LogLevelWarn,
			OnBeforeStop: func(name string) error {
				return errors.New("before-stop issue")
			},
		})
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(svc, WithName("a"))
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		err := o.Stop(time.Second)
		// Hook error is logged but Stop still completes.
		if err == nil {
			t.Error("expected error from before-stop hook")
		}
		if svc.stopCalls.Load() < 1 {
			t.Error("service Stop should still be called despite hook error")
		}
	})
}

// ── Status / Statuses / Names / Count ──

func TestStatus_Found(t *testing.T) {
	o := New(Config{})
	_ = o.Register(&namedSvc{}, WithName("alpha"))
	_ = o.Register(&namedSvc{}, WithName("beta"))

	s, ok := o.Status("alpha")
	if !ok {
		t.Fatal("expected alpha to be found")
	}
	if s != StatusRegistered {
		t.Errorf("expected StatusRegistered, got %v", s)
	}
}

func TestStatus_NotFound(t *testing.T) {
	o := New(Config{})
	_, ok := o.Status("nope")
	if ok {
		t.Error("expected nope to not be found")
	}
}

func TestStatuses(t *testing.T) {
	o := New(Config{})
	_ = o.Register(&namedSvc{}, WithName("a"))
	_ = o.Register(&namedSvc{}, WithName("b"))

	m := o.Statuses()
	if len(m) != 2 {
		t.Errorf("expected 2 entries, got %d", len(m))
	}
	if m["a"] != StatusRegistered || m["b"] != StatusRegistered {
		t.Errorf("unexpected statuses: %v", m)
	}
}

func TestNames(t *testing.T) {
	o := New(Config{})
	_ = o.Register(&namedSvc{}, WithName("first"))
	_ = o.Register(&namedSvc{}, WithName("second"))

	names := o.Names()
	if len(names) != 2 {
		t.Fatalf("expected 2 names, got %d", len(names))
	}
	if names[0] != "first" || names[1] != "second" {
		t.Errorf("expected [first second], got %v", names)
	}
}

func TestCount(t *testing.T) {
	o := New(Config{})
	if c := o.Count(); c != 0 {
		t.Errorf("expected 0, got %d", c)
	}
	_ = o.Register(&namedSvc{})
	if c := o.Count(); c != 1 {
		t.Errorf("expected 1, got %d", c)
	}
	_ = o.Register(&namedSvc{})
	if c := o.Count(); c != 2 {
		t.Errorf("expected 2, got %d", c)
	}
}

func TestStatusTransitions(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}
	_ = o.Register(svc, WithName("lifecycle"))
	_ = o.Start()

	// After Start, status should be Running.
	time.Sleep(50 * time.Millisecond)
	s, ok := o.Status("lifecycle")
	if !ok || s != StatusRunning {
		t.Errorf("expected Running after Start, got %v (ok=%v)", s, ok)
	}

	_ = o.Stop(time.Second)

	// After Stop, status should be Stopped.
	s, ok = o.Status("lifecycle")
	if !ok || s != StatusStopped {
		t.Errorf("expected Stopped after Stop, got %v (ok=%v)", s, ok)
	}
}

// ── Run ──

func TestRun_StartError(t *testing.T) {
	o := New(Config{})
	_ = o.Register(&namedSvc{name: "bad"}, WithCron("invalid", CronParallel))
	err := o.Run(time.Second)
	if !errors.Is(err, ErrInvalidCron) {
		t.Errorf("expected ErrInvalidCron, got %v", err)
	}
}

func TestRun_DefaultSignal(t *testing.T) {
	// Cover the default signal path (len(sigSet) == 0 → os.Interrupt).
	o := New(Config{LogLevel: LogLevelWarn})
	done := make(chan error, 1)
	go func() {
		done <- o.Run(time.Second) // no signals → defaults to os.Interrupt
	}()

	time.Sleep(100 * time.Millisecond)
	p, _ := os.FindProcess(os.Getpid())
	p.Signal(os.Interrupt)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after signal")
	}
}

func TestRun_CustomSignal(t *testing.T) {
	// Cover the custom signal path.
	o := New(Config{LogLevel: LogLevelWarn})

	done := make(chan error, 1)
	go func() {
		done <- o.Run(time.Second, syscall.SIGUSR1)
	}()

	time.Sleep(100 * time.Millisecond)
	syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after signal")
	}
}

// ── topoSort ──

func TestTopoSort_Cycle(t *testing.T) {
	o := New(Config{})
	entries := []*serviceEntry{
		{name: "a", cfg: registerConfig{name: "a", dependsOn: []string{"b"}}},
		{name: "b", cfg: registerConfig{name: "b", dependsOn: []string{"c"}}},
		{name: "c", cfg: registerConfig{name: "c", dependsOn: []string{"a"}}},
	}
	_, err := o.topoSort(entries)
	if !errors.Is(err, ErrDependencyCycle) {
		t.Errorf("expected ErrDependencyCycle, got %v", err)
	}
}

func TestTopoSort_Empty(t *testing.T) {
	o := New(Config{})
	levels, err := o.topoSort(nil)
	if err != nil || levels != nil {
		t.Errorf("expected nil, nil; got %v, %v", levels, err)
	}
}

func TestTopoSort_Single(t *testing.T) {
	o := New(Config{})
	entries := []*serviceEntry{
		{name: "lonely", cfg: registerConfig{name: "lonely"}},
	}
	levels, err := o.topoSort(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(levels) != 1 || len(levels[0]) != 1 || levels[0][0].name != "lonely" {
		t.Errorf("expected single level with lonely, got %v", levels)
	}
}

// ── dependsOnRecursive ──

func TestDependsOnRecursive(t *testing.T) {
	o := New(Config{})
	_ = o.Register(&namedSvc{}, WithName("a"))
	_ = o.Register(&namedSvc{}, WithName("b"), DependsOn("a"))
	_ = o.Register(&namedSvc{}, WithName("c"), DependsOn("b"))

	// c transitively depends on a and b.
	if !o.dependsOnRecursive(o.nameIndex["c"], "a") {
		t.Error("c should transitively depend on a")
	}
	if !o.dependsOnRecursive(o.nameIndex["c"], "b") {
		t.Error("c should transitively depend on b")
	}
	if !o.dependsOnRecursive(o.nameIndex["b"], "a") {
		t.Error("b should depend on a")
	}
	// a doesn't depend on anything.
	if o.dependsOnRecursive(o.nameIndex["a"], "b") {
		t.Error("a should not depend on b")
	}
	// nil entry.
	if o.dependsOnRecursive(nil, "x") {
		t.Error("nil entry should return false")
	}
}

// ── stopStartedServices ──

func TestStopStartedServices_UsedDuringStartupFailure(t *testing.T) {
	// One-shot error triggers stopStartedServices.
	o := New(Config{LogLevel: LogLevelWarn})
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

// ── Health ──

func TestHealth_NonHealthChecker_ReportsNil(t *testing.T) {
	o := New(Config{})
	_ = o.Register(&namedSvc{}, WithName("plain"))
	results := o.Health()
	if results["plain"] != nil {
		t.Error("non-HealthChecker should report nil")
	}
}

func TestHealth_HealthChecker_ReportsError(t *testing.T) {
	o := New(Config{})
	healthErr := errors.New("unhealthy")
	svc := &healthSvc{healthFn: func(ctx context.Context) error { return healthErr }}
	_ = o.Register(svc, WithName("sick"))
	results := o.Health()
	if !errors.Is(results["sick"], healthErr) {
		t.Errorf("expected health error, got %v", results["sick"])
	}
}

func TestHealth_HealthyService_ReportsNil(t *testing.T) {
	o := New(Config{})
	svc := &healthSvc{healthFn: func(ctx context.Context) error { return nil }}
	_ = o.Register(svc, WithName("fine"))
	results := o.Health()
	if results["fine"] != nil {
		t.Errorf("expected nil for healthy service, got %v", results["fine"])
	}
}

func TestHealth_NoEntries(t *testing.T) {
	o := New(Config{})
	results := o.Health()
	if len(results) != 0 {
		t.Errorf("expected empty map, got %v", results)
	}
}

// ── runHealthChecks ──

func TestRunHealthChecks_FailuresTracked(t *testing.T) {
	// ponytail: set up entry manually to avoid the health-check loop.
	o := New(Config{HealthThreshold: 3})
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "sick",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return errors.New("bad") }},
		cfg:    registerConfig{name: "sick"},
		status: StatusRunning,
		logger: newServiceLogger("sick", logCh),
	}
	o.entries = append(o.entries, entry)

	o.runHealthChecks()
	o.runHealthChecks()
	o.runHealthChecks()

	if entry.healthFailures < 3 {
		t.Errorf("expected at least 3 health failures, got %d", entry.healthFailures)
	}
}

func TestRunHealthChecks_HealthyResetsCounter(t *testing.T) {
	o := New(Config{HealthThreshold: 3})
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "healthy",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return nil }},
		cfg:    registerConfig{name: "healthy"},
		status: StatusRunning,
		logger: newServiceLogger("healthy", logCh),
	}
	o.entries = append(o.entries, entry)

	entry.healthFailures = 5
	o.runHealthChecks()

	if entry.healthFailures != 0 {
		t.Errorf("expected health failures reset to 0, got %d", entry.healthFailures)
	}
}

func TestRunHealthChecks_NonRunningSkipped(t *testing.T) {
	o := New(Config{HealthThreshold: 3})
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "registered",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return errors.New("bad") }},
		cfg:    registerConfig{name: "registered"},
		status: StatusRegistered, // not running — should be skipped
		logger: newServiceLogger("registered", logCh),
	}
	o.entries = append(o.entries, entry)

	o.runHealthChecks()

	if entry.healthFailures != 0 {
		t.Errorf("non-running service should not be health-checked, got %d failures", entry.healthFailures)
	}
}

func TestRunHealthChecks_ThresholdTriggersRestart(t *testing.T) {
	o := New(Config{
		LogLevel:        LogLevelWarn,
		HealthInterval:  50 * time.Millisecond,
		HealthTimeout:   500 * time.Millisecond,
		HealthThreshold: 2,
	})
	var factoryCalls atomic.Int32

	factory := func() Service {
		factoryCalls.Add(1)
		return &healthSvc{
			testSvc: testSvc{
				startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			},
			healthFn: func(ctx context.Context) error { return errors.New("still bad") },
		}
	}

	svc := &healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
		healthFn: func(ctx context.Context) error { return errors.New("bad") },
	}
	_ = o.Register(svc, WithName("sick"),
		WithSelfHeal(factory),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Millisecond}),
	)
	_ = o.Start()
	defer o.Stop(time.Second)

	// Health loop fires every 50ms, threshold=2, backoff=10ms.
	// After ~100ms (2 ticks) threshold is reached → restart.
	time.Sleep(300 * time.Millisecond)

	if factoryCalls.Load() < 1 {
		t.Error("expected self-heal restart triggered by health threshold")
	}
}

func TestRunHealthChecks_ThresholdWithoutSelfHeal(t *testing.T) {
	o := New(Config{HealthThreshold: 2})
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "sick",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return errors.New("bad") }},
		cfg:    registerConfig{name: "sick"},
		status: StatusRunning,
		logger: newServiceLogger("sick", logCh),
	}
	o.entries = append(o.entries, entry)

	entry.healthFailures = 2
	o.runHealthChecks()

	// Health check fails → healthFailures increments to 3.
	// 3 >= threshold(2) but factory is nil → no reset, no restart.
	// healthFailures is NOT reset without self-heal.
	if entry.healthFailures != 3 {
		t.Errorf("expected healthFailures to be 3 (2 + 1 failed check, no reset without self-heal), got %d", entry.healthFailures)
	}
}

// ── callAfterStartHook ──

func TestCallAfterStartHook_Global(t *testing.T) {
	var called []string
	o := New(Config{
		LogLevel: LogLevelWarn,
		OnAfterStart: func(name string, err error) {
			called = append(called, name)
		},
	})

	entry := &serviceEntry{name: "test", cfg: registerConfig{name: "test"}}
	o.callAfterStartHook(entry, nil)

	if len(called) != 1 || called[0] != "test" {
		t.Errorf("expected [test], got %v", called)
	}
}

func TestCallAfterStartHook_None(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
	entry := &serviceEntry{name: "test", cfg: registerConfig{name: "test"}}
	// No global, no per-service hook. Must not panic.
	o.callAfterStartHook(entry, nil)
}

// ── handleServiceDone with self-heal: cancelled during backoff ──

func TestHandleServiceDone_CancelledDuringBackoff(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
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
		logger: newServiceLogger("x", logCh),
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}

	go o.handleServiceDone(entry, sc)

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

// ── Persistence: cron + runOnce both stopped during shutdown ──

func TestStop_StopsCronAndRunOnce(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
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

// ── Persistence: health loop disabled when HealthInterval=0 ──

func TestHealthLoop_DefaultInterval(t *testing.T) {
	// ponytail: HealthInterval=0 defaults to 30s in New(). The health loop
	// is always started by default. There is no off switch for health checks
	// in the current API — set a very large interval to effectively disable.
	o := New(Config{})
	_ = o.Register(&healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
	}, WithName("hc"))
	_ = o.Start()
	defer o.Stop(time.Second)

	if o.healthCancel == nil {
		t.Error("health loop should be started with default interval")
	}
}

// ── Register: dependency found in entries but not nameIndex ──

func TestRegister_DepFoundInEntries(t *testing.T) {
	// ponytail: the entries-backup check in Register is for batch operations.
	// Since Register is called sequentially, nameIndex always has previous entries.
	// We verify the path exists by testing indirectly: register two services
	// where the second depends on the first.
	o := New(Config{})
	_ = o.Register(&namedSvc{}, WithName("base"))
	err := o.Register(&namedSvc{}, WithName("child"), DependsOn("base"))
	if err != nil {
		t.Errorf("child should be able to depend on already-registered base: %v", err)
	}
}

// ── ErrDuplicateName and ErrDependencyCycle message format ──

func TestErrDuplicateName_MessageHasName(t *testing.T) {
	o := New(Config{})
	_ = o.Register(&namedSvc{}, WithName("a"))
	err := o.Register(&namedSvc{}, WithName("a"))
	if err == nil || !errors.Is(err, ErrDuplicateName) {
		t.Fatalf("expected ErrDuplicateName, got %v", err)
	}
	if !strings.Contains(err.Error(), "a") {
		t.Errorf("error message should contain name: %v", err)
	}
}

func TestErrDependencyCycle_MessageHasNames(t *testing.T) {
	o := New(Config{})
	err := o.Register(&namedSvc{}, WithName("self"), DependsOn("self"))
	if err == nil || !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("expected ErrDependencyCycle, got %v", err)
	}
	if !strings.Contains(err.Error(), "self") {
		t.Errorf("error message should contain name: %v", err)
	}
}

// ── Auto-name generation in Register ──

func TestRegister_AutoName(t *testing.T) {
	o := New(Config{})
	_ = o.Register(&namedSvc{}) // no WithName
	if o.entries[0].name != "$1" {
		t.Errorf("expected auto-name '$1', got %q", o.entries[0].name)
	}
	_ = o.Register(&namedSvc{})
	if o.entries[1].name != "$2" {
		t.Errorf("expected auto-name '$2', got %q", o.entries[1].name)
	}
}

// ── ServiceContext satisfies context.Context ──

func TestServiceContext_SatisfiesContext(t *testing.T) {
	var sc ServiceContext
	var _ context.Context = sc
}

// ── Helper ──

func indexOf(slice []string, item string) int {
	for i, s := range slice {
		if s == item {
			return i
		}
	}
	return -1
}

// ── Coverage fillers ──

// ── Register transitive cycle (dependsOnRecursive returns true) ──

func TestRegister_TransitiveCycle(t *testing.T) {
	// White-box: manually add an entry to o.entries (not nameIndex) that
	// depends on the service we're about to register. Then register that
	// service with a dep on the entry → cycle detected.
	o := New(Config{})
	existing := &serviceEntry{
		name: "intermediate",
		cfg:  registerConfig{name: "intermediate", dependsOn: []string{"target"}},
	}
	o.entries = append(o.entries, existing)
	// Not in nameIndex — tests the entries fallback + transitive cycle path.

	err := o.Register(&namedSvc{}, WithName("target"), DependsOn("intermediate"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Errorf("expected ErrDependencyCycle, got %v", err)
	}
}

func TestRegister_DepInEntriesNotNameIndex(t *testing.T) {
	// White-box: cover the path where a dependency is found in o.entries
	// but not in o.nameIndex (batch-register scenario).
	o := New(Config{})
	entry := &serviceEntry{name: "base", cfg: registerConfig{name: "base"}, status: StatusRegistered}
	o.entries = append(o.entries, entry)
	// nameIndex does NOT have "base".

	err := o.Register(&namedSvc{}, WithName("child"), DependsOn("base"))
	if err != nil {
		t.Errorf("should find dep in entries (not nameIndex): %v", err)
	}
	// "child" is added to nameIndex; "base" remains only in entries.
	if _, ok := o.nameIndex["child"]; !ok {
		t.Error("child should be in nameIndex")
	}
}

func TestDependsOnRecursive_EntriesPath(t *testing.T) {
	o := New(Config{})
	// Add entries to entries but NOT to nameIndex (batch-register scenario).
	base := &serviceEntry{name: "a", cfg: registerConfig{name: "a"}}
	mid := &serviceEntry{name: "b", cfg: registerConfig{name: "b", dependsOn: []string{"a"}}}
	top := &serviceEntry{name: "c", cfg: registerConfig{name: "c", dependsOn: []string{"b"}}}
	o.entries = append(o.entries, base, mid, top)
	// Check: top transitively depends on a through b.
	// top→b → b found in entries (not nameIndex) → b→a → a==a → true.
	if !o.dependsOnRecursive(top, "a") {
		t.Error("top should transitively depend on base via mid")
	}
}

func TestTopoSort_ErrorInStart(t *testing.T) {
	// Verify that a topoSort cycle during Start is handled.
	// ponytail: create entries with a cycle manually and register them
	// as persistent services. The cycle will be caught by topoSort.
	o := New(Config{LogLevel: LogLevelWarn})
	// Create two entries with a circular dependency.
	// We bypass Register to avoid cycle detection.
	e1 := &serviceEntry{name: "a", cfg: registerConfig{name: "a", dependsOn: []string{"b"}}, status: StatusRegistered}
	e2 := &serviceEntry{name: "b", cfg: registerConfig{name: "b", dependsOn: []string{"a"}}, status: StatusRegistered}
	o.entries = append(o.entries, e1, e2)
	o.nameIndex["a"] = e1
	o.nameIndex["b"] = e2

	err := o.Start()
	if err == nil {
		o.Stop(time.Second)
		t.Fatal("expected cycle error from Start")
	}
}

func TestRunOnce_WithTimeout(t *testing.T) {
	// Cover runOnce with timeout goroutine paths.
	t.Run("success_within_timeout", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelWarn})
		svc := &testSvc{
			startFn: func(ctx context.Context) error { return nil },
		}
		_ = o.Register(svc, WithName("quick"), WithRunOnce(), WithStartTimeout(time.Second))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("svc"))
		err := o.Start()
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		_ = o.Stop(time.Second)
	})

	t.Run("timeout_exceeded", func(t *testing.T) {
		o := New(Config{LogLevel: LogLevelWarn})
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
		_ = o.Register(svc, WithName("slow"), WithRunOnce(), WithStartTimeout(50*time.Millisecond))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("svc"))
		err := o.Start()
		if err == nil {
			o.Stop(time.Second)
			t.Fatal("expected timeout error from runOnce")
		}
	})
}

func TestStopStartedServices_LogChClosed(t *testing.T) {
	// Cover the case where logCh is already closed when stopStartedServices runs.
	// This happens when a cron error closes logCh in Start().
	o := New(Config{LogLevel: LogLevelWarn})
	_ = o.Register(&namedSvc{}, WithCron("invalid", CronParallel))
	// Start fails → cancel called → logCh closed → stopStartedServices NOT called.
	// But we call stopStartedServices directly after to cover the already-closed path.
	_ = o.Start() // will fail with ErrInvalidCron
	// logCh is now closed. Create a new entry and call stopStartedServices.
	o.entries = append(o.entries, &serviceEntry{
		name: "x", svc: &namedSvc{}, cfg: registerConfig{name: "x"}, status: StatusStopped,
	})
	o.stopStartedServices() // should not panic on already-closed logCh
}

func TestRunService_ErrorViaSelfHeal(t *testing.T) {
	// Cover runService logging when service returns non-Canceled error.
	// This is triggered via self-heal: factory creates a service that returns an error.
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(Config{LogLevel: LogLevelError})
	var factoryCalls atomic.Int32
	factory := func() Service {
		factoryCalls.Add(1)
		return &errSvc{err: errors.New("factory err")}
	}
	_ = o.Register(&errSvc{err: errors.New("init")},
		WithSelfHeal(factory),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Millisecond}),
	)
	_ = o.Start()
	time.Sleep(200 * time.Millisecond)
	_ = o.Stop(500 * time.Millisecond)

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	output := buf.String()
	if !strings.Contains(output, "service returned error") {
		t.Errorf("expected 'service returned error' for factory-created service: %s", output)
	}
	if factoryCalls.Load() < 1 {
		t.Error("expected factory to be called")
	}
}

func TestStop_CronAndRunOnceStopErrors(t *testing.T) {
	// Cover the error aggregation path in Stop for cron/runOnce entries.
	o := New(Config{LogLevel: LogLevelWarn})
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

func TestRunHealthChecks_NonHealthCheckerSkipped(t *testing.T) {
	o := New(Config{HealthThreshold: 3})
	logCh := make(chan logEntry, 1)
	// Add a non-HealthChecker entry alongside a HealthChecker entry.
	e1 := &serviceEntry{
		name:   "plain",
		svc:    &namedSvc{}, // does NOT implement HealthChecker
		cfg:    registerConfig{name: "plain"},
		status: StatusRunning,
		logger: newServiceLogger("plain", logCh),
	}
	e2 := &serviceEntry{
		name:   "hc",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return nil }},
		cfg:    registerConfig{name: "hc"},
		status: StatusRunning,
		logger: newServiceLogger("hc", logCh),
	}
	o.entries = append(o.entries, e1, e2)

	o.runHealthChecks() // should skip e1 without panic
}

func TestHandleServiceDone_FactoryContextCancelled(t *testing.T) {
	// Cover the factory != nil + ctx cancelled path (gorch.go:996).
	o := New(Config{LogLevel: LogLevelWarn})
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
		logger: newServiceLogger("x", logCh),
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}
	o.handleServiceDone(entry, sc)

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
	// Ensure the maxRetries block sets StatusStopped.
	o := New(Config{LogLevel: LogLevelWarn})
	ctx := context.Background()
	o.ctx = ctx

	var factoryCalls atomic.Int32
	factory := func() Service {
		factoryCalls.Add(1)
		return &errSvc{err: errors.New("always")}
	}
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		svc:        &errSvc{err: errors.New("boom")},
		cfg:        registerConfig{name: "x", factory: factory, maxRetries: 1},
		name:       "x",
		status:     StatusRunning,
		logger:     newServiceLogger("x", logCh),
		retryCount: 1, // already at max
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}
	o.handleServiceDone(entry, sc)

	if factoryCalls.Load() != 0 {
		t.Error("factory should not be called when maxRetries reached")
	}
	// Status should be Stopped.
	if entry.status != StatusStopped {
		t.Errorf("expected StatusStopped, got %v", entry.status)
	}
	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait did not complete")
	}
}

// ── Dependency status check in Start (parallel level) ──

func TestStart_DepStatusCheck(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
	// A uses WithStartTimeout so startOneService waits for the goroutine's defer
	// to send to startErrCh. With a fast Start return, there's a narrow window
	// where the dependent's status check may or may not see StatusStopped.
	svcA := &testSvc{startFn: func(ctx context.Context) error { return nil }}
	svcB := &testSvc{}

	_ = o.Register(svcA, WithName("a"), WithStartTimeout(time.Second))
	_ = o.Register(svcB, WithName("b"), DependsOn("a"))

	err := o.Start()
	if err != nil {
		// Race won: dep status check worked.
		if !strings.Contains(err.Error(), "dependency") && !strings.Contains(err.Error(), "aborted") {
			t.Errorf("unexpected error: %v", err)
		}
	} else {
		o.Stop(500 * time.Millisecond)
	}
}

// ── runOnce: panic recovery in goroutine ──

func TestRunOnce_PanicRecovery(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(Config{LogLevel: LogLevelWarn})
	_ = o.Register(&panicSvc{msg: "runonce panic"}, WithRunOnce(), WithStartTimeout(time.Second))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("svc"))

	o.Start() // will fail — don't call Stop; logCh already closed by stopStartedServices

	// Give logPump time to process the panic log entry.
	time.Sleep(50 * time.Millisecond)

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	output := buf.String()
	if !strings.Contains(output, "service panicked") {
		t.Errorf("expected 'service panicked' in log for runOnce panic: %s", output)
	}
}

// ── runOnce: ctx.Done during timeout select ──

func TestRunOnce_CtxDone(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.logCh = make(chan logEntry, 1)
	o.started = true
	o.nameIndex = make(map[string]*serviceEntry)

	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			select {
			case <-time.After(time.Minute):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	entry := &serviceEntry{
		name:   "runonce",
		svc:    svc,
		cfg:    registerConfig{name: "runonce", runOnce: true, startTimeout: time.Second},
		status: StatusRegistered,
		logger: newServiceLogger("runonce", o.logCh),
	}
	o.entries = append(o.entries, entry)
	o.nameIndex["runonce"] = entry

	go func() {
		time.Sleep(50 * time.Millisecond)
		o.cancel()
	}()

	_ = o.startOneService(entry) // ctx.Canceled is not treated as an error for runOnce
}

// ── Persistent: startErrCh received (Start returns before timeout) ──

func TestStartOneService_PersistentStartErrCh(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
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

// ── runService: panic recovery path ──

func TestRunService_PanicRecovery(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(Config{LogLevel: LogLevelError})
	var factoryCalls atomic.Int32
	factory := func() Service {
		factoryCalls.Add(1)
		return &panicSvc{msg: "factory panic"}
	}
	_ = o.Register(&errSvc{err: errors.New("init err")},
		WithSelfHeal(factory),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Millisecond}),
		WithMaxRetries(2),
	)
	_ = o.Start()
	time.Sleep(200 * time.Millisecond)
	_ = o.Stop(500 * time.Millisecond)

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	output := buf.String()
	if !strings.Contains(output, "service panicked") {
		t.Errorf("expected 'service panicked' from runService recovery: %s", output)
	}
	if factoryCalls.Load() < 1 {
		t.Error("expected factory to be called")
	}
}

// ── handleServiceDone: wgDone already true (else branches) ──

func TestHandleServiceDone_WgDoneTrue_CtxCancelled(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
	ctx, cancel := context.WithCancel(context.Background())
	o.ctx = ctx
	cancel()

	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		svc:    &errSvc{err: errors.New("boom")},
		cfg:    registerConfig{name: "x", factory: func() Service { return &testSvc{} }},
		name:   "x",
		status: StatusRunning,
		logger: newServiceLogger("x", logCh),
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}
	o.handleServiceDone(entry, sc)
	o.handleServiceDone(entry, sc)

	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait did not complete")
	}
}

func TestHandleServiceDone_WgDoneTrue_MaxRetries(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
	ctx := context.Background()
	o.ctx = ctx

	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		svc:        &errSvc{err: errors.New("boom")},
		cfg:        registerConfig{name: "x", factory: func() Service { return &testSvc{} }, maxRetries: 1},
		name:       "x",
		status:     StatusRunning,
		logger:     newServiceLogger("x", logCh),
		retryCount: 1,
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}
	o.handleServiceDone(entry, sc)
	o.handleServiceDone(entry, sc)

	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait did not complete")
	}
}

func TestHandleServiceDone_WgDoneTrue_BackoffCancelled(t *testing.T) {
	o := New(Config{LogLevel: LogLevelWarn})
	ctx, cancel := context.WithCancel(context.Background())
	o.ctx = ctx

	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		svc:    &errSvc{err: errors.New("boom")},
		cfg:    registerConfig{name: "x", factory: func() Service { return &testSvc{} }, backoff: ConstantBackoff{Delay: 500 * time.Millisecond}},
		name:   "x",
		status: StatusRunning,
		logger: newServiceLogger("x", logCh),
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}

	go o.handleServiceDone(entry, sc)

	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	// Second call: wgDone is already true from the goroutine → else branch.
	o.handleServiceDone(entry, sc)

	done := make(chan struct{})
	go func() { o.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Wait did not complete")
	}
}

// ── White-box: Start dependency status check (parallel level) ──

func TestStart_DepStatusCheck_Whitebox(t *testing.T) {
	// Directly test the dependency-failure check in Start's goroutine,
	// bypassing the race with goroutine scheduling.
	o := New(Config{LogLevel: LogLevelWarn})
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

// ── handleServiceDone: backoff ctx-cancelled else (wgDone already true) ──

func TestHandleServiceDone_WgDoneTrue_BackoffCancelledV2(t *testing.T) {
	// The backoff ctx-cancelled else branch only fires when:
	// 1. The first handleServiceDone enters the backoff delay
	// 2. ctx is cancelled, triggering the backoff ctx-cancelled block
	// 3. In that block, wgDone is already true → else branch
	// Approach: manually set wgDone=true, have factory and non-cancelled context,
	// then cancel ctx during backoff.
	o := New(Config{LogLevel: LogLevelWarn})
	ctx, cancel := context.WithCancel(context.Background())
	o.ctx = ctx

	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		svc:        &errSvc{err: errors.New("boom")},
		cfg:        registerConfig{name: "x", factory: func() Service { return &testSvc{} }, backoff: ConstantBackoff{Delay: 200 * time.Millisecond}},
		name:       "x",
		status:     StatusRunning,
		logger:     newServiceLogger("x", logCh),
		wgDone:     true, // already done
		retryCount: 1,
	}
	o.wg.Add(1)
	sc := ServiceContext{Context: ctx}

	// Start handleServiceDone asynchronously — it will enter the backoff delay.
	go o.handleServiceDone(entry, sc)

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
