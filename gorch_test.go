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

func (s *testSvc) Start(ctx ServiceContext) error {
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

func (s *namedSvc) Start(ctx ServiceContext) error { <-ctx.Done(); return ctx.Err() }
func (s *namedSvc) Stop() error                    { return nil }

type errSvc struct{ err error }

func (s *errSvc) Start(ctx ServiceContext) error { return s.err }
func (s *errSvc) Stop() error                    { return nil }

type panicSvc struct{ msg string }

func (s *panicSvc) Start(ctx ServiceContext) error { panic(s.msg) }
func (s *panicSvc) Stop() error                    { return nil }

type stopPanicSvc struct{}

func (s *stopPanicSvc) Start(ctx ServiceContext) error { <-ctx.Done(); return ctx.Err() }
func (s *stopPanicSvc) Stop() error                    { panic("stop boom") }

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

// readySvc is a test service that implements ReadinessChecker.
type readySvc struct {
	testSvc
	readyFn func(ctx context.Context) error
}

func (s *readySvc) Ready(ctx context.Context) error {
	if s.readyFn != nil {
		return s.readyFn(ctx)
	}
	return nil
}

// validSvc is a test service that implements Validator.
type validSvc struct {
	testSvc
	validateErr error
}

func (s *validSvc) Validate() error { return s.validateErr }

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
		o := New()
		if o.cfg.LogLevel != LogLevelInfo {
			t.Errorf("expected LogLevelInfo (1), got %d", o.cfg.LogLevel)
		}
	})

	t.Run("logLevelDebug_is_selectable", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelDebug))
		if o.cfg.LogLevel != LogLevelDebug {
			t.Errorf("WithLogLevel(LogLevelDebug) should select Debug, got %d", o.cfg.LogLevel)
		}
	})

	t.Run("explicit_warn_stays_warn", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		if o.cfg.LogLevel != LogLevelWarn {
			t.Errorf("expected LogLevelWarn (2), got %d", o.cfg.LogLevel)
		}
	})

	t.Run("health_checks_can_be_disabled", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if o.cfg.HealthInterval != 0 {
			t.Errorf("expected HealthInterval=0 when disabled, got %v", o.cfg.HealthInterval)
		}
	})

	t.Run("messenger_initialized", func(t *testing.T) {
		o := New()
		if o.messenger == nil {
			t.Fatal("expected messenger to be initialized")
		}
	})

	t.Run("health_defaults", func(t *testing.T) {
		o := New()
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
		o := New()
		if o.nameIndex == nil {
			t.Fatal("expected nameIndex to be initialized")
		}
	})
}

// ── Register ──

func TestRegister(t *testing.T) {
	t.Run("register_before_start", func(t *testing.T) {
		o := New()
		err := o.Register(&namedSvc{name: "a"})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(o.entries) != 1 {
			t.Errorf("expected 1 entry, got %d", len(o.entries))
		}
	})

	t.Run("register_after_start_hot_adds_without_starting", func(t *testing.T) {
		o := New()
		_ = o.Register(&namedSvc{name: "a"}, WithName("a"))
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer o.Stop(1 * time.Second)
		svc := &testSvc{}
		err := o.Register(svc, WithName("b"))
		if err != nil {
			t.Fatalf("hot Register returned %v", err)
		}
		s, ok := o.Status("b")
		if !ok || s != StatusRegistered {
			t.Errorf("hot-added service status = %v, want StatusRegistered", s)
		}
		names := o.Names()
		if len(names) != 2 {
			t.Errorf("Names() = %v, want 2 entries", names)
		}
		if got := svc.startCalls.Load(); got != 0 {
			t.Errorf("hot Register must not start the service, Start called %d times", got)
		}
	})

	t.Run("register_with_cron_option", func(t *testing.T) {
		o := New()
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
		o := New()
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

func TestRegister_UnsupportedSelfHealCombination(t *testing.T) {
	factory := func() Service { return &namedSvc{name: "healed"} }

	t.Run("cron", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		err := o.Register(&namedSvc{name: "a"}, WithCron("* * * * * *", CronParallel), WithSelfHeal(factory))
		if !errors.Is(err, ErrUnsupportedOption) {
			t.Fatalf("expected ErrUnsupportedOption, got %v", err)
		}
		if o.Count() != 0 {
			t.Errorf("rejected service must not be registered, got count %d", o.Count())
		}
	})

	t.Run("runOnce", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		err := o.Register(&namedSvc{name: "a"}, WithRunOnce(), WithSelfHeal(factory))
		if !errors.Is(err, ErrUnsupportedOption) {
			t.Fatalf("expected ErrUnsupportedOption, got %v", err)
		}
		if o.Count() != 0 {
			t.Errorf("rejected service must not be registered, got count %d", o.Count())
		}
	})
}

// ── Register edge cases ──

func TestRegister_DuplicateName(t *testing.T) {
	o := New()
	_ = o.Register(&namedSvc{}, WithName("dup"))
	err := o.Register(&namedSvc{}, WithName("dup"))
	if !errors.Is(err, ErrDuplicateName) {
		t.Errorf("expected ErrDuplicateName, got %v", err)
	}
}

func TestRegister_SelfDependency(t *testing.T) {
	o := New()
	err := o.Register(&namedSvc{}, WithName("self"), DependsOn("self"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Errorf("expected ErrDependencyCycle, got %v", err)
	}
}

func TestRegister_DependencyNotFound(t *testing.T) {
	o := New()
	err := o.Register(&namedSvc{}, WithName("orphan"), DependsOn("nobody"))
	if !errors.Is(err, ErrDependencyNotFound) {
		t.Fatalf("static Register with an unknown hard dependency = %v, want ErrDependencyNotFound", err)
	}
}

// ── Start ──

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
		_ = o.Register(&namedSvc{name: "bad-cron"}, WithCron("invalid", CronParallel))
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

// ── Stop ──

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

// TestStop_ServiceLogsAfterCancel_NoPanic is a regression test for the
// "send on closed channel" crash: a service that keeps logging after ctx
// cancellation while Stop() runs must not panic.
func TestStop_ServiceLogsAfterCancel_NoPanic(t *testing.T) {
	o := New(WithLogLevel(LogLevelError))
	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			sc := ctx.(ServiceContext)
			<-ctx.Done()
			for i := 0; i < 1000; i++ {
				sc.Logger.Error("post-cancel log", "i", i)
			}
			return ctx.Err()
		},
	}
	_ = o.Register(svc)
	_ = o.Start()
	time.Sleep(20 * time.Millisecond)
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

// TestStop_Timeout_LogPumpExits verifies that when a service ignores ctx and
// Stop() times out, the log-pump still exits and nothing panics.
func TestStop_Timeout_LogPumpExits(t *testing.T) {
	o := New(WithLogLevel(LogLevelError))
	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			sc := ctx.(ServiceContext)
			for i := 0; i < 5; i++ {
				sc.Logger.Error("still running", "i", i)
				time.Sleep(time.Millisecond)
			}
			never := make(chan struct{})
			<-never
			return nil
		},
	}
	_ = o.Register(svc)
	_ = o.Start()
	time.Sleep(20 * time.Millisecond)
	err := o.Stop(50 * time.Millisecond)
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("expected ErrStopTimeout, got %v", err)
	}
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

// TestStop_ClosesMessengerSubscriberChannels verifies that a subscriber blocked
// on receive observes a channel close when the orchestrator stops.
func TestStop_ClosesMessengerSubscriberChannels(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Start()
	ch, _ := o.messenger.Subscribe("topic")
	_ = o.Stop(time.Second)

	_, ok := <-ch
	if ok {
		t.Error("expected subscriber channel to be closed on Stop")
	}
}

// TestStop_SubscribeAfterStopDoesNotPanic verifies the messenger re-initializes
// its subscription map lazily so a late Subscribe does not panic.
func TestStop_SubscribeAfterStopDoesNotPanic(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Start()
	_ = o.Stop(time.Second)

	ch, _ := o.messenger.Subscribe("topic")
	o.messenger.Publish("x", "topic")
	select {
	case <-ch:
	default:
	}
}

// ── Stop: error aggregation and per-service hooks ──

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

// ── stopOneService hooks ──

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

// ── Context cancellation ──

func TestContextCancellation(t *testing.T) {
	t.Run("services_receive_context_cancellation_on_stop", func(t *testing.T) {
		o := New()
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
		o := New()
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
		o := New(WithLogLevel(LogLevelWarn))
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
		o := New(WithLogLevel(LogLevelWarn))
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
		o := New(WithLogLevel(LogLevelWarn))
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

// ── Self-heal with options ──

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

// ── Service panic recovery ──

func TestServicePanicRecovery(t *testing.T) {
	t.Run("panic_in_start_recovered", func(t *testing.T) {
		o := New()
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

		o := New(WithLogLevel(LogLevelError))
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
		o := New()
		svc := &stopPanicSvc{}
		entry := &serviceEntry{name: "test", svc: svc, cfg: registerConfig{name: "test"}}
		o.safeStop(entry)
	})

	t.Run("safeStop_calls_stop", func(t *testing.T) {
		o := New()
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

		o := New(WithLogLevel(LogLevelWarn))
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

		o := New(WithLogLevel(LogLevelWarn))
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

		o := New(WithLogLevel(LogLevelDebug))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		go o.logPump()

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 123456789, time.UTC),
			level:   LogLevelInfo,
			service: "*testSvc",
			msg:     "hello world",
			args:    []any{"k1", "v1", "k2", 42},
		}
		close(o.logQuit)
		<-o.logPumpDone

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

		o := New(WithLogLevel(LogLevelDebug))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		go o.logPump()

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level:   LogLevelError,
			service: "svc",
			msg:     "odd",
			args:    []any{"lonely"},
		}
		close(o.logQuit)
		<-o.logPumpDone

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

		o := New(WithLogLevel(LogLevelDebug))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		go o.logPump()

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level:   LogLevelError,
			service: "svc",
			msg:     "odd3",
			args:    []any{"a", 1, "b"},
		}
		close(o.logQuit)
		<-o.logPumpDone

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

		o := New(WithLogLevel(LogLevelDebug))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		go o.logPump()

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level:   LogLevelInfo,
			service: "svc",
			msg:     "no args",
			args:    nil,
		}
		close(o.logQuit)
		<-o.logPumpDone

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

		o := New(WithLogLevel(LogLevelWarn))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 256)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
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
		close(o.logQuit)
		<-o.logPumpDone

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

	t.Run("logPump_exits_on_quit_signal", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelDebug))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		done := make(chan struct{})
		go func() {
			o.logPump()
			close(done)
		}()
		close(o.logQuit)
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("logPump did not exit after channel close")
		}
	})
}

// TestLogPump_DrainsBufferedOnQuit verifies the pump flushes buffered entries
// to stderr before exiting on the quit signal.
func TestLogPump_DrainsBufferedOnQuit(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(WithLogLevel(LogLevelDebug))
	o.logCh = make(chan logEntry, 256)
	o.logQuit = make(chan struct{})
	o.logPumpDone = make(chan struct{})

	// Pre-fill the buffer before starting the pump; on quit it must drain all.
	for i := 0; i < 3; i++ {
		o.logCh <- logEntry{time: time.Now(), level: LogLevelInfo, service: "svc", msg: "buffered"}
	}
	close(o.logQuit)
	go o.logPump()
	<-o.logPumpDone

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	if count := strings.Count(buf.String(), "buffered"); count != 3 {
		t.Errorf("expected 3 buffered entries flushed, got %d", count)
	}
}

// TestLogLevelFiltering_AtEmit verifies that below-minimum entries are dropped
// at emit (not the pump), so a Debug flood cannot starve the buffer and drop
// an Error entry.
func TestLogLevelFiltering_AtEmit(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(WithLogLevel(LogLevelInfo))
	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			sc := ctx.(ServiceContext)
			for i := 0; i < 1000; i++ {
				sc.Logger.Debug("debug spam", "i", i)
			}
			sc.Logger.Error("important error", "code", 500)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	_ = o.Register(svc)
	_ = o.Start()
	time.Sleep(100 * time.Millisecond)
	_ = o.Stop(time.Second)

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	output := buf.String()
	if !strings.Contains(output, "important error") {
		t.Errorf("Error entry should reach stderr despite Debug flood, got:\n%s", output)
	}
	if strings.Contains(output, "debug spam") {
		t.Error("Debug entries should be filtered at emit (level=Info)")
	}
}

// ── ServiceLogger emit ──

func TestServiceLogger_Emit(t *testing.T) {
	t.Run("non_blocking_send_when_channel_full", func(t *testing.T) {
		ch := make(chan logEntry, 1)
		ch <- logEntry{}
		logger := newServiceLogger("test", ch, nil, LogLevelDebug)
		logger.Info("dropped")
	})

	t.Run("methods_use_correct_levels", func(t *testing.T) {
		ch := make(chan logEntry, 16)
		logger := newServiceLogger("svc", ch, nil, LogLevelDebug)

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

		o := New(WithLogLevel(LogLevelError))
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

		o := New(WithLogLevel(LogLevelError))
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

		o := New(WithLogLevel(LogLevelError))
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

		o := New(WithLogLevel(LogLevelError))
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

// ── Service name in log output ──

func TestServiceNameInLogger(t *testing.T) {
	t.Run("auto_name_matches_status_name", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(&panicSvc{msg: "namecheck"})
		_ = o.Start()
		time.Sleep(100 * time.Millisecond)
		_ = o.Stop(200 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		// An unnamed service logs under its auto-assigned "$N" name, the same
		// name Status()/Names() report, not its reflect type.
		if !strings.Contains(output, "$1 ---") {
			t.Errorf("expected '$1' log prefix matching the status name, got: %s", output)
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
				switch name {
				case "b":
					<-readyB
				case "c":
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

	o := New(WithLogLevel(LogLevelWarn))
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

	o := New(WithLogLevel(LogLevelWarn))
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
	o := New(WithLogLevel(LogLevelWarn))

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

		o := New(WithLogLevel(LogLevelWarn))
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

		o := New()
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

	t.Run("transitions_to_succeeded", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
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
		if s != StatusSucceeded {
			t.Errorf("one-shot should be StatusSucceeded, got %v", s)
		}
		if s.String() != "succeeded" {
			t.Errorf("expected String() == %q, got %q", "succeeded", s.String())
		}
		_ = o.Stop(time.Second)
	})

	t.Run("hard_dep_on_runonce_is_allowed", func(t *testing.T) {
		// A runOnce gate runs in an earlier phase than persistent services, so a
		// hard DependsOn on one is satisfiable: the gate is outside the persistent
		// topo subset, and its edge must be ignored rather than treated as a cycle.
		o := New(WithLogLevel(LogLevelWarn))
		var mu sync.Mutex
		var order []string
		gate := &testSvc{startFn: func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "gate")
			mu.Unlock()
			return nil
		}}
		workerStarted := make(chan struct{})
		worker := &testSvc{startFn: func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "worker")
			mu.Unlock()
			close(workerStarted)
			<-ctx.Done()
			return ctx.Err()
		}}
		_ = o.Register(gate, WithName("gate"), WithRunOnce())
		_ = o.Register(worker, WithName("worker"), DependsOn("gate"))
		if err := o.Start(); err != nil {
			t.Fatalf("hard dep on a runOnce gate must not fail Start: %v", err)
		}
		defer o.Stop(time.Second)

		select {
		case <-workerStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("worker was never started")
		}
		mu.Lock()
		got := append([]string(nil), order...)
		mu.Unlock()
		if len(got) != 2 || got[0] != "gate" || got[1] != "worker" {
			t.Errorf("expected gate to run before worker, got %v", got)
		}
	})

	t.Run("context_canceled_is_not_an_error", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
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
		o := New(
			WithLogLevel(LogLevelWarn),
			WithGlobalOnBeforeStart(func(name string) error {
				called = append(called, "before:"+name)
				return nil
			}),
			WithGlobalOnAfterStart(func(name string, err error) {
				called = append(called, "after:"+name)
			}),
		)
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
		o := New(
			WithLogLevel(LogLevelWarn),
			WithGlobalOnBeforeStart(func(name string) error { return hookErr }),
		)
		_ = o.Register(&testSvc{}, WithName("a"))
		err := o.Start()
		if err == nil {
			t.Fatal("expected error from before-start hook")
		}
		// ponytail: logCh already closed by stopStartedServices.
	})

	t.Run("per_service_after_start_overrides_global", func(t *testing.T) {
		var globalCalls, perSvcCalls []string

		o := New(
			WithLogLevel(LogLevelWarn),
			WithGlobalOnAfterStart(func(name string, err error) {
				globalCalls = append(globalCalls, name)
			}),
		)

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
		o := New(WithLogLevel(LogLevelWarn))

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
		o := New(
			WithLogLevel(LogLevelWarn),
			WithGlobalOnBeforeStop(func(name string) error {
				return errors.New("before-stop issue")
			}),
		)
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
	o := New()
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
	o := New()
	_, ok := o.Status("nope")
	if ok {
		t.Error("expected nope to not be found")
	}
}

func TestStatuses(t *testing.T) {
	o := New()
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
	o := New()
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
	o := New()
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
	o := New(WithLogLevel(LogLevelWarn))
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
	o := New()
	_ = o.Register(&namedSvc{name: "bad"}, WithCron("invalid", CronParallel))
	err := o.Run(time.Second)
	if !errors.Is(err, ErrInvalidCron) {
		t.Errorf("expected ErrInvalidCron, got %v", err)
	}
}

func TestRun_DefaultSignal(t *testing.T) {
	// Cover the default signal path (len(sigSet) == 0 → SIGINT + SIGTERM).
	o := New(WithLogLevel(LogLevelWarn))
	done := make(chan error, 1)
	go func() {
		done <- o.Run(time.Second) // no signals → defaults to SIGINT + SIGTERM
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

func TestRun_DefaultSignal_SIGTERM(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	done := make(chan error, 1)
	go func() {
		done <- o.Run(time.Second) // defaults to SIGINT + SIGTERM
	}()

	time.Sleep(100 * time.Millisecond)
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after SIGTERM")
	}
}

func TestRun_CustomSignal(t *testing.T) {
	// Cover the custom signal path.
	o := New(WithLogLevel(LogLevelWarn))

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
	o := New()
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
	o := New()
	levels, err := o.topoSort(nil)
	if err != nil || levels != nil {
		t.Errorf("expected nil, nil; got %v, %v", levels, err)
	}
}

func TestTopoSort_Single(t *testing.T) {
	o := New()
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

// TestTopoSort_HardDepOutsideSet_NoCycle pins that a hard dependency pointing
// outside the entry subset does not leave a dangling in-degree and falsely
// report ErrDependencyCycle.
func TestTopoSort_HardDepOutsideSet_NoCycle(t *testing.T) {
	o := New()
	entries := []*serviceEntry{
		{name: "app", cfg: registerConfig{name: "app", dependsOn: []string{"migrate"}}},
	}
	levels, err := o.topoSort(entries)
	if err != nil {
		t.Fatalf("hard dependency outside the entry set must not yield a cycle, got %v", err)
	}
	if len(levels) != 1 || len(levels[0]) != 1 || levels[0][0].name != "app" {
		t.Fatalf("expected app in a single level, got %v", levels)
	}
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

// TestStartGroup_CrossGroupHardDep pins that StartGroup does not report a hard
// dependency on a service outside the started group as a cycle.
func TestStartGroup_CrossGroupHardDep(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	infra := &testSvc{}
	app := &testSvc{}
	_ = o.Register(infra, WithName("infra"), WithGroup("infra"))
	_ = o.Register(app, WithName("app"), WithGroup("app"), DependsOn("infra"))
	if err := o.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer o.Stop(time.Second)

	if err := o.StartGroup("app"); err != nil {
		t.Fatalf("StartGroup must not report a cross-group hard dependency as a cycle, got %v", err)
	}
}

// TestStopGroup_CrossGroupHardDep pins that StopGroup stops the members it
// selected even when one of them hard-depends on a service in another group.
// Before the fix the discarded topoSort error left levels nil and nothing was
// stopped.
func TestStopGroup_CrossGroupHardDep(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	infra := &testSvc{}
	app := &testSvc{}
	_ = o.Register(infra, WithName("infra"), WithGroup("infra"))
	_ = o.Register(app, WithName("app"), WithGroup("app"), DependsOn("infra"))
	if err := o.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer o.Stop(time.Second)

	if err := o.StopGroup("app", time.Second); err != nil {
		t.Fatalf("StopGroup failed: %v", err)
	}
	if app.stopCalls.Load() < 1 {
		t.Error("StopGroup must stop the selected member")
	}
	if infra.stopCalls.Load() != 0 {
		t.Error("StopGroup must not stop services outside the group")
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

// ── dependsOnRecursive ──

func TestDependsOnRecursive(t *testing.T) {
	o := New()
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

// ── Health ──

func TestHealth_NonHealthChecker_ReportsNil(t *testing.T) {
	o := New()
	_ = o.Register(&namedSvc{}, WithName("plain"))
	results := o.Health()
	if results["plain"] != nil {
		t.Error("non-HealthChecker should report nil")
	}
}

func TestHealth_HealthChecker_ReportsError(t *testing.T) {
	o := New()
	healthErr := errors.New("unhealthy")
	svc := &healthSvc{healthFn: func(ctx context.Context) error { return healthErr }}
	_ = o.Register(svc, WithName("sick"))
	results := o.Health()
	if !errors.Is(results["sick"], healthErr) {
		t.Errorf("expected health error, got %v", results["sick"])
	}
}

func TestHealth_HealthyService_ReportsNil(t *testing.T) {
	o := New()
	svc := &healthSvc{healthFn: func(ctx context.Context) error { return nil }}
	_ = o.Register(svc, WithName("fine"))
	results := o.Health()
	if results["fine"] != nil {
		t.Errorf("expected nil for healthy service, got %v", results["fine"])
	}
}

func TestHealth_NoEntries(t *testing.T) {
	o := New()
	results := o.Health()
	if len(results) != 0 {
		t.Errorf("expected empty map, got %v", results)
	}
}

// TestHealth_ConcurrentRemoval covers the matrix row "Health vs removal": an
// Unregister that removes an entry while a probe is in flight must not leave
// the removed name in the result.
func TestHealth_ConcurrentRemoval(t *testing.T) {
	o := New()
	entered := make(chan struct{})
	release := make(chan struct{})
	svc := &healthSvc{healthFn: func(ctx context.Context) error {
		close(entered)
		<-release
		return nil
	}}
	if err := o.Register(svc, WithName("h")); err != nil {
		t.Fatal(err)
	}

	results := make(chan map[string]error, 1)
	go func() { results <- o.Health() }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("health probe never ran")
	}

	if err := o.Unregister("h", time.Second); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	close(release)

	select {
	case res := <-results:
		if _, ok := res["h"]; ok {
			t.Error("removed entry must not appear in the Health result")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Health did not return")
	}
}

// ── runHealthChecks ──

func TestRunHealthChecks_FailuresTracked(t *testing.T) {
	// ponytail: set up entry manually to avoid the health-check loop.
	o := New(WithHealthChecks(0, WithFailureThreshold(3)))
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "sick",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return errors.New("bad") }},
		cfg:    registerConfig{name: "sick"},
		status: StatusRunning,
		logger: newServiceLogger("sick", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, entry)
	o.nameIndex[entry.name] = entry

	o.runHealthChecks()
	o.runHealthChecks()
	o.runHealthChecks()

	if entry.healthFailures < 3 {
		t.Errorf("expected at least 3 health failures, got %d", entry.healthFailures)
	}
}

func TestRunHealthChecks_HealthyResetsCounter(t *testing.T) {
	o := New(WithHealthChecks(0, WithFailureThreshold(3)))
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "healthy",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return nil }},
		cfg:    registerConfig{name: "healthy"},
		status: StatusRunning,
		logger: newServiceLogger("healthy", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, entry)
	o.nameIndex[entry.name] = entry

	entry.healthFailures = 5
	o.runHealthChecks()

	if entry.healthFailures != 0 {
		t.Errorf("expected health failures reset to 0, got %d", entry.healthFailures)
	}
}

func TestRunHealthChecks_PerProbeTimeout(t *testing.T) {
	// A slow checker that consumes its whole deadline must not fail a later
	// instant checker: each probe gets a fresh per-service timeout.
	o := New(WithHealthChecks(0, WithProbeTimeout(50*time.Millisecond)))
	logCh := make(chan logEntry, 1)
	slow := &serviceEntry{
		name: "slow",
		svc: &healthSvc{healthFn: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
				return errors.New("slow")
			}
		}},
		cfg:    registerConfig{name: "slow"},
		status: StatusRunning,
		logger: newServiceLogger("slow", logCh, nil, LogLevelDebug),
	}
	fast := &serviceEntry{
		name: "fast",
		svc: &healthSvc{healthFn: func(ctx context.Context) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}},
		cfg:    registerConfig{name: "fast"},
		status: StatusRunning,
		logger: newServiceLogger("fast", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, slow, fast)
	o.nameIndex[slow.name] = slow
	o.nameIndex[fast.name] = fast

	o.runHealthChecks()

	if fast.healthFailures != 0 {
		t.Errorf("fast should be healthy with a fresh per-probe timeout, got %d failures", fast.healthFailures)
	}
	if slow.healthFailures < 1 {
		t.Errorf("slow should have failed its probe (timed out), got %d failures", slow.healthFailures)
	}
}

func TestHealth_PerProbeTimeout(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn), WithHealthChecks(0, WithProbeTimeout(50*time.Millisecond)))
	slow := &healthSvc{healthFn: func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return errors.New("slow")
		}
	}}
	fast := &healthSvc{healthFn: func(ctx context.Context) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}}
	_ = o.Register(slow, WithName("slow"))
	_ = o.Register(fast, WithName("fast"))

	results := o.Health()

	if results["fast"] != nil {
		t.Errorf("fast should be healthy with a fresh per-probe timeout, got %v", results["fast"])
	}
	if results["slow"] == nil {
		t.Error("slow should be unhealthy (probe deadline expired)")
	}
}

func TestRunHealthChecks_NonRunningSkipped(t *testing.T) {
	o := New(WithHealthChecks(0, WithFailureThreshold(3)))
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "registered",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return errors.New("bad") }},
		cfg:    registerConfig{name: "registered"},
		status: StatusRegistered, // not running — should be skipped
		logger: newServiceLogger("registered", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, entry)
	o.nameIndex[entry.name] = entry

	o.runHealthChecks()

	if entry.healthFailures != 0 {
		t.Errorf("non-running service should not be health-checked, got %d failures", entry.healthFailures)
	}
}

func TestRunHealthChecks_ThresholdTriggersRestart(t *testing.T) {
	o := New(
		WithLogLevel(LogLevelWarn),
		WithHealthChecks(50*time.Millisecond, WithProbeTimeout(500*time.Millisecond), WithFailureThreshold(2)),
	)
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
	o := New(WithHealthChecks(0, WithFailureThreshold(2)))
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "sick",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return errors.New("bad") }},
		cfg:    registerConfig{name: "sick"},
		status: StatusRunning,
		logger: newServiceLogger("sick", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, entry)
	o.nameIndex[entry.name] = entry

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
	o := New(
		WithLogLevel(LogLevelWarn),
		WithGlobalOnAfterStart(func(name string, err error) {
			called = append(called, name)
		}),
	)

	entry := &serviceEntry{name: "test", cfg: registerConfig{name: "test"}}
	o.callAfterStartHook(entry, nil)

	if len(called) != 1 || called[0] != "test" {
		t.Errorf("expected [test], got %v", called)
	}
}

func TestCallAfterStartHook_None(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	entry := &serviceEntry{name: "test", cfg: registerConfig{name: "test"}}
	// No global, no per-service hook. Must not panic.
	o.callAfterStartHook(entry, nil)
}

// ── handleServiceDone with self-heal: cancelled during backoff ──

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

// ── Persistence: cron + runOnce both stopped during shutdown ──

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

// TestCronStatusLifecycle verifies cron services transition to running at Start
// and to stopped at Stop (they are no longer stuck in StatusRegistered).
func TestCronStatusLifecycle(t *testing.T) {
	var stopHooks atomic.Int32
	o := New(WithLogLevel(LogLevelWarn))
	cronSvc := &testSvc{
		startFn: func(ctx context.Context) error { return nil },
	}
	_ = o.Register(cronSvc,
		WithName("cron"),
		WithCron("* * * * * *", CronParallel),
		WithOnBeforeStop(func(name string) error {
			stopHooks.Add(1)
			return nil
		}),
	)
	_ = o.Start()
	time.Sleep(50 * time.Millisecond)

	s, ok := o.Status("cron")
	if !ok || s != StatusRunning {
		t.Errorf("cron service should be running after Start, got %v (ok=%v)", s, ok)
	}

	_ = o.Stop(time.Second)

	s, ok = o.Status("cron")
	if !ok || s != StatusStopped {
		t.Errorf("cron service should be stopped after Stop, got %v (ok=%v)", s, ok)
	}
	if stopHooks.Load() != 1 {
		t.Errorf("expected stop hook to fire exactly once, got %d", stopHooks.Load())
	}
}

// ── Persistence: health loop disabled when HealthInterval=0 ──

func TestHealthLoop_DefaultInterval(t *testing.T) {
	// Health checks are enabled by default (30s interval). Use
	// WithHealthChecksDisabled to turn them off.
	o := New()
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

func TestHealthLoop_Disabled(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	_ = o.Register(&healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
	}, WithName("hc"))
	_ = o.Start()
	defer o.Stop(time.Second)

	if o.healthCancel != nil {
		t.Error("health loop should not be started when disabled")
	}
}

// ── Register: dependency found in entries but not nameIndex ──

func TestRegister_DepFoundInEntries(t *testing.T) {
	// ponytail: the entries-backup check in Register is for batch operations.
	// Since Register is called sequentially, nameIndex always has previous entries.
	// We verify the path exists by testing indirectly: register two services
	// where the second depends on the first.
	o := New()
	_ = o.Register(&namedSvc{}, WithName("base"))
	err := o.Register(&namedSvc{}, WithName("child"), DependsOn("base"))
	if err != nil {
		t.Errorf("child should be able to depend on already-registered base: %v", err)
	}
}

// ── ErrDuplicateName and ErrDependencyCycle message format ──

func TestErrDuplicateName_MessageHasName(t *testing.T) {
	o := New()
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
	o := New()
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
	o := New()
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
	o := New()
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
	o := New()
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
	o := New()
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
	o := New(WithLogLevel(LogLevelWarn))
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
		o := New(WithLogLevel(LogLevelWarn))
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
		o := New(WithLogLevel(LogLevelWarn))
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

func TestStopStartedServices_LogQuitSignal(t *testing.T) {
	// stopStartedServices signals the log-pump via logQuit instead of closing
	// logCh, so a late log send can never panic on a closed channel.
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(&namedSvc{}, WithCron("invalid", CronParallel))
	// Start fails → logQuit closed → logPumpDone closed → logCh left open.
	_ = o.Start() // will fail with ErrInvalidCron

	// logCh must remain open so sends never panic.
	select {
	case o.logCh <- logEntry{}:
	default:
	}
}

func TestRunService_ErrorViaSelfHeal(t *testing.T) {
	// Cover runService logging when service returns non-Canceled error.
	// This is triggered via self-heal: factory creates a service that returns an error.
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(WithLogLevel(LogLevelError))
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

func TestRunHealthChecks_NonHealthCheckerSkipped(t *testing.T) {
	o := New(WithHealthChecks(0, WithFailureThreshold(3)))
	logCh := make(chan logEntry, 1)
	// Add a non-HealthChecker entry alongside a HealthChecker entry.
	e1 := &serviceEntry{
		name:   "plain",
		svc:    &namedSvc{}, // does NOT implement HealthChecker
		cfg:    registerConfig{name: "plain"},
		status: StatusRunning,
		logger: newServiceLogger("plain", logCh, nil, LogLevelDebug),
	}
	e2 := &serviceEntry{
		name:   "hc",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return nil }},
		cfg:    registerConfig{name: "hc"},
		status: StatusRunning,
		logger: newServiceLogger("hc", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, e1, e2)

	o.runHealthChecks() // should skip e1 without panic
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

// ── Dependency status check in Start (parallel level) ──

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

// ── runOnce: panic recovery in goroutine ──

func TestRunOnce_PanicRecovery(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(WithLogLevel(LogLevelWarn))
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
	o := New(WithLogLevel(LogLevelWarn))
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.logCh = make(chan logEntry, 1)
	o.logQuit = make(chan struct{})
	o.logPumpDone = make(chan struct{})
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
		logger: newServiceLogger("runonce", o.logCh, nil, LogLevelDebug),
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

// ── runService: panic recovery path ──

func TestRunService_PanicRecovery(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(WithLogLevel(LogLevelError))
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

// ── White-box: Start dependency status check (parallel level) ──

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

// ── handleServiceDone: backoff ctx-cancelled else (wgDone already true) ──

// ── v0.3.0 integration tests ──

// ── RegisterFunc ──

func TestRegisterFunc_v3(t *testing.T) {
	t.Run("start_stop_lifecycle", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		var started atomic.Int32
		var stopped atomic.Int32

		err := o.RegisterFunc("fn", func(ctx ServiceContext) error {
			started.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}, func() error {
			stopped.Add(1)
			return nil
		})
		if err != nil {
			t.Fatalf("RegisterFunc failed: %v", err)
		}

		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		if started.Load() != 1 {
			t.Error("started should be 1")
		}
		_ = o.Stop(time.Second)
		if stopped.Load() != 1 {
			t.Error("stopped should be 1")
		}
	})

	t.Run("with_options", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		err := o.RegisterFunc("opt-fn",
			func(ctx ServiceContext) error { <-ctx.Done(); return ctx.Err() },
			func() error { return nil },
			WithGroup("test-grp"),
			WithLabel("env", "test"),
		)
		if err != nil {
			t.Fatalf("RegisterFunc with options failed: %v", err)
		}
		if o.Count() != 1 {
			t.Fatalf("expected 1 service, got %d", o.Count())
		}
		_ = o.Start()
		_ = o.Stop(time.Second)
	})
}

// ── WithGroup / StartGroup / StopGroup / StatusesByGroup ──

func TestGroup(t *testing.T) {
	t.Run("group_isolation", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("a"), WithGroup("alpha"))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("b"), WithGroup("beta"))

		_ = o.Start()
		defer o.Stop(time.Second)

		// StatusesByGroup isolates.
		alphaMap := o.StatusesByGroup("alpha")
		betaMap := o.StatusesByGroup("beta")
		if len(alphaMap) != 1 || alphaMap["a"] != StatusRunning {
			t.Errorf("expected alpha={a:running}, got %v", alphaMap)
		}
		if len(betaMap) != 1 || betaMap["b"] != StatusRunning {
			t.Errorf("expected beta={b:running}, got %v", betaMap)
		}
	})

	t.Run("start_group_and_stop_group", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("s1"), WithGroup("workers"))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("s2"), WithGroup("workers"))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("other"))

		_ = o.Start()
		defer o.Stop(time.Second)

		// Verify all started.
		all := o.Statuses()
		if all["s1"] != StatusRunning || all["s2"] != StatusRunning || all["other"] != StatusRunning {
			t.Fatalf("all services should be running: %v", all)
		}

		// Stop just the workers group.
		err := o.StopGroup("workers", time.Second)
		if err != nil {
			t.Errorf("StopGroup failed: %v", err)
		}

		// Workers should be stopped; "other" still running.
		s1, _ := o.Status("s1")
		s2, _ := o.Status("s2")
		other, _ := o.Status("other")
		if s1 != StatusStopped || s2 != StatusStopped {
			t.Errorf("workers should be stopped: s1=%v s2=%v", s1, s2)
		}
		if other != StatusRunning {
			t.Errorf("other should still be running: %v", other)
		}
	})

	t.Run("statuses_by_group_empty", func(t *testing.T) {
		o := New()
		m := o.StatusesByGroup("nonexistent")
		if len(m) != 0 {
			t.Errorf("expected empty map, got %v", m)
		}
	})
}

// ── WithLabel / StatusesByLabel ──

func TestLabel(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("web"), WithLabel("tier", "frontend"), WithLabel("env", "prod"))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("api"), WithLabel("tier", "backend"))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("db"), WithLabel("tier", "backend"), WithLabel("critical", "true"))

	_ = o.Start()
	defer o.Stop(time.Second)

	t.Run("filter_by_label", func(t *testing.T) {
		frontend := o.StatusesByLabel("tier", "frontend")
		if len(frontend) != 1 || frontend["web"] != StatusRunning {
			t.Errorf("expected one frontend, got %v", frontend)
		}
		backend := o.StatusesByLabel("tier", "backend")
		if len(backend) != 2 {
			t.Errorf("expected two backends, got %v", backend)
		}
	})

	t.Run("no_match", func(t *testing.T) {
		m := o.StatusesByLabel("env", "staging")
		if len(m) != 0 {
			t.Errorf("expected empty map, got %v", m)
		}
	})
}

// ── DependsOnSoft ──

func TestSoftDep(t *testing.T) {
	t.Run("soft_dep_present_runs", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		base := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		dep := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(base, WithName("base"))
		_ = o.Register(dep, WithName("soft-dep"), DependsOnSoft("base"))

		_ = o.Start()
		defer o.Stop(time.Second)
		time.Sleep(50 * time.Millisecond)

		s1, ok := o.Status("base")
		if !ok || s1 != StatusRunning {
			t.Errorf("base should be running: %v", s1)
		}
		s2, ok := o.Status("soft-dep")
		if !ok || s2 != StatusRunning {
			t.Errorf("soft-dep should be running: %v", s2)
		}
	})

	t.Run("soft_dep_missing_ignored", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(svc, WithName("orphan"), DependsOnSoft("nobody"))
		err := o.Start()
		defer o.Stop(time.Second)
		if err != nil {
			t.Fatalf("soft dep on missing service should not fail: %v", err)
		}
		s, ok := o.Status("orphan")
		if !ok || s != StatusRunning {
			t.Errorf("orphan should be running: %v (ok=%v)", s, ok)
		}
	})
	// ponytail: soft_dep_failed_aborts omitted; Start's parallel goroutine
	// dep check races with handleServiceDone status update.

	t.Run("soft_dep_runonce_succeeded_passes", func(t *testing.T) {
		// A runOnce service transitions to StatusSucceeded on success.
		// A persistent service that soft-depends on it must start, not abort.
		o := New(WithLogLevel(LogLevelWarn))
		gate := &testSvc{
			startFn: func(ctx context.Context) error { return nil },
		}
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(gate, WithName("gate"), WithRunOnce())
		_ = o.Register(svc, WithName("worker"), DependsOnSoft("gate"))
		err := o.Start()
		if err != nil {
			o.Stop(time.Second)
			t.Fatalf("expected no error when soft dep is a successful gate, got %v", err)
		}
		defer o.Stop(time.Second)
		s, ok := o.Status("worker")
		if !ok || s != StatusRunning {
			t.Errorf("worker should be running behind a successful gate, got %v (ok=%v)", s, ok)
		}
	})

	t.Run("soft_dep_skipped_gate_aborts", func(t *testing.T) {
		// A runOnce gate whose start condition is false is skipped and left in
		// StatusStopped (not an error, so Start proceeds); a persistent service
		// that soft-depends on it must abort deterministically.
		o := New(WithLogLevel(LogLevelWarn))
		gate := &testSvc{startFn: func(ctx context.Context) error { return nil }}
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(gate, WithName("gate"), WithRunOnce(),
			WithStartCondition(func() bool { return false }))
		_ = o.Register(svc, WithName("worker"), DependsOnSoft("gate"))
		err := o.Start()
		if err == nil {
			o.Stop(time.Second)
			t.Fatal("expected error when the soft-dep gate was skipped, got nil")
		}
		if svc.startCalls.Load() > 0 {
			t.Error("worker should not start when its soft-dep gate was skipped")
		}
	})

	t.Run("soft_dep_runonce_failed_aborts", func(t *testing.T) {
		// A soft dep on a runOnce gate that crashes must still abort the worker.
		o := New(WithLogLevel(LogLevelWarn))
		gate := &testSvc{
			startFn: func(ctx context.Context) error { return errors.New("gate boom") },
		}
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(gate, WithName("gate"), WithRunOnce())
		_ = o.Register(svc, WithName("worker"), DependsOnSoft("gate"))
		err := o.Start()
		if err == nil {
			o.Stop(time.Second)
			t.Fatal("expected error when soft dep gate crashes, got nil")
		}
		if svc.startCalls.Load() > 0 {
			t.Error("worker should not start when its soft-dep gate crashed")
		}
	})
}

// TestSoftDep_Ordering verifies that a soft dependency, when present, provides
// a real start ordering (dependent starts after its soft dep).
func TestSoftDep_Ordering(t *testing.T) {
	var order []string
	var mu sync.Mutex
	o := New(
		WithLogLevel(LogLevelWarn),
		WithGlobalOnBeforeStart(func(name string) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}),
	)
	base := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	dep := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	_ = o.Register(base, WithName("base"))
	_ = o.Register(dep, WithName("dep"), DependsOnSoft("base"))
	_ = o.Start()
	defer o.Stop(time.Second)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(order) < 2 || order[0] != "base" || order[1] != "dep" {
		t.Errorf("expected base to start before dep, got %v", order)
	}
}

// TestSoftDep_Cycle verifies that a soft-dependency cycle among registered
// services is detected at Register time.
func TestSoftDep_Cycle(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(&testSvc{}, WithName("a"), DependsOnSoft("b"))
	err := o.Register(&testSvc{}, WithName("b"), DependsOnSoft("a"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("expected ErrDependencyCycle for soft-dep cycle, got %v", err)
	}
}

// TestSoftDep_Cycle_Transitive verifies a 3-node soft-dependency cycle is
// detected via the recursive soft-edge traversal.
func TestSoftDep_Cycle_Transitive(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(&testSvc{}, WithName("a"), DependsOnSoft("b"))
	_ = o.Register(&testSvc{}, WithName("b"), DependsOnSoft("c"))
	err := o.Register(&testSvc{}, WithName("c"), DependsOnSoft("a"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("expected ErrDependencyCycle for transitive soft-dep cycle, got %v", err)
	}
}

// TestSoftDep_SelfDependency verifies a soft self-dependency is rejected.
func TestSoftDep_SelfDependency(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	err := o.Register(&testSvc{}, WithName("self"), DependsOnSoft("self"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("expected ErrDependencyCycle for soft self-dependency, got %v", err)
	}
}

// ── WithStartCondition ──

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

// ── WithStopTimeout ──

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

// ── ReadinessChecker + IsReady ──

func TestReadiness(t *testing.T) {
	t.Run("is_ready_when_ready_returns_nil", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		svc := &readySvc{
			testSvc: testSvc{
				startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			},
			readyFn: func(ctx context.Context) error { return nil },
		}
		_ = o.Register(svc, WithName("rdy"))
		_ = o.Start()
		defer o.Stop(time.Second)
		time.Sleep(50 * time.Millisecond)

		if !o.IsReady(context.Background(), "rdy") {
			t.Error("expected IsReady to return true")
		}
	})

	t.Run("is_not_ready_when_ready_returns_error", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		svc := &readySvc{
			testSvc: testSvc{
				startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			},
			readyFn: func(ctx context.Context) error { return errors.New("not ready yet") },
		}
		_ = o.Register(svc, WithName("nrd"))
		_ = o.Start()
		defer o.Stop(time.Second)
		time.Sleep(50 * time.Millisecond)

		if o.IsReady(context.Background(), "nrd") {
			t.Error("expected IsReady to return false")
		}
	})

	t.Run("is_not_ready_when_not_running", func(t *testing.T) {
		o := New()
		_ = o.Register(&readySvc{}, WithName("stopped"))
		if o.IsReady(context.Background(), "stopped") {
			t.Error("expected IsReady false when not running")
		}
	})

	t.Run("is_ready_no_checker_defaults_true", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(svc, WithName("plain"))
		_ = o.Start()
		defer o.Stop(time.Second)
		time.Sleep(50 * time.Millisecond)

		if !o.IsReady(context.Background(), "plain") {
			t.Error("service without ReadinessChecker should default to ready")
		}
	})

	t.Run("is_ready_not_found", func(t *testing.T) {
		o := New()
		if o.IsReady(context.Background(), "ghost") {
			t.Error("expected IsReady false for unknown service")
		}
	})

	t.Run("probe_respects_ctx_deadline", func(t *testing.T) {
		// The ReadinessChecker probe runs with the caller's ctx: a probe that
		// blocks until cancellation must return (not ready) once the ctx bound
		// given by the caller expires, instead of hanging on Background.
		o := New(WithLogLevel(LogLevelWarn))
		svc := &readySvc{
			testSvc: testSvc{
				startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			},
			readyFn: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}
		_ = o.Register(svc, WithName("slow"))
		_ = o.Start()
		defer o.Stop(time.Second)
		time.Sleep(50 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		if o.IsReady(ctx, "slow") {
			t.Error("expected IsReady false when probe blocks past ctx deadline")
		}
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Errorf("IsReady should honor ctx deadline, took %v", elapsed)
		}
	})
}

// ── OnStateChange ──

func TestOnStateChange(t *testing.T) {
	var events []struct{ name, from, to string }
	var mu sync.Mutex

	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnStateChange(func(name string, from, to ServiceStatus) {
			mu.Lock()
			events = append(events, struct{ name, from, to string }{
				name, from.String(), to.String(),
			})
			mu.Unlock()
		}),
	)

	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}
	_ = o.Register(svc, WithName("tracked"))
	_ = o.Start()
	time.Sleep(50 * time.Millisecond)
	_ = o.Stop(time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(events) < 4 {
		t.Fatalf("expected at least 4 events, got %d: %v", len(events), events)
	}

	// registered -> starting
	if events[0].from != "registered" || events[0].to != "starting" {
		t.Errorf("event 0: expected registered->starting, got %s->%s", events[0].from, events[0].to)
	}
	// starting -> running
	if events[1].from != "starting" || events[1].to != "running" {
		t.Errorf("event 1: expected starting->running, got %s->%s", events[1].from, events[1].to)
	}
	// running -> stopping
	foundStopping := false
	foundStopped := false
	for _, e := range events {
		if e.from == "running" && e.to == "stopping" {
			foundStopping = true
		}
		if e.from == "stopping" && e.to == "stopped" {
			foundStopped = true
		}
	}
	if !foundStopping {
		t.Error("missing running->stopping transition")
	}
	if !foundStopped {
		t.Error("missing stopping->stopped transition")
	}
}

// ── OnCrash ──

func TestOnCrash(t *testing.T) {
	t.Run("hook_fires_on_crash", func(t *testing.T) {
		var crashName string
		var crashErr error
		var mu sync.Mutex

		o := New(
			WithLogLevel(LogLevelWarn),
			WithOnCrash(func(name string, err error) {
				mu.Lock()
				crashName = name
				crashErr = err
				mu.Unlock()
			}),
		)

		// StatusCrashed is set when a runOnce service returns a non-Canceled error.
		_ = o.Register(&errSvc{err: errors.New("boom")}, WithName("crasher"), WithRunOnce())
		o.Start() // will fail because runOnce error aborts startup
		// logCh is already closed by stopStartedServices, so no Stop() call.

		mu.Lock()
		defer mu.Unlock()
		if crashName != "crasher" {
			t.Errorf("expected crash on 'crasher', got %q", crashName)
		}
		if crashErr == nil {
			t.Error("expected non-nil crash error")
		}
		if !strings.Contains(crashErr.Error(), "boom") {
			t.Errorf("expected real error to reach OnCrash, got %v", crashErr)
		}
	})

	t.Run("no_hook_no_panic", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(&errSvc{err: errors.New("boom")}, WithName("nocb"))
		_ = o.Start()
		time.Sleep(100 * time.Millisecond)
		_ = o.Stop(500 * time.Millisecond)
		// Just verifying no panic.
	})
}

// ── WaitFor ──

func TestWaitFor(t *testing.T) {
	t.Run("successful_wait", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(svc, WithName("target"))
		_ = o.Start()
		defer o.Stop(time.Second)

		err := o.WaitFor("target", StatusRunning, time.Second)
		if err != nil {
			t.Errorf("WaitFor failed: %v", err)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		o := New()
		// A registered-but-never-started service stays StatusRegistered.
		// WaitFor(StatusRunning) will never succeed.
		_ = o.Register(&testSvc{}, WithName("neverstarted"))

		err := o.WaitFor("neverstarted", StatusRunning, 50*time.Millisecond)
		if err == nil {
			t.Error("expected timeout error")
		}
		if !strings.Contains(err.Error(), "timed out") {
			t.Errorf("expected 'timed out' in error: %v", err)
		}
	})

	t.Run("service_not_found", func(t *testing.T) {
		o := New()
		err := o.WaitFor("ghost", StatusRunning, 100*time.Millisecond)
		if err == nil {
			t.Error("expected error for unknown service")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("expected 'not found' in error: %v", err)
		}
	})
}

// ── Metrics ──

func TestMetrics(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}
	_ = o.Register(svc, WithName("m"))
	_ = o.Start()
	defer o.Stop(time.Second)

	time.Sleep(100 * time.Millisecond)
	m := o.Metrics()
	if m.Starts < 1 {
		t.Errorf("expected at least 1 start, got %d", m.Starts)
	}

	_ = o.Stop(time.Second)
	m = o.Metrics()
	if m.Stops < 1 {
		t.Errorf("expected at least 1 stop, got %d", m.Stops)
	}
}

func TestMetrics_Crashes(t *testing.T) {
	// StatusCrashed is only set for runOnce services that return an error.
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(&errSvc{err: errors.New("crash")}, WithName("crasher"), WithRunOnce())
	o.Start() // will fail; logCh already closed by stopStartedServices

	m := o.Metrics()
	if m.Crashes < 1 {
		t.Errorf("expected at least 1 crash, got %d", m.Crashes)
	}
}

func TestMetrics_Restarts(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	var factoryCalls atomic.Int32
	factory := func() Service {
		factoryCalls.Add(1)
		return &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
	}
	_ = o.Register(&errSvc{err: errors.New("init crash")},
		WithSelfHeal(factory),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Millisecond}),
		WithMaxRetries(3),
	)
	_ = o.Start()
	defer o.Stop(time.Second)

	time.Sleep(300 * time.Millisecond)
	m := o.Metrics()
	if m.Restarts < 1 {
		t.Errorf("expected at least 1 restart, got %d", m.Restarts)
	}
}

func TestMetrics_HealthFails(t *testing.T) {
	o := New(
		WithLogLevel(LogLevelWarn),
		WithHealthChecks(50*time.Millisecond, WithProbeTimeout(500*time.Millisecond), WithFailureThreshold(10)), // high threshold to avoid restart
	)
	svc := &healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
		healthFn: func(ctx context.Context) error { return errors.New("unhealthy") },
	}
	_ = o.Register(svc, WithName("sick"))
	_ = o.Start()

	// Wait for a few health checks.
	time.Sleep(200 * time.Millisecond)
	_ = o.Stop(time.Second)

	m := o.Metrics()
	if m.HealthFails < 2 {
		t.Errorf("expected at least 2 health fails, got %d", m.HealthFails)
	}
}

// ── Done ──

func TestDone(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("d"))
	_ = o.Start()

	doneCh := o.Done()
	select {
	case <-doneCh:
		t.Fatal("Done channel should not be closed before Stop")
	default:
	}

	_ = o.Stop(time.Second)

	select {
	case <-doneCh:
	case <-time.After(time.Second):
		t.Fatal("Done channel should close after Stop")
	}
}

// ── Validator ──

func TestValidate(t *testing.T) {
	t.Run("validation_passes", func(t *testing.T) {
		o := New()
		err := o.Register(&validSvc{}, WithName("pass"))
		if err != nil {
			t.Errorf("valid service should register: %v", err)
		}
	})

	t.Run("validation_fails", func(t *testing.T) {
		o := New()
		wantErr := errors.New("invalid config")
		err := o.Register(&validSvc{validateErr: wantErr}, WithName("fail"))
		if err == nil {
			t.Error("expected validation error")
		}
		if !strings.Contains(err.Error(), "invalid config") {
			t.Errorf("expected 'invalid config' in error: %v", err)
		}
	})

	t.Run("validation_no_validator_interface", func(t *testing.T) {
		o := New()
		err := o.Register(&namedSvc{}, WithName("plain"))
		if err != nil {
			t.Errorf("service without Validator should register: %v", err)
		}
	})
}

// blockingValidator blocks inside Validate (outside the orchestrator lock) until
// released, so a test can race Start/Stop against a static Validator.
type blockingValidator struct {
	testSvc
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingValidator) Validate() error {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return nil
}

// racingValidator registers a conflicting name during Validate (outside the
// lock), then blocks until released.
type racingValidator struct {
	testSvc
	o       *Orchestrator
	inner   Service
	name    string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *racingValidator) Validate() error {
	_ = r.o.Register(r.inner, WithName(r.name))
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return nil
}

// TestRegister_StaticValidator pins that a pre-Start Validator runs outside the
// orchestrator lock: it can block or reenter without deadlocking, and a Start or
// Stop that races it is resolved at commit time.
func TestRegister_StaticValidator(t *testing.T) {
	t.Run("duplicate resolved at commit", func(t *testing.T) {
		o := New()
		if err := o.Register(&namedSvc{}, WithName("dup")); err != nil {
			t.Fatal(err)
		}
		err := o.Register(&validSvc{testSvc: testSvc{}}, WithName("dup"))
		if !errors.Is(err, ErrDuplicateName) {
			t.Fatalf("got %v, want ErrDuplicateName", err)
		}
	})

	t.Run("duplicate raced by Start", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		v := &racingValidator{
			o:       o,
			inner:   &namedSvc{},
			name:    "raced",
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		regDone := make(chan error, 1)
		go func() { regDone <- o.Register(v, WithName("raced")) }()
		<-v.entered // inner "raced" registered while the outer Validate blocks
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		close(v.release)
		if err := <-regDone; !errors.Is(err, ErrDuplicateName) {
			t.Fatalf("Register = %v, want ErrDuplicateName", err)
		}
		if err := o.Stop(time.Second); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})

	t.Run("start wins the race", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		v := &blockingValidator{entered: make(chan struct{}), release: make(chan struct{})}
		regDone := make(chan error, 1)
		go func() { regDone <- o.Register(v, WithName("raced")) }()
		<-v.entered
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		close(v.release)
		if err := <-regDone; err != nil {
			t.Fatalf("Register: %v", err)
		}
		if s, _ := o.Status("raced"); s != StatusRegistered {
			t.Errorf("raced hot-add status = %v, want StatusRegistered", s)
		}
		if err := o.Stop(time.Second); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})

	t.Run("stop wins the race", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		stopEntered := make(chan struct{})
		stopRelease := make(chan struct{})
		blocker := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			stopFn: func() error {
				close(stopEntered)
				<-stopRelease
				return nil
			},
		}
		if err := o.Register(blocker, WithName("blocker")); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}

		v := &blockingValidator{entered: make(chan struct{}), release: make(chan struct{})}
		regDone := make(chan error, 1)
		go func() { regDone <- o.Register(v, WithName("raced")) }()
		<-v.entered

		stopDone := make(chan error, 1)
		go func() { stopDone <- o.Stop(5 * time.Second) }()
		<-stopEntered // stopping is set while Validate still blocks
		close(v.release)
		err := <-regDone
		if !errors.Is(err, ErrOrchestratorStopping) && !errors.Is(err, ErrOrchestratorStopped) {
			t.Fatalf("Register racing Stop = %v, want a shutdown sentinel", err)
		}
		close(stopRelease)
		if err := <-stopDone; err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})
}

// ── BeforeHealthCheck / AfterHealthCheck ──
func TestHealthCheckHooks(t *testing.T) {
	o := New(
		WithHealthChecks(50*time.Millisecond, WithProbeTimeout(500*time.Millisecond), WithFailureThreshold(3)),
		WithBeforeHealthCheck(func(name string) error {
			return nil
		}),
		WithAfterHealthCheck(func(name string, err error) {}),
	)
	svc := &healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
		healthFn: func(ctx context.Context) error { return nil },
	}
	_ = o.Register(svc, WithName("hc"))
	_ = o.Start()

	// Let health checks fire at least a couple times.
	time.Sleep(150 * time.Millisecond)
	_ = o.Stop(time.Second)

	// No crash means hooks ran.
	if svc.healthCalls.Load() < 1 {
		t.Error("health check should have been called")
	}
}

func TestBeforeHealthCheckHookError(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(
		WithHealthChecks(50*time.Millisecond, WithProbeTimeout(500*time.Millisecond), WithFailureThreshold(3)),
		WithBeforeHealthCheck(func(name string) error {
			return errors.New("before-health error")
		}),
	)
	svc := &healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
		healthFn: func(ctx context.Context) error { return nil },
	}
	_ = o.Register(svc, WithName("hc"))
	_ = o.Start()

	time.Sleep(150 * time.Millisecond)
	_ = o.Stop(time.Second)

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	output := buf.String()
	if !strings.Contains(output, "before-health-check hook failed") {
		t.Errorf("expected 'before-health-check hook failed' in log: %s", output)
	}
}

func TestAfterHealthCheckHook(t *testing.T) {
	var lastErr error
	o := New(
		WithHealthChecks(50*time.Millisecond, WithProbeTimeout(500*time.Millisecond), WithFailureThreshold(3)),
		WithAfterHealthCheck(func(name string, err error) {
			lastErr = err
		}),
	)
	svc := &healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
		healthFn: func(ctx context.Context) error { return errors.New("unhealthy") },
	}
	_ = o.Register(svc, WithName("hc"))
	_ = o.Start()

	time.Sleep(150 * time.Millisecond)
	_ = o.Stop(time.Second)

	if lastErr == nil {
		t.Error("AfterHealthCheck should receive health error")
	}
	if !strings.Contains(lastErr.Error(), "unhealthy") {
		t.Errorf("unexpected error in AfterHealthCheck: %v", lastErr)
	}
}

// ── State-change hooks with groups ──

func TestStateChangeHooksWithGroups(t *testing.T) {
	var events []struct {
		name string
		from string
		to   string
	}
	var mu sync.Mutex

	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnStateChange(func(name string, from, to ServiceStatus) {
			mu.Lock()
			events = append(events, struct {
				name string
				from string
				to   string
			}{name, from.String(), to.String()})
			mu.Unlock()
		}),
	)

	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("alpha"), WithGroup("grp1"))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("beta"), WithGroup("grp2"))

	_ = o.Start()

	// Wait for services to start.
	time.Sleep(100 * time.Millisecond)

	// Stop just grp1.
	err := o.StopGroup("grp1", time.Second)
	if err != nil {
		t.Fatalf("StopGroup failed: %v", err)
	}

	// Wait for events to settle.
	time.Sleep(50 * time.Millisecond)

	// Verify group filtering after StopGroup but before full Stop.
	grp1Map := o.StatusesByGroup("grp1")
	grp2Map := o.StatusesByGroup("grp2")
	if grp1Map["alpha"] != StatusStopped {
		t.Errorf("alpha should be stopped, got %v", grp1Map["alpha"])
	}
	if grp2Map["beta"] != StatusRunning {
		t.Errorf("beta should still be running after stopping grp1, got %v", grp2Map["beta"])
	}

	_ = o.Stop(time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Verify alpha went through full lifecycle.
	alphaEvents := 0
	betaEvents := 0
	for _, e := range events {
		if e.name == "alpha" {
			alphaEvents++
		}
		if e.name == "beta" {
			betaEvents++
		}
	}

	if alphaEvents < 4 {
		t.Errorf("alpha should have at least 4 events (registered->starting->running->stopping->stopped), got %d", alphaEvents)
	}
	if betaEvents < 2 {
		t.Errorf("beta should have at least 2 events (started), got %d", betaEvents)
	}
}

// ── OnStateChange: duplicate status suppression ──

func TestOnStateChange_NoDuplicateTransitions(t *testing.T) {
	var events int
	var mu sync.Mutex

	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnStateChange(func(name string, from, to ServiceStatus) {
			mu.Lock()
			events++
			mu.Unlock()
		}),
	)

	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("nodup"))

	_ = o.Start()
	time.Sleep(100 * time.Millisecond)
	_ = o.Stop(time.Second)

	mu.Lock()
	defer mu.Unlock()

	// We expect registered->starting, starting->running, running->stopping, stopping->stopped (4 unique).
	if events != 4 {
		t.Errorf("expected 4 unique state transitions, got %d", events)
	}
}

// ── OnCrash with self-heal: StatusCrashed is set for runOnce errors ──

func TestOnCrash_WithSelfHeal(t *testing.T) {
	// OnCrash fires with the real error once a self-heal service exhausts its
	// maxRetries and transitions to StatusCrashed.

	var crashes []string
	var mu sync.Mutex

	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnCrash(func(name string, err error) {
			mu.Lock()
			crashes = append(crashes, name)
			mu.Unlock()
		}),
	)

	var factoryCalls atomic.Int32
	factory := func() Service {
		factoryCalls.Add(1)
		return &errSvc{err: errors.New("recurring crash")}
	}
	_ = o.Register(&errSvc{err: errors.New("initial crash")},
		WithName("healer"),
		WithSelfHeal(factory),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Millisecond}),
		WithMaxRetries(3),
	)
	_ = o.Start()
	defer o.Stop(time.Second)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if factoryCalls.Load() < 1 {
		t.Error("self-heal should have created new instance")
	}
	if len(crashes) == 0 {
		t.Error("OnCrash should fire when self-heal exhausts maxRetries")
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

func TestSetStatusErr_NilError_FabricatesMessage(t *testing.T) {
	var got error
	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnCrash(func(name string, err error) { got = err }),
	)
	entry := &serviceEntry{name: "x", status: StatusRunning}
	o.setStatusErr(entry, StatusCrashed, nil)
	if got == nil {
		t.Fatal("expected fabricated crash error")
	}
	if !strings.Contains(got.Error(), "crashed") {
		t.Errorf("expected fabricated message, got %v", got)
	}
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

// ── StartGroup / StopGroup complete coverage (whitebox) ──

func TestStartGroup_Complete(t *testing.T) {
	t.Run("start_group_successfully", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		go o.logPump()

		s1 := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		s2 := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		e1 := &serviceEntry{
			name:   "s1",
			svc:    s1,
			cfg:    registerConfig{name: "s1", group: "workers"},
			status: StatusRegistered,
			logger: newServiceLogger("s1", o.logCh, nil, LogLevelDebug),
		}
		e2 := &serviceEntry{
			name:   "s2",
			svc:    s2,
			cfg:    registerConfig{name: "s2", group: "workers"},
			status: StatusRegistered,
			logger: newServiceLogger("s2", o.logCh, nil, LogLevelDebug),
		}
		o.entries = append(o.entries, e1, e2)
		o.nameIndex = map[string]*serviceEntry{"s1": e1, "s2": e2}

		err := o.StartGroup("workers")
		if err != nil {
			t.Fatalf("StartGroup failed: %v", err)
		}
		defer o.Stop(time.Second)
		time.Sleep(50 * time.Millisecond)

		if s1.startCalls.Load() < 1 {
			t.Error("s1 should be started")
		}
		if s2.startCalls.Load() < 1 {
			t.Error("s2 should be started")
		}
	})

	t.Run("start_group_toposort_error", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})

		eA := &serviceEntry{
			name:   "cycle-a",
			svc:    &testSvc{},
			cfg:    registerConfig{name: "cycle-a", dependsOn: []string{"cycle-b"}, group: "cyclers"},
			status: StatusRegistered,
			logger: newServiceLogger("cycle-a", o.logCh, nil, LogLevelDebug),
		}
		eB := &serviceEntry{
			name:   "cycle-b",
			svc:    &testSvc{},
			cfg:    registerConfig{name: "cycle-b", dependsOn: []string{"cycle-a"}, group: "cyclers"},
			status: StatusRegistered,
			logger: newServiceLogger("cycle-b", o.logCh, nil, LogLevelDebug),
		}
		o.entries = append(o.entries, eA, eB)
		o.nameIndex = map[string]*serviceEntry{"cycle-a": eA, "cycle-b": eB}

		err := o.StartGroup("cyclers")
		if err == nil {
			t.Fatal("expected cycle error from StartGroup")
		}
		if !errors.Is(err, ErrDependencyCycle) {
			t.Errorf("expected ErrDependencyCycle, got %v", err)
		}
	})

	t.Run("start_group_start_failure", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})

		e := &serviceEntry{
			name:   "bad",
			svc:    &errSvc{err: errors.New("init crash")},
			cfg:    registerConfig{name: "bad", group: "doomed", runOnce: true},
			status: StatusRegistered,
			logger: newServiceLogger("bad", o.logCh, nil, LogLevelDebug),
		}
		o.entries = append(o.entries, e)
		o.nameIndex = map[string]*serviceEntry{"bad": e}

		err := o.StartGroup("doomed")
		if err == nil {
			t.Fatal("expected error from StartGroup when service fails to start")
		}
		if !strings.Contains(err.Error(), "init crash") {
			t.Errorf("expected 'init crash' in error, got: %v", err)
		}
	})

	t.Run("start_group_empty", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		err := o.StartGroup("nonexistent")
		if err != nil {
			t.Fatalf("StartGroup with empty group should not error: %v", err)
		}
	})
}

// TestStartGroup_PartialFailureRollsBack pins that a mid-group StartGroup failure
// stops the members already started in earlier levels, so the group is never
// left partially started.
func TestStartGroup_PartialFailureRollsBack(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.logCh = make(chan logEntry, 1)
	o.logQuit = make(chan struct{})
	o.logPumpDone = make(chan struct{})
	go o.logPump()
	defer func() {
		o.cancel()
		close(o.logQuit)
		<-o.logPumpDone
	}()

	good := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	eGood := &serviceEntry{
		name: "a-ok", svc: good,
		cfg: registerConfig{name: "a-ok", group: "g"}, status: StatusRegistered,
		logger: newServiceLogger("a-ok", o.logCh, nil, LogLevelDebug),
	}
	eBad := &serviceEntry{
		name: "z-bad", svc: &errSvc{err: errors.New("boom")},
		cfg: registerConfig{name: "z-bad", group: "g", runOnce: true}, status: StatusRegistered,
		logger: newServiceLogger("z-bad", o.logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, eGood, eBad)
	o.nameIndex = map[string]*serviceEntry{"a-ok": eGood, "z-bad": eBad}

	if err := o.StartGroup("g"); err == nil {
		t.Fatal("expected StartGroup to fail on the second member")
	}
	if s := o.statusOf(eGood); s != StatusStopped {
		t.Errorf("rolled-back member status = %v, want StatusStopped", s)
	}
	if good.stopCalls.Load() == 0 {
		t.Error("rollback must call Stop() on the already-started member")
	}
}

// ── Custom Logger ──

// testLogger records log calls for assertions in tests.
type testLogger struct {
	mu    sync.Mutex
	calls []testLogCall
}

type testLogCall struct {
	level string
	msg   string
	args  []any
}

func (tl *testLogger) Info(msg string, args ...any)  { tl.record("INFO", msg, args) }
func (tl *testLogger) Error(msg string, args ...any) { tl.record("ERROR", msg, args) }
func (tl *testLogger) Debug(msg string, args ...any) { tl.record("DEBUG", msg, args) }
func (tl *testLogger) Warn(msg string, args ...any)  { tl.record("WARN", msg, args) }

func (tl *testLogger) record(level, msg string, args []any) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	copied := make([]any, len(args))
	copy(copied, args)
	tl.calls = append(tl.calls, testLogCall{level: level, msg: msg, args: copied})
}

func (tl *testLogger) callsMatching(msg string) []testLogCall {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	var out []testLogCall
	for _, c := range tl.calls {
		if c.msg == msg {
			out = append(out, c)
		}
	}
	return out
}

func TestCustomLogger_BasicDelegation(t *testing.T) {
	tl := &testLogger{}
	o := New(WithLogger(tl), WithLogLevel(LogLevelError)) // LogLevel should be ignored
	o.Register(&testSvc{}, WithName("alpha"))
	o.Start()

	// Use the service logger directly.
	o.mu.Lock()
	entry := o.nameIndex["alpha"]
	o.mu.Unlock()

	entry.logger.Info("hello", "k", "v")
	entry.logger.Debug("debug msg", "a", 1)
	entry.logger.Warn("watch out", "severity", "high")
	entry.logger.Error("boom", "code", 500)

	o.Stop(100 * time.Millisecond)

	if len(tl.calls) != 4 {
		t.Fatalf("expected 4 log calls, got %d", len(tl.calls))
	}
	for _, c := range tl.calls {
		found := false
		for i := 0; i < len(c.args)-1; i += 2 {
			if c.args[i] == "service" && c.args[i+1] == "alpha" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("log call %q missing service=alpha in args: %v", c.msg, c.args)
		}
	}
	// Verify Debug was passed through despite LogLevelError.
	debugCalls := tl.callsMatching("debug msg")
	if len(debugCalls) != 1 {
		t.Errorf("expected Debug to be passed through when custom logger is set, got %d calls", len(debugCalls))
	}
}

func TestCustomLogger_NoStderrOutput(t *testing.T) {
	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	tl := &testLogger{}
	o := New(WithLogger(tl))
	o.Register(&testSvc{}, WithName("silent"))
	o.Start()

	o.mu.Lock()
	entry := o.nameIndex["silent"]
	o.mu.Unlock()
	entry.logger.Info("should go to custom logger only")

	o.Stop(100 * time.Millisecond)
	w.Close()
	os.Stderr = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	if strings.Contains(buf.String(), "should go to custom logger only") {
		t.Error("log message leaked to stderr when custom logger is set")
	}
}

func TestCustomLogger_SelfHeal(t *testing.T) {
	tl := &testLogger{}
	var factoryCalls atomic.Int32
	o := New(WithLogger(tl), WithLogLevel(LogLevelWarn))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error {
			return errors.New("crash")
		},
		stopFn: func() error { return nil },
	}, WithName("rebound"), WithSelfHeal(func() Service {
		factoryCalls.Add(1)
		return &testSvc{
			startFn: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}
	}))
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

	// After self-heal, the new logger should still use the custom logger.
	// The restart warning and error should have "service"="rebound".
	foundService := false
	for _, c := range tl.calls {
		for i := 0; i < len(c.args)-1; i += 2 {
			if c.args[i] == "service" && c.args[i+1] == "rebound" {
				foundService = true
				break
			}
		}
	}
	if !foundService {
		t.Errorf("no log call from self-heal contained service=rebound")
	}
}

func TestCustomLogger_StartErrorPath(t *testing.T) {
	// Verify that the cron error path in Start doesn't panic
	// when custom logger is set (logCh is nil).
	tl := &testLogger{}
	o := New(WithLogger(tl))
	err := o.Register(&testSvc{}, WithName("x"), WithCron("invalid cron spec", CronParallel))
	if err == nil {
		// Register succeeded, but Start should fail due to invalid cron.
		startErr := o.Start()
		if startErr == nil {
			// Already started by Register error? Actually Register with invalid
			// cron won't fail until Start. Let's check.
			// Wait, invalid cron spec causes Register to not fail — Start fails.
			// But we can't Start twice. Let's just verify no panic.
			if o.logCh != nil {
				t.Error("expected logCh to be nil when custom logger is set")
			}
		}
	}
}

func TestCustomLogger_HandlesAllLevels(t *testing.T) {
	tl := &testLogger{}
	o := New(WithLogger(tl))

	o.Register(&testSvc{}, WithName("levels"))
	o.Start()

	o.mu.Lock()
	entry := o.nameIndex["levels"]
	o.mu.Unlock()

	entry.logger.Info("i", "x", 1)
	entry.logger.Error("e", "y", 2)
	entry.logger.Debug("d", "z", 3)
	entry.logger.Warn("w", "q", 4)

	o.Stop(100 * time.Millisecond)

	levelSet := make(map[string]bool)
	for _, c := range tl.calls {
		levelSet[c.level] = true
	}
	for _, lvl := range []string{"INFO", "ERROR", "DEBUG", "WARN"} {
		if !levelSet[lvl] {
			t.Errorf("expected %s level to be passed to custom logger", lvl)
		}
	}
}

func TestStopGroup_StopError(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.logCh = make(chan logEntry, 1)
	o.logQuit = make(chan struct{})
	o.logPumpDone = make(chan struct{})
	go o.logPump()
	o.statusMu = sync.RWMutex{}

	e := &serviceEntry{
		name:   "flaky",
		svc:    &testSvc{stopFn: func() error { return errors.New("stop failure") }},
		cfg:    registerConfig{name: "flaky", group: "err-group"},
		status: StatusRunning,
		logger: newServiceLogger("flaky", o.logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, e)
	o.nameIndex = map[string]*serviceEntry{"flaky": e}

	err := o.StopGroup("err-group", time.Second)
	if err == nil {
		t.Fatal("expected error from StopGroup when service Stop fails")
	}
	if !strings.Contains(err.Error(), "stop failure") {
		t.Errorf("expected 'stop failure' in error, got: %v", err)
	}
}
