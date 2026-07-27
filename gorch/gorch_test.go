package gorch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
		// ponytail: LogLevelDebug == 0, indistinguishable from zero-value.
		// New() defaults zero to Info. User who wants Debug must set it
		// after construction.
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
		// startOnce.Do ensures the setup runs exactly once.
		// Second Start() just returns nil (startErr is zero).
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
		// White-box: construct orchestrator where started==true but
		// startOnce hasn't fired yet, to reach the inner started check.
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
		// Service blocks forever (never checks ctx, never returns).
		// wg.Done is never called, so wg.Wait blocks until timeout.
		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				never := make(chan struct{})
				<-never // blocks forever
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
		// Publish after cleanup onto nil subs — must not panic.
		o.messenger.Publish("msg")
		select {
		case <-ch:
		default:
		}
	})
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
				// Block long enough for another tick to fire concurrently.
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
				// Block longer than cron interval so next tick would overlap.
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
		// With 2.5s block and 1s interval, most ticks are skipped.
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
				// Block >1s so the next tick finds running=true and spins.
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
			// Return a service that exits immediately with no error.
			// Avoids logging to logCh after Stop closes it.
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
		o.handleServiceDone(entry, sc) // second call: wgDone already true, no-op

		done := make(chan struct{})
		go func() { o.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("double handleServiceDone caused wg imbalance")
		}
	})
}

// ── Service panic recovery ──

func TestServicePanicRecovery(t *testing.T) {
	t.Run("panic_in_start_recovered", func(t *testing.T) {
		o := New(Config{})
		_ = o.Register(&panicSvc{msg: "start panic"})
		_ = o.Start()
		time.Sleep(100 * time.Millisecond)
		// No self-heal, so service exits after panic. Stop should succeed.
		err := o.Stop(500 * time.Millisecond)
		// The service panicked and exited; wg.Done was called.
		// Stop should not timeout.
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
		o.safeStop(svc) // must not propagate panic
	})

	t.Run("safeStop_calls_stop", func(t *testing.T) {
		o := New(Config{})
		svc := &testSvc{}
		o.safeStop(svc)
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
				// Block longer than the cron interval.
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

		// Debug and Info below threshold; Warn and Error at or above.
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
		ch <- logEntry{} // fill the only slot
		logger := newServiceLogger("test", ch)
		// Must not block (default case in select).
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
		m.Publish("nil broadcast") // no topics arg — broadcast
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
		// Fill channel (cap 16).
		for i := 0; i < 16; i++ {
			m.Publish(i, "t")
		}
		// This publish must not block.
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
		// Drain.
		for i := 0; i < 16; i++ {
			<-ch
		}
	})

	t.Run("subscribe_unsubscribe_not_found_path", func(t *testing.T) {
		m := newMessenger()
		_, unsub := m.Subscribe("t")
		// Clear subs to exercise the "channel not found" path
		// inside the sync.OnceValue unsubscribe closure.
		m.mu.Lock()
		m.subs = nil
		m.mu.Unlock()
		// Must not panic.
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
		// Simulate messengerDone cleanup.
		m.mu.Lock()
		m.subs = nil
		m.mu.Unlock()
		// Must not panic — range over nil map is a no-op.
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

		// Concurrent publishes and unsubscribes exercise RWMutex.
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
