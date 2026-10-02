package gorch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// This file pins panic recovery: a service that panics in Start is contained and
// surfaced to the caller as an error rather than unwinding the orchestrator.

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
