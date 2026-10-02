package gorch

import (
	"context"
	"strings"
	"testing"
	"time"
)

// This file pins WaitFor: polling until a target status, timeout reporting,
// and the unknown-service error.

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
