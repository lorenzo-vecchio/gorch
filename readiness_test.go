package gorch

import (
	"context"
	"errors"
	"testing"
	"time"
)

// This file pins IsReady: the running-and-hard-dependencies-satisfied
// condition and the ReadinessChecker probe.

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
