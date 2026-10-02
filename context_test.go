package gorch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// This file pins orchestrator-context cancellation: every running service
// observes cancellation when the orchestrator stops.

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
