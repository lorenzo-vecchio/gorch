package gorch

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins cron runtime behaviour: the Skip/Queue concurrency modes and
// the status lifecycle of a scheduled entry.

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
