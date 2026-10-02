package gorch

import (
	"context"
	"testing"
	"time"
)

// This file pins the Done channel: it closes only once a Stop call completes
// (never before, not on a failed Start), is fresh after a failed-start retry,
// and closes on an all-Unregister shutdown without a Stop.

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

// assertDoneOpen fails if Done() has closed. It uses a non-blocking receive with
// no timeout, so a wrong implementation fails fast instead of hanging.
func assertDoneOpen(t *testing.T, done <-chan struct{}, when string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("Done() closed %s", when)
	default:
	}
}

// TestDone_DoesNotCloseBeforeStop pins that Done() is a shutdown-completed
// signal, not a raw WaitGroup drain. A custom logger is used because it leaves
// logPumpDone nil, which is the configuration in which the old
// sync.OnceValue-wrapping-o.wg.Wait Done goroutine closed the channel as soon as
// dynamic membership drained the counter while services were still live.
func TestDone_DoesNotCloseBeforeStop(t *testing.T) {
	t.Run("hot_add", func(t *testing.T) {
		o := New(WithHealthChecksDisabled(), WithLogger(&testLogger{}))
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		// Done is captured while the wait group is at zero (no services yet): the
		// old implementation closed the channel here at once. Give that goroutine
		// time to run so the hot add below is a real regression probe.
		done := o.Done()
		time.Sleep(20 * time.Millisecond)

		if err := o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("hot")); err != nil {
			t.Fatal(err)
		}
		if err := o.StartService("hot"); err != nil {
			t.Fatalf("StartService: %v", err)
		}
		waitForStatus(t, o, "hot", StatusRunning)
		assertDoneOpen(t, done, "while a hot-added service is live")

		if err := o.Stop(time.Second); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})

	t.Run("restart", func(t *testing.T) {
		o := New(WithHealthChecksDisabled(), WithLogger(&testLogger{}))
		if err := o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("svc")); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		waitForStatus(t, o, "svc", StatusRunning)
		done := o.Done()

		// StopService lets the only instance exit, draining o.wg to zero; the old
		// Done goroutine would close the channel in that gap.
		if err := o.StopService("svc", time.Second); err != nil {
			t.Fatalf("StopService: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
		assertDoneOpen(t, done, "after the only instance exited")

		// The restart feeds o.wg again; Done must still be open throughout.
		if err := o.StartService("svc"); err != nil {
			t.Fatalf("StartService: %v", err)
		}
		waitForStatus(t, o, "svc", StatusRunning)
		assertDoneOpen(t, done, "after a restart")

		if err := o.Stop(time.Second); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})
}

// TestDone_ClosesAfterStop pins the positive half of the contract: the channel
// stays open before Stop and closes once Stop has returned.
func TestDone_ClosesAfterStop(t *testing.T) {
	o := New(WithHealthChecksDisabled(), WithLogger(&testLogger{}))
	if err := o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("svc")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForStatus(t, o, "svc", StatusRunning)

	done := o.Done()
	assertDoneOpen(t, done, "before Stop")

	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Done() did not close after Stop returned")
	}
}

// TestDone_NotStarted_BehaviourIsDocumented pins the chosen not-started
// contract: Done() stays open until Stop is called, and a no-op Stop on an
// orchestrator that was never started closes it.
func TestDone_NotStarted_BehaviourIsDocumented(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	done := o.Done()
	assertDoneOpen(t, done, "before a no-op Stop on a never-started orchestrator")

	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("no-op Stop: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a no-op Stop on a never-started orchestrator must close Done()")
	}
}

// TestDone_FailedStartThenRetry_ChannelIsFresh pins that a failed Start — which
// never calls Stop — leaves Done() open, and that the same channel (no longer a
// cached o.wg.Wait snapshot) is still open after a successful retry and closes
// only at the eventual Stop.
func TestDone_FailedStartThenRetry_ChannelIsFresh(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	// Captured before any Start; under the old lazy OnceValue this channel was
	// closed immediately (o.wg was zero) and stayed closed across the retry.
	done := o.Done()

	contexts, retry := startWithHotAddFailure(t, o)
	// startWithHotAddFailure runs the failing Start itself and has already
	// returned by now; the first instance's context is queued.
	select {
	case <-contexts:
	case <-time.After(2 * time.Second):
		t.Fatal("hot never started before the failed Start")
	}
	assertDoneOpen(t, done, "after a failed Start")

	if err := retry(); err != nil {
		t.Fatalf("retry Start: %v", err)
	}
	waitForStatus(t, o, "hot", StatusRunning)
	assertDoneOpen(t, done, "after a successful retry")

	select {
	case <-contexts:
	case <-time.After(2 * time.Second):
		t.Fatal("hot did not restart on the retry")
	}

	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop after the retry: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Done() did not close after the eventual Stop")
	}
}

// TestDone_UnregisterAllWithoutStop pins that removing every service without
// calling Stop leaves Done() open: the log-pump and health loop are still
// running, and Done is a shutdown-completed signal. A custom logger is used so a
// wait-group drain at the last Unregister cannot close it by accident.
func TestDone_UnregisterAllWithoutStop(t *testing.T) {
	o := New(WithHealthChecksDisabled(), WithLogger(&testLogger{}))
	if err := o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForStatus(t, o, "s", StatusRunning)

	done := o.Done()
	if err := o.Unregister("s", time.Second); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	assertDoneOpen(t, done, "when the last service was unregistered without Stop")

	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Done() did not close after Stop")
	}
}
