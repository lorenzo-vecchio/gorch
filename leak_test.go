package gorch

import (
	"testing"
	"time"
)

// This file holds the resource-assertion suite from issue #54. Every test that
// starts work ends with assertNoLeak, which checks the invariants a leak would
// violate: goroutines return to baseline, both owner maps drain, no entry is
// left reserved, and entries/nameIndex agree. The repeated-cycle tests exist
// because a leak that is invisible in one cycle is unbounded in a reload-heavy
// process.

// TestLeak_ReloadCycles_NoGrowth drives Register → StartService → Unregister N
// times in the same running orchestrator. A reload supervisor does exactly this,
// so a per-cycle leak in the owner maps or the name index would grow without
// bound; the test asserts each cycle returns to empty before the next starts.
func TestLeak_ReloadCycles_NoGrowth(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	base := captureLeakBaseline(o)
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	const cycles = 10
	for i := 0; i < cycles; i++ {
		svc := newRecordingService()
		if err := o.Register(svc, WithName("svc")); err != nil {
			t.Fatalf("cycle %d: Register: %v", i, err)
		}
		if err := o.StartService("svc"); err != nil {
			t.Fatalf("cycle %d: StartService: %v", i, err)
		}
		if err := o.Unregister("svc", 3*time.Second); err != nil {
			t.Fatalf("cycle %d: Unregister: %v", i, err)
		}
		if got := rootOwnerCount(o); got != 0 {
			t.Fatalf("cycle %d: root owner map = %d, want 0", i, got)
		}
		if got := totalEntryOwnerCount(o); got != 0 {
			t.Fatalf("cycle %d: entry owner maps = %d, want 0", i, got)
		}
	}

	if err := o.Stop(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	assertNoLeak(t, o, base)
}

// TestLeak_StartStopCycles_ContextsReleased re-starts one entry N times without
// removing it, asserting the per-instance teardown context is released every
// cycle (the v0.8.0 guarantee: child contexts must not accumulate under the
// orchestrator context across stop/start cycles).
func TestLeak_StartStopCycles_ContextsReleased(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	base := captureLeakBaseline(o)
	if err := o.Register(newRecordingService(), WithName("svc")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	const cycles = 20
	for i := 0; i < cycles; i++ {
		if err := o.StartService("svc"); err != nil {
			t.Fatalf("cycle %d: StartService: %v", i, err)
		}
		if err := o.StopService("svc", 3*time.Second); err != nil {
			t.Fatalf("cycle %d: StopService: %v", i, err)
		}
		if got := totalEntryOwnerCount(o); got != 0 {
			t.Fatalf("cycle %d: entry owner maps = %d, want 0", i, got)
		}
		e := mustEntry(t, o, "svc")
		if tc := e.getTeardown(); tc != nil {
			t.Fatalf("cycle %d: teardown context not released", i)
		}
	}

	if err := o.Stop(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	assertNoLeak(t, o, base)
}

// TestLeak_CronTicks_OwnerDrain runs several cron ticks whose Start subscribes
// and returns, then asserts every tick's owner was released: a completed tick
// must not leave a subscription behind, or a 1s schedule would leak an owner per
// second for the life of the process.
func TestLeak_CronTicks_OwnerDrain(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	base := captureLeakBaseline(o)

	svc := newRecordingService()
	svc.startFn = func(_ int, ctx ServiceContext) error {
		ctx.Messenger.Subscribe("tick")
		return nil
	}
	if err := o.Register(svc, WithName("svc"), WithCron("@every 1s", CronQueue)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("svc"); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = o.Stop(3 * time.Second) }()

	const ticks = 3
	// A tick's owner is released when the tick returns, so wait for the drain
	// rather than only for the ticks to have started: startCount reaches ticks
	// while the last tick may still hold its owner.
	waitForCondition(t, 6*time.Second, func() bool {
		return svc.startCount() >= ticks && rootOwnerCount(o) == 0 && totalEntryOwnerCount(o) == 0
	}, "three cron ticks to complete and drain their owners")
	if got := rootOwnerCount(o); got != 0 {
		t.Errorf("root owner map = %d after %d completed ticks, want 0", got, ticks)
	}
	if got := totalEntryOwnerCount(o); got != 0 {
		t.Errorf("entry owner maps = %d after %d completed ticks, want 0", got, ticks)
	}

	if err := o.Stop(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	assertNoLeak(t, o, base)
}
