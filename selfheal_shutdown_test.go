package gorch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// This file pins the shutdown half of handleServiceDone's self-heal path (#108):
// a self-heal instance released by orchestrator shutdown must not commit
// StatusStopped on its own. Stop() owns the terminal status through
// stopOneService (running→stopping→stopped) and the Stops accounting, so the
// observable sequence and Metrics().Stops must be identical whether Stop's loop
// reaches the entry before or after the instance's exit handler. The tests drive
// both orders explicitly (a dependency-parking blocker for handler-first, a
// Stop-released Start for teardown-first) rather than relying on scheduling.

// stateChangeRecorder collects OnStateChange transitions per service name,
// preserving order, so a test can assert an exact sequence instead of only an
// endpoint.
type stateChangeRecorder struct {
	mu  sync.Mutex
	seq map[string][]string
}

func newStateChangeRecorder() *stateChangeRecorder {
	return &stateChangeRecorder{seq: make(map[string][]string)}
}

func (r *stateChangeRecorder) record(name string, from, to ServiceStatus) {
	r.mu.Lock()
	r.seq[name] = append(r.seq[name], fmt.Sprintf("%s->%s", from, to))
	r.mu.Unlock()
}

func (r *stateChangeRecorder) changes(name string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seq[name]...)
}

// TestSelfHeal_Shutdown_StatusSequenceAndStopAccounting drives the two exit
// orders issue #108 calls out. In both, a self-heal service stopped by the
// whole-orchestrator Stop() must produce Running→Stopping→Stopped and count
// exactly one Stops increment for that service.
func TestSelfHeal_Shutdown_StatusSequenceAndStopAccounting(t *testing.T) {
	const victim = "victim"

	wantShutdown := []string{
		"registered->starting",
		"starting->running",
		"running->stopping",
		"stopping->stopped",
	}

	// teardown_first parks the instance inside Start until Stop's own sequence
	// calls its Stop(), so the teardown path deterministically reaches the entry
	// before the instance's exit handler.
	t.Run("teardown_first", func(t *testing.T) {
		rec := newStateChangeRecorder()
		o := New(WithHealthChecksDisabled(), WithOnStateChange(rec.record))

		release := make(chan struct{})
		var releaseOnce sync.Once
		newVictim := func() Service {
			return &testSvc{
				// Deliberately ignores context cancellation: only Stop() releases
				// it, which forces the teardown to enter beginStop first.
				startFn: func(context.Context) error { <-release; return context.Canceled },
				stopFn:  func() error { releaseOnce.Do(func() { close(release) }); return nil },
			}
		}
		if err := o.Register(newVictim(), WithName(victim),
			WithSelfHeal(newVictim),
			WithBackoff(ConstantBackoff{Delay: 0}),
		); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer o.Stop(2 * time.Second)

		waitForStatus(t, o, victim, StatusRunning)
		before := o.Metrics().Stops

		if err := o.Stop(2 * time.Second); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		assertStatusSequence(t, rec.changes(victim), wantShutdown)
		if got := o.Metrics().Stops - before; got != 1 {
			t.Errorf("Stops delta = %d, want 1 for a self-heal service stopped by Stop()", got)
		}
	})

	// handler_first lets the instance's exit handler win the race against Stop's
	// loop. A dependent blocker is stopped first (reverse topological order) and
	// parks in its Stop() until the victim's own exit handler has fully run; only
	// then does Stop's loop reach the victim. This makes the order deterministic.
	t.Run("handler_first", func(t *testing.T) {
		rec := newStateChangeRecorder()
		o := New(WithHealthChecksDisabled(), WithOnStateChange(rec.record))

		newVictim := func() Service {
			return &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
		}
		if err := o.Register(newVictim(), WithName(victim),
			WithSelfHeal(newVictim),
			WithBackoff(ConstantBackoff{Delay: 0}),
		); err != nil {
			t.Fatal(err)
		}

		var victimEntry *serviceEntry
		blocker := &testSvc{startFn: func(context.Context) error { return nil }}
		blocker.stopFn = func() error {
			<-victimEntry.getDone()
			return nil
		}
		if err := o.Register(blocker, WithName("blocker"), DependsOn(victim)); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer o.Stop(2 * time.Second)

		victimEntry = mustEntry(t, o, victim)
		waitForStatus(t, o, victim, StatusRunning)

		// The blocker exits cleanly on its own and its stop is accounted before
		// Stop() runs; wait for that so the baseline already includes it.
		waitForStatus(t, o, "blocker", StatusStopped)
		waitForCondition(t, 3*time.Second,
			func() bool { return o.Metrics().Stops == 1 },
			"the blocker's clean self-exit stop was not counted")
		before := o.Metrics().Stops

		if err := o.Stop(2 * time.Second); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		assertStatusSequence(t, rec.changes(victim), wantShutdown)
		if got := o.Metrics().Stops - before; got != 1 {
			t.Errorf("Stops delta = %d, want 1 for a self-heal service stopped by Stop()", got)
		}
	})
}

// TestSelfHeal_ShutdownAfterRestart pins the restart-path regression asked for by
// #108: a self-heal crash still yields Running→Crashed→Running and no Stop, and
// the live incarnation is then stopped exactly once by Stop() — including its
// Stopping→Stopped tail.
func TestSelfHeal_ShutdownAfterRestart(t *testing.T) {
	const victim = "victim"

	rec := newStateChangeRecorder()
	o := New(WithHealthChecksDisabled(), WithOnStateChange(rec.record))

	started := make(chan struct{}, 4)
	crash := make(chan struct{}, 1)
	gate := func() Service { return &gatedSvc{started: started, crash: crash, err: errors.New("boom")} }
	if err := o.Register(gate(), WithName(victim),
		WithSelfHeal(gate),
		WithBackoff(ConstantBackoff{Delay: 0}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer o.Stop(2 * time.Second)

	waitRecv(t, started) // incarnation 1 is live
	crash <- struct{}{}  // crash it; the factory supplies incarnation 2
	waitRecv(t, started) // incarnation 2 is live
	waitForStatus(t, o, victim, StatusRunning)

	if got := o.Metrics().Stops; got != 0 {
		t.Fatalf("Stops after a self-heal restart = %d, want 0: a restart is not a stop", got)
	}
	assertStatusSequence(t, rec.changes(victim), []string{
		"registered->starting",
		"starting->running",
		"running->crashed",
		"crashed->running",
	})

	before := o.Metrics().Stops
	if err := o.Stop(2 * time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	assertStatusSequence(t, rec.changes(victim), []string{
		"registered->starting",
		"starting->running",
		"running->crashed",
		"crashed->running",
		"running->stopping",
		"stopping->stopped",
	})
	if got := o.Metrics().Stops - before; got != 1 {
		t.Errorf("Stops delta = %d, want 1 for the live incarnation stopped by Stop()", got)
	}
}
