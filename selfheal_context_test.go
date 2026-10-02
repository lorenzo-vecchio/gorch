package gorch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins the context-release half of handleServiceDone's self-heal path
// (#109): when an incarnation dies, the restart must cancel the per-service
// context it derived from the entry's teardown context. A self-heal entry keeps
// one teardown context for its whole start era (only StopService/Unregister/Stop
// cancel it), so an uncancelled instance context stays registered as one of its
// children until the entry is torn down — one retained context per restart,
// unbounded under a zero-backoff crash loop. The tests stash each incarnation's
// context from the factory and assert the dead ones are Done while the
// orchestrator is still running, without sleeping: the restart's own observable
// progress (the next context, the Restarts counter) is the synchronisation.

// contextRecordingFactory builds self-heal incarnations that record the
// ServiceContext they run under. Incarnations 1 through crashes fail Start with
// crashErr; every later incarnation blocks until its context is cancelled, so a
// test can observe a restart without racing an immediate re-crash. The recorded
// contexts let a test assert that a dead incarnation's context was released.
type contextRecordingFactory struct {
	crashErr error
	crashes  int
	created  atomic.Int32

	mu   sync.Mutex
	ctxs []context.Context
}

// new produces the next incarnation.
func (f *contextRecordingFactory) new() Service {
	id := int(f.created.Add(1))
	s := newRecordingService()
	s.startFn = func(_ int, sc ServiceContext) error {
		f.mu.Lock()
		f.ctxs = append(f.ctxs, sc.Context)
		f.mu.Unlock()
		if id <= f.crashes {
			return f.crashErr
		}
		<-sc.Done()
		return sc.Err()
	}
	return s
}

// count returns how many incarnations the factory has produced.
func (f *contextRecordingFactory) count() int { return int(f.created.Load()) }

// contextAt returns the context of the i-th incarnation (0-based) and whether it
// has started yet.
func (f *contextRecordingFactory) contextAt(i int) (context.Context, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i < 0 || i >= len(f.ctxs) {
		return nil, false
	}
	return f.ctxs[i], true
}

// contextDone reports whether ctx has been cancelled, without blocking.
func contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// TestSelfHeal_RestartCancelsDeadIncarnationContext is the regression #109 calls
// for: incarnation 1 crashes, incarnation 2 becomes live, and the dead
// incarnation's context is Done while the orchestrator is still running. It also
// pins that the release does not disturb the Restarts counter and does not
// cancel the entry's teardown context, which a later stop needs to abort the
// live incarnation.
func TestSelfHeal_RestartCancelsDeadIncarnationContext(t *testing.T) {
	f := &contextRecordingFactory{crashErr: errors.New("boom"), crashes: 1}

	o := New(WithHealthChecksDisabled())
	if err := o.Register(f.new(), WithName("svc"),
		WithSelfHeal(f.new),
		WithBackoff(ConstantBackoff{Delay: 0}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer o.Stop(2 * time.Second)

	waitForCondition(t, 3*time.Second, func() bool {
		_, second := f.contextAt(1)
		s, _ := o.Status("svc")
		return second && s == StatusRunning && o.Metrics().Restarts == 1
	}, "the self-heal restart to complete with a live second incarnation")

	firstCtx, ok := f.contextAt(0)
	if !ok {
		t.Fatal("incarnation 1's context was never recorded")
	}
	if !contextDone(firstCtx) {
		t.Error("incarnation 1's context was never cancelled: retained for the entry's lifetime")
	}

	if got := o.Metrics().Restarts; got != 1 {
		t.Errorf("Restarts = %d, want 1: releasing the dead context must not change the counter", got)
	}

	// The teardown context must survive the restart: it is the handle a later
	// StopService uses to abort the live incarnation.
	entry := mustEntry(t, o, "svc")
	if tc := entry.getTeardown(); tc == nil || tc.Err() != nil {
		t.Fatalf("teardown context = %v, want a live context after the restart", tc)
	}
}

// TestSelfHeal_RepeatedRestarts_AllDeadContextsCancelled drives N consecutive
// zero-backoff crashes, the unbounded case the issue describes, and asserts each
// dead incarnation's context was released while the next one ran, the live one
// is still live, and a later Stop returns nil.
func TestSelfHeal_RepeatedRestarts_AllDeadContextsCancelled(t *testing.T) {
	const restarts = 5
	f := &contextRecordingFactory{crashErr: errors.New("boom"), crashes: restarts}

	o := New(WithHealthChecksDisabled())
	if err := o.Register(f.new(), WithName("svc"),
		WithSelfHeal(f.new),
		WithBackoff(ConstantBackoff{Delay: 0}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitForCondition(t, 3*time.Second, func() bool {
		_, live := f.contextAt(restarts)
		return live && o.Metrics().Restarts == restarts
	}, "all consecutive restarts to complete")

	if got := f.count(); got != restarts+1 {
		t.Fatalf("incarnations = %d, want %d (no orphaned spawn)", got, restarts+1)
	}
	for i := 0; i < restarts; i++ {
		ctx, ok := f.contextAt(i)
		if !ok {
			t.Fatalf("incarnation %d's context was never recorded", i+1)
		}
		if !contextDone(ctx) {
			t.Errorf("incarnation %d's context was never cancelled: retained across the restart", i+1)
		}
	}
	live, _ := f.contextAt(restarts)
	if contextDone(live) {
		t.Error("the live incarnation's context was cancelled: a later stop could no longer abort it")
	}

	if got := o.Metrics().Restarts; got != restarts {
		t.Errorf("Restarts = %d, want %d", got, restarts)
	}

	if err := o.Stop(3 * time.Second); err != nil {
		t.Fatalf("Stop = %v, want nil after the released contexts", err)
	}
}

// TestSelfHeal_RestartKeepsTeardownAbortable is the other half of the contract:
// the restart cancels only the dead incarnation's context, never the entry's
// teardown context, so StopService can still abort the live incarnation.
func TestSelfHeal_RestartKeepsTeardownAbortable(t *testing.T) {
	f := &contextRecordingFactory{crashErr: errors.New("boom"), crashes: 1}

	o := New(WithHealthChecksDisabled())
	if err := o.Register(f.new(), WithName("svc"),
		WithSelfHeal(f.new),
		WithBackoff(ConstantBackoff{Delay: 0}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer o.Stop(2 * time.Second)

	waitForCondition(t, 3*time.Second, func() bool {
		_, live := f.contextAt(1)
		return live && o.Metrics().Restarts == 1
	}, "the self-heal restart to complete")

	entry := mustEntry(t, o, "svc")
	teardown := entry.getTeardown()
	if teardown == nil || teardown.Err() != nil {
		t.Fatalf("teardown context = %v, want live: the restart must not cancel it", teardown)
	}
	live, _ := f.contextAt(1)
	if contextDone(live) {
		t.Fatal("the live incarnation's context was cancelled by the restart")
	}

	if err := o.StopService("svc", 2*time.Second); err != nil {
		t.Fatalf("StopService = %v, want nil", err)
	}
	if teardown.Err() == nil {
		t.Error("StopService did not cancel the teardown context")
	}
	if !contextDone(live) {
		t.Error("the live incarnation's context was not aborted by StopService")
	}
	if s, _ := o.Status("svc"); s != StatusStopped {
		t.Errorf("status after StopService = %v, want StatusStopped", s)
	}
}
