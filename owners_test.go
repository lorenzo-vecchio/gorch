package gorch

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ── owner-map helpers ──
//
// The two owner maps are the serviceEntry's live-owner set (entry.owners,
// guarded by ownerMu) and the root Messenger's owner-state map
// (Messenger.owners, guarded by Messenger.mu). Tests read both to prove no
// allocation is ever missed by a release path.

// rootOwnerCount returns the number of live owner states in the root Messenger.
func rootOwnerCount(o *Orchestrator) int {
	m := o.messenger.root()
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.owners)
}

// rootHasOwner reports whether id is a live owner state in the root Messenger.
func rootHasOwner(o *Orchestrator, id uint64) bool {
	m := o.messenger.root()
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.owners[id]
	return ok
}

// entryOwnerIDs returns a copy of the entry's live owner ids without clearing
// them (takeOwners is the clearing variant).
func entryOwnerIDs(e *serviceEntry) []uint64 {
	e.ownerMu.Lock()
	defer e.ownerMu.Unlock()
	ids := make([]uint64, 0, len(e.owners))
	for id := range e.owners {
		ids = append(ids, id)
	}
	return ids
}

// totalEntryOwnerCount sums the live owner ids across every registered entry.
func totalEntryOwnerCount(o *Orchestrator) int {
	o.mu.RLock()
	entries := make([]*serviceEntry, len(o.entries))
	copy(entries, o.entries)
	o.mu.RUnlock()
	total := 0
	for _, e := range entries {
		total += len(entryOwnerIDs(e))
	}
	return total
}

// restartProbeSvc blocks in Start until the test crashes it (or the entry's
// context is cancelled), so a self-heal restart can be stepped deterministically.
type restartProbeSvc struct {
	started chan struct{}
	crash   chan struct{}
}

func (s *restartProbeSvc) Start(ctx ServiceContext) error {
	s.started <- struct{}{}
	select {
	case <-s.crash:
		return errors.New("boom")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *restartProbeSvc) Stop() error { return nil }

// tickSubSvc subscribes on every tick and returns immediately, so a completed
// tick's owner must be released before invokeCron returns.
type tickSubSvc struct{}

func (s *tickSubSvc) Start(ctx ServiceContext) error {
	ctx.Messenger.Subscribe("tick")
	return nil
}

func (s *tickSubSvc) Stop() error { return nil }

// stubbornTickSvc blocks in Start until released, ignoring context
// cancellation, so its cron tick can be abandoned by a stop deadline.
type stubbornTickSvc struct {
	started chan struct{}
	release chan struct{}
}

func (s *stubbornTickSvc) Start(ctx ServiceContext) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.release
	return nil
}

func (s *stubbornTickSvc) Stop() error { return nil }

// viewCaptureSvc hands its scoped Messenger to the test and then blocks until
// the entry is torn down, so a post-drain Subscribe can be attempted on the
// exact view the drained owner minted.
type viewCaptureSvc struct {
	view chan *Messenger
}

func (s *viewCaptureSvc) Start(ctx ServiceContext) error {
	s.view <- ctx.Messenger
	<-ctx.Done()
	return ctx.Err()
}

func (s *viewCaptureSvc) Stop() error { return nil }

// TestOwners_NoLeakAcrossRestarts drives N self-heal restarts and asserts the
// root owner map returns to its baseline after every restart: the crashed
// instance's owner is released before the replacement is allocated, so the
// monotonic ownerSeq cannot leak one id per crash.
func TestOwners_NoLeakAcrossRestarts(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	started := make(chan struct{})
	crash := make(chan struct{})
	newSvc := func() Service { return &restartProbeSvc{started: started, crash: crash} }
	if err := o.Register(newSvc(), WithName("s"),
		WithSelfHeal(newSvc),
		WithBackoff(ConstantBackoff{Delay: time.Millisecond}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	entry := entryNamed(t, o, "s")
	<-started // the first instance is live and owns one id
	baseline := rootOwnerCount(o)
	if baseline != 1 {
		t.Fatalf("baseline root owners = %d, want 1", baseline)
	}

	const restarts = 25
	for i := 0; i < restarts; i++ {
		crash <- struct{}{} // crash the live instance
		<-started           // wait for the self-heal replacement
		if got := rootOwnerCount(o); got != baseline {
			t.Fatalf("restart %d: root owners = %d, want baseline %d", i+1, got, baseline)
		}
		if ids := entryOwnerIDs(entry); len(ids) != 1 {
			t.Fatalf("restart %d: entry owners = %v, want exactly one live id", i+1, ids)
		}
	}
}

// TestOwners_NoLeakAcrossCronTicks drives N ticks in each cron mode and asserts
// both owner maps are back to baseline after each tick: every tick's owner is
// released by invokeCron's defer, so ticks never accumulate under one id.
func TestOwners_NoLeakAcrossCronTicks(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	modes := []struct {
		name string
		mode CronMode
	}{
		{"parallel", CronParallel},
		{"queue", CronQueue},
		{"skip", CronSkip},
	}
	for _, m := range modes {
		if err := o.Register(&tickSubSvc{}, WithName(m.name), WithCron("0 0 0 1 1 *", m.mode)); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	if got := rootOwnerCount(o); got != 0 {
		t.Fatalf("baseline root owners = %d, want 0", got)
	}

	const ticks = 20
	for _, m := range modes {
		entry := entryNamed(t, o, m.name)
		for i := 0; i < ticks; i++ {
			// invokeCron is synchronous here: on return its defer has released
			// the tick's owner, so both maps must already be back to baseline.
			o.invokeCron(entry, entry.cronGeneration())
			if got := rootOwnerCount(o); got != 0 {
				t.Fatalf("%s tick %d: root owners = %d, want 0", m.name, i, got)
			}
			if ids := entryOwnerIDs(entry); len(ids) != 0 {
				t.Fatalf("%s tick %d: entry owners = %v, want none", m.name, i, ids)
			}
		}
	}
}

// TestOwners_NoLeakAfterUnregister pins that Unregister releases every id of
// the removed entry from both maps, including an id held by an in-flight cron
// tick that its own deferred release cannot reach before the stop times out.
func TestOwners_NoLeakAfterUnregister(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	svc := &ownerSubSvc{ready: make(chan struct{}), got: make(chan any, 8)}
	tick := &stubbornTickSvc{started: make(chan struct{}, 1), release: make(chan struct{})}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(tick, WithName("c"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)
	<-svc.ready

	entry := entryNamed(t, o, "s")
	persistentID := entry.currentOwner()
	if persistentID == 0 || !rootHasOwner(o, persistentID) {
		t.Fatalf("running service owner %d is not registered in the root map", persistentID)
	}

	// Leave a cron tick in flight so Unregister must drain it through
	// drainService rather than rely on the tick's own deferred release.
	cronEntry := entryNamed(t, o, "c")
	go func() { o.invokeCron(cronEntry, cronEntry.cronGeneration()) }()
	<-tick.started
	cronID := cronEntry.currentOwner()
	if cronID == 0 || !rootHasOwner(o, cronID) {
		t.Fatalf("in-flight tick owner %d is not registered in the root map", cronID)
	}

	if err := o.Unregister("s", time.Second); err != nil {
		t.Fatalf("Unregister persistent: %v", err)
	}
	if rootHasOwner(o, persistentID) {
		t.Errorf("persistent owner %d survived Unregister in the root map", persistentID)
	}
	if ids := entryOwnerIDs(entry); len(ids) != 0 {
		t.Errorf("persistent entry owners = %v after Unregister, want none", ids)
	}

	// The blocked tick makes the cron stop time out; drainService must still
	// have released its owner.
	if err := o.Unregister("c", 50*time.Millisecond); !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("Unregister busy cron = %v, want ErrStopTimeout", err)
	}
	if rootHasOwner(o, cronID) {
		t.Errorf("cron tick owner %d survived Unregister in the root map", cronID)
	}
	if ids := entryOwnerIDs(cronEntry); len(ids) != 0 {
		t.Errorf("cron entry owners = %v after Unregister, want none", ids)
	}

	close(tick.release)
}

// TestCronTick_AbandonedMidFlight_OwnersDrained is the key regression guard: a
// cron tick whose Start blocks past the stop deadline is abandoned, so its
// deferred releaseOwner never runs. Teardown's drainService must be the safety
// net that still clears both owner maps. The tick is then released and its own
// deferred release must be a harmless no-op.
func TestCronTick_AbandonedMidFlight_OwnersDrained(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	tick := &stubbornTickSvc{started: make(chan struct{}, 1), release: make(chan struct{})}
	if err := o.Register(tick, WithName("c"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	entry := entryNamed(t, o, "c")
	tickDone := make(chan struct{})
	go func() {
		o.invokeCron(entry, entry.cronGeneration())
		close(tickDone)
	}()
	<-tick.started

	ownerID := entry.currentOwner()
	if ownerID == 0 || !rootHasOwner(o, ownerID) {
		t.Fatalf("in-flight tick owner %d is not registered in the root map", ownerID)
	}

	if err := o.StopService("c", 50*time.Millisecond); !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("StopService with a blocked tick = %v, want ErrStopTimeout", err)
	}
	// The tick is still blocked, proving it was abandoned — yet drainService
	// has already cleared both maps.
	select {
	case <-tickDone:
		t.Fatal("tick was expected to still be in flight (abandoned)")
	default:
	}
	if got := rootOwnerCount(o); got != 0 {
		t.Fatalf("root owners = %d after abandoned-tick teardown, want 0", got)
	}
	if ids := entryOwnerIDs(entry); len(ids) != 0 {
		t.Fatalf("entry owners = %v after abandoned-tick teardown, want none", ids)
	}

	close(tick.release)
	select {
	case <-tickDone:
	case <-time.After(time.Second):
		t.Fatal("abandoned tick did not unwind after release")
	}
	if got := rootOwnerCount(o); got != 0 {
		t.Fatalf("root owners = %d after the abandoned tick unwound, want 0", got)
	}
	if ids := entryOwnerIDs(entry); len(ids) != 0 {
		t.Fatalf("entry owners = %v after the abandoned tick unwound, want none", ids)
	}
}

// TestOwners_DrainedOwnerCannotResubscribe asserts, per owner, the v0.8.0
// guarantee: once an owner is drained its scoped view gets an already-closed
// channel for Subscribe and an error for Request, so a surviving goroutine
// cannot resurrect the owner's subscriptions.
func TestOwners_DrainedOwnerCannotResubscribe(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	svc := &viewCaptureSvc{view: make(chan *Messenger, 1)}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	view := <-svc.view
	// Keep the subscription registered so the owner's drain (not a manual
	// unsubscribe) is what closes the channel.
	ch, _ := view.Subscribe("t")
	o.messenger.Publish("v", "t")
	select {
	case got := <-ch:
		if got != "v" {
			t.Fatalf("live view got %v, want v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("live view did not receive its own subscription's message")
	}

	if err := o.Unregister("s", time.Second); err != nil {
		t.Fatalf("Unregister: %v", err)
	}

	if _, ok := <-ch; ok {
		t.Error("drained owner's channel should be closed")
	}
	dead, unsubDead := view.Subscribe("t")
	unsubDead()
	if _, ok := <-dead; ok {
		t.Error("post-drain Subscribe on a drained view should return an already-closed channel")
	}
	if _, err := view.Request(context.Background(), "x", "t"); err == nil {
		t.Error("post-drain Request on a drained view should fail")
	}

	_ = o.Stop(time.Second)
}
