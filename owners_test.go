package gorch

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

// ownerMapsConsistentSnapshot returns a single consistent view of both owner
// maps: the root Messenger's live owner count and the summed live owner count
// across every registered entry, read under one root-lock hold.
//
// The root map and the entry maps are mutated by separate locks, and newOwner
// registers in the root before recording on the entry while every release path
// removes from the entry before draining the root. Reading the root count and
// the entry counts under separate locks therefore lets a sampler observe a
// half-applied newOwner — root already inserted, entry not yet recorded — which
// shows up as a spurious "root < entry" even though the implementation is
// correct (#72). Holding the root lock across both reads blocks registerOwner
// and drainOwner for the whole snapshot, so the transition cannot be sampled
// mid-flight. No owner path holds an entry lock while taking the root lock
// (releaseOwner and drainService release ownerMu before calling drainOwner), so
// this lock order cannot deadlock.
func ownerMapsConsistentSnapshot(o *Orchestrator) (root, total int) {
	o.mu.RLock()
	entries := make([]*serviceEntry, len(o.entries))
	copy(entries, o.entries)
	o.mu.RUnlock()

	m := o.messenger.root()
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, e := range entries {
		total += len(entryOwnerIDs(e))
	}
	return len(m.owners), total
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

// abandonStartSvc subscribes and then blocks in Start, ignoring context
// cancellation, so a failed-Start rollback abandons it with a live owner. Each
// instance signals the test just before returning, after the test has released
// it.
type abandonStartSvc struct {
	release  chan struct{}
	subs     chan (<-chan any)
	returned chan struct{}
}

func (s *abandonStartSvc) Start(ctx ServiceContext) error {
	ch, _ := ctx.Messenger.Subscribe("t")
	s.subs <- ch
	<-s.release
	s.returned <- struct{}{}
	return errors.New("abandoned instance exited")
}

func (s *abandonStartSvc) Stop() error { return nil }

// TestFailedStart_AbandonedInstance_OwnersDrained pins the failed-Start owner
// obligation: a service that ignores cancellation and outlives
// WithFailedStartTimeout never runs its deferred releaseOwner, yet after the
// failed Start both owner maps must be back to baseline even though the
// instance goroutine is still alive. Because a failed Start is retryable, the
// same failed Start is repeated: no attempt may accumulate an owner id or leave
// a subscription registered. The instances are then released to prove their own
// deferred release stays a harmless no-op on an already-drained owner.
func TestFailedStart_AbandonedInstance_OwnersDrained(t *testing.T) {
	const attempts = 3
	o := New(WithHealthChecksDisabled(), WithFailedStartTimeout(60*time.Millisecond))

	release := make(chan struct{})
	released := false
	subs := make(chan (<-chan any), attempts)
	returned := make(chan struct{}, attempts)
	stubborn := &abandonStartSvc{release: release, subs: subs, returned: returned}
	if err := o.Register(stubborn, WithName("stubborn")); err != nil {
		t.Fatal(err)
	}
	// The gate starts only after stubborn (its hard dependency) and fails before
	// its Start runs, forcing the rollback of the already-running stubborn.
	if err := o.Register(&testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
		WithName("gate"), DependsOn("stubborn"),
		WithOnBeforeStart(func(string) error { return errors.New("gate closed") })); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})

	entry := entryNamed(t, o, "stubborn")
	if got := rootOwnerCount(o); got != 0 {
		t.Fatalf("baseline root owners = %d, want 0", got)
	}

	for i := 0; i < attempts; i++ {
		err := o.Start()
		if !errors.Is(err, ErrStopTimeout) {
			t.Fatalf("attempt %d: Start = %v, want ErrStopTimeout from the abandoned-instance wait", i, err)
		}
		// The instance is still blocked, so its own deferred releaseOwner has not
		// run: only the rollback's drain can have cleared the maps.
		if n := len(returned); n != 0 {
			t.Fatalf("attempt %d: %d stubborn instance(s) returned; the rollback should have abandoned them", i, n)
		}
		if got := rootOwnerCount(o); got != 0 {
			t.Fatalf("attempt %d: root owners = %d after failed Start, want 0", i, got)
		}
		if ids := entryOwnerIDs(entry); len(ids) != 0 {
			t.Fatalf("attempt %d: entry owners = %v after failed Start, want none", i, ids)
		}
		// The drained owner's subscription must be dead: the abandoned instance
		// can neither receive a later publish nor resubscribe on its view.
		ch := <-subs
		o.messenger.Publish("v", "t")
		select {
		case _, ok := <-ch:
			if ok {
				t.Fatalf("attempt %d: the abandoned instance's subscription was not drained", i)
			}
		case <-time.After(time.Second):
			t.Fatalf("attempt %d: the drained subscription was neither closed nor delivered", i)
		}
	}

	// Releasing the instances lets their deferred releaseOwner run; it must be a
	// no-op that does not resurrect a drained owner.
	close(release)
	released = true
	for i := 0; i < attempts; i++ {
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Fatalf("abandoned instance %d did not unwind after release", i)
		}
	}
	if got := rootOwnerCount(o); got != 0 {
		t.Fatalf("root owners = %d after the abandoned instances unwound, want 0", got)
	}
	if ids := entryOwnerIDs(entry); len(ids) != 0 {
		t.Fatalf("entry owners = %v after the abandoned instances unwound, want none", ids)
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

// TestOwnerMapsConsistentSnapshot_DetectsStaleEntryOwner pins the detector the
// fuzz invariant is built on: the atomic snapshot must still report root < entry
// when an entry holds an id the root map never saw — the stale-entry state a
// release path that drained the root before the entry would leave. Without this,
// making the check race-free could have hollowed it out into one that no longer
// catches a genuine missed release.
func TestOwnerMapsConsistentSnapshot_DetectsStaleEntryOwner(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(&namedSvc{}, WithName("s")); err != nil {
		t.Fatal(err)
	}
	entry := entryNamed(t, o, "s")

	// No owners anywhere: the pair is consistent (0 >= 0).
	if root, total := ownerMapsConsistentSnapshot(o); root < total {
		t.Fatalf("empty snapshot = root %d, entry sum %d, want root >= entry", root, total)
	}

	// A matched owner on both maps is consistent and exactly one on each side.
	owner := o.newOwner(entry)
	if root, total := ownerMapsConsistentSnapshot(o); root != 1 || total != 1 {
		t.Fatalf("matched snapshot = root %d, entry sum %d, want 1 and 1", root, total)
	}

	// Record an id on the entry without registering it in the root: the stale
	// state a missed root release leaves. The snapshot must expose root < entry,
	// the condition the fuzz assertion turns into a failure.
	entry.addOwner(1 << 62)
	root, total := ownerMapsConsistentSnapshot(o)
	if root >= total {
		t.Fatalf("snapshot hid a stale entry owner: root %d, entry sum %d, want root < entry", root, total)
	}

	o.releaseOwner(entry, owner.id)
}

// TestOwnerMapsConsistentSnapshot_StableDuringOwnerChurn is the regression guard
// for #72. A goroutine continuously mints and releases owners — the
// root-then-entry insert of newOwner and the entry-then-root release of
// releaseOwner — while the test samples both maps through the snapshot. The old
// two-separate-lock check could read the root just before an insert and the
// entries just after it and fail spuriously; the snapshot holds the root lock
// across both reads, so no half-applied transition is observable and the
// invariant never reports a false positive.
func TestOwnerMapsConsistentSnapshot_StableDuringOwnerChurn(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	const entryCount = 16
	entries := make([]*serviceEntry, 0, entryCount)
	for i := 0; i < entryCount; i++ {
		name := fmt.Sprintf("s-%d", i)
		if err := o.Register(&namedSvc{}, WithName(name)); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entryNamed(t, o, name))
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 5000; i++ {
			e := entries[i%len(entries)]
			o.releaseOwner(e, o.newOwner(e).id)
		}
	}()

	close(start)
	for i := 0; i < 5000; i++ {
		if root, total := ownerMapsConsistentSnapshot(o); root < total {
			t.Fatalf("owner churn produced a false positive: root %d < entry sum %d", root, total)
		}
	}
	wg.Wait()

	// Once the churn goroutine has completed every cycle, both maps must be
	// empty again; the sampling above must not have perturbed the bookkeeping.
	root, total := ownerMapsConsistentSnapshot(o)
	if root != 0 || total != 0 {
		t.Fatalf("after churn: root %d, entry sum %d, want 0 and 0", root, total)
	}
}
