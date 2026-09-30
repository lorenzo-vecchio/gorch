package gorch

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fuzzGroupSvc is a persistent service that counts every Start/Stop and whose
// before-start and before-stop hooks can be made to block. The blocking hooks
// let a fuzz iteration force two group operations to overlap inside exactly one
// operation's reservation window without a sleep, so the reservation-coherence
// and idempotence assertions are deterministic rather than racy.
type fuzzGroupSvc struct {
	starts atomic.Int32
	stops  atomic.Int32

	startHookCalls atomic.Int32
	stopHookCalls  atomic.Int32

	startEntered chan struct{} // closed when the before-start hook is entered
	startRelease chan struct{} // closed to let the before-start hook return
	startCalled  chan struct{} // closed when the service's Start is entered
	stopEntered  chan struct{} // closed when the before-stop hook is entered
	stopRelease  chan struct{} // closed to let the before-stop hook return

	startHookOnce sync.Once
	startCallOnce sync.Once
	stopHookOnce  sync.Once
}

func newFuzzGroupSvc() *fuzzGroupSvc {
	return &fuzzGroupSvc{
		startEntered: make(chan struct{}),
		startRelease: make(chan struct{}),
		startCalled:  make(chan struct{}),
		stopEntered:  make(chan struct{}),
		stopRelease:  make(chan struct{}),
	}
}

func (s *fuzzGroupSvc) Start(ctx ServiceContext) error {
	s.starts.Add(1)
	s.startCallOnce.Do(func() { close(s.startCalled) })
	// Park on the context so the instance stays live until a teardown cancels
	// it. StopGroup runs Stop() without cancelling the context (fuzzTrackSvc
	// documents the same asymmetry), so this one is released by the whole Stop.
	<-ctx.Done()
	return ctx.Err()
}

func (s *fuzzGroupSvc) Stop() error {
	s.stops.Add(1)
	return nil
}

// registerOptions wires the blocking hooks under name and group.
func (s *fuzzGroupSvc) registerOptions(name, group string) []RegisterOption {
	return []RegisterOption{
		WithName(name),
		WithGroup(group),
		WithOnBeforeStart(func(string) error {
			s.startHookCalls.Add(1)
			s.startHookOnce.Do(func() { close(s.startEntered) })
			<-s.startRelease
			return nil
		}),
		WithOnBeforeStop(func(string) error {
			s.stopHookCalls.Add(1)
			s.stopHookOnce.Do(func() { close(s.stopEntered) })
			<-s.stopRelease
			return nil
		}),
	}
}

// fuzzOrderSvc parks in Start until its context is cancelled and appends its
// name to a shared log in Stop, so a StartGroup rollback can be checked for
// reverse start order.
type fuzzOrderSvc struct {
	name  string
	order *fuzzStopLog
}

func (s *fuzzOrderSvc) Start(ctx ServiceContext) error { <-ctx.Done(); return ctx.Err() }

func (s *fuzzOrderSvc) Stop() error {
	s.order.record(s.name)
	return nil
}

type fuzzStopLog struct {
	mu   sync.Mutex
	seen []string
}

func (l *fuzzStopLog) record(name string) {
	l.mu.Lock()
	l.seen = append(l.seen, name)
	l.mu.Unlock()
}

func (l *fuzzStopLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.seen...)
}

// fuzzEntry returns the registered entry, failing the iteration if it is gone.
func fuzzEntry(t *testing.T, o *Orchestrator, name string) *serviceEntry {
	t.Helper()
	o.mu.RLock()
	e := o.nameIndex[name]
	o.mu.RUnlock()
	if e == nil {
		t.Fatalf("entry %s is not registered", name)
	}
	return e
}

// fuzzActive reports whether name is in a live lifecycle state.
func fuzzActive(o *Orchestrator, name string) bool {
	s, ok := o.Status(name)
	return ok && (s == StatusRunning || s == StatusStarting)
}

// fuzzAwait waits for a synchronization channel, failing on a bounded timeout.
// It is a safety net, never the synchronization itself.
func fuzzAwait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// FuzzGroupSurface drives the group surface — StartGroup, StopGroup and their
// collisions with per-member StartService/StopService — against a graph with two
// groups, a cross-group hard dependency, a runOnce gate, a cron entry and a soft
// dependency shared across groups. The issue's acceptance obligations are pinned
// by deterministic set-pieces that run on every input, then exercised by the
// generated alphabet:
//
//   - reservation coherence: every operation releases its own starting/removing
//     reservation and never clears another operation's (a leaked flag bricks the
//     entry);
//   - idempotence: a StartGroup/StopGroup overlapping another one starts/stops
//     each member and runs each hook exactly once;
//   - rollback: a StartGroup that fails mid-way stops the members it already
//     started in reverse start order and leaves none of them running;
//   - cross-group ordering: a StartService on a member whose hard dependency
//     lives in another group never succeeds while that dependency is down.
//     StartGroup deliberately ignores out-of-group edges (a group is a tag, so
//     its subset sort cannot order against what it will not visit), so the
//     dependency gate is pinned on StartService, which owns it;
//   - cron routing: a cron member is rescheduled by StartGroup, never run as a
//     persistent instance.
//
// The set-pieces make the target fail if StartGroup's rollback, its defer
// clearStarting, or StopGroup's defer removing.Store(false) is removed.
func FuzzGroupSurface(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	f.Add([]byte{9, 0, 1, 10, 11, 2, 3, 10})
	f.Add([]byte{10, 8, 0, 10, 1, 10, 9})
	f.Fuzz(func(t *testing.T, data []byte) {
		cronSvc := &fuzzCronSvc{}
		o := New(WithHealthChecksDisabled())
		// g1 holds a hard-dependency pair, a runOnce gate and a cron entry; g2
		// holds a member soft-depending on g1's b (a member shared across groups
		// through a soft edge). cgA/cgB split a hard dependency across cg1/cg2.
		_ = o.Register(&fuzzTrackSvc{}, WithName("a"), WithGroup("g1"))
		_ = o.Register(&fuzzTrackSvc{}, WithName("b"), WithGroup("g1"), DependsOn("a"))
		_ = o.Register(&fuzzGateSvc{}, WithName("gate"), WithGroup("g1"), WithRunOnce())
		_ = o.Register(cronSvc, WithName("cron"), WithGroup("g1"), WithCron("@every 1h", CronParallel))
		_ = o.Register(&fuzzTrackSvc{}, WithName("c"), WithGroup("g2"))
		_ = o.Register(&fuzzTrackSvc{}, WithName("d"), WithGroup("g2"), DependsOnSoft("b"))
		_ = o.Register(&fuzzTrackSvc{}, WithName("cgA"), WithGroup("cg1"))
		_ = o.Register(&fuzzTrackSvc{}, WithName("cgB"), WithGroup("cg2"), DependsOn("cgA"))
		if err := o.Start(); err != nil {
			t.Fatalf("Start on a legal graph = %v, want nil", err)
		}

		// assertNoReservationLeak pins the reservation lifecycle for both flags.
		// Once every membership op has returned, no entry may still be flagged
		// starting or removing: a leaked removal is what leaves an entry stuck in
		// StatusStopping and rejects every later membership op.
		assertNoReservationLeak := func() {
			o.mu.RLock()
			entries := make([]*serviceEntry, len(o.entries))
			copy(entries, o.entries)
			o.mu.RUnlock()
			for _, e := range entries {
				if e.starting.Load() {
					t.Fatalf("entry %s kept a start reservation", e.name)
				}
				if e.removing.Load() {
					t.Fatalf("entry %s kept a removing reservation", e.name)
				}
			}
		}

		fuzzGroupCronReschedule(t, o, cronSvc)
		fuzzGroupCrossGroupDependency(t, o)
		fuzzGroupRollback(t, o)
		fuzzGroupStartReservationCoherence(t, o)
		fuzzGroupStopReservationCoherence(t, o)
		fuzzGroupStopGroupReleasesReservation(t, o)

		for _, b := range data {
			switch b % 12 {
			case 0, 9:
				// StartGroup may have to reschedule a cron entry stopped by 8, or
				// restart a/b stopped individually.
				_ = o.StartGroup("g1")
			case 1:
				_ = o.StopGroup("g1", 50*time.Millisecond)
			case 2:
				_ = o.StartGroup("g2")
			case 3:
				_ = o.StopGroup("g2", 50*time.Millisecond)
			case 4:
				_ = o.StartService("a")
			case 5:
				_ = o.StopService("a", 50*time.Millisecond)
			case 6:
				_ = o.StartService("b")
			case 7:
				_ = o.StopService("b", 50*time.Millisecond)
			case 8:
				_ = o.StopService("cron", 50*time.Millisecond)
			case 10:
				fuzzGroupConcurrentBurst(o)
			case 11:
				_ = o.StopService("c", 50*time.Millisecond)
			}
			assertNoReservationLeak()
		}

		_ = o.Stop(2 * time.Second)
		assertNoReservationLeak()
		select {
		case <-o.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("goroutines did not wind down after Stop")
		}
	})
}

// fuzzGroupCronReschedule pins that a cron member stopped individually is
// rescheduled by StartGroup — status Running and a live schedule — and is not
// run as a persistent instance. Before the fix StartGroup sent every member
// through startOneService, so it launched the cron service's Start continuously
// and left the schedule uninstalled.
func fuzzGroupCronReschedule(t *testing.T, o *Orchestrator, cron *fuzzCronSvc) {
	t.Helper()
	if err := o.StopService("cron", time.Second); err != nil {
		t.Fatalf("StopService(cron) = %v, want nil", err)
	}
	if err := o.StartGroup("g1"); err != nil {
		t.Fatalf("StartGroup(g1) = %v, want nil", err)
	}
	if s, _ := o.Status("cron"); s != StatusRunning {
		t.Fatalf("cron status after StartGroup = %v, want StatusRunning", s)
	}
	if e := fuzzEntry(t, o, "cron"); e.cronID == 0 {
		t.Fatal("StartGroup left the cron member unscheduled")
	}
	if v := cron.running.Load(); v != 0 {
		t.Fatalf("StartGroup ran the cron member as a persistent instance: %d live", v)
	}
}

// fuzzGroupCrossGroupDependency pins that a member whose hard dependency lives
// in another group is never started while that dependency is down: StartService
// returns the documented ErrDependencyNotRunning instead of a silent success.
func fuzzGroupCrossGroupDependency(t *testing.T, o *Orchestrator) {
	t.Helper()
	for _, name := range []string{"cgB", "cgA"} {
		if err := o.StopService(name, time.Second); err != nil {
			t.Fatalf("StopService(%s) = %v, want nil", name, err)
		}
	}
	if err := o.StartService("cgB"); !errors.Is(err, ErrDependencyNotRunning) {
		t.Fatalf("StartService(cgB) with its cross-group hard dependency stopped = %v, want ErrDependencyNotRunning", err)
	}
	if fuzzActive(o, "cgB") {
		t.Fatal("cgB started despite its cross-group hard dependency not running")
	}
	if err := o.StartService("cgA"); err != nil {
		t.Fatalf("StartService(cgA) = %v, want nil", err)
	}
	if err := o.StartService("cgB"); err != nil {
		t.Fatalf("StartService(cgB) after its dependency started = %v, want nil", err)
	}
	if !fuzzActive(o, "cgB") {
		t.Fatal("cgB did not start after its cross-group hard dependency started")
	}
}

// fuzzGroupRollback pins the v0.9.0 guarantee: a StartGroup that fails mid-way
// stops the members it already started in reverse start order, leaves none of
// them running, and releases the reservation of a member that was selected but
// never reached (the defer clearStarting).
func fuzzGroupRollback(t *testing.T, o *Orchestrator) {
	t.Helper()
	log := &fuzzStopLog{}
	_ = o.Register(&fuzzOrderSvc{name: "rb0", order: log}, WithName("rb0"), WithGroup("roll"))
	_ = o.Register(&fuzzOrderSvc{name: "rb1", order: log}, WithName("rb1"), WithGroup("roll"))
	// rbBad is a runOnce gate that fails synchronously, so StartGroup aborts
	// after both later-level members have started. rbAfter depends on rbBad, so
	// it is reserved but never reached: only the defer clears its flag.
	_ = o.Register(&errSvc{err: errors.New("group boom")},
		WithName("rbBad"), WithGroup("roll"), WithRunOnce(), DependsOn("rb0", "rb1"))
	_ = o.Register(&fuzzTrackSvc{}, WithName("rbAfter"), WithGroup("roll"), DependsOn("rbBad"))

	if err := o.StartGroup("roll"); err == nil {
		t.Fatal("StartGroup(roll) = nil, want the mid-group failure")
	}
	if got := log.snapshot(); !slices.Equal(got, []string{"rb1", "rb0"}) {
		t.Fatalf("rollback stop order = %v, want [rb1 rb0] (reverse start order)", got)
	}
	for _, name := range []string{"rb0", "rb1", "rbAfter"} {
		if fuzzActive(o, name) {
			t.Fatalf("rolled-back group member %s is still active", name)
		}
	}
	if fuzzEntry(t, o, "rbAfter").starting.Load() {
		t.Fatal("rbAfter kept its start reservation: StartGroup's clearStarting did not run")
	}
}

// fuzzGroupStartReservationCoherence pins that an in-flight StartGroup keeps its
// start reservation against a concurrent StartGroup and StopGroup on the same
// group: neither takes it nor clears it, no second instance starts, and no hook
// runs twice.
func fuzzGroupStartReservationCoherence(t *testing.T, o *Orchestrator) {
	t.Helper()
	ov := newFuzzGroupSvc()
	// This member is never stopped by the set-piece, but the whole-orchestrator
	// Stop's before-stop hook must not block on stopRelease.
	close(ov.stopRelease)
	if err := o.Register(ov, ov.registerOptions("ov", "ov")...); err != nil {
		t.Fatalf("Register(ov) = %v, want nil", err)
	}
	startDone := make(chan error, 1)
	go func() { startDone <- o.StartGroup("ov") }()
	fuzzAwait(t, ov.startEntered, "ov before-start hook")

	// While the first StartGroup holds the reservation, neither op may take it.
	if err := o.StartGroup("ov"); err != nil {
		t.Fatalf("a second StartGroup beside a held reservation = %v, want nil", err)
	}
	if err := o.StopGroup("ov", time.Second); err != nil {
		t.Fatalf("StopGroup beside a held start reservation = %v, want nil", err)
	}
	if !fuzzEntry(t, o, "ov").starting.Load() {
		t.Fatal("a concurrent group op cleared the in-flight start reservation")
	}
	if n := ov.startHookCalls.Load(); n != 1 {
		t.Fatalf("before-start hook ran %d times beside a reservation, want 1", n)
	}

	close(ov.startRelease)
	if err := <-startDone; err != nil {
		t.Fatalf("StartGroup(ov) = %v, want nil", err)
	}
	fuzzAwait(t, ov.startCalled, "ov Start")
	if fuzzEntry(t, o, "ov").starting.Load() {
		t.Fatal("StartGroup left its start reservation set")
	}
	if n := ov.starts.Load(); n != 1 {
		t.Fatalf("ov started %d times, want 1 (a group start is idempotent)", n)
	}
	if n := ov.stops.Load(); n != 0 {
		t.Fatalf("a StopGroup beside the reservation stopped ov %d times, want 0", n)
	}
}

// fuzzGroupStopReservationCoherence pins the mirror of the start case: an
// in-flight StopGroup keeps its removing reservation against a concurrent
// StartGroup and StopGroup, so the member is not restarted and no hook runs
// twice.
func fuzzGroupStopReservationCoherence(t *testing.T, o *Orchestrator) {
	t.Helper()
	ov := newFuzzGroupSvc()
	close(ov.startRelease) // starting it must not block here
	if err := o.Register(ov, ov.registerOptions("ov2", "ov2")...); err != nil {
		t.Fatalf("Register(ov2) = %v, want nil", err)
	}
	if err := o.StartService("ov2"); err != nil {
		t.Fatalf("StartService(ov2) = %v, want nil", err)
	}
	fuzzAwait(t, ov.startCalled, "ov2 Start")

	stopDone := make(chan error, 1)
	go func() { stopDone <- o.StopGroup("ov2", time.Second) }()
	fuzzAwait(t, ov.stopEntered, "ov2 before-stop hook")

	e := fuzzEntry(t, o, "ov2")
	if !e.removing.Load() {
		t.Fatal("the stop reservation was not held during the before-stop hook")
	}
	if err := o.StartGroup("ov2"); err != nil {
		t.Fatalf("StartGroup beside a held stop reservation = %v, want nil", err)
	}
	if err := o.StopGroup("ov2", time.Second); err != nil {
		t.Fatalf("a second StopGroup beside a held stop reservation = %v, want nil", err)
	}
	if !e.removing.Load() {
		t.Fatal("a concurrent group op cleared the in-flight stop reservation")
	}
	if n := ov.stopHookCalls.Load(); n != 1 {
		t.Fatalf("before-stop hook ran %d times beside a reservation, want 1", n)
	}
	if n := ov.starts.Load(); n != 1 {
		t.Fatalf("a StartGroup beside the stop reservation started ov2 %d times, want 1", n)
	}

	close(ov.stopRelease)
	if err := <-stopDone; err != nil {
		t.Fatalf("StopGroup(ov2) = %v, want nil", err)
	}
	if e.removing.Load() {
		t.Fatal("StopGroup left its stop reservation set")
	}
	if fuzzActive(o, "ov2") {
		t.Fatal("StopGroup returned nil but ov2 is still active")
	}
}

// fuzzGroupStopGroupReleasesReservation pins that StopGroup clears its own
// removing reservation: a leaked one would reject every later membership op on
// the member, the "bricked orchestrator" failure mode. The stop sequence never
// clears it, so only the defer can.
func fuzzGroupStopGroupReleasesReservation(t *testing.T, o *Orchestrator) {
	t.Helper()
	rm := &fuzzTrackSvc{}
	if err := o.Register(rm, WithName("rm0"), WithGroup("rm")); err != nil {
		t.Fatalf("Register(rm0) = %v, want nil", err)
	}
	if err := o.StartGroup("rm"); err != nil {
		t.Fatalf("StartGroup(rm) = %v, want nil", err)
	}
	if err := o.StopGroup("rm", time.Second); err != nil {
		t.Fatalf("StopGroup(rm) = %v, want nil", err)
	}
	e := fuzzEntry(t, o, "rm0")
	if e.removing.Load() {
		t.Fatal("StopGroup left a removing reservation set: the entry is bricked")
	}
	if fuzzActive(o, "rm0") {
		t.Fatal("StopGroup returned nil but rm0 is still active")
	}
}

// fuzzGroupConcurrentBurst hammers one group set from several goroutines at once
// — the mixed StartGroup/StopGroup/StartService/StopService shape the alphabet
// alone cannot interleave. It only requires that every op returns and releases
// its reservation; the caller re-checks the flags afterwards.
func fuzzGroupConcurrentBurst(o *Orchestrator) {
	ops := []func() error{
		func() error { return o.StartGroup("g1") },
		func() error { return o.StopGroup("g1", 50*time.Millisecond) },
		func() error { return o.StartGroup("g2") },
		func() error { return o.StopGroup("g2", 50*time.Millisecond) },
		func() error { return o.StartService("a") },
		func() error { return o.StopService("a", 50*time.Millisecond) },
		func() error { return o.StartService("c") },
		func() error { return o.StopService("c", 50*time.Millisecond) },
	}
	var wg sync.WaitGroup
	wg.Add(len(ops))
	for _, op := range ops {
		op := op
		go func() {
			defer wg.Done()
			_ = op()
		}()
	}
	wg.Wait()
}
