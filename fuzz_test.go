package gorch

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fuzzPayload is the decode target for FuzzTypedSubscribeDecode. It only needs
// to be a gob-decodable struct; the point is the byte path, not the type.
type fuzzPayload struct {
	A int
	B string
}

// FuzzTypedSubscribeDecode feeds arbitrary bytes through the Messenger's typed
// decode path (TypedSubscribe), the one surface that consumes bytes published
// by arbitrary services. It asserts the decoder never panics on malformed input.
func FuzzTypedSubscribeDecode(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte{})
	f.Add([]byte{0x0e, 0x00, 0x01, 0xff})
	f.Fuzz(func(t *testing.T, payload []byte) {
		m := newMessenger()
		ch, _ := TypedSubscribe[fuzzPayload](m, "fuzz")
		m.Publish(Message{Payload: payload, TypeName: "fuzz"}, "fuzz")
		m.Drain() // close raw channel so the decode goroutine exits
		for range ch {
		}
	})
}

// fuzzCronSvc blocks in a cron tick until cancelled and counts the ticks
// currently in flight, so the fuzz can assert that none survives a stop.
type fuzzCronSvc struct {
	running atomic.Int32
}

func (s *fuzzCronSvc) Start(ctx ServiceContext) error {
	s.running.Add(1)
	<-ctx.Done()
	s.running.Add(-1)
	return ctx.Err()
}

func (s *fuzzCronSvc) Stop() error { return nil }

// fuzzStubbornSvc ignores context cancellation and blocks in Start until
// released, while Stop() returns immediately. It is the service that exposes a
// dishonest status: a stop that times out with the instance goroutine still live
// must never be reported stopped.
type fuzzStubbornSvc struct {
	release chan struct{}
	running atomic.Int32
}

func (s *fuzzStubbornSvc) Start(ctx ServiceContext) error {
	s.running.Add(1)
	<-s.release
	s.running.Add(-1)
	return nil
}

func (s *fuzzStubbornSvc) Stop() error { return nil }

// fuzzTrackSvc records how many times each instance was started and stopped, so
// the fuzz can assert the stop obligation: an entry asked to stop must actually
// have had Stop() called. Unlike a service that parks only on the context, Stop
// releases the current Start through a per-instance channel, so the instance
// genuinely exits on every stop path — including StopGroup, which runs Stop()
// without cancelling the entry's context. starts and stops are per-entry
// counters across restarts: a restart is a second Start, its stop a second Stop.
type fuzzTrackSvc struct {
	starts atomic.Int32
	stops  atomic.Int32

	mu     sync.Mutex
	stopCh chan struct{}
}

func (s *fuzzTrackSvc) Start(ctx ServiceContext) error {
	ch := make(chan struct{})
	s.mu.Lock()
	s.starts.Add(1)
	s.stopCh = ch
	s.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-ch:
	}
	return ctx.Err()
}

func (s *fuzzTrackSvc) Stop() error {
	s.mu.Lock()
	s.stops.Add(1)
	if s.stopCh != nil {
		close(s.stopCh)
		s.stopCh = nil
	}
	s.mu.Unlock()
	return nil
}

// fuzzGateSvc is a runOnce gate that succeeds immediately. A persistent service
// hard-depending on it is legal, and it puts that gate outside the persistent
// subset that Start and Stop sort — the same out-of-subset shape as a group
// member depending on a service in another group.
type fuzzGateSvc struct{}

func (s *fuzzGateSvc) Start(ServiceContext) error { return nil }
func (s *fuzzGateSvc) Stop() error                { return nil }

// FuzzMembershipTransitions drives a random add/start/stop/remove/crash/restart
// sequence against a small dependency graph. Besides asserting no panic or
// deadlock and no goroutine leak, it checks semantic properties grep-style
// invariants miss: a successful stop leaves an honest status, never counts one
// stop twice, no cron tick survives a teardown, and a group stop both reports no
// phantom cycle and actually stops every running member. The group "g" holds b
// and d, and b hard-depends on a outside the group, so the topoSort subset bug
// (counting a dependency that the operation will not visit as a cycle) has to
// fail these obligations.
func FuzzMembershipTransitions(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{5, 5, 4, 3, 2, 1, 0, 2, 2, 0})
	// 8 fires a cron tick at teardown; 9 stops group "g" (b's dependency is
	// outside it); 10 restarts the group. The graph also always carries the
	// self-heal crash (c) and the runOnce gate with its persistent dependent.
	f.Add([]byte{8})
	f.Add([]byte{9})
	f.Add([]byte{10, 9})
	f.Add([]byte{0, 9, 10, 9, 2})
	f.Fuzz(func(t *testing.T, data []byte) {
		// A self-heal crash must be observable on every surface at once: each
		// OnCrash call and each Running→Crashed transition has a matching Crashes
		// increment, however the membership ops interleave. Count them atomically
		// and reconcile at the end, once every goroutine has wound down.
		var onCrash, crashTransitions atomic.Int64
		o := New(
			WithHealthChecksDisabled(),
			WithOnCrash(func(string, error) { onCrash.Add(1) }),
			WithOnStateChange(func(_ string, _, to ServiceStatus) {
				if to == StatusCrashed {
					crashTransitions.Add(1)
				}
			}),
		)
		_ = o.Register(&namedSvc{}, WithName("a"))
		// b and d form group "g"; b's hard dependency a is outside it, so the
		// group is a legal subset whose ordering must ignore the out-of-group
		// edge. Both are instrumented so the stop obligation is checkable.
		bSvc := &fuzzTrackSvc{}
		_ = o.Register(bSvc, WithName("b"), DependsOn("a"), WithGroup("g"))
		dSvc := &fuzzTrackSvc{}
		_ = o.Register(dSvc, WithName("d"), DependsOn("b"), WithGroup("g"))
		_ = o.Register(&crashSignalSvc{sig: make(chan struct{})}, WithName("c"),
			DependsOn("b"),
			WithSelfHeal(func() Service { return &crashSignalSvc{sig: make(chan struct{})} }),
			WithBackoff(ConstantBackoff{Delay: 200 * time.Millisecond}),
		)
		// A runOnce gate with a persistent hard dependent: the gate is outside
		// the persistent subset that Start and Stop sort, the shape that used to
		// be mistaken for a cycle on those paths.
		_ = o.Register(&fuzzGateSvc{}, WithName("gate"), WithRunOnce())
		_ = o.Register(&namedSvc{}, WithName("gdep"), DependsOn("gate"))
		cronSvc := &fuzzCronSvc{}
		_ = o.Register(cronSvc, WithName("cron"), WithCron("* * * * * *", CronParallel))
		// Start must succeed on this legal graph. It is the first obligation a
		// subset-ordering bug breaks: the persistent set excludes the runOnce
		// gate gdep depends on, and treating that out-of-set edge as a cycle
		// would fail the whole start.
		if err := o.Start(); err != nil {
			t.Fatalf("Start on a legal graph = %v, want nil", err)
		}
		tracked := map[string]*fuzzTrackSvc{"b": bSvc, "d": dSvc}

		// assertNoReservationLeak pins the reservation lifecycle: once every
		// membership op of an iteration has returned, no entry may still be
		// flagged starting. A leaked reservation is what would leave an entry
		// stuck in StatusStarting and reject every later membership op.
		assertNoReservationLeak := func() {
			o.mu.RLock()
			entries := make([]*serviceEntry, len(o.entries))
			copy(entries, o.entries)
			o.mu.RUnlock()
			for _, e := range entries {
				if e.starting.Load() {
					t.Fatalf("entry %s left a start reservation in flight", e.name)
				}
			}
		}

		// assertOwnerMapsConsistent pins the two owner maps together. newOwner
		// registers in the root before recording on the entry, and every release
		// path removes from the entry before draining the root, so at any instant
		// the root map holds at least as many owners as the entries collectively
		// claim. A root count below that sum would mean an entry references an id
		// the root already forgot: a stale "dead" view that could reject a later
		// Subscribe. Both counts come from one atomic snapshot so a half-applied
		// newOwner in the window between the two maps is never observed as a
		// false positive.
		assertOwnerMapsConsistent := func() {
			root, total := ownerMapsConsistentSnapshot(o)
			if root < total {
				t.Fatalf("root owner count %d < entry owner sum %d: an entry references an id the root map lost", root, total)
			}
		}

		// runOp drives one operation and asserts the abandoned-goroutine counter
		// did not move when the operation completed without an overrun. A leaked
		// teardown goroutine must only ever follow ErrStopTimeout/ErrHookTimeout,
		// never the happy path; a non-timeout error (busy, not found, ...) is
		// still held to that bar.
		runOp := func(op func() error) error {
			before := o.Metrics().AbandonedGoroutines
			err := op()
			if !errors.Is(err, ErrStopTimeout) && !errors.Is(err, ErrHookTimeout) {
				if after := o.Metrics().AbandonedGoroutines; after != before {
					t.Fatalf("abandoned-goroutine count grew from %d to %d without a timeout error: %v", before, after, err)
				}
			}
			return err
		}

		// stopChecked runs a stop op and, when it succeeds, asserts the public
		// status is honest and the Stops metric moved by at most maxDelta, so a
		// teardown/done race cannot double-count one stop. A refused op that is
		// not a timeout (busy, not found, reentrant) tears nothing down, so it
		// must leave Stops unchanged; only a timeout can have partially stopped.
		stopChecked := func(name string, maxDelta int64, stop func() error) {
			before := o.Metrics().Stops
			err := runOp(stop)
			if err != nil {
				if !errors.Is(err, ErrStopTimeout) && !errors.Is(err, ErrHookTimeout) {
					if delta := o.Metrics().Stops - before; delta != 0 {
						t.Fatalf("refused stop %s moved Stops by %d: %v", name, delta, err)
					}
				}
				return
			}
			if delta := o.Metrics().Stops - before; delta > maxDelta {
				t.Fatalf("stop %s counted %d stops, want <= %d", name, delta, maxDelta)
			}
			if s, ok := o.Status(name); ok && (s == StatusRunning || s == StatusStarting) {
				t.Fatalf("stop %s returned nil but status is %v", name, s)
			}
		}

		// stopActiveChecked pins the exact Stops accounting for an entry that
		// cannot restart in the background (a and b have no factory), so "Stops
		// equals the number of completed caller-initiated stops" is checkable: a
		// successful stop of an active entry adds exactly one, a no-op stop of an
		// inactive entry adds none, and a refused op adds none.
		stopActiveChecked := func(name string, stop func() error) {
			active := false
			if s, ok := o.Status(name); ok && (s == StatusRunning || s == StatusStarting) {
				active = true
			}
			before := o.Metrics().Stops
			if err := runOp(stop); err != nil {
				if !errors.Is(err, ErrStopTimeout) && !errors.Is(err, ErrHookTimeout) {
					if delta := o.Metrics().Stops - before; delta != 0 {
						t.Fatalf("refused stop %s moved Stops by %d: %v", name, delta, err)
					}
				}
				return
			}
			want := int64(0)
			if active {
				want = 1
			}
			if delta := o.Metrics().Stops - before; delta != want {
				t.Fatalf("stop %s moved Stops by %d, want %d (active=%v)", name, delta, want, active)
			}
			if s, ok := o.Status(name); ok && (s == StatusRunning || s == StatusStarting) {
				t.Fatalf("stop %s returned nil but status is %v", name, s)
			}
		}

		// isActive reports whether name is in a live lifecycle state.
		isActive := func(name string) bool {
			s, ok := o.Status(name)
			return ok && (s == StatusRunning || s == StatusStarting)
		}

		// stopGroupChecked drives a whole-group stop and asserts the two group
		// obligations at once. (1) A legal group must stop without reporting a
		// hard dependency outside it as a cycle. (2) Every member that was
		// running must actually have had Stop() called and must no longer report
		// a live status. Both are checked unconditionally: the original topoSort
		// bug discarded the cycle error and stopped nothing, and the fallback fix
		// still returns the error, so only holding the operation to both bars
		// catches either form.
		stopGroupChecked := func() {
			type beforeState struct {
				active bool
				stops  int32
			}
			before := make(map[string]beforeState, len(tracked))
			for name, s := range tracked {
				before[name] = beforeState{active: isActive(name), stops: s.stops.Load()}
			}
			if err := o.StopGroup("g", 50*time.Millisecond); err != nil {
				t.Fatalf("StopGroup stopped a legal group but returned %v", err)
			}
			for name, s := range tracked {
				b := before[name]
				if !b.active {
					continue
				}
				if s.stops.Load() == b.stops {
					t.Fatalf("StopGroup left running member %s without calling Stop()", name)
				}
				if isActive(name) {
					t.Fatalf("StopGroup returned nil but member %s is still live", name)
				}
			}
		}

		var prev Metrics
		const maxLate = 8
		late := 0
		for _, b := range data {
			switch b % 11 {
			case 0:
				_ = runOp(func() error { return o.StartService("a") })
			case 1:
				stopActiveChecked("a", func() error { return o.StopService("a", 50*time.Millisecond) })
			case 2:
				// Cascade stops b plus its hard dependents c and d (and a
				// background self-heal of c may add one more).
				stopChecked("b", 4, func() error { return o.StopService("b", 50*time.Millisecond, WithCascadeStop()) })
			case 3:
				before := o.Metrics().Stops
				if err := runOp(func() error { return o.Unregister("c", 50*time.Millisecond) }); err == nil {
					if _, ok := o.Status("c"); ok {
						t.Fatal("Unregister returned nil but c is still registered")
					}
					// c self-heals, so a background restart may account a stop in
					// the same window; a systematic double count would add more.
					if o.Metrics().Stops-before > 2 {
						t.Fatal("Unregister double-counted a stop")
					}
				}
			case 4:
				// Hot-add a fresh dependent of a and try to start it. The cap
				// keeps a large input from exploding the registry, and unique
				// names keep each registration accepted.
				if late < maxLate {
					late++
					name := fmt.Sprintf("late-%d", late)
					_ = runOp(func() error {
						if err := o.Register(&namedSvc{}, WithName(name), DependsOn("a")); err != nil {
							return err
						}
						return o.StartService(name)
					})
				}
			case 5:
				_ = runOp(func() error { return o.StartService("c") })
			case 6:
				// c self-heals, so allow one background restart's accounting.
				stopChecked("c", 2, func() error { return o.StopService("c", 50*time.Millisecond) })
			case 7:
				stopActiveChecked("b", func() error { return o.StopService("b", 50*time.Millisecond) })
			case 8:
				// Fire a cron tick directly (the scheduler's second cadence is too
				// slow for a fuzz iteration), tear the entry down, and require the
				// in-flight tick to be gone: no tick may survive the stop.
				o.mu.RLock()
				e := o.nameIndex["cron"]
				o.mu.RUnlock()
				if e == nil {
					break
				}
				go o.invokeCron(e, e.cronGeneration())
				time.Sleep(2 * time.Millisecond)
				if err := runOp(func() error { return o.StopService("cron", 50*time.Millisecond) }); err == nil {
					if v := cronSvc.running.Load(); v != 0 {
						t.Fatalf("cron tick survived StopService: %d still running", v)
					}
					if s, _ := o.Status("cron"); s != StatusStopped {
						t.Fatalf("cron status after stop = %v, want StatusStopped", s)
					}
				}
				_ = runOp(func() error { return o.StartService("cron") })
			case 9:
				stopGroupChecked()
			case 10:
				_ = runOp(func() error { return o.StartGroup("g") })
			}
			// Every counter is monotonic: no operation may decrement one. A
			// restart that was mistakenly accounted as a stop, or a stop counted
			// twice, would surface here as a decrease or as a delta the per-op
			// checks above missed.
			now := o.Metrics()
			if now.Starts < prev.Starts || now.Stops < prev.Stops || now.Crashes < prev.Crashes ||
				now.Restarts < prev.Restarts || now.HealthFails < prev.HealthFails ||
				now.CronFailures < prev.CronFailures || now.AbandonedGoroutines < prev.AbandonedGoroutines {
				t.Fatalf("a metric decreased: before=%+v after=%+v", prev, now)
			}
			prev = now
			assertNoReservationLeak()
			assertOwnerMapsConsistent()
		}
		// Snapshot which tracked members are live before shutdown, so the whole
		// Stop can be held to the same obligation as a group stop.
		type liveState struct {
			active bool
			stops  int32
		}
		stopObligation := make(map[string]liveState, len(tracked))
		for name, s := range tracked {
			stopObligation[name] = liveState{active: isActive(name), stops: s.stops.Load()}
		}
		_ = runOp(func() error { return o.Stop(2 * time.Second) })
		assertNoReservationLeak()
		for name, s := range o.Statuses() {
			if s == StatusStarting {
				t.Fatalf("entry %s still StatusStarting after shutdown", name)
			}
		}
		select {
		case <-o.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("goroutines did not wind down after Stop")
		}

		// Whole-Stop obligation: every tracked member that was running when
		// shutdown began had Stop() called and is no longer live. It is checked
		// as "Stop() was called at least once per stopped member", not as
		// stops == starts: the library can briefly overlap an old instance with
		// a restart, so one Stop() may cover more than one Start() for the same
		// entry. That is a separate known defect (duplicated instance spawning),
		// not the stop obligation this fuzz is pinning.
		for name, s := range tracked {
			if s.starts.Load() > 0 && s.stops.Load() == 0 {
				t.Fatalf("entry %s: %d starts but Stop() was never called", name, s.starts.Load())
			}
			before := stopObligation[name]
			if !before.active {
				continue
			}
			if s.stops.Load() == before.stops {
				t.Fatalf("whole Stop left running entry %s without calling Stop()", name)
			}
			if isActive(name) {
				t.Fatalf("entry %s still live after whole Stop", name)
			}
		}

		// Every owner is either released by its instance/tick or swept by the
		// teardown drain; after shutdown neither map may retain an id. Draining
		// each remaining entry first proves the entry-side map is fully
		// releasable and leaves the root map with no reachable owner.
		o.mu.RLock()
		remaining := make([]*serviceEntry, len(o.entries))
		copy(remaining, o.entries)
		o.mu.RUnlock()
		for _, e := range remaining {
			o.drainService(e)
		}
		if root := rootOwnerCount(o); root != 0 {
			t.Fatalf("root owner map holds %d owners after shutdown, want 0", root)
		}
		for _, e := range remaining {
			if ids := entryOwnerIDs(e); len(ids) != 0 {
				t.Fatalf("entry %s holds owners %v after shutdown, want none", e.name, ids)
			}
		}

		// Reconcile the crash surfaces: a crash is counted exactly once, and
		// never observed on one surface without the other.
		crashes := o.Metrics().Crashes
		if onCrash.Load() != crashes {
			t.Fatalf("OnCrash calls = %d, Crashes = %d: crash observability diverged", onCrash.Load(), crashes)
		}
		if crashTransitions.Load() > crashes {
			t.Fatalf("Crashed transitions = %d, Crashes = %d: a transition was reported without a crash count", crashTransitions.Load(), crashes)
		}
	})
}

// FuzzFailedStartOwners extends the #20 owner-map obligation to the failed-Start
// rollback: for any input, after a Start that fails while a service ignores
// cancellation and outlives the rollback budget, neither owner map may retain an
// id, and repeated failed Starts may not accumulate one. The abandoned instance
// is still live when the assertion runs, so only the rollback's drain — not the
// instance's own deferred release — can have cleared the maps. Each attempt
// re-runs the same failed Start, the shape that leaked an id per retry before
// the rollback drained.
func FuzzFailedStartOwners(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{2, 1, 0, 2})
	f.Fuzz(func(t *testing.T, data []byte) {
		attempts := 1
		if len(data) > 0 {
			attempts += int(data[0]) % 3
		}
		o := New(WithHealthChecksDisabled(), WithFailedStartTimeout(30*time.Millisecond))

		release := make(chan struct{})
		released := false
		subs := make(chan (<-chan any), attempts)
		returned := make(chan struct{}, attempts)
		stubborn := &abandonStartSvc{release: release, subs: subs, returned: returned}
		if err := o.Register(stubborn, WithName("stubborn")); err != nil {
			t.Fatal(err)
		}
		// The gate depends on stubborn, so stubborn starts first and is the
		// service the failing level's rollback must tear down.
		if err := o.Register(&namedSvc{}, WithName("gate"), DependsOn("stubborn"),
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
			if err := o.Start(); !errors.Is(err, ErrStopTimeout) {
				t.Fatalf("attempt %d: Start = %v, want ErrStopTimeout from the abandoned-instance wait", i, err)
			}
			// The instance is still blocked: only the drain can have cleared the
			// maps. Read the subscription it registered to pin the drain too.
			if n := len(returned); n != 0 {
				t.Fatalf("attempt %d: %d abandoned instance(s) returned early", i, n)
			}
			if got := rootOwnerCount(o); got != 0 {
				t.Fatalf("attempt %d: root owners = %d after failed Start, want 0", i, got)
			}
			if got := totalEntryOwnerCount(o); got != 0 {
				t.Fatalf("attempt %d: entry owner sum = %d after failed Start, want 0", i, got)
			}
			<-subs
			if ids := entryOwnerIDs(entry); len(ids) != 0 {
				t.Fatalf("attempt %d: stubborn entry owners = %v after failed Start, want none", i, ids)
			}
		}

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
	})
}

// FuzzStopTimeoutStatus drives a stop against a service that ignores context
// cancellation, so the stop times out with the instance goroutine still live. It
// asserts the semantic obligation the deadline path must never break: while the
// instance is live, the entry is never reported StatusStopped. The first input
// byte picks a bounded timeout and the second picks whole-orchestrator Stop
// versus StopService.
func FuzzStopTimeoutStatus(f *testing.F) {
	f.Add([]byte{1})
	f.Add([]byte{50, 0})
	f.Add([]byte{200, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		timeout := time.Duration(int(data[0])%200+1) * time.Millisecond
		whole := len(data) > 1 && data[1]%2 == 1

		release := make(chan struct{})
		svc := &fuzzStubbornSvc{release: release}
		o := New(WithHealthChecksDisabled())
		_ = o.Register(svc, WithName("s"))
		_ = o.Start()

		// Wait until the instance goroutine is actually live before stopping.
		live := time.After(time.Second)
		for svc.running.Load() == 0 {
			select {
			case <-live:
				t.Fatal("service did not start")
			case <-time.After(time.Millisecond):
			}
		}

		if whole {
			_ = o.Stop(timeout)
		} else {
			_ = o.StopService("s", timeout)
		}
		if s, ok := o.Status("s"); ok && s == StatusStopped && svc.running.Load() != 0 {
			t.Fatalf("entry reported Stopped while its instance goroutine is still live")
		}

		// Release the instance and let everything wind down.
		close(release)
		_ = o.Stop(2 * time.Second)
		select {
		case <-o.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("goroutines did not wind down after Stop")
		}
	})
}
