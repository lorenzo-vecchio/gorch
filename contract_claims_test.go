package gorch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file holds the named tests from issue #38: each test pins one sentence
// of the documented contract (doc.go / README.md) so the link between claim and
// test is greppable. The tests are deliberately deterministic: transitions are
// observed through channels or status polls, and a bounded sleep appears only
// where the claim is the absence of activity (no tick before StartService). All
// of them run under -race.

// ── Lifecycle and state ──

// TestStop_BudgetSharedAcrossCascade pins the documented sentence "timeout
// bounds the whole stop — the before/after-stop hooks, the service's own Stop(),
// and the wait for its instance or in-flight cron ticks to exit — and is shared
// across a cascade (one budget, not one per service)". Three services whose
// Stop() blocks for far longer than the caller's timeout must be torn down
// within roughly one shared budget, not three per-service budgets.
func TestStop_BudgetSharedAcrossCascade(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})

	newSvc := func() *testSvc {
		return &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			stopFn:  func() error { <-release; return nil },
		}
	}
	// A per-service stop timeout much larger than the shared budget: if the
	// budget were applied per entry, the three stops would take ~3×2s, not one
	// ~200ms budget.
	if err := o.Register(newSvc(), WithName("base"), WithStopTimeout(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(newSvc(), WithName("d1"), WithStopTimeout(2*time.Second), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(newSvc(), WithName("d2"), WithStopTimeout(2*time.Second), DependsOn("d1")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	before := o.Metrics().AbandonedGoroutines
	const budget = 200 * time.Millisecond
	start := time.Now()
	err := o.StopService("base", budget, WithCascadeStop())
	elapsed := time.Since(start)

	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("cascade stop = %v, want ErrStopTimeout", err)
	}
	if elapsed > 2*budget {
		t.Errorf("cascade took %v; the budget must be shared across the set, not per entry", elapsed)
	}
	if got := o.Metrics().AbandonedGoroutines; got <= before {
		t.Errorf("AbandonedGoroutines = %d, want > %d (a blocked Stop must be abandoned by the shared deadline)", got, before)
	}

	close(release)
	if err := o.Stop(2 * time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestCascade_NonRunningDependent pins the documented sentences "Dependents in
// any other status (Stopping, Registered, Crashed, Stopped, Succeeded) do not
// block" and "WithCascadeStop tears the dependents down in reverse topological
// order". A Registered hard dependent neither blocks a plain stop nor is stopped
// by it, but a cascade does reach it.
func TestCascade_NonRunningDependent(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	// Hot-add a hard dependent, which stays StatusRegistered: it has no
	// instance yet. It must not block a plain stop of base.
	dep := &testSvc{}
	if err := o.Register(dep, WithName("dep"), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	if s, _ := o.Status("dep"); s != StatusRegistered {
		t.Fatalf("hot-added dependent status = %v, want Registered", s)
	}

	if err := o.StopService("base", time.Second); err != nil {
		t.Fatalf("plain stop with a Registered dependent = %v, want nil (non-blocking)", err)
	}
	if s, _ := o.Status("base"); s != StatusStopped {
		t.Errorf("base status after plain stop = %v, want Stopped", s)
	}
	if s, _ := o.Status("dep"); s != StatusRegistered {
		t.Errorf("plain stop touched the non-running dependent: status = %v, want Registered", s)
	}
	if got := dep.stopCalls.Load(); got != 0 {
		t.Errorf("plain stop ran the dependent's Stop() %d times, want 0", got)
	}

	// A cascade, by contrast, is documented to tear the dependent down too.
	if err := o.StartService("base"); err != nil {
		t.Fatalf("restart base: %v", err)
	}
	if err := o.StopService("base", time.Second, WithCascadeStop()); err != nil {
		t.Fatalf("cascade stop = %v", err)
	}
	if s, _ := o.Status("base"); s != StatusStopped {
		t.Errorf("base status after cascade = %v, want Stopped", s)
	}
	if got := dep.stopCalls.Load(); got != 1 {
		t.Errorf("cascade ran the dependent's Stop() %d times, want 1", got)
	}
	if s, _ := o.Status("dep"); s != StatusStopped {
		t.Errorf("cascaded dependent status = %v, want Stopped", s)
	}
}

// TestCascade_ExcludesSoftDependents pins the documented sentence "Soft
// dependencies never block and are never cascaded". A soft dependent neither
// refuses a plain stop nor is reached by a cascade.
func TestCascade_ExcludesSoftDependents(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	base := &testSvc{}
	dep := &testSvc{}
	if err := o.Register(base, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(dep, WithName("dep"), DependsOnSoft("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	// The soft dependent is Running, yet it must not block a plain stop.
	if s, _ := o.Status("dep"); s != StatusRunning {
		t.Fatalf("soft dependent status = %v, want Running", s)
	}
	if err := o.StopService("base", time.Second); err != nil {
		t.Fatalf("plain stop with a running soft dependent = %v, want nil (soft never blocks)", err)
	}
	if s, _ := o.Status("dep"); s != StatusRunning {
		t.Errorf("plain stop touched the soft dependent: status = %v, want Running", s)
	}

	// Restart base and cascade: the soft dependent must be excluded from the
	// cascade set, so it is neither stopped nor its Stop() called.
	if err := o.StartService("base"); err != nil {
		t.Fatalf("restart base: %v", err)
	}
	if err := o.StopService("base", time.Second, WithCascadeStop()); err != nil {
		t.Fatalf("cascade stop = %v", err)
	}
	if s, _ := o.Status("dep"); s != StatusRunning {
		t.Errorf("cascade reached the soft dependent: status = %v, want Running", s)
	}
	if got := dep.stopCalls.Load(); got != 0 {
		t.Errorf("cascade ran the soft dependent's Stop() %d times, want 0", got)
	}
}

// ── Cron ──

// TestCronStaging_NoTickBeforeStartService pins the documented sentence "A
// hot-added cron service is staged by Register and only scheduled by
// StartService, so it never ticks while reporting StatusRegistered". Time passes
// with no tick, then StartService installs the schedule and ticks begin.
func TestCronStaging_NoTickBeforeStartService(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	var calls atomic.Int32
	cron := &testSvc{startFn: func(context.Context) error { calls.Add(1); return nil }}
	if err := o.Register(cron, WithName("c"), WithCron("* * * * * *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	entry := entryNamed(t, o, "c")
	if entry.cronID != 0 {
		t.Fatal("hot-added cron entry was scheduled by Register")
	}
	if s, _ := o.Status("c"); s != StatusRegistered {
		t.Fatalf("staged cron status = %v, want Registered", s)
	}

	// A full second would produce a tick if the schedule were live.
	time.Sleep(1100 * time.Millisecond)
	if got := calls.Load(); got != 0 {
		t.Fatalf("staged cron ticked %d times before StartService, want 0", got)
	}

	if err := o.StartService("c"); err != nil {
		t.Fatalf("StartService = %v", err)
	}
	if s, _ := o.Status("c"); s != StatusRunning {
		t.Errorf("status after StartService = %v, want Running", s)
	}
	if entry.cronID == 0 {
		t.Fatal("StartService did not install the staged cron schedule")
	}
	waitFor(t, 3*time.Second, func() bool { return calls.Load() > 0 }, "started cron never ticked")
}

// TestCronStaging_InvalidSpec_RejectedAtRegister pins the documented sentence "A
// WithCron spec is validated by Register on both the static and the hot-add path,
// so the same spec is accepted or rejected identically". An invalid spec — empty
// or malformed — returns ErrInvalidCron and is never registered.
func TestCronStaging_InvalidSpec_RejectedAtRegister(t *testing.T) {
	t.Run("static", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		for _, spec := range []string{"", "not a spec"} {
			err := o.Register(&namedSvc{}, WithName("bad"), WithCron(spec, CronParallel))
			if !errors.Is(err, ErrInvalidCron) {
				t.Fatalf("static Register(WithCron(%q)) = %v, want ErrInvalidCron", spec, err)
			}
		}
		if len(o.Names()) != 0 {
			t.Errorf("a rejected cron spec was registered: Names() = %v", o.Names())
		}
	})

	t.Run("hot_add", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)
		for _, spec := range []string{"", "not a spec"} {
			err := o.Register(&namedSvc{}, WithName("bad"), WithCron(spec, CronParallel))
			if !errors.Is(err, ErrInvalidCron) {
				t.Fatalf("hot add Register(WithCron(%q)) = %v, want ErrInvalidCron", spec, err)
			}
		}
		if _, ok := o.Status("bad"); ok {
			t.Error("a rejected hot-added cron spec was registered")
		}
	})
}

// TestCronTick_DoesNotSurviveUnregister pins the documented sentence
// "StopService/Unregister cancel every in-flight tick through it and wait for
// them to return". A tick in flight at Unregister time is cancelled and awaited
// before Unregister returns.
func TestCronTick_DoesNotSurviveUnregister(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	svc := &overlapCronSvc{entered: make(chan struct{})}
	// A schedule that never fires on its own, so the only tick is the one we
	// invoke deterministically.
	if err := o.Register(svc, WithName("c"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("c"); err != nil {
		t.Fatal(err)
	}
	entry := entryNamed(t, o, "c")
	go o.invokeCron(entry, entry.cronGeneration())
	select {
	case <-svc.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("cron tick never started")
	}

	if err := o.Unregister("c", time.Second); err != nil {
		t.Fatalf("Unregister = %v, want nil (the in-flight tick is cancelled and awaited)", err)
	}
	if got := svc.exited.Load(); got != 1 {
		t.Errorf("exited ticks = %d, want 1 (the in-flight tick must not survive Unregister)", got)
	}
	if _, ok := <-svc.channel(0); ok {
		t.Error("the unregistered tick's subscription must be closed")
	}
}

// queueProbeSvc records how many ticks ran, how many were live at once, and the
// peak concurrency, so a test can prove CronQueue serializes ticks.
type queueProbeSvc struct {
	mu          sync.Mutex
	invocations int
	running     int
	maxRunning  int
	entered     chan struct{}
	once        sync.Once
}

func (s *queueProbeSvc) Start(ctx ServiceContext) error {
	s.mu.Lock()
	s.invocations++
	s.running++
	if s.running > s.maxRunning {
		s.maxRunning = s.running
	}
	s.mu.Unlock()
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	s.mu.Lock()
	s.running--
	s.mu.Unlock()
	return ctx.Err()
}

func (s *queueProbeSvc) Stop() error { return nil }

func (s *queueProbeSvc) snapshot() (invocations, running, maxRunning int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.invocations, s.running, s.maxRunning
}

// TestCronQueue_SerializesAfterRemove pins the documented sentences "CronQueue:
// Serialize — wait for the previous run to finish" and "StopService/Unregister
// cancel every in-flight tick through it and wait for them to return". A tick
// blocked on the queue mutex behind an in-flight tick neither runs concurrently
// with it nor survives the removal: teardown cancels both and returns.
func TestCronQueue_SerializesAfterRemove(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	svc := &queueProbeSvc{entered: make(chan struct{})}
	if err := o.Register(svc, WithName("q"), WithCron("0 0 0 1 1 *", CronQueue)); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("q"); err != nil {
		t.Fatal(err)
	}
	entry := entryNamed(t, o, "q")

	// First tick: in flight, holding the queue mutex.
	go o.invokeCron(entry, entry.cronGeneration())
	select {
	case <-svc.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first queued tick never started")
	}
	// Second tick: it passes cronBegin (incrementing the active counter) and
	// then blocks on the queue mutex. Wait for it to be counted as active so the
	// assertion is not racing the scheduler.
	go o.invokeCron(entry, entry.cronGeneration())
	waitFor(t, 2*time.Second, func() bool {
		entry.cronTrackMu.Lock()
		defer entry.cronTrackMu.Unlock()
		return entry.cronActive >= 2
	}, "second queued tick never reached the queue")

	if err := o.Unregister("q", time.Second); err != nil {
		t.Fatalf("Unregister = %v, want nil (both ticks cancelled and awaited)", err)
	}
	invocations, running, maxRunning := svc.snapshot()
	if invocations != 2 {
		t.Errorf("invocations = %d, want 2 (both queued ticks reached Start)", invocations)
	}
	if running != 0 {
		t.Errorf("running = %d after Unregister, want 0", running)
	}
	if maxRunning != 1 {
		t.Errorf("peak concurrency = %d, want 1 (CronQueue must serialize)", maxRunning)
	}
}

// TestCronParallel_CancellationReachesAllInFlightTicks pins the documented
// sentence "StopService/Unregister cancel every in-flight tick through it and
// wait for them to return" for CronParallel: with overlapping ticks, every one
// is cancelled and awaited, not only the newest.
func TestCronParallel_CancellationReachesAllInFlightTicks(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	svc := &overlapCronSvc{entered: make(chan struct{})}
	if err := o.Register(svc, WithName("p"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("p"); err != nil {
		t.Fatal(err)
	}
	entry := entryNamed(t, o, "p")
	go o.invokeCron(entry, entry.cronGeneration())
	go o.invokeCron(entry, entry.cronGeneration())
	select {
	case <-svc.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("a parallel tick never started")
	}
	waitFor(t, 2*time.Second, func() bool { return svc.running.Load() >= 2 }, "second parallel tick never started")

	if err := o.StopService("p", time.Second); err != nil {
		t.Fatalf("StopService = %v", err)
	}
	if got := svc.exited.Load(); got != 2 {
		t.Errorf("exited ticks = %d, want 2 (all parallel ticks must be cancelled and awaited)", got)
	}
}

// ── Messenger ──

// TestMessenger_DrainRetiresExistingViews_NewViewWorks pins both halves of the
// documented sentence "After Drain, the Messenger is empty, Publish is a no-op,
// and a subsequent Subscribe re-initializes the subscription map. Every scoped
// owner is retired too, so an existing view cannot resubscribe after shutdown (a
// new view can)". An existing view's subscriptions close and it cannot
// resubscribe; a fresh view can subscribe and receive again.
func TestMessenger_DrainRetiresExistingViews_NewViewWorks(t *testing.T) {
	m := newMessenger()
	view := m.scoped(1)
	ch, _ := view.Subscribe("t")
	m.Publish("v", "t")
	select {
	case got := <-ch:
		if got != "v" {
			t.Fatalf("live view got %v, want v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("live view did not receive before Drain")
	}

	m.Drain()

	if _, ok := <-ch; ok {
		t.Error("Drain must close an existing view's subscription")
	}
	dead, _ := view.Subscribe("t")
	if _, ok := <-dead; ok {
		t.Error("an existing view must not resubscribe after Drain")
	}

	fresh := m.scoped(2)
	freshCh, unsub := fresh.Subscribe("t")
	defer unsub()
	m.Publish("after", "t")
	select {
	case got := <-freshCh:
		if got != "after" {
			t.Errorf("fresh view got %v, want after", got)
		}
	case <-time.After(time.Second):
		t.Error("a fresh view must subscribe after a global Drain")
	}
}

// TestMessenger_RetiredOwner_SubscribeReturnsClosedChannel pins the documented
// sentence "a drained id cannot be reminted into a later view" per owner: after
// drainOwner retires an owner, that owner's view gets an already-closed channel
// from Subscribe, while another owner is unaffected.
func TestMessenger_RetiredOwner_SubscribeReturnsClosedChannel(t *testing.T) {
	m := newMessenger()
	view := m.scoped(7)
	liveCh, _ := view.Subscribe("t")

	m.drainOwner(7)

	select {
	case _, ok := <-liveCh:
		if ok {
			t.Error("drainOwner must close the retired owner's subscription")
		}
	default:
		t.Error("the retired owner's subscription must be closed")
	}
	retired, noop := view.Subscribe("t")
	noop()
	select {
	case _, ok := <-retired:
		if ok {
			t.Error("a retired owner's Subscribe must return an already-closed channel")
		}
	default:
		t.Error("a retired owner's Subscribe channel must be closed")
	}

	// Per owner, not global: an unrelated live owner still works.
	other := m.scoped(8)
	otherCh, _ := other.Subscribe("t")
	m.Publish("ping", "t")
	select {
	case got := <-otherCh:
		if got != "ping" {
			t.Errorf("other owner got %v, want ping", got)
		}
	case <-time.After(time.Second):
		t.Error("retiring owner 7 must not affect owner 8")
	}
}

// TestMessenger_RequestOnDrainedView_ReturnsError pins the documented sentence
// "Request/RequestAsync/TypedRequest return an error instead of a nil reply"
// when the view's owner has been drained.
func TestMessenger_RequestOnDrainedView_ReturnsError(t *testing.T) {
	m := newMessenger()
	view := m.scoped(42)
	m.drainOwner(42)

	if _, err := view.Request(context.Background(), "x", "t"); err == nil {
		t.Error("Request on a drained view must return an error, not a nil reply")
	}
	if _, err := view.RequestAsync(context.Background(), "x", "t"); err == nil {
		t.Error("RequestAsync on a drained view must return an error")
	}
	if _, err := TypedRequest[string, string](view, context.Background(), "x", "t"); err == nil {
		t.Error("TypedRequest on a drained view must return an error, not a zero reply")
	}
}

// TestMessenger_RestartReleasesOnlyItsOwnSubscriptions pins the documented
// sentence "subscriptions are scoped to the instance ... and their owner is
// released on every exit path": a self-heal restart releases the crashed
// instance's subscriptions while another service keeps receiving.
func TestMessenger_RestartReleasesOnlyItsOwnSubscriptions(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	defer o.Stop(time.Second)

	heal := &healSubSvc{second: make(chan struct{})}
	other := &subTrackingSvc{ready: make(chan struct{})}
	if err := o.Register(heal, WithName("heal"),
		WithSelfHeal(func() Service { return heal }),
		WithMaxRetries(2),
		WithBackoff(ConstantBackoff{Delay: time.Millisecond}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(other, WithName("other")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	<-other.ready
	select {
	case <-heal.second:
	case <-time.After(2 * time.Second):
		t.Fatal("self-heal never restarted the crasher")
	}

	// The crashed instance's subscription is released...
	select {
	case _, ok := <-heal.channel(0):
		if ok {
			t.Error("the crashed instance's subscription must be released on restart")
		}
	default:
		t.Error("the crashed instance's subscription channel must be closed")
	}

	// ...while the other service's subscription still receives, and the
	// restarted instance's fresh subscription does too.
	o.messenger.Publish("ping", "t")
	select {
	case got := <-other.channel(0):
		if got != "ping" {
			t.Errorf("other service got %v, want ping", got)
		}
	case <-time.After(time.Second):
		t.Error("a restart must not release another service's subscriptions")
	}
	select {
	case got := <-heal.channel(1):
		if got != "ping" {
			t.Errorf("restarted instance got %v, want ping", got)
		}
	case <-time.After(time.Second):
		t.Error("the restarted instance must own a fresh, live subscription")
	}
}

// ── Hooks and deadlines ──

// TestHookTimeout_StillMatchesErrStopTimeout pins the documented sentence
// "ErrHookTimeout is always joined into the stop error with ErrStopTimeout ...
// so callers that only classify whole-stop timeouts keep working": an
// overrunning before-stop hook yields an error that matches both sentinels.
func TestHookTimeout_StillMatchesErrStopTimeout(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	var once sync.Once
	svc := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(svc, WithName("s"), WithOnBeforeStop(func(string) error {
		once.Do(func() { <-release })
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		_ = o.Stop(time.Second)
	})

	err := o.StopService("s", 100*time.Millisecond)
	if !errors.Is(err, ErrHookTimeout) {
		t.Fatalf("StopService = %v, want ErrHookTimeout", err)
	}
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("StopService = %v, want it to also match ErrStopTimeout", err)
	}
}

// TestAfterStopHook_ReceivesServiceStopError pins the documented behaviour that
// the after-stop hook is called with the service's Stop() result: an error from
// Stop() is delivered to the hook, and also returned from the stop call.
func TestAfterStopHook_ReceivesServiceStopError(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	stopErr := errors.New("stop failed")
	svc := &testSvc{stopFn: func() error { return stopErr }}

	var gotErr error
	var called atomic.Bool
	if err := o.Register(svc, WithName("s"), WithOnAfterStop(func(name string, err error) {
		called.Store(true)
		gotErr = err
	})); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	err := o.StopService("s", time.Second)
	if !errors.Is(err, stopErr) {
		t.Fatalf("StopService = %v, want it to carry the service Stop() error", err)
	}
	if !called.Load() {
		t.Fatal("the after-stop hook was not called")
	}
	if !errors.Is(gotErr, stopErr) {
		t.Errorf("after-stop hook received %v, want the service Stop() error", gotErr)
	}
}

// TestGlobalAndPerServiceHooks_PerServiceWins pins the documented precedence
// "per-service over global": when a service defines its own start/stop hooks,
// the global hooks do not also fire for it.
func TestGlobalAndPerServiceHooks_PerServiceWins(t *testing.T) {
	var globalBeforeStart, globalAfterStart, globalBeforeStop, globalAfterStop atomic.Int32
	var perBeforeStart, perAfterStart, perBeforeStop, perAfterStop atomic.Int32

	o := New(WithHealthChecksDisabled(),
		WithGlobalOnBeforeStart(func(string) error { globalBeforeStart.Add(1); return nil }),
		WithGlobalOnAfterStart(func(string, error) { globalAfterStart.Add(1) }),
		WithGlobalOnBeforeStop(func(string) error { globalBeforeStop.Add(1); return nil }),
		WithGlobalOnAfterStop(func(string, error) { globalAfterStop.Add(1) }),
	)
	svc := &testSvc{}
	if err := o.Register(svc, WithName("s"),
		WithOnBeforeStart(func(string) error { perBeforeStart.Add(1); return nil }),
		WithOnAfterStart(func(string, error) { perAfterStart.Add(1) }),
		WithOnBeforeStop(func(string) error { perBeforeStop.Add(1); return nil }),
		WithOnAfterStop(func(string, error) { perAfterStop.Add(1) }),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	if err := o.Stop(time.Second); err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		name   string
		per    *atomic.Int32
		global *atomic.Int32
	}{
		{"before-start", &perBeforeStart, &globalBeforeStart},
		{"after-start", &perAfterStart, &globalAfterStart},
		{"before-stop", &perBeforeStop, &globalBeforeStop},
		{"after-stop", &perAfterStop, &globalAfterStop},
	}
	for _, c := range checks {
		if got := c.per.Load(); got != 1 {
			t.Errorf("%s per-service hook ran %d times, want 1", c.name, got)
		}
		if got := c.global.Load(); got != 0 {
			t.Errorf("%s global hook ran %d times, want 0 (per-service must win)", c.name, got)
		}
	}
}

// ── Introspection ──

// TestHealth_ReturnsNilForNonHealthChecker pins the documented sentence "a
// running service that does not implement HealthChecker is reported with a nil
// error": it is included in the result, not omitted and not errored.
func TestHealth_ReturnsNilForNonHealthChecker(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(&namedSvc{}, WithName("plain")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)
	waitForStatus(t, o, "plain", StatusRunning)

	err, ok := o.Health()["plain"]
	if !ok {
		t.Fatal("a running service without HealthChecker must still appear in Health()")
	}
	if err != nil {
		t.Errorf("Health()[plain] = %v, want nil for a non-checker", err)
	}
}

// TestWaitFor_PollsAndTimesOut pins the documented sentence "WaitFor blocks until
// the named service reaches target status or timeout expires. Polls at 50ms
// intervals. Returns an error on timeout or if the service is not found. A
// non-positive timeout does not wait".
func TestWaitFor_PollsAndTimesOut(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	svc := &testSvc{}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)
	waitForStatus(t, o, "s", StatusRunning)

	t.Run("unknown_service", func(t *testing.T) {
		if err := o.WaitFor("ghost", StatusRunning, 10*time.Millisecond); err == nil {
			t.Error("WaitFor of an unknown service must return an error")
		}
	})

	t.Run("non_positive_timeout", func(t *testing.T) {
		if err := o.WaitFor("s", StatusRunning, 0); err != nil {
			t.Errorf("WaitFor with timeout 0 on a matching status = %v, want nil", err)
		}
		start := time.Now()
		if err := o.WaitFor("s", StatusStopped, 0); err == nil {
			t.Error("WaitFor with timeout 0 on a non-matching status must time out at once")
		}
		if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
			t.Errorf("a non-positive timeout waited %v; it must not wait", elapsed)
		}
	})

	t.Run("times_out", func(t *testing.T) {
		start := time.Now()
		err := o.WaitFor("s", StatusStopped, 80*time.Millisecond)
		if err == nil {
			t.Error("WaitFor to an unreachable status must time out")
		}
		if elapsed := time.Since(start); elapsed < 70*time.Millisecond {
			t.Errorf("WaitFor returned after %v; it must poll until the deadline", elapsed)
		}
	})

	t.Run("polls_to_success", func(t *testing.T) {
		if err := o.StopService("s", time.Second); err != nil {
			t.Fatalf("StopService: %v", err)
		}
		go func() {
			time.Sleep(80 * time.Millisecond)
			_ = o.StartService("s")
		}()
		start := time.Now()
		if err := o.WaitFor("s", StatusRunning, 2*time.Second); err != nil {
			t.Fatalf("WaitFor after the status changed = %v, want nil", err)
		}
		if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
			t.Errorf("WaitFor returned after %v; it must have polled at least one interval", elapsed)
		}
	})
}

// TestIsReady_HonoursCallerContext pins the documented sentence "The
// ReadinessChecker probe (if any) runs with the given ctx, so callers can bound
// how long they wait". The probe observes the caller's context and its
// cancellation; a non-checker reports ready while running.
func TestIsReady_HonoursCallerContext(t *testing.T) {
	type ctxKey struct{}

	o := New(WithHealthChecksDisabled())
	svc := &readySvc{}
	svc.readyFn = func(ctx context.Context) error {
		if ctx.Value(ctxKey{}) != "marker" {
			return errors.New("probe did not receive the caller's context")
		}
		return nil
	}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("plain")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)
	waitForStatus(t, o, "s", StatusRunning)

	t.Run("caller_values_reach_the_probe", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), ctxKey{}, "marker")
		if !o.IsReady(ctx, "s") {
			t.Error("IsReady must pass the caller's context through to Ready")
		}
	})

	t.Run("cancelled_context_is_observed", func(t *testing.T) {
		svc.readyFn = func(ctx context.Context) error { return ctx.Err() }
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if o.IsReady(ctx, "s") {
			t.Error("IsReady must observe the caller's cancelled context")
		}
	})

	t.Run("deadline_bounds_the_probe", func(t *testing.T) {
		svc.readyFn = func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		if o.IsReady(ctx, "s") {
			t.Error("IsReady must report not-ready when the caller's deadline expires")
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("IsReady took %v; the caller's deadline must bound the probe", elapsed)
		}
	})

	t.Run("non_checker_is_ready", func(t *testing.T) {
		if !o.IsReady(context.Background(), "plain") {
			t.Error("a running service without ReadinessChecker must report ready")
		}
	})
}

// TestStatusesByLabel_And_ByGroup pins the documented introspection surface: the
// status maps are filtered by group and by label key-value pair, and an empty
// match returns an empty (non-nil) map.
func TestStatusesByLabel_And_ByGroup(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(&namedSvc{}, WithName("a"), WithGroup("g1"), WithLabel("tier", "frontend")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("b"), WithGroup("g1"), WithLabel("tier", "backend")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("c"), WithGroup("g2"), WithLabel("tier", "frontend")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	assertStatuses := func(got map[string]ServiceStatus, want map[string]ServiceStatus) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("status map = %v, want %v", got, want)
		}
		for name, st := range want {
			if got[name] != st {
				t.Errorf("status[%s] = %v, want %v", name, got[name], st)
			}
		}
	}

	assertStatuses(o.StatusesByGroup("g1"), map[string]ServiceStatus{"a": StatusRunning, "b": StatusRunning})
	assertStatuses(o.StatusesByGroup("g2"), map[string]ServiceStatus{"c": StatusRunning})
	assertStatuses(o.StatusesByGroup("missing"), map[string]ServiceStatus{})
	assertStatuses(o.StatusesByLabel("tier", "frontend"), map[string]ServiceStatus{"a": StatusRunning, "c": StatusRunning})
	assertStatuses(o.StatusesByLabel("tier", "backend"), map[string]ServiceStatus{"b": StatusRunning})
	assertStatuses(o.StatusesByLabel("tier", "missing"), map[string]ServiceStatus{})

	if got := o.StatusesByGroup("missing"); got == nil {
		t.Error("an empty group match must return an initialized map, not nil")
	}
}

// waitFor polls cond up to timeout, failing the test if it never becomes true.
// It delegates to the shared waitForCondition so the suite has one polling
// implementation; it is a safety net for a channel-driven assertion, not the
// synchronization itself.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	waitForCondition(t, timeout, cond, msg)
}
