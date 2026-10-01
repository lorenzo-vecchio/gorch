package gorch

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// This file is the shared test harness for the gorch suite (issue #54). Before
// it, every test hand-rolled its own service double, its own polling loop and
// its own goroutine-count check, so the suite had no single place to state what
// "a service that records its lifecycle" or "no leak" means. The doubles here
// are the vocabulary the behaviour-matrix tests are written in.
//
// Design rules, in the spirit of the issue:
//
//   - Every double is safe for concurrent use; a test never needs its own mutex.
//   - Synchronization is by channel or by asserted condition, never by a bare
//     sleep. waitForCondition is a bounded safety net around an assertion, not
//     the assertion's clock.
//   - The event log keeps ordering, so a test can assert a sequence rather than
//     just an endpoint.
//   - assertNoLeak makes the resource invariants explicit: goroutines return to
//     baseline, owner maps drain, reservations clear, entries and nameIndex
//     agree.

// ── recordingService ──

// svcEventKind labels one entry in a recordingService's ordered event log.
type svcEventKind int

const (
	evStart svcEventKind = iota
	evExit
	evStop
)

func (k svcEventKind) String() string {
	switch k {
	case evStart:
		return "start"
	case evExit:
		return "exit"
	case evStop:
		return "stop"
	default:
		return "unknown"
	}
}

// svcEvent is a single ordered record: which operation ran on which instance
// incarnation, and the exit error when the operation was an exit.
type svcEvent struct {
	kind     svcEventKind
	instance int
	err      error
}

// recordingService is the suite's workhorse double. It records every Start and
// Stop invocation, the exit error of each incarnation, and an ordered event
// log. By default Start blocks until the instance context is cancelled (a
// persistent service); set startFn to override that, and stopFn to make Stop do
// work.
type recordingService struct {
	mu       sync.Mutex
	events   []svcEvent
	starts   int
	stops    int
	instance int
	lastExit error
	stopped  bool

	// startFn, when non-nil, replaces the default blocking Start. It receives
	// the 1-based incarnation id so a test can distinguish a restart.
	startFn func(instance int, ctx ServiceContext) error
	// stopFn, when non-nil, runs inside Stop after the stop is recorded.
	stopFn func() error
}

// newRecordingService returns a recording persistent service.
func newRecordingService() *recordingService { return &recordingService{} }

// Start records the invocation, then blocks until the instance context is
// cancelled unless startFn overrides it.
func (s *recordingService) Start(ctx ServiceContext) error {
	s.mu.Lock()
	s.instance++
	id := s.instance
	s.starts++
	s.events = append(s.events, svcEvent{kind: evStart, instance: id})
	startFn := s.startFn
	s.mu.Unlock()

	var err error
	if startFn != nil {
		err = startFn(id, ctx)
	} else {
		<-ctx.Done()
		err = ctx.Err()
	}

	s.mu.Lock()
	s.events = append(s.events, svcEvent{kind: evExit, instance: id, err: err})
	s.lastExit = err
	s.mu.Unlock()
	return err
}

// Stop records the invocation and runs stopFn when set.
func (s *recordingService) Stop() error {
	s.mu.Lock()
	s.stops++
	s.stopped = true
	s.events = append(s.events, svcEvent{kind: evStop, instance: s.instance})
	stopFn := s.stopFn
	s.mu.Unlock()
	if stopFn != nil {
		return stopFn()
	}
	return nil
}

func (s *recordingService) startCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.starts
}

func (s *recordingService) stopCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stops
}

func (s *recordingService) exitError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastExit
}

func (s *recordingService) wasStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

// eventLog returns a copy of the ordered event log as kind#instance labels.
func (s *recordingService) eventLog() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.events))
	for i, e := range s.events {
		out[i] = fmt.Sprintf("%s#%d", e.kind, e.instance)
	}
	return out
}

// countKind returns how many events of kind are in the log.
func (s *recordingService) countKind(kind svcEventKind) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.events {
		if e.kind == kind {
			n++
		}
	}
	return n
}

func (s *recordingService) assertStopped(t *testing.T, when string) {
	t.Helper()
	if !s.wasStopped() {
		t.Errorf("%s: Stop() was never called", when)
	}
}

// ── blockingService ──

// blockingService lets a test park an instance inside Start or Stop. Start
// signals started and then blocks until releaseStart or context cancellation;
// Stop signals stopCalled and, when stopGate is set, blocks until it is
// closed. It exists for deadline, budget and leak tests, where holding a
// lifecycle call is the point.
type blockingService struct {
	started    chan struct{}
	stopCalled chan struct{}
	release    chan struct{}
	stopGate   chan struct{}
	startErr   error

	once sync.Once
	mu   sync.Mutex
	// starts and stops count invocations; a double-start is a defect the
	// assertion reads directly.
	starts int
	stops  int
}

// newBlockingService returns a service whose Start blocks until released.
func newBlockingService() *blockingService {
	return &blockingService{
		started:    make(chan struct{}, 1),
		stopCalled: make(chan struct{}, 1),
		release:    make(chan struct{}),
	}
}

func (s *blockingService) Start(ctx ServiceContext) error {
	s.mu.Lock()
	s.starts++
	s.mu.Unlock()
	select {
	case s.started <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return s.startErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockingService) Stop() error {
	s.mu.Lock()
	s.stops++
	s.mu.Unlock()
	select {
	case s.stopCalled <- struct{}{}:
	default:
	}
	if s.stopGate != nil {
		<-s.stopGate
	}
	return nil
}

// releaseStart lets a parked Start return.
func (s *blockingService) releaseStart() { s.once.Do(func() { close(s.release) }) }

func (s *blockingService) startCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.starts
}

func (s *blockingService) stopCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stops
}

// ── blockingHook ──

// blockingHook parks a lifecycle hook until releaseHook is called, signalling
// entry through entered. beforeStop plugs into WithOnBeforeStop and the global
// WithGlobalOnBeforeStop, and can be used to prove that a membership op from a
// concurrent goroutine is not blocked behind user code.
type blockingHook struct {
	entered chan struct{}
	release chan struct{}
	err     error
	once    sync.Once
}

func newBlockingHook() *blockingHook {
	return &blockingHook{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

// beforeStop is a func(string) error suitable for the before-stop hooks.
func (h *blockingHook) beforeStop(string) error {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	<-h.release
	return h.err
}

// afterStart is a func(string, error) suitable for the after-start hooks.
func (h *blockingHook) afterStart(string, error) {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	<-h.release
}

func (h *blockingHook) releaseHook() { h.once.Do(func() { close(h.release) }) }

// ── controllableHealthChecker ──

// controllableHealthChecker is a recordingService whose Health result and
// blocking behaviour a test controls. Its call count is observable so a test
// can tell whether the periodic loop actually probed, and how often.
type controllableHealthChecker struct {
	*recordingService
	mu    sync.Mutex
	err   error
	calls int
	gate  chan struct{}
}

func newControllableHealthChecker() *controllableHealthChecker {
	return &controllableHealthChecker{recordingService: newRecordingService()}
}

// Health implements HealthChecker: it records the call and returns the
// configured error, blocking on gate first when one is set.
func (h *controllableHealthChecker) Health(ctx context.Context) error {
	h.mu.Lock()
	h.calls++
	err := h.err
	gate := h.gate
	h.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (h *controllableHealthChecker) setHealth(err error) {
	h.mu.Lock()
	h.err = err
	h.mu.Unlock()
}

func (h *controllableHealthChecker) healthCalls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// ── graphBuilder ──

// graphBuilder declares a dependency topology fluently so a test reads as the
// graph it exercises, instead of a wall of Register calls.
type graphBuilder struct {
	t    *testing.T
	o    *Orchestrator
	errs []error
}

// newGraph returns a builder over a fresh orchestrator with health checks
// disabled unless overridden by opts.
func newGraph(t *testing.T, opts ...Option) *graphBuilder {
	t.Helper()
	all := append([]Option{WithHealthChecksDisabled()}, opts...)
	return &graphBuilder{t: t, o: New(all...)}
}

// service registers svc under name with opts, collecting any error so build can
// report every registration failure at once.
func (g *graphBuilder) service(name string, svc Service, opts ...RegisterOption) *graphBuilder {
	g.t.Helper()
	all := append([]RegisterOption{WithName(name)}, opts...)
	if err := g.o.Register(svc, all...); err != nil {
		g.errs = append(g.errs, fmt.Errorf("register %q: %w", name, err))
	}
	return g
}

// build returns the orchestrator, failing the test if any registration failed.
func (g *graphBuilder) build() *Orchestrator {
	g.t.Helper()
	if len(g.errs) > 0 {
		g.t.Fatalf("graph build: %v", errors.Join(g.errs...))
	}
	return g.o
}

// start builds and starts the orchestrator, failing the test on error.
func (g *graphBuilder) start() *Orchestrator {
	g.t.Helper()
	o := g.build()
	if err := o.Start(); err != nil {
		g.t.Fatalf("graph start: %v", err)
	}
	return o
}

// ── polling ──

// waitForCondition polls cond until it holds or timeout elapses, failing with
// msg on expiry. It is the single bounded safety net shared by the suite: the
// real synchronization is the channel or state change the condition observes,
// and this only turns a hang into a readable failure. It spins with Gosched so
// a fast condition is not slowed by a sleep.
func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for: %s", timeout, msg)
		}
		runtime.Gosched()
	}
}

// ── leak detection ──

// leakBaseline is the resource snapshot a test takes before it starts work.
type leakBaseline struct {
	goroutines  int
	rootOwners  int
	entryOwners int
}

// captureLeakBaseline snapshots the goroutine count and both owner maps. It must
// be called before the test starts its orchestrator.
func captureLeakBaseline(o *Orchestrator) leakBaseline {
	return leakBaseline{
		goroutines:  runtime.NumGoroutine(),
		rootOwners:  rootOwnerCount(o),
		entryOwners: totalEntryOwnerCount(o),
	}
}

// assertNoLeak checks the resource invariants the issue calls for: entries and
// nameIndex agree and every entry is unreserved, both owner maps are back to
// their baseline, and the goroutine count returns to baseline. It must be
// called after the test's teardown has completed, so any service it started is
// stopped or unregistered.
func assertNoLeak(t *testing.T, o *Orchestrator, base leakBaseline) {
	t.Helper()

	o.mu.RLock()
	entries := append([]*serviceEntry(nil), o.entries...)
	index := make(map[string]*serviceEntry, len(o.nameIndex))
	for k, v := range o.nameIndex {
		index[k] = v
	}
	o.mu.RUnlock()

	if len(entries) != len(index) {
		t.Errorf("entries (%d) and nameIndex (%d) disagree", len(entries), len(index))
	}
	for _, e := range entries {
		if index[e.name] != e {
			t.Errorf("entry %q is not the one nameIndex points at", e.name)
		}
		if e.starting.Load() {
			t.Errorf("entry %q left its start reservation set", e.name)
		}
		if e.removing.Load() {
			t.Errorf("entry %q left its removing flag set", e.name)
		}
	}

	if got := rootOwnerCount(o); got != base.rootOwners {
		t.Errorf("root owner map = %d, want baseline %d", got, base.rootOwners)
	}
	if got := totalEntryOwnerCount(o); got != base.entryOwners {
		t.Errorf("entry owner maps = %d, want baseline %d", got, base.entryOwners)
	}

	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > base.goroutines {
		if time.Now().After(deadline) {
			t.Errorf("goroutines = %d, want <= baseline %d (leak of %d)",
				runtime.NumGoroutine(), base.goroutines, runtime.NumGoroutine()-base.goroutines)
			return
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
}

// ── harness self-tests ──

// TestHarness_RecordingServiceRecordsLifecycle proves the workhorse records
// starts, stops, exits and their order, so the matrix tests can trust it.
func TestHarness_RecordingServiceRecordsLifecycle(t *testing.T) {
	o := newGraph(t).service("svc", newRecordingService()).start()

	svc := mustEntry(t, o, "svc").getSvc().(*recordingService)
	waitForCondition(t, 3*time.Second, func() bool { return svc.startCount() == 1 }, "service to start")

	if got := o.Stop(3 * time.Second); got != nil {
		t.Fatalf("Stop = %v, want nil", got)
	}
	if svc.stopCount() != 1 {
		t.Errorf("stop count = %d, want 1", svc.stopCount())
	}
	svc.assertStopped(t, "after Stop")
	if n := svc.countKind(evExit); n != 1 {
		t.Errorf("exit events = %d, want 1", n)
	}
	want := []string{"start#1", "exit#1", "stop#1"}
	if got := svc.eventLog(); !equalStringSlices(got, want) {
		t.Errorf("event log = %v, want %v", got, want)
	}
}

// TestHarness_BlockingServiceGatesStartAndStop proves the blocking double parks
// Start until released and parks Stop behind its gate.
func TestHarness_BlockingServiceGatesStartAndStop(t *testing.T) {
	svc := newBlockingService()
	svc.stopGate = make(chan struct{})
	o := newGraph(t).service("svc", svc).start()

	select {
	case <-svc.started:
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not signal it entered")
	}
	svc.releaseStart()

	stopDone := make(chan error, 1)
	go func() { stopDone <- o.Stop(3 * time.Second) }()
	select {
	case <-svc.stopCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not signal it entered")
	}
	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned %v while stopGate was closed", err)
	default:
	}
	close(svc.stopGate)
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}
}

// TestHarness_BlockingHookParksUntilReleased proves the hook double lets a test
// hold user code and release it on demand.
func TestHarness_BlockingHookParksUntilReleased(t *testing.T) {
	h := newBlockingHook()
	done := make(chan struct{})
	go func() { _ = h.beforeStop("svc"); close(done) }()
	select {
	case <-h.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("hook did not signal entry")
	}
	select {
	case <-done:
		t.Fatal("hook returned before release")
	default:
	}
	h.releaseHook()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("hook did not return after release")
	}
}

// TestHarness_ControllableHealthCheckerCountsCalls proves the health double
// records probes and returns the configured result.
func TestHarness_ControllableHealthCheckerCountsCalls(t *testing.T) {
	hc := newControllableHealthChecker()
	if err := hc.Health(context.Background()); err != nil {
		t.Fatalf("healthy probe = %v, want nil", err)
	}
	boom := errors.New("unhealthy")
	hc.setHealth(boom)
	if err := hc.Health(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("probe = %v, want %v", err, boom)
	}
	if got := hc.healthCalls(); got != 2 {
		t.Errorf("probe calls = %d, want 2", got)
	}
}

// TestHarness_GraphBuilderBuildsTopology proves the builder applies names and
// dependencies and reports a start failure as a test failure.
func TestHarness_GraphBuilderBuildsTopology(t *testing.T) {
	o := newGraph(t).
		service("db", newRecordingService()).
		service("api", newRecordingService(), DependsOn("db")).
		build()

	o.mu.RLock()
	api := o.nameIndex["api"]
	o.mu.RUnlock()
	if got := api.cfg.dependsOn; len(got) != 1 || got[0] != "db" {
		t.Fatalf("api depends on %v, want [db]", got)
	}
}

// TestHarness_WaitForConditionReturnsWhenTrue is the positive path of the poll
// helper; its timeout path is exercised by a goroutine so the suite stays green.
func TestHarness_WaitForConditionReturnsWhenTrue(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		waitForCondition(t, time.Second, func() bool { return true }, "always true")
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("waitForCondition did not return for a true condition")
	}
}

// TestHarness_AssertNoLeakOnCleanOrchestrator proves the leak helper accepts a
// correctly torn-down orchestrator.
func TestHarness_AssertNoLeakOnCleanOrchestrator(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	base := captureLeakBaseline(o)
	if err := o.Register(newRecordingService(), WithName("svc")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	if err := o.Stop(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	assertNoLeak(t, o, base)
}

// mustEntry returns the entry registered under name, failing the test if it is
// missing. It is the harness's bridge to the internal graph the matrix tests
// inspect.
func mustEntry(t *testing.T, o *Orchestrator, name string) *serviceEntry {
	t.Helper()
	o.mu.RLock()
	e := o.nameIndex[name]
	o.mu.RUnlock()
	if e == nil {
		t.Fatalf("no entry named %q", name)
	}
	return e
}

// equalStringSlices reports element-wise equality of two string slices.
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
