package gorch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
)

type serviceEntry struct {
	// stateMu guards svc, logger, cancel, retryCount, stableSince,
	// healthFailures, done, and teardown — the fields the self-heal restart
	// path mutates while the health-check loop and introspection methods
	// (Health, IsReady) read them.
	stateMu sync.Mutex

	svc    Service
	cfg    registerConfig
	logger *ServiceLogger

	// runtime state
	name      string
	status    ServiceStatus
	cancel    context.CancelFunc // per-service cancellation (nil until started)
	startedAt time.Time          // when the current instance started

	// ownerMu guards owners: the Messenger owner ids currently live for this
	// entry, one per running instance or cron tick. Teardown drains them all.
	ownerMu sync.Mutex
	owners  map[uint64]struct{}

	// teardown and teardownCancel parent every instance context of a running
	// service. Cancelling them aborts the current instance and any pending
	// self-heal restart (StopService/Unregister/whole-orchestrator Stop). The
	// instance context handed to Start is a child, so a health-threshold restart
	// (which cancels only the instance) does not abort the entry.
	teardown       context.Context
	teardownCancel context.CancelFunc

	// retry / backoff state
	retryCount  int
	stableSince time.Time // when the current stable run began (for resetAfter)

	// health check state
	healthFailures int

	// cron state
	cronID cron.EntryID
	// self-heal state for non-cron services
	wgDone bool // true once wg.Done() has been called for this entry
	// starting is the start *reservation*: it is true while this entry is
	// reserved for an in-flight start — during startOneService's synchronous
	// work, or for every member StartGroup selected. It makes a membership op
	// that would recurse (or collide with another goroutine's start) rejectable
	// instead of deadlocking. Who owns the reservation is not encoded here; that
	// is startGoid's job.
	starting atomic.Bool
	// removing is true while a StopService/Unregister is tearing this entry
	// down. It rejects new hard-dependency edges from Register (C11).
	removing atomic.Bool
	// startGoid / stopGoid hold the goroutine id currently executing this
	// entry's user Start()/Stop() (0 = none). They distinguish a genuine
	// same-goroutine re-entry from a concurrent reservation collision: the
	// starting/removing flags only say an operation is in flight.
	startGoid atomic.Uint64
	stopGoid  atomic.Uint64
	// removed is set permanently when Unregister deletes the entry from the
	// registry. It lets a caller that selected the entry before the removal
	// (e.g. StartGroup) refuse to start it afterwards.
	removed atomic.Bool
	// done is closed once the current instance's goroutine has fully exited,
	// including its self-heal decision. It is replaced on each (re)start; nil
	// before the first start and for runOnce entries.
	done chan struct{}
	// CronSkip / CronQueue gate
	running atomic.Bool
	// stopsCounted latches the per-instance stop metric: a teardown and the
	// instance's own exit handler can both observe the same stop, but only one
	// of them may count it.
	stopsCounted atomic.Bool
	// CronQueue serialization lock (serialize ticks mode)
	cronMu sync.Mutex
	// cronTrack guards the per-schedule tick accounting below. All ticks of one
	// schedule derive from cronCtx, so cancelling it cancels every in-flight
	// tick; cronActive lets stopEntry wait for them all before returning. cronGen
	// distinguishes schedules so a stale tick from a stopped generation cannot
	// touch the next one's accounting.
	cronTrackMu  sync.Mutex
	cronCtx      context.Context
	cronCancel   context.CancelFunc
	cronGen      uint64
	cronActive   int
	cronDraining bool
	cronDrained  chan struct{}
}

// lifecycleOwnedBy reports whether goid is the goroutine currently inside this
// entry's own user Start() or Stop(). It is what separates a genuine re-entry
// (same goroutine) from a concurrent reservation collision (another goroutine).
func (e *serviceEntry) lifecycleOwnedBy(goid uint64) bool {
	return goid != 0 && (e.startGoid.Load() == goid || e.stopGoid.Load() == goid)
}

// getSvc returns the current service instance (thread-safe).
func (e *serviceEntry) getSvc() Service {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	return e.svc
}

// setSvc replaces the service instance (thread-safe).
func (e *serviceEntry) setSvc(svc Service) {
	e.stateMu.Lock()
	e.svc = svc
	e.stateMu.Unlock()
}

// getLogger returns the current logger (thread-safe).
func (e *serviceEntry) getLogger() *ServiceLogger {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	return e.logger
}

// setLogger replaces the logger (thread-safe).
func (e *serviceEntry) setLogger(l *ServiceLogger) {
	e.stateMu.Lock()
	e.logger = l
	e.stateMu.Unlock()
}

// getCancel returns the per-service cancel func (thread-safe).
func (e *serviceEntry) getCancel() context.CancelFunc {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	return e.cancel
}

// setCancel replaces the per-service cancel func (thread-safe).
func (e *serviceEntry) setCancel(c context.CancelFunc) {
	e.stateMu.Lock()
	e.cancel = c
	e.stateMu.Unlock()
}

// getRetryCount returns the retry counter (thread-safe).
func (e *serviceEntry) getRetryCount() int {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	return e.retryCount
}

// setRetryCount sets the retry counter (thread-safe).
func (e *serviceEntry) setRetryCount(n int) {
	e.stateMu.Lock()
	e.retryCount = n
	e.stateMu.Unlock()
}

// getStableSince returns the stability-window start (thread-safe).
func (e *serviceEntry) getStableSince() time.Time {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	return e.stableSince
}

// setStableSince sets the stability-window start (thread-safe).
func (e *serviceEntry) setStableSince(t time.Time) {
	e.stateMu.Lock()
	e.stableSince = t
	e.stateMu.Unlock()
}

// getHealthFailures returns the health-failure counter (thread-safe).
func (e *serviceEntry) getHealthFailures() int {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	return e.healthFailures
}

// setHealthFailures sets the health-failure counter (thread-safe).
func (e *serviceEntry) setHealthFailures(n int) {
	e.stateMu.Lock()
	e.healthFailures = n
	e.stateMu.Unlock()
}

// getDone returns the current instance's exit channel (thread-safe).
func (e *serviceEntry) getDone() chan struct{} {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	return e.done
}

// setDone replaces the current instance's exit channel (thread-safe).
func (e *serviceEntry) setDone(d chan struct{}) {
	e.stateMu.Lock()
	e.done = d
	e.stateMu.Unlock()
}

// getTeardown returns the entry's teardown context (thread-safe).
func (e *serviceEntry) getTeardown() context.Context {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	return e.teardown
}

// getTeardownCancel returns the teardown cancel func (thread-safe).
func (e *serviceEntry) getTeardownCancel() context.CancelFunc {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	return e.teardownCancel
}

// setTeardown replaces the entry's teardown context and cancel func
// (thread-safe).
func (e *serviceEntry) setTeardown(ctx context.Context, cancel context.CancelFunc) {
	e.stateMu.Lock()
	e.teardown = ctx
	e.teardownCancel = cancel
	e.stateMu.Unlock()
}

// clearTeardown drops the entry's teardown context and cancel func
// (thread-safe).
func (e *serviceEntry) clearTeardown() {
	e.stateMu.Lock()
	e.teardown = nil
	e.teardownCancel = nil
	e.stateMu.Unlock()
}

// teardownActive reports whether this entry is being torn down: the removing
// flag is set, or the per-entry teardown context was cancelled. An exit that
// observes an active teardown is reported StatusStopped rather than Crashed, so
// StopService/Unregister owns the terminal status.
func (e *serviceEntry) teardownActive() bool {
	if e.removing.Load() {
		return true
	}
	if tc := e.getTeardown(); tc != nil && tc.Err() != nil {
		return true
	}
	return false
}

// addOwner records id as a live Messenger owner of the entry (one per running
// instance or cron tick). Thread-safe.
func (e *serviceEntry) addOwner(id uint64) {
	e.ownerMu.Lock()
	if e.owners == nil {
		e.owners = make(map[uint64]struct{})
	}
	e.owners[id] = struct{}{}
	e.ownerMu.Unlock()
}

// removeOwner drops id from the entry's live owners. Thread-safe.
func (e *serviceEntry) removeOwner(id uint64) {
	e.ownerMu.Lock()
	delete(e.owners, id)
	e.ownerMu.Unlock()
}

// takeOwners returns the entry's live owner ids and clears the set, so a
// teardown drains exactly what is still live. Thread-safe.
func (e *serviceEntry) takeOwners() []uint64 {
	e.ownerMu.Lock()
	defer e.ownerMu.Unlock()
	ids := make([]uint64, 0, len(e.owners))
	for id := range e.owners {
		ids = append(ids, id)
	}
	e.owners = nil
	return ids
}

// currentOwner returns one live owner id of the entry, or 0 if none. It is used
// by introspection; a cron entry may have several owners at once.
func (e *serviceEntry) currentOwner() uint64 {
	e.ownerMu.Lock()
	defer e.ownerMu.Unlock()
	for id := range e.owners {
		return id
	}
	return 0
}

// cronGeneration returns the current schedule generation (thread-safe).
func (e *serviceEntry) cronGeneration() uint64 {
	e.cronTrackMu.Lock()
	defer e.cronTrackMu.Unlock()
	return e.cronGen
}

// releaseTeardownIf cancels and drops the entry's teardown context only when it
// is still the one the caller owns. A concurrent start installs a fresh teardown
// before the old instance's exit handler runs, and the old handler must not
// cancel it. Thread-safe.
func (e *serviceEntry) releaseTeardownIf(ctx context.Context) {
	e.stateMu.Lock()
	if e.teardown != ctx {
		e.stateMu.Unlock()
		return
	}
	cancel := e.teardownCancel
	e.teardown = nil
	e.teardownCancel = nil
	e.stateMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// startCronSchedule installs a fresh shared context for a cron schedule and
// resets the tick accounting, so a re-scheduled entry never inherits a drained
// or cancelled context. It returns the new schedule generation, which every tick
// must present to cronBegin/cronEnd. parent is the orchestrator context.
// Thread-safe.
func (e *serviceEntry) startCronSchedule(parent context.Context) uint64 {
	e.cronTrackMu.Lock()
	defer e.cronTrackMu.Unlock()
	// Release the previous generation's context before replacing it, so a
	// re-schedule does not leak uncancelled children of o.ctx.
	if e.cronCancel != nil {
		e.cronCancel()
	}
	e.cronGen++
	e.cronCtx, e.cronCancel = context.WithCancel(parent)
	e.cronActive = 0
	e.cronDraining = false
	e.cronDrained = nil
	return e.cronGen
}

// cronBegin marks a tick of generation gen as in flight and returns the schedule
// context every tick derives from. ok is false when the schedule is draining, is
// from another generation, or its context is already cancelled, so the tick must
// not run user code. Thread-safe.
func (e *serviceEntry) cronBegin(gen uint64) (context.Context, bool) {
	e.cronTrackMu.Lock()
	defer e.cronTrackMu.Unlock()
	if e.cronDraining || e.cronCtx == nil || gen != e.cronGen || e.cronCtx.Err() != nil {
		return nil, false
	}
	e.cronActive++
	return e.cronCtx, true
}

// cronEnd marks a tick of generation gen finished and closes the drain latch
// once the last one of that generation exits. A stale generation is ignored so
// it cannot drive the current schedule's counter negative. Thread-safe.
func (e *serviceEntry) cronEnd(gen uint64) {
	e.cronTrackMu.Lock()
	defer e.cronTrackMu.Unlock()
	if gen != e.cronGen {
		return
	}
	e.cronActive--
	if e.cronActive == 0 && e.cronDraining && e.cronDrained != nil {
		close(e.cronDrained)
		e.cronDrained = nil
	}
}

// cronDrain latches the schedule as draining (refusing any tick not yet past
// cronBegin), cancels every in-flight tick, and returns a channel closed once
// they have all exited. It returns nil when nothing is in flight, so callers can
// skip the wait. Idempotent. Thread-safe.
func (e *serviceEntry) cronDrain() <-chan struct{} {
	e.cronTrackMu.Lock()
	defer e.cronTrackMu.Unlock()
	e.cronDraining = true
	if e.cronCancel != nil {
		e.cronCancel()
	}
	if e.cronActive == 0 {
		return nil
	}
	if e.cronDrained == nil {
		e.cronDrained = make(chan struct{})
	}
	return e.cronDrained
}

// Orchestrator manages service lifecycles.
type Orchestrator struct {
	cfg     config
	started bool
	// stopping is true from the moment Stop begins until it returns; stopped is
	// true once Stop has completed. Both are guarded by mu and gate dynamic
	// membership ops (C6, D10).
	stopping bool
	stopped  bool
	mu       sync.RWMutex // protects started, stopping, stopped, entries slice, nameIndex

	// membershipMu serializes StopService, Unregister, StartGroup and StopGroup
	// so their entry selection and reservation cannot interleave (C3, D16). It
	// is released before any user code runs (Start/Stop and hooks), so a service
	// may call a membership op from its own Start or Stop without deadlocking.
	membershipMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	logCh       chan logEntry
	logQuit     chan struct{} // closed to signal the log-pump to drain and exit
	logPumpDone chan struct{} // closed when the log-pump goroutine exits
	messenger   *Messenger

	cronSched *cron.Cron
	entries   []*serviceEntry
	nameIndex map[string]*serviceEntry // name -> entry lookup
	autoSeq   int                      // auto-name sequence counter
	ownerSeq  atomic.Uint64            // monotonic Messenger owner-id source

	// Status tracking
	statusMu sync.RWMutex

	// Health check
	healthCancel context.CancelFunc // cancel health-check loop goroutine
	healthDone   chan struct{}      // closed when health-check loop exits

	wg        sync.WaitGroup
	stopOnce  sync.Once
	startOnce sync.Once

	// doneCh lazily caches the Done() channel via sync.OnceValue.
	doneCh func() <-chan struct{}

	metricsStarts      atomic.Int64
	metricsStops       atomic.Int64
	metricsCrashes     atomic.Int64
	metricsRestarts    atomic.Int64
	metricsHealthFails atomic.Int64
}

// New creates a new Orchestrator. Each call returns a fresh, independent instance.
// Orchestrators can be nested: a service may create its own gorch to manage
// sub-services. Configure via Option functions; the zero-option call uses the
// defaults (LogLevelInfo, health checks every 30s with a 5s probe timeout, a 30s
// failed-Start rollback budget).
func New(opts ...Option) *Orchestrator {
	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if !cfg.logLevelSet {
		cfg.LogLevel = LogLevelInfo
	}
	// Health defaults (or disable when explicitly requested).
	if cfg.healthDisabled {
		cfg.HealthInterval = 0
	} else {
		if cfg.HealthInterval == 0 {
			cfg.HealthInterval = 30 * time.Second
		}
		if cfg.HealthTimeout == 0 {
			cfg.HealthTimeout = 5 * time.Second
		}
		if cfg.HealthThreshold == 0 {
			cfg.HealthThreshold = 3
		}
	}
	// A failed Start's rollback is bounded by default so a service that blocks in
	// Stop() cannot hang Start; 0 selects the 30s default, negative disables it.
	if cfg.failedStartTimeout == 0 {
		cfg.failedStartTimeout = 30 * time.Second
	}
	o := &Orchestrator{
		cfg:       cfg,
		messenger: newMessenger(),
		nameIndex: make(map[string]*serviceEntry),
	}
	o.doneCh = sync.OnceValue(func() <-chan struct{} {
		ch := make(chan struct{})
		go func() {
			o.wg.Wait()
			if o.logPumpDone != nil {
				<-o.logPumpDone
			}
			close(ch)
		}()
		return ch
	})
	return o
}

// Register adds a service to the orchestrator.
//
// Before Start the registry is static: any service may be added, and the whole
// graph is validated at once. After Start (a "hot add") the service is accepted
// into the live graph as StatusRegistered but is NOT auto-started; use
// StartService to start it. While Stop is in progress Register returns
// ErrOrchestratorStopping, and after Stop it returns ErrOrchestratorStopped.
//
// Returns ErrDuplicateName if WithName conflicts with another service.
// Returns ErrDependencyCycle if DependsOn introduces a cycle.
// Returns ErrDependencyNotFound if DependsOn names a service that is not yet
// registered: a hard dependency must always be registered before the service
// that names it, both statically and on a hot add.
// Returns ErrDependencyRemoving if a hot add names a hard dependency that is
// being removed (stopped/removed concurrently): a retryable "not now"
// condition, distinct from ErrHasDependents.
// Returns ErrNilService if svc is nil.
// Thread-safe.
func (o *Orchestrator) Register(svc Service, opts ...RegisterOption) error {
	if svc == nil {
		return fmt.Errorf("%w: Register called with a nil Service", ErrNilService)
	}
	o.mu.Lock()
	if o.started {
		o.mu.Unlock()
		return o.registerDynamic(svc, opts)
	}

	// Static registration is pre-Start. Validate is user code: it must never run
	// under o.mu (a blocking or reentrant Validator would deadlock), matching the
	// dynamic path. The common no-validator case stays entirely under the lock.
	if _, ok := svc.(Validator); !ok {
		cfg, err := o.parseRegisterOptions(opts)
		if err != nil {
			o.mu.Unlock()
			return err
		}
		o.appendStaticEntryLocked(svc, cfg)
		o.mu.Unlock()
		return nil
	}

	// Resolve the requested name for the validation error without touching the
	// graph, then run Validate outside the lock.
	var pre registerConfig
	for _, opt := range opts {
		opt(&pre)
	}
	o.mu.Unlock()

	if err := callErr(svc.(Validator).Validate); err != nil {
		return fmt.Errorf("gorch: service %s validation failed: %w", pre.name, err)
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	// Start may have won the race while Validate ran: the service is now a live
	// hot add, appended as StatusRegistered (never auto-started). Validation has
	// already run, so it is not repeated.
	if o.started {
		cfg, err := o.parseRegisterOptions(opts)
		if err != nil {
			return err
		}
		return o.commitDynamicLocked(svc, cfg)
	}
	cfg, err := o.parseRegisterOptions(opts)
	if err != nil {
		return err
	}
	o.appendStaticEntryLocked(svc, cfg)
	return nil
}

// appendStaticEntryLocked adds a pre-Start entry to the registry. The caller
// must hold o.mu and have parsed cfg via parseRegisterOptions.
func (o *Orchestrator) appendStaticEntryLocked(svc Service, cfg registerConfig) {
	entry := &serviceEntry{svc: svc, cfg: cfg, name: cfg.name, status: StatusRegistered}
	o.entries = append(o.entries, entry)
	o.nameIndex[cfg.name] = entry
}

// parseRegisterOptions applies opts and performs the structural validation that
// does not call user code: the self-heal combination check, auto-naming,
// duplicate-name detection, hard-dependency existence and cycle detection, and
// soft-dependency cycle detection. The caller must hold o.mu, because the graph
// is inspected via lookupEntry/dependsOnRecursive. A missing hard dependency is
// reported with ErrDependencyNotFound on both the static and the dynamic path,
// so callers can classify it with errors.Is regardless of when they register.
func (o *Orchestrator) parseRegisterOptions(opts []RegisterOption) (registerConfig, error) {
	cfg := registerConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	// Self-heal is only wired into the persistent-service path. A cron tick and a
	// runOnce gate never consume the factory, so reject the combination instead
	// of silently ignoring it.
	if cfg.factory != nil && (cfg.cronSpec != "" || cfg.runOnce) {
		return cfg, fmt.Errorf("%w: WithSelfHeal cannot be combined with WithCron or WithRunOnce", ErrUnsupportedOption)
	}

	// Auto-name if no WithName set.
	o.autoSeq++
	if cfg.name == "" {
		cfg.name = fmt.Sprintf("$%d", o.autoSeq)
	}

	// Validate uniqueness.
	if _, exists := o.nameIndex[cfg.name]; exists {
		return cfg, fmt.Errorf("%w: %s", ErrDuplicateName, cfg.name)
	}

	// Validate all dependencies exist and detect cycles.
	for _, dep := range cfg.dependsOn {
		if dep == cfg.name {
			return cfg, fmt.Errorf("%w: service %s depends on itself", ErrDependencyCycle, cfg.name)
		}
		depEntry := o.lookupEntry(dep)
		if depEntry == nil {
			return cfg, fmt.Errorf("%w: dependency %q not found for service %s", ErrDependencyNotFound, dep, cfg.name)
		}
		if depEntry.removing.Load() {
			return cfg, fmt.Errorf("%w: dependency %q is being removed for service %s", ErrDependencyRemoving, dep, cfg.name)
		}
		// Check if dep transitively depends on cfg.name (would create a cycle).
		if o.dependsOnRecursive(depEntry, cfg.name) {
			return cfg, fmt.Errorf("%w: %s -> %s", ErrDependencyCycle, cfg.name, dep)
		}
	}

	// Soft dependencies: only validate cycles when the target is already
	// registered; missing targets are tolerated by design.
	for _, dep := range cfg.softDependsOn {
		if dep == cfg.name {
			return cfg, fmt.Errorf("%w: service %s soft-depends on itself", ErrDependencyCycle, cfg.name)
		}
		depEntry := o.lookupEntry(dep)
		if depEntry == nil {
			continue
		}
		if o.dependsOnRecursive(depEntry, cfg.name) {
			return cfg, fmt.Errorf("%w: %s -> %s", ErrDependencyCycle, cfg.name, dep)
		}
	}

	return cfg, nil
}

// registerDynamic adds a service to a running orchestrator. The entry is
// appended as StatusRegistered and is never auto-started (D1); persistent and
// runOnce services wait for StartService. A cron entry is added to the live
// scheduler immediately (so the schedule is live) but still reports
// StatusRegistered until StartService marks it running. Validate runs without
// the orchestrator lock held, so user code can never deadlock against a
// membership op (the structural graph checks above it run under the lock).
func (o *Orchestrator) registerDynamic(svc Service, opts []RegisterOption) error {
	o.mu.Lock()
	if err := o.membershipGateLocked(); err != nil {
		o.mu.Unlock()
		return err
	}
	cfg, err := o.parseRegisterOptions(opts)
	if err != nil {
		o.mu.Unlock()
		return err
	}
	o.mu.Unlock()

	// Validate is user code: never invoke it while holding the orchestrator lock.
	if v, ok := svc.(Validator); ok {
		if err := callErr(v.Validate); err != nil {
			return fmt.Errorf("gorch: service %s validation failed: %w", cfg.name, err)
		}
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	return o.commitDynamicLocked(svc, cfg)
}

// commitDynamicLocked appends a hot-added entry to a running orchestrator. The
// caller must hold o.mu; Validator.Validate (if any) must already have run
// outside the lock. The entry stays StatusRegistered and is never auto-started
// (D1); a cron entry is only validated here, not scheduled, so its schedule and
// status stay consistent until StartService.
func (o *Orchestrator) commitDynamicLocked(svc Service, cfg registerConfig) error {
	if err := o.membershipGateLocked(); err != nil {
		return err
	}
	if _, exists := o.nameIndex[cfg.name]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateName, cfg.name)
	}
	if cfg.cronSpec != "" {
		if err := validateCronSpec(cfg.cronSpec); err != nil {
			return err
		}
	}
	// Read the log fields under o.mu: a concurrent Start publishes them in one
	// critical section, so reading them without the lock would race.
	entry := &serviceEntry{svc: svc, cfg: cfg, name: cfg.name, status: StatusRegistered}
	if o.cfg.Logger != nil {
		entry.setLogger(newServiceLoggerWith(entry.name, o.cfg.Logger))
	} else {
		entry.setLogger(newServiceLogger(entry.name, o.logCh, o.logQuit, o.cfg.LogLevel))
	}
	o.entries = append(o.entries, entry)
	o.nameIndex[cfg.name] = entry
	return nil
}

// membershipGateLocked rejects dynamic membership once whole-orchestrator
// shutdown has begun. The caller must hold o.mu.
func (o *Orchestrator) membershipGateLocked() error {
	if o.stopping {
		return ErrOrchestratorStopping
	}
	if o.stopped {
		return ErrOrchestratorStopped
	}
	return nil
}

// lookupEntry returns the registered entry with the given name, searching the
// nameIndex first and then the entries slice (for entries registered in the
// same batch). Returns nil if no such entry exists.
func (o *Orchestrator) lookupEntry(name string) *serviceEntry {
	if e, ok := o.nameIndex[name]; ok {
		return e
	}
	for _, e := range o.entries {
		if e.cfg.name == name {
			return e
		}
	}
	return nil
}

// Busy reports whether the named service is registered and currently holds an
// in-flight membership reservation: a Start is running, a Stop is tearing it
// down, or a group operation has reserved it. It is the predicate a caller can
// poll to decide whether StartService/StopService/Unregister would be rejected
// with the transient ErrMembershipBusy, instead of racing and retrying blind. It
// returns false for an unknown name and for a registered but idle entry.
// Thread-safe.
func (o *Orchestrator) Busy(name string) bool {
	o.mu.RLock()
	entry := o.lookupEntry(name)
	o.mu.RUnlock()
	if entry == nil {
		return false
	}
	return entry.starting.Load() || entry.removing.Load()
}

// dependsOnRecursive checks whether entry transitively depends on target via
// either hard or soft dependency edges.
// ponytail: DFS on small graphs (registration-time only); O(V+E) fine.
func (o *Orchestrator) dependsOnRecursive(entry *serviceEntry, target string) bool {
	if entry == nil {
		return false
	}
	for _, dep := range entry.cfg.dependsOn {
		if dep == target {
			return true
		}
		if o.dependsOnRecursive(o.lookupEntry(dep), target) {
			return true
		}
	}
	for _, dep := range entry.cfg.softDependsOn {
		if dep == target {
			return true
		}
		if o.dependsOnRecursive(o.lookupEntry(dep), target) {
			return true
		}
	}
	return false
}

// Start begins the orchestrator lifecycle. Returns ErrAlreadyStarted if already started.
// If Start fails, the orchestrator is reset and may be started again (e.g. to retry
// after a transient dependency failure). The rollback is bounded by
// WithFailedStartTimeout (default 30s) and shares one budget across every stop and
// the final wait, so a service that blocks in Stop() cannot hang Start; an overrun
// is reported as ErrStopTimeout and the reset is best-effort.
// A persistent service that returns an error synchronously aborts Start only when a
// start timeout is set; without one its launch is fire-and-forget by construction.
// The whole-orchestrator lifecycle is single-shot: after a successful Stop it cannot
// be restarted, and a subsequent Start returns ErrAlreadyStarted. Registering after
// Stop returns ErrOrchestratorStopped instead.
// Thread-safe.
func (o *Orchestrator) Start() error {
	o.mu.Lock()
	if o.started {
		o.mu.Unlock()
		return ErrAlreadyStarted
	}
	o.mu.Unlock()

	var startErr error
	// rollbackDeadline bounds the failed-Start cleanup. It is captured at the
	// moment of failure (not at Start's beginning) so the synchronous startup
	// work that preceded the failure does not consume the reset budget.
	var rollbackDeadline time.Time
	// entries is the graph this Start owns; it is set under o.mu inside the
	// startOnce closure and reused by resetAfterStartFailure on a failed start.
	var entries []*serviceEntry
	o.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		// Only create log channels when using the default (channel-based) logger.
		var logCh chan logEntry
		var logQuit chan struct{}
		var logPumpDone chan struct{}
		if o.cfg.Logger == nil {
			logCh = make(chan logEntry, 256)
			logQuit = make(chan struct{})
			logPumpDone = make(chan struct{})
		}

		// Publish the runtime fields and snapshot the graph in one critical
		// section, before started=true is observable. A concurrent Register
		// therefore either lands before the snapshot (and is included) or sees a
		// live orchestrator and appends on the dynamic path under o.mu. Start then
		// operates only on the snapshot, so it never reads a field a hot
		// Register is mutating. membershipMu reserves every snapshotted entry
		// (starting=true) so a StopService/Unregister racing the gap before
		// startOneService is rejected rather than tearing an entry down mid-start
		// (D16). The subscription is released by clearStarting once Start's
		// synchronous work is done.
		o.membershipMu.Lock()
		o.mu.Lock()
		o.started = true
		// A no-op Stop() before Start() is harmless but consumes stopOnce; clear it
		// now that a genuine start is under way so the real shutdown is not skipped.
		o.stopOnce = sync.Once{}
		o.ctx = ctx
		o.cancel = cancel
		o.logCh = logCh
		o.logQuit = logQuit
		o.logPumpDone = logPumpDone
		o.cronSched = cron.New(cron.WithSeconds())
		entries = make([]*serviceEntry, len(o.entries))
		copy(entries, o.entries)
		nameIndex := make(map[string]*serviceEntry, len(o.nameIndex))
		for name, e := range o.nameIndex {
			nameIndex[name] = e
		}
		for _, e := range entries {
			e.starting.Store(true)
		}
		o.mu.Unlock()
		o.membershipMu.Unlock()
		defer o.clearStarting(entries)

		// Assign loggers keyed by the service name (WithName or auto "$N"), so
		// log output correlates with Status/name lookups.
		for _, entry := range entries {
			if o.cfg.Logger != nil {
				entry.setLogger(newServiceLoggerWith(entry.name, o.cfg.Logger))
			} else {
				entry.setLogger(newServiceLogger(entry.name, logCh, logQuit, o.cfg.LogLevel))
			}
		}

		// Spawn log-pump goroutine (default logger only).
		if o.cfg.Logger == nil {
			go o.logPump(logCh, logQuit, logPumpDone)
		}

		// Set up and start the cron scheduler.
		if err := o.setupCron(entries); err != nil {
			cancel()
			rollbackDeadline = o.failedStartDeadline()
			if logQuit != nil {
				close(logQuit)
				// Bounded wait: the reset below repeats it and surfaces
				// ErrStopTimeout if the log-pump is still stuck.
				_ = o.awaitDone(logPumpDone, rollbackDeadline)
			}
			o.cronSched.Stop()
			startErr = errors.Join(startErr, err)
			return
		}

		// Partition: runOnce vs persistent services.
		var runOnce, persistent []*serviceEntry
		for _, entry := range entries {
			if entry.cfg.cronSpec != "" {
				continue // cron services don't go through Start goroutine
			}
			if entry.cfg.runOnce {
				runOnce = append(runOnce, entry)
			} else {
				persistent = append(persistent, entry)
			}
		}

		// Phase 1: run runOnce services sequentially (they're gates).
		for _, entry := range runOnce {
			if err := o.startOneService(entry); err != nil {
				startErr = errors.Join(startErr, fmt.Errorf("%s: %w", entry.name, err))
				// runOnce failure aborts — do not start persistent services.
				rollbackDeadline = o.failedStartDeadline()
				startErr = errors.Join(startErr, o.stopStartedServices(entries, rollbackDeadline))
				return
			}
		}

		// Phase 2: persistent services in topological order.
		levels, topoErr := o.topoSort(persistent)
		if topoErr != nil {
			startErr = errors.Join(startErr, topoErr)
			rollbackDeadline = o.failedStartDeadline()
			startErr = errors.Join(startErr, o.stopStartedServices(entries, rollbackDeadline))
			return
		}

		for _, level := range levels {
			// Start all services in this level in parallel.
			var wg sync.WaitGroup
			var mu sync.Mutex
			var failed map[string]error

			for _, entry := range level {
				wg.Add(1)
				go func(e *serviceEntry) {
					defer wg.Done()
					// Check if any dependencies failed.
					for _, dep := range e.cfg.dependsOn {
						depEntry := nameIndex[dep]
						o.statusMu.RLock()
						depStatus := depEntry.status
						o.statusMu.RUnlock()
						if depStatus == StatusCrashed || depStatus == StatusStopped {
							failureMsg := fmt.Sprintf("dependency %s failed or was skipped", dep)
							mu.Lock()
							if failed == nil {
								failed = make(map[string]error)
							}
							failed[e.name] = fmt.Errorf("%w: %s", ErrStartAborted, failureMsg)
							mu.Unlock()
							o.setStatus(e, StatusStopped)
							return
						}
					}
					// Soft dependencies: check if registered, skip if missing.
					for _, dep := range e.cfg.softDependsOn {
						depEntry, ok := nameIndex[dep]
						if !ok {
							continue
						}
						o.statusMu.RLock()
						depStatus := depEntry.status
						o.statusMu.RUnlock()
						if depStatus == StatusCrashed || depStatus == StatusStopped {
							failureMsg := fmt.Sprintf("soft dependency %s failed or was skipped", dep)
							mu.Lock()
							if failed == nil {
								failed = make(map[string]error)
							}
							failed[e.name] = fmt.Errorf("%w: %s", ErrStartAborted, failureMsg)
							mu.Unlock()
							o.setStatus(e, StatusStopped)
							return
						}
					}
					if err := o.startOneService(e); err != nil {
						mu.Lock()
						if failed == nil {
							failed = make(map[string]error)
						}
						failed[e.name] = err
						mu.Unlock()
					}
				}(entry)
			}
			wg.Wait()

			for name, err := range failed {
				startErr = errors.Join(startErr, fmt.Errorf("%s: %w", name, err))
			}

			// If any in this level failed, stop all and skip remaining levels.
			if len(failed) > 0 {
				rollbackDeadline = o.failedStartDeadline()
				startErr = errors.Join(startErr, o.stopStartedServices(entries, rollbackDeadline))
				return
			}
		}

		// Start health-check loop.
		if o.cfg.HealthInterval > 0 {
			healthCtx, hCancel := context.WithCancel(context.Background())
			o.healthCancel = hCancel
			o.healthDone = make(chan struct{})
			o.wg.Add(1)
			go o.healthCheckLoop(healthCtx)
		}
	})
	if startErr != nil {
		startErr = errors.Join(startErr, o.resetAfterStartFailure(entries, rollbackDeadline))
	}
	return startErr
}

// failedStartDeadline computes the deadline for a failed-Start rollback from the
// configured failedStartTimeout. It returns the zero time.Time when the
// effective budget is non-positive, meaning the rollback is unbounded.
func (o *Orchestrator) failedStartDeadline() time.Time {
	if o.cfg.failedStartTimeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(o.cfg.failedStartTimeout)
}

// resetAfterStartFailure rolls back all state mutated by a failed Start so the
// orchestrator can be started again. It resets only the entries that Start
// snapshotted: a service hot-added while the failing Start ran keeps its
// registration state and logger. It first waits for all service goroutines and
// the log-pump to fully wind down (they were signalled by stopStartedServices or
// the cron-error path); both waits share deadline, and on expiry it joins
// ErrStopTimeout into the returned error and still performs the reset. The reset
// is therefore best-effort: a goroutine that ignores cancellation and outlives
// the failed Start may keep running, but the orchestrator is left restartable so
// Start can be retried.
func (o *Orchestrator) resetAfterStartFailure(entries []*serviceEntry, deadline time.Time) error {
	var resetErr error

	// Bound the orchestrator-wide wait for instance goroutines. It is
	// orchestrator-wide, not just the Start snapshot: a StartService that ran
	// during the failing Start also fed o.wg, so one instance ignoring
	// cancellation must not hang the reset.
	wgDone := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(wgDone)
	}()
	if !o.awaitDone(wgDone, deadline) {
		resetErr = errors.Join(resetErr, ErrStopTimeout)
	}
	// logPumpDone is nil when a custom Logger is set; awaitDone treats that as
	// already done.
	if !o.awaitDone(o.logPumpDone, deadline) {
		resetErr = errors.Join(resetErr, ErrStopTimeout)
	}

	o.mu.Lock()
	o.started = false
	o.stopping = false
	o.stopped = false
	o.ctx = nil
	o.cancel = nil
	o.cronSched = nil
	o.logCh = nil
	o.logQuit = nil
	o.logPumpDone = nil
	o.healthCancel = nil
	o.healthDone = nil

	for _, entry := range entries {
		entry.status = StatusRegistered
		entry.wgDone = false
		entry.setCancel(nil)
		entry.setDone(nil)
		entry.clearTeardown()
		entry.setRetryCount(0)
		entry.setHealthFailures(0)
		entry.setStableSince(time.Time{})
		entry.setLogger(nil)
	}
	o.mu.Unlock()
	o.startOnce = sync.Once{}
	o.stopOnce = sync.Once{}
	return resetErr
}

// Stop shuts down the orchestrator, bounding the whole shutdown by timeout:
// every service's before/after-stop hooks and Stop() call, plus the wait for
// goroutines to exit, share the one budget. Returns aggregated errors from all
// Stop failures, or ErrStopTimeout if the deadline is exceeded.
// Thread-safe. Safe to call on an orchestrator that was never started (no-op).
// The whole-orchestrator lifecycle is single-shot: after a successful Stop it
// cannot be restarted, and a subsequent Start returns ErrAlreadyStarted.
// Registering after Stop returns ErrOrchestratorStopped instead.
func (o *Orchestrator) Stop(timeout time.Duration) error {
	var stopErr error
	o.stopOnce.Do(func() {
		o.mu.RLock()
		if !o.started {
			o.mu.RUnlock()
			return
		}
		o.mu.RUnlock()

		// Mark shutdown as in progress so dynamic membership ops are rejected
		// while Stop tears the graph down (C6).
		o.mu.Lock()
		o.stopping = true
		o.mu.Unlock()

		// One deadline for the whole shutdown: the per-service stop sequence and
		// the final wait share it, so a blocking hook cannot outlast the caller.
		var stopDeadline time.Time
		if timeout > 0 {
			stopDeadline = time.Now().Add(timeout)
		}

		// Stop health-check loop.
		if o.healthCancel != nil {
			o.healthCancel()
			<-o.healthDone // wait for health loop goroutine to exit before touching logCh
		}

		// 1. Cancel context to signal all services.
		o.cancel()

		// 2. Stop cron scheduler (waits for in-flight cron jobs).
		if o.cronSched != nil {
			<-o.cronSched.Stop().Done()
		}

		// 3. Call Stop() on services in reverse topological order. Each stop is
		// recorded so its terminal status can be committed only after the final
		// wait proves every instance goroutine exited.
		type pendingStop struct {
			entry     *serviceEntry
			wasActive bool
			completed bool
		}
		var pending []pendingStop
		stopOne := func(entry *serviceEntry) {
			wasActive, completed, err := o.stopOneServiceDeadline(entry, stopDeadline)
			pending = append(pending, pendingStop{entry: entry, wasActive: wasActive, completed: completed})
			if err != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("%s: %w", entry.name, err))
			}
		}
		persistent := o.persistentEntries()
		levels, topoErr := o.topoSortForStop(persistent)
		// A cyclic subset still stops every persistent entry (registration-order
		// fallback); surface the ordering failure rather than leaving them running.
		stopErr = errors.Join(stopErr, topoErr)
		for i := len(levels) - 1; i >= 0; i-- {
			for _, entry := range levels[i] {
				stopOne(entry)
			}
		}
		// Also stop any remaining entries not in levels (e.g., cron-only, runOnce that failed).
		for _, entry := range o.entries {
			if entry.cfg.runOnce || entry.cfg.cronSpec != "" {
				stopOne(entry)
			}
		}

		// 4. Signal log-pump to drain and exit.
		if o.logQuit != nil {
			close(o.logQuit)
		}

		// 5. Clean up messenger subscriptions.
		o.messenger.Drain()

		// 6. Wait for all services + log-pump with whatever budget is left. A
		// non-positive timeout waits indefinitely. Only once every goroutine has
		// exited is the stop verified and its terminal status committed; on a
		// timeout the entries stay StatusStopping rather than falsely claiming
		// they stopped.
		done := make(chan struct{})
		go func() {
			o.wg.Wait()
			if o.logPumpDone != nil {
				<-o.logPumpDone
			}
			close(done)
		}()
		allDone := false
		if stopDeadline.IsZero() {
			<-done
			allDone = true
		} else {
			remaining := max(time.Until(stopDeadline), 0)
			select {
			case <-done:
				allDone = true
			case <-time.After(remaining):
				stopErr = errors.Join(stopErr, ErrStopTimeout)
			}
		}
		if allDone {
			for _, p := range pending {
				if p.completed {
					o.finishStop(p.entry, p.wasActive)
				}
			}
		}

		// Shutdown is complete: persist the terminal flag so later membership
		// ops report ErrOrchestratorStopped rather than ErrOrchestratorStopping.
		o.mu.Lock()
		o.stopping = false
		o.stopped = true
		o.mu.Unlock()
	})
	return stopErr
}

// Run starts the orchestrator, blocks on SIGINT/SIGTERM, then stops.
// Returns any error from Start or aggregated errors from Stop.
// Optional signals override the default signal set (SIGINT, SIGTERM).
func (o *Orchestrator) Run(stopTimeout time.Duration, signals ...os.Signal) error {
	if err := o.Start(); err != nil {
		return err
	}

	sigSet := signals
	if len(sigSet) == 0 {
		sigSet = []os.Signal{os.Interrupt, syscall.SIGTERM}
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, sigSet...)
	<-ch
	signal.Stop(ch)

	return o.Stop(stopTimeout)
}

// RegisterFunc registers a closure-based service under the given name.
// Thread-safe.
func (o *Orchestrator) RegisterFunc(name string, startFn func(ctx ServiceContext) error, stopFn func() error, opts ...RegisterOption) error {
	if startFn == nil {
		return fmt.Errorf("%w: RegisterFunc called with a nil start function", ErrNilService)
	}
	svc := &funcService{startFn: startFn, stopFn: stopFn}
	allOpts := make([]RegisterOption, 0, len(opts)+1)
	allOpts = append(allOpts, WithName(name))
	allOpts = append(allOpts, opts...)
	return o.Register(svc, allOpts...)
}
