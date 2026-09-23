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
	owner     uint64 // per-instance Messenger ownership id
	status    ServiceStatus
	cancel    context.CancelFunc // per-service cancellation (nil until started)
	startedAt time.Time          // when the current instance started

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
	// starting is true while startOneService is invoking this entry's user code
	// synchronously on the caller's goroutine. It lets a reentrant membership
	// op (a service starting itself from its own Start) be rejected instead of
	// recursing (C17).
	starting atomic.Bool
	// removing is true while a StopService/Unregister is tearing this entry
	// down. It rejects new hard-dependency edges from Register (C11).
	removing atomic.Bool
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
	// CronQueue serialization lock (serialize ticks mode)
	cronMu sync.Mutex
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
// defaults (LogLevelInfo, health checks every 30s with a 5s probe timeout).
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
// Returns ErrDependencyNotFound if a dynamic Register names an unknown hard
// dependency.
// Thread-safe.
func (o *Orchestrator) Register(svc Service, opts ...RegisterOption) error {
	o.mu.Lock()
	if o.started {
		o.mu.Unlock()
		return o.registerDynamic(svc, opts)
	}
	cfg, err := o.parseRegisterOptions(opts, false)
	if err != nil {
		o.mu.Unlock()
		return err
	}

	// Static registration happens under the lock (historical behavior); the
	// dynamic path deliberately moves Validate outside it.
	if v, ok := svc.(Validator); ok {
		if err := v.Validate(); err != nil {
			o.mu.Unlock()
			return fmt.Errorf("gorch: service %s validation failed: %w", cfg.name, err)
		}
	}

	entry := &serviceEntry{svc: svc, cfg: cfg, name: cfg.name, owner: o.ownerSeq.Add(1), status: StatusRegistered}
	o.entries = append(o.entries, entry)
	o.nameIndex[cfg.name] = entry
	o.mu.Unlock()
	return nil
}

// parseRegisterOptions applies opts and performs the structural validation that
// does not call user code: the self-heal combination check, auto-naming,
// duplicate-name detection, hard-dependency existence and cycle detection, and
// soft-dependency cycle detection. The caller must hold o.mu, because the graph
// is inspected via lookupEntry/dependsOnRecursive. dynamic selects the
// ErrDependencyNotFound sentinel for a missing hard dependency (static
// registration keeps its historical message).
func (o *Orchestrator) parseRegisterOptions(opts []RegisterOption, dynamic bool) (registerConfig, error) {
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
			if dynamic {
				return cfg, fmt.Errorf("%w: dependency %q not found for service %s", ErrDependencyNotFound, dep, cfg.name)
			}
			return cfg, fmt.Errorf("gorch: dependency %q not found for service %s", dep, cfg.name)
		}
		if depEntry.removing.Load() {
			return cfg, fmt.Errorf("%w: dependency %q is being removed for service %s", ErrHasDependents, dep, cfg.name)
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
	cfg, err := o.parseRegisterOptions(opts, true)
	if err != nil {
		o.mu.Unlock()
		return err
	}
	// Capture the log fields under o.mu: a concurrent Start publishes them in
	// one critical section, so reading them here without the lock would race.
	customLogger := o.cfg.Logger
	logCh := o.logCh
	logQuit := o.logQuit
	logLevel := o.cfg.LogLevel
	o.mu.Unlock()

	// Validate is user code: never invoke it while holding the orchestrator lock.
	if v, ok := svc.(Validator); ok {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("gorch: service %s validation failed: %w", cfg.name, err)
		}
	}

	entry := &serviceEntry{svc: svc, cfg: cfg, name: cfg.name, owner: o.ownerSeq.Add(1), status: StatusRegistered}
	if customLogger != nil {
		entry.setLogger(newServiceLoggerWith(entry.name, customLogger))
	} else {
		entry.setLogger(newServiceLogger(entry.name, logCh, logQuit, logLevel))
	}

	o.mu.Lock()
	if _, exists := o.nameIndex[cfg.name]; exists {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrDuplicateName, cfg.name)
	}
	if cfg.cronSpec != "" {
		if err := o.scheduleEntry(entry); err != nil {
			o.mu.Unlock()
			return err
		}
	}
	o.entries = append(o.entries, entry)
	o.nameIndex[cfg.name] = entry
	o.mu.Unlock()
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
// after a transient dependency failure).
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
		// Register is mutating.
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
		o.mu.Unlock()

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
			go o.logPump()
		}

		// Set up and start the cron scheduler.
		if err := o.setupCron(entries); err != nil {
			cancel()
			if logQuit != nil {
				close(logQuit)
				<-logPumpDone
			}
			o.cronSched.Stop()
			startErr = err
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
				o.stopStartedServices(entries)
				return
			}
		}

		// Phase 2: persistent services in topological order.
		levels, topoErr := o.topoSort(persistent)
		if topoErr != nil {
			startErr = errors.Join(startErr, topoErr)
			o.stopStartedServices(entries)
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
				o.stopStartedServices(entries)
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
		o.resetAfterStartFailure(entries)
	}
	return startErr
}

// resetAfterStartFailure rolls back all state mutated by a failed Start so the
// orchestrator can be started again. It resets only the entries that Start
// snapshotted: a service hot-added while the failing Start ran keeps its
// registration state and logger. It first waits for all service goroutines and
// the log-pump to fully wind down (they were signalled by stopStartedServices or
// the cron-error path) before resetting.
func (o *Orchestrator) resetAfterStartFailure(entries []*serviceEntry) {
	o.wg.Wait()
	if o.logPumpDone != nil {
		<-o.logPumpDone
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
}

// Stop shuts down the orchestrator, waiting up to timeout for services to
// finish. Returns aggregated errors from all Stop failures, or ErrStopTimeout
// if services don't all stop within the timeout.
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

		// 3. Call Stop() on services in reverse topological order.
		persistent := o.persistentEntries()
		levels, _ := o.topoSort(persistent) // ignore error, graph already validated
		for i := len(levels) - 1; i >= 0; i-- {
			for _, entry := range levels[i] {
				err := o.stopOneService(entry)
				if err != nil {
					stopErr = errors.Join(stopErr, fmt.Errorf("%s: %w", entry.name, err))
				}
			}
		}
		// Also stop any remaining entries not in levels (e.g., cron-only, runOnce that failed).
		for _, entry := range o.entries {
			if entry.cfg.runOnce || entry.cfg.cronSpec != "" {
				err := o.stopOneService(entry)
				if err != nil {
					stopErr = errors.Join(stopErr, fmt.Errorf("%s: %w", entry.name, err))
				}
			}
		}

		// 4. Signal log-pump to drain and exit.
		if o.logQuit != nil {
			close(o.logQuit)
		}

		// 5. Clean up messenger subscriptions.
		o.messenger.Drain()

		// 6. Wait for all services + log-pump with timeout.
		done := make(chan struct{})
		go func() {
			o.wg.Wait()
			if o.logPumpDone != nil {
				<-o.logPumpDone
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(timeout):
			stopErr = errors.Join(stopErr, ErrStopTimeout)
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
	svc := &funcService{startFn: startFn, stopFn: stopFn}
	allOpts := make([]RegisterOption, 0, len(opts)+1)
	allOpts = append(allOpts, WithName(name))
	allOpts = append(allOpts, opts...)
	return o.Register(svc, allOpts...)
}
