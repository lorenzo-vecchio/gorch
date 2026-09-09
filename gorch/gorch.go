package gorch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
)

type serviceEntry struct {
	// stateMu guards svc, logger, cancel, retryCount, stableSince, and
	// healthFailures — the fields the self-heal restart path mutates while the
	// health-check loop and introspection methods (Health, IsReady) read them.
	stateMu sync.Mutex

	svc    Service
	cfg    registerConfig
	logger *ServiceLogger

	// runtime state
	name      string
	status    ServiceStatus
	cancel    context.CancelFunc // per-service cancellation (nil until started)
	startedAt time.Time          // when the current instance started

	// retry / backoff state
	retryCount  int
	stableSince time.Time // when the current stable run began (for resetAfter)

	// health check state
	healthFailures int

	// cron state
	cronID cron.EntryID
	// self-heal state for non-cron services
	wgDone bool // true once wg.Done() has been called for this entry
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

// Orchestrator manages service lifecycles.
type Orchestrator struct {
	cfg     config
	started bool
	mu      sync.RWMutex // protects started, entries slice, nameIndex

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

// Register adds a service to the orchestrator. Must be called before Start().
// Returns ErrAlreadyStarted if the orchestrator has already been started.
// Returns ErrDuplicateName if WithName conflicts with another service.
// Returns ErrDependencyCycle if DependsOn introduces a cycle.
// Thread-safe.
func (o *Orchestrator) Register(svc Service, opts ...RegisterOption) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return ErrAlreadyStarted
	}

	cfg := registerConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	// Auto-name if no WithName set.
	o.autoSeq++
	if cfg.name == "" {
		cfg.name = fmt.Sprintf("$%d", o.autoSeq)
	}

	// Validate uniqueness.
	if _, exists := o.nameIndex[cfg.name]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateName, cfg.name)
	}

	// Validate all dependencies exist and detect cycles.
	for _, dep := range cfg.dependsOn {
		if dep == cfg.name {
			return fmt.Errorf("%w: service %s depends on itself", ErrDependencyCycle, cfg.name)
		}
		depEntry := o.lookupEntry(dep)
		if depEntry == nil {
			return fmt.Errorf("gorch: dependency %q not found for service %s", dep, cfg.name)
		}
		// Check if dep transitively depends on cfg.name (would create a cycle).
		if o.dependsOnRecursive(depEntry, cfg.name) {
			return fmt.Errorf("%w: %s -> %s", ErrDependencyCycle, cfg.name, dep)
		}
	}

	// Soft dependencies: only validate cycles when the target is already
	// registered; missing targets are tolerated by design.
	for _, dep := range cfg.softDependsOn {
		if dep == cfg.name {
			return fmt.Errorf("%w: service %s soft-depends on itself", ErrDependencyCycle, cfg.name)
		}
		depEntry := o.lookupEntry(dep)
		if depEntry == nil {
			continue
		}
		if o.dependsOnRecursive(depEntry, cfg.name) {
			return fmt.Errorf("%w: %s -> %s", ErrDependencyCycle, cfg.name, dep)
		}
	}

	// Check Validator interface.
	if v, ok := svc.(Validator); ok {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("gorch: service %s validation failed: %w", cfg.name, err)
		}
	}

	entry := &serviceEntry{svc: svc, cfg: cfg, name: cfg.name, status: StatusRegistered}
	o.entries = append(o.entries, entry)
	o.nameIndex[cfg.name] = entry
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
// An orchestrator is single-shot: after a successful Stop it cannot be restarted; a
// subsequent Start (or Register) returns ErrAlreadyStarted.
// Thread-safe.
func (o *Orchestrator) Start() error {
	o.mu.Lock()
	if o.started {
		o.mu.Unlock()
		return ErrAlreadyStarted
	}
	o.mu.Unlock()

	var startErr error
	o.startOnce.Do(func() {
		o.mu.Lock()
		o.started = true
		// A no-op Stop() before Start() is harmless but consumes stopOnce; clear it
		// now that a genuine start is under way so the real shutdown is not skipped.
		o.stopOnce = sync.Once{}
		o.mu.Unlock()

		o.ctx, o.cancel = context.WithCancel(context.Background())
		// Only create log channel when using the default (channel-based) logger.
		if o.cfg.Logger == nil {
			o.logCh = make(chan logEntry, 256)
			o.logQuit = make(chan struct{})
			o.logPumpDone = make(chan struct{})
		}

		// Assign loggers: use cfg.name if WithName was set, else reflect type.
		for _, entry := range o.entries {
			svcName := entry.name
			// ponytail: if user didn't set WithName, the name is auto "$N".
			// Use reflect type for logging to keep backward compat.
			if svcName == "" || svcName[0] == '$' {
				svcName = reflect.TypeOf(entry.getSvc()).String()
			}
			if o.cfg.Logger != nil {
				entry.setLogger(newServiceLoggerWith(svcName, o.cfg.Logger))
			} else {
				entry.setLogger(newServiceLogger(svcName, o.logCh, o.logQuit, o.cfg.LogLevel))
			}
		}

		// Spawn log-pump goroutine (default logger only).
		if o.cfg.Logger == nil {
			go o.logPump()
		}

		// Set up and start the cron scheduler.
		if err := o.setupCron(); err != nil {
			o.cancel()
			if o.logQuit != nil {
				close(o.logQuit)
				<-o.logPumpDone
			}
			o.cronSched.Stop()
			startErr = err
			return
		}

		// Partition: runOnce vs persistent services.
		var runOnce, persistent []*serviceEntry
		for _, entry := range o.entries {
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
				o.stopStartedServices()
				return
			}
		}

		// Phase 2: persistent services in topological order.
		levels, topoErr := o.topoSort(persistent)
		if topoErr != nil {
			startErr = errors.Join(startErr, topoErr)
			o.stopStartedServices()
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
						depEntry := o.nameIndex[dep]
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
						depEntry, ok := o.nameIndex[dep]
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
				o.stopStartedServices()
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
		o.resetAfterStartFailure()
	}
	return startErr
}

// resetAfterStartFailure rolls back all state mutated by a failed Start so the
// orchestrator can be started again. It first waits for all service goroutines
// and the log-pump to fully wind down (they were signalled by stopStartedServices
// or the cron-error path) before resetting.
func (o *Orchestrator) resetAfterStartFailure() {
	o.wg.Wait()
	if o.logPumpDone != nil {
		<-o.logPumpDone
	}

	o.mu.Lock()
	o.started = false
	o.mu.Unlock()
	o.startOnce = sync.Once{}
	o.stopOnce = sync.Once{}

	o.ctx = nil
	o.cancel = nil
	o.cronSched = nil
	o.logCh = nil
	o.logQuit = nil
	o.logPumpDone = nil
	o.healthCancel = nil
	o.healthDone = nil

	for _, entry := range o.entries {
		entry.status = StatusRegistered
		entry.wgDone = false
		entry.setCancel(nil)
		entry.setRetryCount(0)
		entry.setHealthFailures(0)
		entry.setStableSince(time.Time{})
		entry.setLogger(nil)
	}
}

// Stop shuts down the orchestrator, waiting up to timeout for services to
// finish. Returns aggregated errors from all Stop failures, or ErrStopTimeout
// if services don't all stop within the timeout.
// Thread-safe. Safe to call on an orchestrator that was never started (no-op).
// An orchestrator is single-shot: after a successful Stop it cannot be
// restarted; a subsequent Start (or Register) returns ErrAlreadyStarted.
func (o *Orchestrator) Stop(timeout time.Duration) error {
	var stopErr error
	o.stopOnce.Do(func() {
		o.mu.RLock()
		if !o.started {
			o.mu.RUnlock()
			return
		}
		o.mu.RUnlock()

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
	})
	return stopErr
}

// stopOneService calls Stop on a service with hooks and panic recovery.
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

// Status returns the current lifecycle status of a named service.
// ok is false if no service with that name is registered.
// Thread-safe.
func (o *Orchestrator) RegisterFunc(name string, startFn func(ctx ServiceContext) error, stopFn func() error, opts ...RegisterOption) error {
	svc := &funcService{startFn: startFn, stopFn: stopFn}
	allOpts := make([]RegisterOption, 0, len(opts)+1)
	allOpts = append(allOpts, WithName(name))
	allOpts = append(allOpts, opts...)
	return o.Register(svc, allOpts...)
}
