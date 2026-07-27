package gorch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
)

// LogLevel controls minimum log severity output by the log-pump.
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	default:
		return "????"
	}
}

// Config holds orchestrator configuration.
type Config struct {
	LogLevel LogLevel // defaults to LogLevelInfo if zero

	// DefaultStartTimeout is the default per-service start deadline.
	// 0 means no timeout (use WithStartTimeout per-service).
	DefaultStartTimeout time.Duration

	// Health check configuration.
	// HealthInterval: how often to probe. Default: 30s.
	// HealthTimeout: per-probe deadline. Default: 5s.
	// HealthThreshold: consecutive failures before restart. Default: 3.
	// A zero HealthInterval disables health checks entirely.
	HealthInterval  time.Duration
	HealthTimeout   time.Duration
	HealthThreshold int

	// Global lifecycle hooks (called for every service unless overridden).
	OnBeforeStart func(name string) error
	OnAfterStart  func(name string, err error)
	OnBeforeStop  func(name string) error
	OnAfterStop   func(name string, err error)

	// State-change callbacks.
	OnStateChange func(name string, from, to ServiceStatus)
	OnCrash       func(name string, err error)

	// Health check hooks.
	BeforeHealthCheck func(name string) error
	AfterHealthCheck  func(name string, err error)
}

// Metrics holds counter snapshots for orchestrator-level events.
type Metrics struct {
	Starts      int64
	Stops       int64
	Crashes     int64
	Restarts    int64
	HealthFails int64
}

// logEntry is an internal log record sent from ServiceLogger to the log-pump.
type logEntry struct {
	time    time.Time
	level   LogLevel
	service string
	msg     string
	args    []any
}

// serviceEntry tracks a registered service with its options and runtime state.
type serviceEntry struct {
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
}

// Orchestrator manages service lifecycles.
type Orchestrator struct {
	cfg     Config
	started bool
	mu      sync.Mutex // protects started, entries slice, nameIndex

	ctx    context.Context
	cancel context.CancelFunc

	logCh     chan logEntry
	messenger *Messenger

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

	// messengerDone computes the messenger shutdown cleanup once via sync.OnceValue.
	messengerDone func() func()

	metricsStarts      atomic.Int64
	metricsStops       atomic.Int64
	metricsCrashes     atomic.Int64
	metricsRestarts    atomic.Int64
	metricsHealthFails atomic.Int64
}

// New creates a new Orchestrator. Each call returns a fresh, independent instance.
// Orchestrators can be nested: a service may create its own gorch to manage sub-services.
func New(cfg Config) *Orchestrator {
	if cfg.LogLevel == 0 {
		cfg.LogLevel = LogLevelInfo
	}
	// Health defaults
	if cfg.HealthInterval == 0 {
		cfg.HealthInterval = 30 * time.Second
	}
	if cfg.HealthTimeout == 0 {
		cfg.HealthTimeout = 5 * time.Second
	}
	if cfg.HealthThreshold == 0 {
		cfg.HealthThreshold = 3
	}
	o := &Orchestrator{
		cfg:       cfg,
		messenger: newMessenger(),
		nameIndex: make(map[string]*serviceEntry),
	}
	o.messengerDone = sync.OnceValue(func() func() {
		return func() {
			o.messenger.mu.Lock()
			o.messenger.subs = nil
			o.messenger.mu.Unlock()
		}
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
		depEntry, ok := o.nameIndex[dep]
		if !ok {
			// Check if dep is being registered in this batch (entries not yet in nameIndex).
			found := false
			for _, e := range o.entries {
				if e.cfg.name == dep {
					found = true
					depEntry = e
					break
				}
			}
			if !found {
				return fmt.Errorf("gorch: dependency %q not found for service %s", dep, cfg.name)
			}
		}
		// Check if dep transitively depends on cfg.name (would create a cycle).
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

// dependsOnRecursive checks whether entry transitively depends on target.
// ponytail: DFS on small graphs (registration-time only); O(V+E) fine.
func (o *Orchestrator) dependsOnRecursive(entry *serviceEntry, target string) bool {
	if entry == nil {
		return false
	}
	for _, dep := range entry.cfg.dependsOn {
		if dep == target {
			return true
		}
		depEntry, ok := o.nameIndex[dep]
		if !ok {
			// check entries slice for newly registered
			for _, e := range o.entries {
				if e.cfg.name == dep {
					depEntry = e
					ok = true
					break
				}
			}
		}
		if ok && o.dependsOnRecursive(depEntry, target) {
			return true
		}
	}
	return false
}

// Start begins the orchestrator lifecycle. Returns ErrAlreadyStarted if already started.
// Idempotent: calling Start multiple times returns nil after the first.
// Thread-safe.
func (o *Orchestrator) Start() error {
	var startErr error
	o.startOnce.Do(func() {
		o.mu.Lock()
		if o.started {
			o.mu.Unlock()
			startErr = ErrAlreadyStarted
			return
		}
		o.started = true
		o.mu.Unlock()

		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 256)

		// Assign loggers: use cfg.name if WithName was set, else reflect type.
		for _, entry := range o.entries {
			svcName := entry.name
			// ponytail: if user didn't set WithName, the name is auto "$N".
			// Use reflect type for logging to keep backward compat.
			if svcName == "" || svcName[0] == '$' {
				svcName = reflect.TypeOf(entry.svc).String()
			}
			entry.logger = newServiceLogger(svcName, o.logCh)
		}

		// Spawn log-pump goroutine.
		o.wg.Add(1)
		go o.logPump()

		// Set up cron scheduler (with seconds field).
		o.cronSched = cron.New(cron.WithSeconds())
		for _, entry := range o.entries {
			if entry.cfg.cronSpec == "" {
				continue
			}
			entry := entry
			id, err := o.cronSched.AddFunc(entry.cfg.cronSpec, func() {
				o.invokeCron(entry)
			})
			if err != nil {
				o.cancel()
				close(o.logCh)
				o.cronSched.Stop()
				startErr = fmt.Errorf("%w: %w", ErrInvalidCron, err)
				return
			}
			entry.cronID = id
		}

		// Start cron scheduler.
		o.cronSched.Start()

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
	return startErr
}

// startOneService runs a single non-cron service with timeout and hooks.
// Does NOT add to wg — the runService goroutine inside does that.
func (o *Orchestrator) startOneService(entry *serviceEntry) error {
	// --- before-start hook ---
	hook := entry.cfg.onBeforeStart
	if hook == nil {
		hook = o.cfg.OnBeforeStart
	}
	if hook != nil {
		if err := hook(entry.name); err != nil {
			o.setStatus(entry, StatusStopped)
			return fmt.Errorf("before-start hook: %w", err)
		}
	}

	// Check start condition.
	if entry.cfg.startCondition != nil && !entry.cfg.startCondition() {
		o.setStatus(entry, StatusStopped)
		return nil
	}

	o.setStatus(entry, StatusStarting)

	// Per-service context with optional timeout.
	svcCtx, svcCancel := context.WithCancel(o.ctx)
	entry.cancel = svcCancel
	entry.startedAt = time.Now()
	entry.stableSince = time.Now()

	sc := ServiceContext{
		Context:   svcCtx,
		Logger:    entry.logger,
		Messenger: o.messenger,
	}

	// Determine timeout.
	timeout := entry.cfg.startTimeout
	if timeout == 0 {
		timeout = o.cfg.DefaultStartTimeout
	}

	if entry.cfg.runOnce {
		// RunOnce: run Start synchronously with timeout, don't spawn goroutine.
		o.setStatus(entry, StatusRunning)
		o.metricsStarts.Add(1)
		var err error
		if timeout > 0 {
			var cancel context.CancelFunc
			sc.Context, cancel = context.WithTimeout(svcCtx, timeout)
			defer cancel()
			// Use a goroutine to run Start with timeout
			done := make(chan error, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						entry.logger.Error("service panicked", "panic", fmt.Sprint(r))
						done <- fmt.Errorf("panic: %v", r)
					}
				}()
				done <- entry.svc.Start(sc)
			}()
			select {
			case err = <-done:
				// got result
			case <-o.ctx.Done():
				err = o.ctx.Err()
			case <-time.After(timeout):
				err = fmt.Errorf("start timeout after %v", timeout)
				svcCancel()
			}
		} else {
			err = entry.svc.Start(sc)
		}
		entry.cancel = nil
		svcCancel()

		if err != nil && err != context.Canceled {
			o.setStatus(entry, StatusCrashed)
			o.callAfterStartHook(entry, err)
			return err
		}
		entry.wgDone = true
		o.setStatus(entry, StatusStopped)
		o.callAfterStartHook(entry, nil)
		// wg.Done not needed—runOnce doesn't add to wg
		return nil
	}

	// Persistent service: start in goroutine.
	o.setStatus(entry, StatusRunning)
	o.metricsStarts.Add(1)
	o.wg.Add(1)

	// Create a detachable start context. If timeout is set, we use a separate
	// context for the race window rather than wrapping the service context.
	startErrCh := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				sc.Logger.Error("service panicked", "panic", fmt.Sprint(r))
			}
			// Signal whether Start returned cleanly within the timeout window.
			select {
			case startErrCh <- nil:
			default:
			}
			o.handleServiceDone(entry, sc)
		}()
		err := entry.svc.Start(sc)
		if err != nil && err != context.Canceled {
			sc.Logger.Error("service returned error", "error", err.Error())
		}
	}()

	if timeout > 0 {
		select {
		case <-startErrCh:
			// Start returned (possibly to handleServiceDone via defer).
			// The service is now in handleServiceDone.
			o.callAfterStartHook(entry, nil)
			return nil
		case <-time.After(timeout):
			// Timeout: service Start didn't return in time.
			// The goroutine is still running. Cancel its context.
			svcCancel()
			o.callAfterStartHook(entry, fmt.Errorf("start timeout after %v", timeout))
			return fmt.Errorf("start timeout after %v", timeout)
		}
	}

	// No timeout: return immediately.
	o.callAfterStartHook(entry, nil)
	return nil
}

// callAfterStartHook invokes the after-start hook (per-service override or global).
func (o *Orchestrator) callAfterStartHook(entry *serviceEntry, err error) {
	hook := entry.cfg.onAfterStart
	if hook == nil {
		hook = o.cfg.OnAfterStart
	}
	if hook != nil {
		hook(entry.name, err)
	}
}

// stopStartedServices stops all running services (used for cleanup on start failure).
// ponytail: sequential stop; parallel Stop is premature.
func (o *Orchestrator) stopStartedServices() {
	// Cancel context.
	if o.cancel != nil {
		o.cancel()
	}
	// Stop cron.
	if o.cronSched != nil {
		<-o.cronSched.Stop().Done()
	}
	// Stop services in reverse registration order.
	for i := len(o.entries) - 1; i >= 0; i-- {
		entry := o.entries[i]
		o.statusMu.RLock()
		s := entry.status
		o.statusMu.RUnlock()
		if s == StatusRunning || s == StatusStarting {
			o.safeStop(entry)
		}
	}
	// Close log channel.
	if o.logCh != nil {
		select {
		case <-o.logCh:
		// already closed
		default:
			close(o.logCh)
		}
	}
}

// setStatus updates the service status (thread-safe).
func (o *Orchestrator) setStatus(entry *serviceEntry, s ServiceStatus) {
	o.statusMu.Lock()
	old := entry.status
	entry.status = s
	o.statusMu.Unlock()

	if o.cfg.OnStateChange != nil && old != s {
		o.cfg.OnStateChange(entry.name, old, s)
	}
	if s == StatusCrashed {
		o.metricsCrashes.Add(1)
	}
	if o.cfg.OnCrash != nil && s == StatusCrashed {
		o.cfg.OnCrash(entry.name, fmt.Errorf("service %s crashed", entry.name))
	}
}

// Stop gracefully shuts down the orchestrator. Waits up to timeout for services
// to finish. Returns aggregated errors from all Stop failures, or ErrStopTimeout
// if services don't all stop within the timeout.
// Thread-safe. Safe to call on an orchestrator that was never started.
func (o *Orchestrator) Stop(timeout time.Duration) error {
	var stopErr error
	o.stopOnce.Do(func() {
		o.mu.Lock()
		if !o.started {
			o.mu.Unlock()
			return
		}
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

		// 4. Close log channel to signal log-pump to drain and exit.
		close(o.logCh)

		// 5. Clean up messenger subscriptions.
		cleanup := o.messengerDone()
		cleanup()

		// 6. Wait for all services + log-pump with timeout.
		done := make(chan struct{})
		go func() {
			o.wg.Wait()
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
func (o *Orchestrator) stopOneService(entry *serviceEntry) error {
	o.statusMu.RLock()
	wasActive := entry.status == StatusRunning || entry.status == StatusStarting
	o.statusMu.RUnlock()

	o.setStatus(entry, StatusStopping)

	// --- before-stop hook ---
	hook := entry.cfg.onBeforeStop
	if hook == nil {
		hook = o.cfg.OnBeforeStop
	}
	var hookErr error
	if hook != nil {
		hookErr = hook(entry.name)
	}

	// --- stop ---
	var stopErr error
	timeout := entry.cfg.stopTimeout
	if timeout > 0 {
		done := make(chan error, 1)
		go func() { done <- o.safeStopWithResult(entry.svc) }()
		select {
		case stopErr = <-done:
		case <-time.After(timeout):
			stopErr = fmt.Errorf("stop timeout after %v", timeout)
		}
	} else {
		stopErr = o.safeStopWithResult(entry.svc)
	}

	// --- after-stop hook ---
	afterHook := entry.cfg.onAfterStop
	if afterHook == nil {
		afterHook = o.cfg.OnAfterStop
	}
	if afterHook != nil {
		afterHook(entry.name, stopErr)
	}

	o.setStatus(entry, StatusStopped)
	if wasActive {
		o.metricsStops.Add(1)
	}
	return errors.Join(hookErr, stopErr)
}

// safeStopWithResult calls Stop with panic recovery, returning any error.
func (o *Orchestrator) safeStopWithResult(svc Service) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("stop panicked: %v", r)
		}
	}()
	return svc.Stop()
}

// safeStop calls Stop with panic recovery. Best-effort; errors and panics
// are silently discarded.
func (o *Orchestrator) safeStop(entry *serviceEntry) {
	_ = o.stopOneService(entry)
}

// persistentEntries returns non-cron, non-runOnce entries.
func (o *Orchestrator) persistentEntries() []*serviceEntry {
	var out []*serviceEntry
	for _, e := range o.entries {
		if e.cfg.cronSpec == "" && !e.cfg.runOnce {
			out = append(out, e)
		}
	}
	return out
}

// Run starts the orchestrator, blocks on SIGINT/SIGTERM, then stops.
// Returns any error from Start or aggregated errors from Stop.
// Optional signals override the default signal set.
func (o *Orchestrator) Run(stopTimeout time.Duration, signals ...os.Signal) error {
	if err := o.Start(); err != nil {
		return err
	}

	sigSet := signals
	if len(sigSet) == 0 {
		sigSet = []os.Signal{os.Interrupt}
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
func (o *Orchestrator) Status(name string) (ServiceStatus, bool) {
	o.mu.Lock()
	entry, ok := o.nameIndex[name]
	o.mu.Unlock()
	if !ok {
		return 0, false
	}
	o.statusMu.RLock()
	s := entry.status
	o.statusMu.RUnlock()
	return s, true
}

// Statuses returns a map of service name to status for all registered services.
// Thread-safe.
func (o *Orchestrator) Statuses() map[string]ServiceStatus {
	o.mu.Lock()
	entries := make([]*serviceEntry, len(o.entries))
	copy(entries, o.entries)
	o.mu.Unlock()

	result := make(map[string]ServiceStatus, len(entries))
	o.statusMu.RLock()
	defer o.statusMu.RUnlock()
	for _, e := range entries {
		result[e.name] = e.status
	}
	return result
}

// Names returns the names of all registered services in registration order.
// Thread-safe.
func (o *Orchestrator) Names() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	names := make([]string, len(o.entries))
	for i, e := range o.entries {
		names[i] = e.name
	}
	return names
}

// Count returns the total number of registered services.
// Thread-safe.
func (o *Orchestrator) Count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.entries)
}

// Health probes all registered services that implement HealthChecker.
// Returns a map of service name to error (nil = healthy).
// Services that don't implement HealthChecker are reported as nil.
// Thread-safe.
func (o *Orchestrator) Health() map[string]error {
	o.mu.Lock()
	entries := make([]*serviceEntry, len(o.entries))
	copy(entries, o.entries)
	o.mu.Unlock()

	result := make(map[string]error, len(entries))
	ctx, cancel := context.WithTimeout(context.Background(), o.cfg.HealthTimeout)
	defer cancel()

	for _, e := range entries {
		hc, ok := e.svc.(HealthChecker)
		if !ok {
			result[e.name] = nil
			continue
		}
		result[e.name] = hc.Health(ctx)
	}
	return result
}

// RegisterFunc registers a closure-based service.
func (o *Orchestrator) RegisterFunc(name string, startFn func(ctx ServiceContext) error, stopFn func() error, opts ...RegisterOption) error {
	svc := &funcService{startFn: func(ctx context.Context) error { return startFn(ctx.(ServiceContext)) }, stopFn: stopFn}
	allOpts := make([]RegisterOption, 0, len(opts)+1)
	allOpts = append(allOpts, WithName(name))
	allOpts = append(allOpts, opts...)
	return o.Register(svc, allOpts...)
}

// IsReady reports whether a named service is running and ready to serve.
func (o *Orchestrator) IsReady(name string) bool {
	o.mu.Lock()
	entry, ok := o.nameIndex[name]
	o.mu.Unlock()
	if !ok {
		return false
	}
	o.statusMu.RLock()
	s := entry.status
	o.statusMu.RUnlock()
	if s != StatusRunning {
		return false
	}
	rc, ok := entry.svc.(ReadinessChecker)
	if !ok {
		return true
	}
	return rc.Ready(context.Background()) == nil
}

// StartGroup starts all services in the named group in topological order.
func (o *Orchestrator) StartGroup(group string) error {
	o.mu.Lock()
	entries := make([]*serviceEntry, 0)
	for _, e := range o.entries {
		if e.cfg.group == group {
			entries = append(entries, e)
		}
	}
	o.mu.Unlock()
	levels, err := o.topoSort(entries)
	if err != nil {
		return err
	}
	for _, level := range levels {
		for _, entry := range level {
			if err := o.startOneService(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// StopGroup stops all non-cron, non-runOnce services in the named group in
// reverse topological order. Errors are aggregated via errors.Join.
func (o *Orchestrator) StopGroup(group string, timeout time.Duration) error {
	o.mu.Lock()
	persistent := make([]*serviceEntry, 0)
	for _, e := range o.entries {
		if e.cfg.group == group && e.cfg.cronSpec == "" && !e.cfg.runOnce {
			persistent = append(persistent, e)
		}
	}
	o.mu.Unlock()
	levels, _ := o.topoSort(persistent)
	var stopErr error
	for i := len(levels) - 1; i >= 0; i-- {
		for _, entry := range levels[i] {
			if err := o.stopOneService(entry); err != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("%s: %w", entry.name, err))
			}
		}
	}
	return stopErr
}

// StatusesByGroup returns a map of service name to status for all services
// in the named group. Thread-safe.
func (o *Orchestrator) StatusesByGroup(group string) map[string]ServiceStatus {
	o.mu.Lock()
	entries := make([]*serviceEntry, 0)
	for _, e := range o.entries {
		if e.cfg.group == group {
			entries = append(entries, e)
		}
	}
	o.mu.Unlock()
	result := make(map[string]ServiceStatus, len(entries))
	o.statusMu.RLock()
	defer o.statusMu.RUnlock()
	for _, e := range entries {
		result[e.name] = e.status
	}
	return result
}

// StatusesByLabel returns a map of service name to status for all services
// matching the given label key-value pair. Thread-safe.
func (o *Orchestrator) StatusesByLabel(key, value string) map[string]ServiceStatus {
	o.mu.Lock()
	entries := make([]*serviceEntry, 0)
	for _, e := range o.entries {
		if e.cfg.labels != nil && e.cfg.labels[key] == value {
			entries = append(entries, e)
		}
	}
	o.mu.Unlock()
	result := make(map[string]ServiceStatus, len(entries))
	o.statusMu.RLock()
	defer o.statusMu.RUnlock()
	for _, e := range entries {
		result[e.name] = e.status
	}
	return result
}

// WaitFor blocks until the named service reaches target status or timeout expires.
// Polls at 50ms intervals. Returns an error on timeout or if the service is not found.
func (o *Orchestrator) WaitFor(name string, target ServiceStatus, timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, ok := o.Status(name)
		if !ok {
			return fmt.Errorf("gorch: service %s not found", name)
		}
		if status == target {
			return nil
		}
		select {
		case <-deadline:
			return fmt.Errorf("gorch: WaitFor %s -> %s timed out after %v (current: %s)", name, target, timeout, status)
		case <-ticker.C:
		}
	}
}

// Metrics returns a snapshot of orchestrator-level event counters.
func (o *Orchestrator) Metrics() Metrics {
	return Metrics{
		Starts:      o.metricsStarts.Load(),
		Stops:       o.metricsStops.Load(),
		Crashes:     o.metricsCrashes.Load(),
		Restarts:    o.metricsRestarts.Load(),
		HealthFails: o.metricsHealthFails.Load(),
	}
}

// Done returns a channel that closes when all managed goroutines (services,
// log-pump, health-check loop) have exited. The orchestrator must be stopped
// (via Stop or Run returning) before the channel closes.
func (o *Orchestrator) Done() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(ch)
	}()
	return ch
}

// ── Topological sort ──

// topoSort groups entries into levels based on their dependsOn chains.
// Services in the same level are independent and can start in parallel.
// Returns ErrDependencyCycle if a cycle is detected.
func (o *Orchestrator) topoSort(entries []*serviceEntry) ([][]*serviceEntry, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	// Build in-degree map and adjacency list.
	inDegree := make(map[string]int)
	children := make(map[string][]string)
	byName := make(map[string]*serviceEntry)

	for _, e := range entries {
		name := e.name
		byName[name] = e
		if _, ok := inDegree[name]; !ok {
			inDegree[name] = 0
		}
		for _, dep := range e.cfg.dependsOn {
			children[dep] = append(children[dep], name)
			inDegree[name]++
		}
	}

	var levels [][]*serviceEntry
	visited := make(map[string]bool)

	for len(visited) < len(entries) {
		// Collect nodes with zero in-degree (not yet visited).
		var level []string
		for name := range byName {
			if visited[name] {
				continue
			}
			if inDegree[name] == 0 {
				level = append(level, name)
			}
		}

		if len(level) == 0 {
			return nil, ErrDependencyCycle
		}

		// Sort for deterministic output.
		sort.Strings(level)

		var levelEntries []*serviceEntry
		for _, name := range level {
			visited[name] = true
			levelEntries = append(levelEntries, byName[name])
			for _, child := range children[name] {
				inDegree[child]--
			}
		}
		levels = append(levels, levelEntries)
	}

	return levels, nil
}

// ── Health check loop ──

func (o *Orchestrator) healthCheckLoop(ctx context.Context) {
	defer o.wg.Done()
	defer close(o.healthDone)
	ticker := time.NewTicker(o.cfg.HealthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.runHealthChecks()
		}
	}
}

func (o *Orchestrator) runHealthChecks() {
	o.mu.Lock()
	entries := make([]*serviceEntry, len(o.entries))
	copy(entries, o.entries)
	o.mu.Unlock()

	probeCtx, cancel := context.WithTimeout(context.Background(), o.cfg.HealthTimeout)
	defer cancel()

	for _, e := range entries {
		hc, ok := e.svc.(HealthChecker)
		if !ok {
			continue
		}

		o.statusMu.RLock()
		s := e.status
		o.statusMu.RUnlock()
		if s != StatusRunning {
			continue
		}

		if o.cfg.BeforeHealthCheck != nil {
			if err := o.cfg.BeforeHealthCheck(e.name); err != nil {
				e.logger.Warn("before-health-check hook failed", "error", err.Error())
			}
		}

		healthErr := hc.Health(probeCtx)
		if o.cfg.AfterHealthCheck != nil {
			o.cfg.AfterHealthCheck(e.name, healthErr)
		}
		if healthErr != nil {
			e.healthFailures++
			o.metricsHealthFails.Add(1)
			e.logger.Warn("health check failed", "failures", e.healthFailures, "error", healthErr.Error())
			if e.healthFailures >= o.cfg.HealthThreshold && e.cfg.factory != nil {
				e.logger.Error("health threshold reached, restarting service", "failures", e.healthFailures)
				e.healthFailures = 0
				// Cancel the service's context → Start returns → handleServiceDone → self-heal.
				if e.cancel != nil {
					e.cancel()
				}
			}
		} else {
			e.healthFailures = 0
		}
	}
}

// ── Internal helpers ──

func (o *Orchestrator) logPump() {
	defer o.wg.Done()
	for entry := range o.logCh {
		if entry.level < o.cfg.LogLevel {
			continue
		}
		levelStr := entry.level.String()
		ts := entry.time.Format("2006-01-02 15:04:05.000")
		var argsStr string
		for i := 0; i < len(entry.args)-1; i += 2 {
			if i > 0 {
				argsStr += " "
			}
			argsStr += fmt.Sprintf("%v=%v", entry.args[i], entry.args[i+1])
		}
		if len(entry.args)%2 != 0 && len(entry.args) > 0 {
			if argsStr != "" {
				argsStr += " "
			}
			argsStr += fmt.Sprintf("%v=(missing)", entry.args[len(entry.args)-1])
		}
		_, _ = fmt.Fprintf(os.Stderr, "%s %-5s %s --- %s %s\n",
			ts, levelStr, entry.service, entry.msg, argsStr)
	}
}

// runService starts a non-cron service. handleServiceDone is called exactly once
// via defer — covering both normal return and panic recovery paths.
func (o *Orchestrator) runService(entry *serviceEntry, sc ServiceContext) {
	defer func() {
		if r := recover(); r != nil {
			sc.Logger.Error("service panicked", "panic", fmt.Sprint(r))
		}
		o.handleServiceDone(entry, sc)
	}()

	err := entry.svc.Start(sc)
	if err != nil && err != context.Canceled {
		sc.Logger.Error("service returned error", "error", err.Error())
	}
}

// handleServiceDone is called when a non-cron service exits (normally or via panic).
// For services without self-heal it decrements the waitgroup once.
// For self-heal services it uses the configured backoff and retry policy.
func (o *Orchestrator) handleServiceDone(entry *serviceEntry, sc ServiceContext) {
	if entry.cfg.factory == nil {
		// No self-heal: service stays dead.
		// If orchestrator is shutting down, Stop() handles status transitions
		// via stopOneService (running→stopping→stopped). Don't double-transition.
		if o.ctx != nil {
			select {
			case <-o.ctx.Done():
				o.mu.Lock()
				alreadyDone := entry.wgDone
				entry.wgDone = true
				o.mu.Unlock()
				if !alreadyDone {
					o.wg.Done()
				}
				return
			default:
			}
		}
		o.setStatus(entry, StatusStopped)
		o.metricsStops.Add(1)
		o.mu.Lock()
		if !entry.wgDone {
			entry.wgDone = true
			o.mu.Unlock()
			o.wg.Done()
		} else {
			o.mu.Unlock()
		}
		return
	}

	// Check if context was cancelled (orchestrator shutting down).
	select {
	case <-o.ctx.Done():
		o.setStatus(entry, StatusStopped)
		o.mu.Lock()
		if !entry.wgDone {
			entry.wgDone = true
			o.mu.Unlock()
			o.wg.Done()
		} else {
			o.mu.Unlock()
		}
		return
	default:
	}

	// Check resetAfter: if the service was stable long enough, reset retry count.
	if entry.cfg.resetAfter > 0 {
		if time.Since(entry.stableSince) >= entry.cfg.resetAfter {
			entry.retryCount = 0
		}
	}

	// Check maxRetries.
	if entry.cfg.maxRetries > 0 && entry.retryCount >= entry.cfg.maxRetries {
		entry.logger.Error("max retries reached, giving up", "retries", entry.retryCount)
		o.setStatus(entry, StatusStopped)
		o.metricsStops.Add(1)
		o.mu.Lock()
		if !entry.wgDone {
			entry.wgDone = true
			o.mu.Unlock()
			o.wg.Done()
		} else {
			o.mu.Unlock()
		}
		return
	}

	entry.retryCount++

	// Compute backoff delay.
	backoff := entry.cfg.backoff
	if backoff == nil {
		backoff = ConstantBackoff{Delay: 1 * time.Second}
	}
	delay := backoff.Next(entry.retryCount)

	entry.logger.Warn("self-heal: restarting service",
		"retry", entry.retryCount, "delay", delay.String())

	select {
	case <-o.ctx.Done():
		o.setStatus(entry, StatusStopped)
		o.mu.Lock()
		if !entry.wgDone {
			entry.wgDone = true
			o.mu.Unlock()
			o.wg.Done()
		} else {
			o.mu.Unlock()
		}
		return
	case <-time.After(delay):
	}

	o.safeStop(entry) // best-effort cleanup of old instance

	newSvc := entry.cfg.factory()
	o.mu.Lock()
	entry.svc = newSvc
	o.mu.Unlock()

	// Update logger for the new instance.
	svcName := entry.name
	if svcName == "" || svcName[0] == '$' {
		svcName = reflect.TypeOf(newSvc).String()
	}
	entry.logger = newServiceLogger(svcName, o.logCh)
	entry.stableSince = time.Now()

	// New per-service context.
	svcCtx, svcCancel := context.WithCancel(o.ctx)
	entry.cancel = svcCancel
	newSc := ServiceContext{Context: svcCtx, Logger: entry.logger, Messenger: o.messenger}

	go o.runService(entry, newSc)
	o.metricsRestarts.Add(1)
}

// invokeCron executes a cron-triggered service tick. Concurrency policy is
// determined by the entry's cronMode.
func (o *Orchestrator) invokeCron(entry *serviceEntry) {
	switch entry.cfg.cronMode {
	case CronSkip:
		if !entry.running.CompareAndSwap(false, true) {
			entry.logger.Warn("cron tick skipped: previous invocation still running")
			return
		}
		defer entry.running.Store(false)
	case CronQueue:
		for !entry.running.CompareAndSwap(false, true) {
			time.Sleep(100 * time.Millisecond)
		}
		defer entry.running.Store(false)
	case CronParallel:
	}

	sc := ServiceContext{
		Context:   o.ctx,
		Logger:    entry.logger,
		Messenger: o.messenger,
	}

	defer func() {
		if r := recover(); r != nil {
			entry.logger.Error("cron service panicked", "panic", fmt.Sprint(r))
		}
	}()

	err := entry.svc.Start(sc)
	if err != nil && err != context.Canceled {
		entry.logger.Error("cron service returned error", "error", err.Error())
	}
}
