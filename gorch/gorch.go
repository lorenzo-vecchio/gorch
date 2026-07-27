package gorch

import (
	"context"
	"fmt"
	"os"
	"reflect"
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
	mu      sync.Mutex // protects started, entries slice

	ctx    context.Context
	cancel context.CancelFunc

	logCh     chan logEntry
	messenger *Messenger

	cronSched *cron.Cron
	entries   []*serviceEntry
	wg        sync.WaitGroup

	stopOnce  sync.Once
	startOnce sync.Once

	// messengerDone computes the messenger shutdown cleanup once via sync.OnceValue.
	// Resetting subs to nil lets GC collect subscriber channels; range over nil map
	// is a no-op so any post-shutdown Publish calls are harmless.
	messengerDone func() func()
}

// New creates a new Orchestrator. Each call returns a fresh, independent instance.
// Orchestrators can be nested: a service may create its own gorch to manage sub-services.
func New(cfg Config) *Orchestrator {
	// ponytail: can't distinguish "user explicitly set Debug" from "zero-value struct".
	// Default to Info; user who wants Debug must set it explicitly (non-zero after this).
	if cfg.LogLevel == 0 {
		cfg.LogLevel = LogLevelInfo
	}
	o := &Orchestrator{cfg: cfg, messenger: newMessenger()}
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
	o.entries = append(o.entries, &serviceEntry{svc: svc, cfg: cfg})
	return nil
}

// Start begins the orchestrator lifecycle. Returns ErrAlreadyStarted if already started.
// Idempotent: calling Start multiple times returns the same error.
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

		// Assign loggers to each entry.
		for _, entry := range o.entries {
			svcName := reflect.TypeOf(entry.svc).String()
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
				// Cron parse error: tear down everything and return.
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

		// Start non-cron services (each gets a wg.Add(1) goroutine).
		for _, entry := range o.entries {
			if entry.cfg.cronSpec != "" {
				continue
			}
			entry := entry
			sc := ServiceContext{
				Context:   o.ctx,
				Logger:    entry.logger,
				Messenger: o.messenger,
			}
			o.wg.Add(1)
			go o.runService(entry, sc)
		}
	})
	return startErr
}

// Stop gracefully shuts down the orchestrator. Waits up to timeout for services
// to finish. Returns ErrStopTimeout if services don't all stop within the timeout.
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

		// 1. Cancel context to signal all services.
		o.cancel()

		// 2. Stop cron scheduler (waits for in-flight cron jobs).
		if o.cronSched != nil {
			<-o.cronSched.Stop().Done()
		}

		// 3. Call Stop() on every service (best-effort, recover panics).
		for _, entry := range o.entries {
			o.safeStop(entry.svc)
		}

		// 4. Close log channel to signal log-pump to drain and exit.
		close(o.logCh)

		// 5. Clean up messenger subscriptions (computed once via sync.OnceValue).
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
			stopErr = ErrStopTimeout
		}
	})
	return stopErr
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
		// Handle odd arg count: append the last one as key=(missing)
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
		// ponytail: stringifying error via Error() — use full %+v if structured logging needed
	}
}

// handleServiceDone is called when a non-cron service exits (normally or via panic).
// For services without self-heal it decrements the waitgroup once.
// For self-heal services it spawns a replacement after a 1s backoff.
func (o *Orchestrator) handleServiceDone(entry *serviceEntry, sc ServiceContext) {
	if entry.cfg.factory == nil {
		// No self-heal: service stays dead.
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

	// Self-heal enabled (only for non-cron services).
	time.Sleep(1 * time.Second) // backoff to avoid tight-loop

	o.safeStop(entry.svc) // best-effort cleanup of old instance

	newSvc := entry.cfg.factory()
	o.mu.Lock()
	entry.svc = newSvc
	o.mu.Unlock()

	// Update logger for the new instance.
	svcName := reflect.TypeOf(newSvc).String()
	entry.logger = newServiceLogger(svcName, o.logCh)
	newSc := ServiceContext{Context: o.ctx, Logger: entry.logger, Messenger: o.messenger}

	go o.runService(entry, newSc)
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
		// just run; no gate
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

// safeStop calls svc.Stop() with panic recovery. Best-effort; errors and panics
// are silently discarded.
func (o *Orchestrator) safeStop(svc Service) {
	defer func() {
		if r := recover(); r != nil {
			// ponytail: panic in Stop() is logged nowhere (logCh may be closed). Discard.
			_ = r
		}
	}()
	_ = svc.Stop()
}
