package gorch

import (
	"context"
	"errors"
	"time"
)

type Service interface {
	Start(ctx ServiceContext) error // blocks; for cron: runs per-tick; for non-cron: runs until ctx cancelled
	Stop() error                    // cleanup signal beyond context cancellation
}

// ServiceContext — what the orchestrator hands each service.
// Embeds context.Context so it satisfies the context.Context interface and can be
// passed directly to Service.Start. Carries the orchestrator's cancellation context.
type ServiceContext struct {
	context.Context
	Logger    *ServiceLogger
	Messenger *Messenger
}

// Logger is the logging interface used by gorch. Services receive a *ServiceLogger
// which satisfies this interface. Inject a custom implementation via Config.Logger.
// The standard library's *slog.Logger satisfies this interface directly.
type registerConfig struct {
	cronSpec string
	cronMode CronMode
	factory  func() Service // non-nil means self-heal is enabled

	// dependency ordering
	name      string
	dependsOn []string

	// timeouts
	startTimeout time.Duration

	// one-shot
	runOnce bool

	// backoff / retry for self-heal
	maxRetries int           // 0 = unlimited
	backoff    Backoff       // nil = default (1s constant)
	resetAfter time.Duration // 0 = never reset retry counter

	// lifecycle hooks (per-service overrides)
	onBeforeStart func(name string) error
	onAfterStart  func(name string, err error)
	onBeforeStop  func(name string) error
	onAfterStop   func(name string, err error)

	// group / labels for filtering
	group  string
	labels map[string]string

	// soft dependencies: start after if present, ignore if missing
	softDependsOn []string

	// per-service stop timeout
	stopTimeout time.Duration

	// startCondition: if set and returns false, service is skipped at startup
	startCondition func() bool
}

// RegisterOption — functional options for Register.
type RegisterOption func(*registerConfig)

// WithCron registers the service to run on a 6-field cron schedule (seconds included).
func WithCron(spec string, mode CronMode) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.cronSpec = spec
		cfg.cronMode = mode
	}
}

// WithSelfHeal enables auto-restart: when the service crashes (returns error or panics),
// the orchestrator calls factory() for a fresh instance and restarts it.
func WithSelfHeal(factory func() Service) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.factory = factory
	}
}

// WithName assigns a human-readable name used for dependency ordering,
// status queries, and lifecycle hooks. Names must be unique across all
// registered services.
func WithName(name string) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.name = name
	}
}

// DependsOn declares that this service must start after the named services
// and stop before them. Cycles are detected at registration time.
func DependsOn(names ...string) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.dependsOn = append(cfg.dependsOn, names...)
	}
}

// WithStartTimeout sets the maximum time to wait for this service's Start
// to return. Overrides Config.DefaultStartTimeout. A zero duration means no
// timeout (use with caution).
// With a timeout, a synchronous error from a persistent service's Start aborts
// the whole orchestrator Start (deterministic failure); without one the launch
// is fire-and-forget. Self-heal services are never aborted this way: an exit is
// handled by their restart policy.
func WithStartTimeout(d time.Duration) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.startTimeout = d
	}
}

// WithRunOnce marks a service as a one-shot init task. It runs before
// persistent services and transitions to StatusSucceeded when Start returns.
// Stop() is called at orchestrator shutdown — make Stop idempotent.
// If Start returns an error, startup aborts.
func WithRunOnce() RegisterOption {
	return func(cfg *registerConfig) {
		cfg.runOnce = true
	}
}

// WithMaxRetries sets the maximum number of self-heal restarts.
// 0 means unlimited (up to context cancellation). After the limit
// is reached, the service transitions to StatusStopped.
func WithMaxRetries(max int) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.maxRetries = max
	}
}

// WithBackoff sets the backoff strategy for self-heal restarts.
// If nil or not set, the default is 1s constant backoff.
func WithBackoff(b Backoff) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.backoff = b
	}
}

// WithResetAfter sets a stability window. If the service runs continuously
// for this duration without crashing, the retry counter resets to zero.
func WithResetAfter(d time.Duration) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.resetAfter = d
	}
}

// WithOnBeforeStart sets a per-service hook called just before Start().
// If the hook returns an error, Start() is aborted for this service.
func WithOnBeforeStart(fn func(name string) error) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.onBeforeStart = fn
	}
}

// WithOnAfterStart sets a per-service hook called after Start() returns.
func WithOnAfterStart(fn func(name string, err error)) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.onAfterStart = fn
	}
}

// WithOnBeforeStop sets a per-service hook called just before Stop().
// If the hook returns an error, Stop() is still called.
func WithOnBeforeStop(fn func(name string) error) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.onBeforeStop = fn
	}
}

// WithOnAfterStop sets a per-service hook called after Stop() returns.
func WithOnAfterStop(fn func(name string, err error)) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.onAfterStop = fn
	}
}

// WithGroup assigns the service to a named group for filtering.
func WithGroup(name string) RegisterOption {
	return func(cfg *registerConfig) { cfg.group = name }
}

// WithLabel attaches a key-value label to the service for filtering.
func WithLabel(key, value string) RegisterOption {
	return func(cfg *registerConfig) {
		if cfg.labels == nil {
			cfg.labels = make(map[string]string)
		}
		cfg.labels[key] = value
	}
}

// DependsOnSoft declares soft dependencies: start after the named services
// if they are present, but ignore any that are not registered.
func DependsOnSoft(names ...string) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.softDependsOn = append(cfg.softDependsOn, names...)
	}
}

// WithStopTimeout sets a per-service timeout on Stop(). If Stop() does not
// return within this duration, the orchestrator proceeds with shutdown.
func WithStopTimeout(d time.Duration) RegisterOption {
	return func(cfg *registerConfig) { cfg.stopTimeout = d }
}

// WithStartCondition sets a function called at startup. If it returns false,
// the service is skipped (not started). nil or not set means always start.
func WithStartCondition(fn func() bool) RegisterOption {
	return func(cfg *registerConfig) { cfg.startCondition = fn }
}

// Sentinel errors
var (
	ErrAlreadyStarted    = errors.New("gorch: orchestrator already started")
	ErrInvalidCron       = errors.New("gorch: invalid cron expression")
	ErrStopTimeout       = errors.New("gorch: stop timed out waiting for services")
	ErrDuplicateName     = errors.New("gorch: duplicate service name")
	ErrDependencyCycle   = errors.New("gorch: dependency cycle detected")
	ErrStartAborted      = errors.New("gorch: start aborted due to dependency failure")
	ErrUnsupportedOption = errors.New("gorch: unsupported option combination")

	// Dynamic membership sentinels. See the Contract section of README.md.
	ErrServiceNotFound      = errors.New("gorch: service not found")
	ErrOrchestratorStopping = errors.New("gorch: orchestrator is stopping")
	ErrOrchestratorStopped  = errors.New("gorch: orchestrator already stopped")
	ErrDependencyNotRunning = errors.New("gorch: dependency not running")
	ErrDependencyNotFound   = errors.New("gorch: dependency not found")
)

// funcService wraps closures as a Service. Used by RegisterFunc.
type funcService struct {
	startFn func(ctx ServiceContext) error
	stopFn  func() error
}

func (f *funcService) Start(ctx ServiceContext) error { return f.startFn(ctx) }
func (f *funcService) Stop() error {
	if f.stopFn != nil {
		return f.stopFn()
	}
	return nil
}

var _ Service = (*funcService)(nil)

// Messenger — pub-sub with topics (Socket.IO rooms style).
// A nil or empty topics slice in Publish broadcasts to ALL subscribers.
