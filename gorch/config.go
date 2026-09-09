package gorch

import "time"

type config struct {
	// Logger is an optional custom logger. When set, gorch sends all log output
	// through it instead of the built-in stderr logger. The service name is
	// prepended as a "service"=<name> key-value pair to every call.
	// When nil (default), gorch uses its built-in channel-based logger which
	// writes to stderr with a fixed timestamp+level+service format.
	Logger Logger

	LogLevel    LogLevel // defaults to LogLevelInfo; ignored when Logger is set
	logLevelSet bool     // true if WithLogLevel was called (Debug is otherwise indistinguishable from zero)

	// DefaultStartTimeout is the default per-service start deadline.
	// 0 means no timeout (use WithStartTimeout per-service).
	DefaultStartTimeout time.Duration

	// Health check configuration.
	// HealthInterval: how often to probe. Default: 30s.
	// HealthTimeout: per-probe deadline. Default: 5s.
	// HealthThreshold: consecutive failures before restart. Default: 3.
	HealthInterval  time.Duration
	HealthTimeout   time.Duration
	HealthThreshold int
	healthDisabled  bool // true when WithHealthChecksDisabled is used

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

// Option configures an Orchestrator at construction via New.
type Option func(*config)

// WithLogger sets a custom logger. When set, gorch sends all log output through
// it instead of the built-in stderr logger.
func WithLogger(l Logger) Option {
	return func(c *config) { c.Logger = l }
}

// WithLogLevel sets the minimum log level. Defaults to LogLevelInfo when absent.
// Ignored when a custom Logger is set.
func WithLogLevel(lvl LogLevel) Option {
	return func(c *config) { c.LogLevel = lvl; c.logLevelSet = true }
}

// WithDefaultStartTimeout sets the default per-service start deadline.
// 0 means no timeout (use WithStartTimeout per-service).
func WithDefaultStartTimeout(d time.Duration) Option {
	return func(c *config) { c.DefaultStartTimeout = d }
}

// WithHealthChecks enables periodic health checks with the given interval,
// per-probe timeout, and consecutive-failure threshold. Zero values fall back
// to the defaults (30s interval, 5s timeout, threshold 3).
func WithHealthChecks(interval, timeout time.Duration, threshold int) Option {
	return func(c *config) {
		c.HealthInterval = interval
		c.HealthTimeout = timeout
		c.HealthThreshold = threshold
	}
}

// WithHealthChecksDisabled disables the periodic health-check loop entirely.
func WithHealthChecksDisabled() Option {
	return func(c *config) { c.healthDisabled = true }
}

// WithGlobalOnBeforeStart sets a global hook called just before each service's Start.
func WithGlobalOnBeforeStart(fn func(name string) error) Option {
	return func(c *config) { c.OnBeforeStart = fn }
}

// WithGlobalOnAfterStart sets a global hook called after each service's Start returns.
func WithGlobalOnAfterStart(fn func(name string, err error)) Option {
	return func(c *config) { c.OnAfterStart = fn }
}

// WithGlobalOnBeforeStop sets a global hook called just before each service's Stop.
func WithGlobalOnBeforeStop(fn func(name string) error) Option {
	return func(c *config) { c.OnBeforeStop = fn }
}

// WithGlobalOnAfterStop sets a global hook called after each service's Stop returns.
func WithGlobalOnAfterStop(fn func(name string, err error)) Option {
	return func(c *config) { c.OnAfterStop = fn }
}

// WithOnStateChange sets a callback fired on every status transition.
func WithOnStateChange(fn func(name string, from, to ServiceStatus)) Option {
	return func(c *config) { c.OnStateChange = fn }
}

// WithOnCrash sets a callback fired when a service reaches StatusCrashed.
func WithOnCrash(fn func(name string, err error)) Option {
	return func(c *config) { c.OnCrash = fn }
}

// WithBeforeHealthCheck sets a hook fired before each health probe.
func WithBeforeHealthCheck(fn func(name string) error) Option {
	return func(c *config) { c.BeforeHealthCheck = fn }
}

// WithAfterHealthCheck sets a hook fired after each health probe.
func WithAfterHealthCheck(fn func(name string, err error)) Option {
	return func(c *config) { c.AfterHealthCheck = fn }
}
