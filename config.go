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

	// failedStartTimeout bounds the whole rollback of a failed Start: the
	// per-service stop sequences and the final wait for instance and log-pump
	// goroutines share it. 0 means the default (30s); a negative value disables
	// the bound.
	failedStartTimeout time.Duration

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

// WithFailedStartTimeout bounds the whole rollback of a failed Start: the
// per-service stop sequences (before/after-stop hooks and Stop()) and the final
// wait for instance and log-pump goroutines all share one budget, mirroring
// Stop's shutdown contract. Zero means the default (30s); a negative value
// removes the bound, which is not recommended because a service that blocks in
// Stop() could then hang Start forever. When the budget is exceeded the returned
// error matches ErrStopTimeout, and the reset is best-effort so Start can still
// be retried.
func WithFailedStartTimeout(d time.Duration) Option {
	return func(c *config) { c.failedStartTimeout = d }
}

// HealthCheckOption refines the periodic health-check loop configured by
// WithHealthChecks. It exists so the probe timeout and failure threshold cannot
// be swapped by position at the call site.
type HealthCheckOption func(*config)

// WithProbeTimeout sets the per-probe deadline for each health check.
// Zero falls back to the default (5s).
func WithProbeTimeout(d time.Duration) HealthCheckOption {
	return func(c *config) { c.HealthTimeout = d }
}

// WithFailureThreshold sets how many consecutive probe failures are tolerated
// before a self-healing service is restarted. Zero falls back to the default (3).
func WithFailureThreshold(n int) HealthCheckOption {
	return func(c *config) { c.HealthThreshold = n }
}

// WithHealthChecks enables periodic health checks at the given interval.
// The probe timeout and failure threshold default to 5s and 3; override them
// with WithProbeTimeout and WithFailureThreshold. An interval of zero enables
// the loop at the default 30s. Use WithHealthChecksDisabled to turn it off.
func WithHealthChecks(interval time.Duration, opts ...HealthCheckOption) Option {
	return func(c *config) {
		c.HealthInterval = interval
		for _, opt := range opts {
			opt(c)
		}
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
