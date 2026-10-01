package gorch

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	// cronSet records that WithCron was called, even with an empty spec. The
	// empty string doubles as the "not a cron service" marker, so without this
	// flag WithCron("") would silently register a non-cron service. It lets
	// validation tell an explicit empty spec (rejected) from no WithCron at all.
	cronSet bool
	factory func() Service // non-nil means self-heal is enabled

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

// WithCron registers the service to run on a 6-field cron schedule (seconds
// included). The spec is validated by Register on both the static and the
// hot-add path, so an empty or malformed spec returns ErrInvalidCron instead of
// silently registering a non-cron service or deferring the error to Start. The
// underlying parser clamps an "@every" interval below one second (including
// "@every 0s") to one second rather than rejecting it.
func WithCron(spec string, mode CronMode) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.cronSpec = spec
		cfg.cronMode = mode
		cfg.cronSet = true
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
// A gate that reached StatusSucceeded keeps it: a later stop (Stop,
// StopService, Unregister, or a cascade) still runs Stop() but leaves the status
// StatusSucceeded, so the gate's success is never erased and its dependents are
// not retroactively blocked. Stop() is called at orchestrator shutdown — make
// Stop idempotent. If Start returns an error, startup aborts.
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

// StopOption configures StopService, Unregister, and UnregisterGroup.
type StopOption func(*stopConfig)

// stopConfig holds the resolved stop options.
type stopConfig struct {
	cascade bool
	orphans bool
}

// WithCascadeStop extends StopService/Unregister to the target's transitive
// hard dependents (services whose DependsOn chain reaches the target). They are
// stopped — or removed — in reverse topological order under the one shared
// timeout. Without it both operations refuse when a hard dependent is Running or
// Starting (ErrHasDependents, with the message naming each blocker and its
// status). A dependent already Stopping is left to its own in-flight teardown,
// never stopped twice. Soft dependencies never block and are never cascaded.
// UnregisterGroup reads it against each group member, so the cascade covers the
// transitive hard dependents that lie outside the group too.
func WithCascadeStop() StopOption {
	return func(c *stopConfig) { c.cascade = true }
}

// Orphans lets StopService/Unregister proceed when the target has Running or
// Starting hard dependents, instead of refusing with ErrHasDependents. Those
// dependents are left running but degraded: they keep StatusRunning, while
// IsReady reports them not ready for as long as a declared hard dependency is
// neither Running nor a runOnce gate that reached StatusSucceeded — the same
// edge condition StartService enforces. StartService of the orphan therefore
// keeps failing with ErrDependencyNotRunning (or ErrDependencyNotFound if the
// dependency was unregistered) until the dependency is back, and the orphan
// becomes ready again automatically once it is. Soft dependencies are never
// involved: they neither block nor are left degraded.
//
// Orphans is the explicit "take this down and let the survivors degrade" mode
// (the kubectl delete --cascade=orphan analogue); the default stays the refusal,
// because a hard dependent opted into aborting when its dependency goes away. It
// applies to UnregisterGroup the same way, leaving each member's outside hard
// dependents Running but degraded. It is mutually exclusive with WithCascadeStop
// — a stop cannot both leave the dependents alone and tear them down — and the
// pair is rejected with ErrUnsupportedOption.
func Orphans() StopOption {
	return func(c *stopConfig) { c.orphans = true }
}

// Sentinel errors. Every exported sentinel is classified so a caller can decide
// how to react with errors.Is, without reading the orchestrator's internals.
// The taxonomy has four classes:
//
//   - permanent: a bug in the caller's code or configuration; an identical call
//     keeps failing until code or configuration changes. Not retryable.
//   - transient: a condition that may clear on its own or after another
//     operation; retry the same call once it clears (Busy observes a
//     reservation). Retryable.
//   - terminal: the whole-orchestrator lifecycle has already begun or ended; by
//     design the operation can never succeed. Not retryable.
//   - environmental: user code or teardown overran a deadline and the outcome is
//     unverified; the timed-out teardown is not retried. Not retryable.
//
// Only a transient sentinel is retryable. Start, Stop, and the group ops
// aggregate failures with errors.Join, so errors.Is walks the joined tree and
// classifies every cause. See the README's sentinel taxonomy table for the full
// mapping.
var (
	// ErrAlreadyStarted is terminal: the whole-orchestrator lifecycle is
	// single-shot, so a Start after the first one (including after Stop) can
	// never succeed. Not retryable.
	ErrAlreadyStarted = errors.New("gorch: orchestrator already started")
	// ErrInvalidCron is permanent: the WithCron spec is malformed. Fix the spec;
	// not retryable.
	ErrInvalidCron = errors.New("gorch: invalid cron expression")
	// ErrStopTimeout is environmental: a stop did not finish within the caller's
	// timeout — the before/after-stop hooks, the service's own Stop(), or the
	// wait for its instance to exit was still in flight. The service is left
	// StatusStopping, not StatusStopped, and the stop is not counted in
	// Metrics().Stops, because its teardown is unverified. Not retryable: the
	// timed-out teardown is abandoned. On a failed Start it means the bounded
	// rollback budget was exceeded; the orchestrator is still left restartable.
	ErrStopTimeout = errors.New("gorch: stop timed out waiting for services")
	// ErrDuplicateName is permanent: two services share a WithName. Not
	// retryable.
	ErrDuplicateName = errors.New("gorch: duplicate service name")
	// ErrNilService is permanent: a nil Service, or a nil start function passed
	// to RegisterFunc. Not retryable.
	ErrNilService = errors.New("gorch: nil service")
	// ErrHookTimeout is environmental: it is always joined into the stop error
	// with ErrStopTimeout because a before-stop hook overran the budget reserved
	// for it. The pair attributes the deadline (ErrStopTimeout) and its cause
	// (ErrHookTimeout), so a caller can tell a hook that would not return from a
	// service whose own Stop() would not return. Callers that classify only
	// ErrStopTimeout keep working. Not retryable.
	ErrHookTimeout = errors.New("gorch: stop hook timed out")
	// ErrDependencyCycle is permanent: a hard or soft dependency chain loops.
	// Register rejects it up front; not retryable.
	ErrDependencyCycle = errors.New("gorch: dependency cycle detected")
	// ErrStartAborted is transient: a hard or soft dependency failed or was
	// skipped, so the dependent never started. A failed Start does not consume
	// the lifecycle and may be retried; the joined cause classifies the
	// underlying failure. Retryable.
	ErrStartAborted = errors.New("gorch: start aborted due to dependency failure")
	// ErrUnsupportedOption is permanent: an incoherent option combination (for
	// example WithSelfHeal with WithCron or WithRunOnce). Not retryable.
	ErrUnsupportedOption = errors.New("gorch: unsupported option combination")
	// ErrInvalidBufferSize is permanent: SubscribeWithBuffer was given a negative
	// channel capacity, which cannot back a buffered channel. Fix the size; not
	// retryable.
	ErrInvalidBufferSize = errors.New("gorch: invalid messenger buffer size")
	// ErrNilContext is permanent: Request, RequestAsync, or TypedRequest was
	// handed a nil context. A context is required for cancellation and reply
	// deadlines; pass context.Background() for none. Not retryable.
	ErrNilContext = errors.New("gorch: nil context")

	// Dynamic membership sentinels. See the Contract section of README.md.
	// ErrServiceNotFound is permanent: no registered service has that name. Not
	// retryable.
	ErrServiceNotFound = errors.New("gorch: service not found")
	// ErrHasDependents is transient: a plain StopService/Unregister would break a
	// hard dependent that is Running or Starting. The returned error is a
	// *HasDependentsError naming each blocker; pass WithCascadeStop to tear those
	// dependents down too, or retry once they stop. Dependents in any other status
	// do not block. Retryable.
	ErrHasDependents = errors.New("gorch: service has active dependents")
	// ErrOrchestratorStopping is transient: whole-orchestrator Stop is in
	// progress; retry once it completes. Retryable.
	ErrOrchestratorStopping = errors.New("gorch: orchestrator is stopping")
	// ErrOrchestratorStopped is terminal: whole-orchestrator Stop has completed,
	// so the lifecycle cannot be re-entered. Not retryable.
	ErrOrchestratorStopped = errors.New("gorch: orchestrator already stopped")
	// ErrOrchestratorNotStarted is transient: StartService/StartGroup was called
	// before the orchestrator started, so there is no scheduler or service
	// context yet; retry after Start. Retryable.
	ErrOrchestratorNotStarted = errors.New("gorch: orchestrator not started")
	// ErrDependencyNotRunning is transient: a hard dependency exists but is not
	// StatusRunning; retry once it is. Retryable.
	ErrDependencyNotRunning = errors.New("gorch: dependency not running")
	// ErrDependencyNotFound is permanent: a hard dependency is not registered
	// (dynamically removed, or never added). Not retryable.
	ErrDependencyNotFound = errors.New("gorch: dependency not found")
	// ErrDependencyRemoving is transient: Register named a hard dependency that
	// is mid-teardown (being stopped or removed concurrently). It is distinct
	// from ErrHasDependents, which is only about the target's own running
	// dependents blocking a stop; here the caller has no dependents at all.
	// Retry after the teardown completes. Retryable.
	ErrDependencyRemoving = errors.New("gorch: dependency is being removed")
	// ErrDependencyDepthExceeded is permanent: the walk that checks a new
	// dependency for a cycle exceeded maxDependencyDepth edges. The registered
	// graph is only expected to reach this depth under an unbounded
	// registration/reload loop — registration is otherwise a startup-sized,
	// acyclic graph. The cap turns what would be a fatal, unrecoverable stack
	// overflow into a typed error the caller can classify and act on. Prune the
	// graph before registering again; the rejected call is not retryable.
	ErrDependencyDepthExceeded = errors.New("gorch: dependency depth limit exceeded")
	// ErrReentrantMembership is permanent: a membership operation re-entered
	// from a service's own Start or Stop callback on the same goroutine (for
	// example, a service stopping itself from its Start). It is a programming
	// error, not a recoverable state: fix the code, do not retry. A collision
	// with a reservation held by another goroutine instead returns the retryable
	// ErrMembershipBusy. Exported so callers can classify the rejection with
	// errors.Is.
	ErrReentrantMembership = errors.New("gorch: reentrant membership operation")
	// ErrMembershipBusy is transient: a membership operation is blocked by an
	// in-flight reservation on the target entry (another goroutine's Start,
	// Stop, or group operation). Retryable: poll Busy(name) or retry once the
	// reservation clears. Distinct from ErrReentrantMembership, a same-goroutine
	// re-entry and a programming error.
	ErrMembershipBusy = errors.New("gorch: membership operation blocked by an in-flight reservation")
)

// HasDependentsError is the typed error returned by StopService and Unregister
// when a plain stop would break a hard dependent, i.e. when Dependents(name) is
// non-empty. Name is the service the caller tried to stop or unregister, and
// Dependents holds the transitive hard dependents that block it — the ones
// Running or Starting — in reverse topological order, the same slice
// Dependents(name) returns.
//
// It matches ErrHasDependents with errors.Is, so an existing caller that only
// classifies the sentinel keeps working, while one that needs the names — to
// render a "stopping db will also stop api, worker; continue?" prompt, or to
// decide without parsing the message — can read them off the typed error:
//
//	var depErr *gorch.HasDependentsError
//	if errors.As(err, &depErr) {
//	    fmt.Println("will also stop:", depErr.Dependents)
//	}
type HasDependentsError struct {
	Name       string
	Dependents []string
}

// Error implements error. The message leads with ErrHasDependents so the
// rendered form keeps matching the sentinel's wording.
func (e *HasDependentsError) Error() string {
	return fmt.Sprintf("%s: %s is depended on by %s", ErrHasDependents, e.Name, strings.Join(e.Dependents, ", "))
}

// Is reports that the typed error matches ErrHasDependents, so errors.Is works
// across both the typed and the bare sentinel.
func (e *HasDependentsError) Is(target error) bool { return target == ErrHasDependents }

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
