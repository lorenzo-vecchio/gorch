package gorch

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
)

// Orchestrator manages service lifecycles. Create it with New, or use the zero
// value directly: a zero-value Orchestrator is usable because the first public
// call lazily initialises it with the same defaults as New(), so
// `var o Orchestrator` behaves like `New()` and never panics on an
// uninitialised registry, Messenger, or shutdown channel.
type Orchestrator struct {
	cfg     config
	started bool
	// startClaimed closes the window between Start's o.started pre-check and the
	// point where the startOnce closure publishes o.started. The Start call that
	// wins the claim sets it; resetAfterStartFailure clears it when that Start
	// fails, so a retry can claim again. A concurrent Start that finds it set
	// returns ErrAlreadyStarted immediately instead of blocking on startOnce and
	// returning a startErr it never wrote (nil) while nothing started (issue #32).
	// Guarded by mu.
	startClaimed bool
	// stopping is true from the moment Stop begins until it returns; stopped is
	// true once Stop has completed. Both are guarded by mu and gate dynamic
	// membership ops (C6, D10).
	stopping bool
	stopped  bool
	mu       sync.RWMutex // protects started, startClaimed, stopping, stopped, entries slice, nameIndex

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
	// ownerSeq is the monotonic Messenger owner-id source. Ids are never reused
	// (see ownerState): a reused id could hit a drained ownerState and black-hole
	// a later view's Subscribe.
	ownerSeq atomic.Uint64

	// Status tracking
	statusMu sync.RWMutex

	// Health check
	healthCancel context.CancelFunc // cancel health-check loop goroutine
	healthDone   chan struct{}      // closed when health-check loop exits

	wg        sync.WaitGroup
	stopOnce  sync.Once
	startOnce sync.Once

	// initOnce runs initialize at most once: New performs it eagerly, while a
	// zero-value Orchestrator performs it lazily on its first public call. It is
	// what makes `var o Orchestrator` usable instead of panicking on a nil
	// registry, Messenger, or shutdown channel.
	initOnce sync.Once

	// shutdownDone is closed exactly once, by signalShutdownDone, when a Stop
	// call completes. Done() returns it directly: unlike the old
	// sync.OnceValue that wrapped o.wg.Wait, it can never close early when a
	// hot add or restart reuses the WaitGroup while services are live.
	shutdownDone     chan struct{}
	shutdownDoneOnce sync.Once

	metricsStarts       atomic.Int64
	metricsStops        atomic.Int64
	metricsCrashes      atomic.Int64
	metricsRestarts     atomic.Int64
	metricsHealthFails  atomic.Int64
	metricsCronFailures atomic.Int64
	// metricsAbandoned counts teardown goroutines walked away from because a
	// deadline won. It is monotonic: there is no signal that an abandoned
	// goroutine later returned, so it is never decremented.
	metricsAbandoned atomic.Int64
}

// New creates a new Orchestrator. Each call returns a fresh, independent instance.
// Orchestrators can be nested: a service may create its own gorch to manage
// sub-services. Configure via Option functions; the zero-option call uses the
// defaults (LogLevelInfo, health checks every 30s with a 5s probe timeout, a 30s
// failed-Start rollback budget).
//
// The zero value is usable too: `var o Orchestrator` behaves like New() with no
// options, because every public entry point lazily initialises it on first use.
func New(opts ...Option) *Orchestrator {
	o := &Orchestrator{}
	o.initOnce.Do(func() { o.initialize(opts) })
	return o
}

// ensureInit lazily initialises a zero-value Orchestrator with the same defaults
// as New(). Every public entry point calls it, so `var o Orchestrator` behaves
// like New() rather than panicking on an uninitialised registry, Messenger, or
// shutdown channel. sync.Once makes it safe under concurrent first calls and a
// no-op on an orchestrator already built by New, so state registered before the
// first public call is never discarded.
func (o *Orchestrator) ensureInit() {
	o.initOnce.Do(func() { o.initialize(nil) })
}

// initialize applies opts over the defaults and creates the runtime state. It is
// the single initializer behind New and ensureInit; initOnce guarantees it runs
// at most once per Orchestrator.
func (o *Orchestrator) initialize(opts []Option) {
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
	o.cfg = cfg
	o.messenger = newMessenger()
	o.nameIndex = make(map[string]*serviceEntry)
	o.shutdownDone = make(chan struct{})
}

// signalShutdownDone closes the Done channel at most once. Stop defers it on
// every return path — including its never-started no-op — so Done means "a Stop
// call completed", not "every goroutine exited".
func (o *Orchestrator) signalShutdownDone() {
	o.shutdownDoneOnce.Do(func() { close(o.shutdownDone) })
}

// Run starts the orchestrator, blocks on SIGINT/SIGTERM, then stops.
// Returns any error from Start or aggregated errors from Stop.
// Optional signals override the default signal set (SIGINT, SIGTERM). A nil
// element in signals is inert (it can never match a delivered signal); passing
// only nil therefore registers no catchable signal and Run never returns. Pass
// at least one real signal, or no argument to use the defaults.
func (o *Orchestrator) Run(stopTimeout time.Duration, signals ...os.Signal) error {
	o.ensureInit()
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
