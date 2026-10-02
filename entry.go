package gorch

import (
	"context"
	"sync"
	"sync/atomic"
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

// resetEntryLocked restores the entry's per-instance lifecycle state to the
// pristine state of a registered, never-started service. It is the single
// definition of "fresh" shared by the failed-Start reset (the entry is retained
// so Start can be retried) and Unregister's discard (the entry is additionally
// marked removed and dropped from the registry). The caller must hold o.mu.
//
// It leaves the registration identity (svc, cfg, name), the start reservation
// (starting), and the removal flags (removing, removed) untouched: those belong
// to the registration/reservation discipline, not to one instance's run. The
// logger is also left in place, so the next Start can rebind it for a retained
// entry and an abandoned live instance can still log for a discarded one.
func (e *serviceEntry) resetEntryLocked() {
	e.status = StatusRegistered
	e.wgDone = false
	e.setCancel(nil)
	e.setDone(nil)
	e.clearTeardown()
	e.setRetryCount(0)
	e.setHealthFailures(0)
	e.setStableSince(time.Time{})
	e.stopsCounted.Store(false)
	e.running.Store(false)
	e.startGoid.Store(0)
	e.stopGoid.Store(0)
	// The scheduler the entry was scheduled on is gone (a failed Start tears it
	// down; Unregister stops it). Drop the schedule id so a later Start
	// re-schedules instead of treating a dead id as a live schedule.
	e.cronID = 0
	e.resetCronAccounting()
}

// resetCronAccounting cancels the entry's shared schedule context, if any, and
// clears the per-schedule tick accounting. cronGen is advanced so an in-flight
// tick of the previous schedule is ignored rather than driving the fresh
// counter negative. Thread-safe.
func (e *serviceEntry) resetCronAccounting() {
	e.cronTrackMu.Lock()
	if e.cronCancel != nil {
		e.cronCancel()
	}
	e.cronGen++
	e.cronCtx = nil
	e.cronCancel = nil
	e.cronActive = 0
	e.cronDraining = false
	e.cronDrained = nil
	e.cronTrackMu.Unlock()
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
