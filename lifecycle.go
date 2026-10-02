package gorch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// defaultRestartStopTimeout bounds the before-stop hook of a self-heal
// restart's best-effort cleanup when the service sets no WithStopTimeout. A
// restart has no caller to supply a budget, but the hook must still be bounded
// or a hook that never returns would strand the restart goroutine forever
// (#56); 30s matches the default failed-Start rollback budget.
const defaultRestartStopTimeout = 30 * time.Second

func (o *Orchestrator) startOneService(entry *serviceEntry) error {
	// A removed (or mid-teardown) entry was selected before Unregister deleted
	// it (e.g. a StartGroup reservation): never revive it.
	if entry.removed.Load() || entry.removing.Load() {
		return fmt.Errorf("%w: %s", ErrServiceNotFound, entry.name)
	}
	// Guard against reentrant membership ops from this service's own Start: a
	// nested StartService on this entry is rejected rather than recursing (C17).
	entry.starting.Store(true)
	defer entry.starting.Store(false)
	// A fresh instance gets a fresh stop-metric latch.
	entry.stopsCounted.Store(false)

	// --- before-start hook ---
	hook := entry.cfg.onBeforeStart
	if hook == nil {
		hook = o.cfg.OnBeforeStart
	}
	if hook != nil {
		if err := callErr(func() error { return hook(entry.name) }); err != nil {
			o.setStatus(entry, StatusStopped)
			return fmt.Errorf("before-start hook: %w", err)
		}
	}

	// Check start condition.
	if entry.cfg.startCondition != nil {
		ok, err := callBool(entry.cfg.startCondition)
		if err != nil {
			o.setStatus(entry, StatusStopped)
			return fmt.Errorf("start condition: %w", err)
		}
		if !ok {
			o.setStatus(entry, StatusStopped)
			return nil
		}
	}

	o.setStatus(entry, StatusStarting)

	// Each instance gets a fresh Messenger owner, so a restart or a later cron
	// tick never reuses (or accumulates under) the previous instance's id.
	owner := o.newOwner(entry)

	// Per-entry teardown context, fresh for each start era. Instance contexts
	// are its children, so only StopService/Unregister (or orchestrator Stop)
	// aborts a pending self-heal; a health-threshold restart cancels just the
	// instance.
	entryCtx, entryCancel := context.WithCancel(o.ctx)
	entry.setTeardown(entryCtx, entryCancel)

	// Per-service context with optional timeout.
	svcCtx, svcCancel := context.WithCancel(entryCtx)
	entry.setCancel(svcCancel)
	entry.startedAt = time.Now()
	entry.setStableSince(time.Now())

	sc := ServiceContext{
		Context:   svcCtx,
		Logger:    entry.getLogger(),
		Messenger: o.messenger.view(owner),
	}

	// Determine timeout.
	timeout := entry.cfg.startTimeout
	if timeout == 0 {
		timeout = o.cfg.DefaultStartTimeout
	}

	if entry.cfg.runOnce {
		// runOnce runs synchronously, so its subscriptions have no use once Start
		// returns: release the owner as the start unwinds.
		defer o.releaseOwner(entry, owner.id)
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
				id := curGoroutineID()
				entry.startGoid.Store(id)
				defer entry.startGoid.CompareAndSwap(id, 0)
				defer func() {
					if r := recover(); r != nil {
						entry.getLogger().Error("service panicked", "panic", fmt.Sprint(r))
						done <- fmt.Errorf("panic: %v", r)
					}
				}()
				done <- entry.getSvc().Start(sc)
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
			id := curGoroutineID()
			entry.startGoid.Store(id)
			err = callErr(func() error { return entry.getSvc().Start(sc) })
			entry.startGoid.CompareAndSwap(id, 0)
		}
		entry.setCancel(nil)
		svcCancel()
		// runOnce does not self-heal, so its teardown context has no further use.
		entry.clearTeardown()
		entryCancel()

		if err != nil && err != context.Canceled {
			o.setStatusErr(entry, StatusCrashed, err)
			o.callAfterStartHook(entry, err)
			return err
		}
		entry.wgDone = true
		o.setStatus(entry, StatusSucceeded)
		o.callAfterStartHook(entry, nil)
		// wg.Done not needed—runOnce doesn't add to wg
		return nil
	}

	// Persistent service: start in goroutine.
	o.setStatus(entry, StatusRunning)
	o.metricsStarts.Add(1)
	// Clear the wait-group latch for this instance. A restarted entry may still
	// carry wgDone = true from a previous incarnation: StartService resets it on
	// its own path, but a StartGroup restart and a hot-added survivor of a failed
	// Start (which resetAfterStartFailure's snapshot does not reach) do not. An
	// instance whose exit sees a stale wgDone skips o.wg.Done(), so the counter
	// never returns to zero, Done() never closes, and Stop reports ErrStopTimeout.
	// startOneService is the single start path that adds to the wait group, so the
	// latch is cleared here for every new instance.
	o.mu.Lock()
	entry.wgDone = false
	o.mu.Unlock()
	o.wg.Add(1)

	// done closes when this instance's goroutine has fully exited, so a
	// StopService/Unregister can wait for exactly this run.
	done := make(chan struct{})
	entry.setDone(done)

	// Create a detachable start context. If timeout is set, we use a separate
	// context for the race window rather than wrapping the service context.
	startErrCh := make(chan error, 1)
	o.spawnInstance(entry, sc, done, owner.id, startErrCh)

	if timeout > 0 {
		select {
		case startErr := <-startErrCh:
			// Start returned synchronously within the window. A real error is a
			// start failure (status already Crashed via handleServiceDone above);
			// a clean return or context.Canceled is not.
			if startErr != nil && !errors.Is(startErr, context.Canceled) {
				o.callAfterStartHook(entry, startErr)
				return startErr
			}
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
		callVoid(func() { hook(entry.name, err) })
	}
}

// stopStartedServices stops all running services (used for cleanup on start
// failure). It works on the Start snapshot rather than o.entries, so a
// concurrent hot Register cannot race the cleanup. Services are torn down in
// reverse topological order — the same order Start/Stop/StopGroup honour — so a
// dependency is never stopped before its dependent, regardless of registration
// order. topoSortForStop falls back to registration order for a cyclic subset
// and still returns the ordering error, so every entry is reached and the
// failure is surfaced rather than silently leaving a service running. The
// deadline bounds the whole rollback and is shared unchanged by every entry, so
// a service that blocks in a hook or Stop() cannot hang Start past it; a zero
// deadline means no bound. The returned error aggregates every unverified
// teardown, including ErrStopTimeout/ErrHookTimeout, plus any ordering error.
// ponytail: sequential stop; parallel Stop is premature.
func (o *Orchestrator) stopStartedServices(entries []*serviceEntry, deadline time.Time) error {
	// Cancel context.
	if o.cancel != nil {
		o.cancel()
	}
	// Stop cron.
	if o.cronSched != nil {
		<-o.cronSched.Stop().Done()
	}
	// Stop services in reverse topological order. A cyclic subset still reaches
	// every entry (registration-order fallback) and contributes topoErr below.
	levels, topoErr := o.topoSortForStop(entries)
	stopErr := topoErr
	for i := len(levels) - 1; i >= 0; i-- {
		for _, entry := range levels[i] {
			// Entries that never started need no rollback: their Start either
			// never ran or was skipped before the failure, so there is nothing
			// to tear down.
			o.statusMu.RLock()
			s := entry.status
			o.statusMu.RUnlock()
			if s == StatusRunning || s == StatusStarting {
				if _, _, err := o.stopOneServiceDeadline(entry, deadline); err != nil {
					stopErr = errors.Join(stopErr, fmt.Errorf("%s: %w", entry.name, err))
				}
			}
		}
	}
	// Drain every Messenger owner the snapshot registered, mirroring teardown's
	// stop-then-drain pairing (stopEntry/drainService). A service that ignored
	// cancellation and outlives the rollback never runs its deferred
	// releaseOwner, so without this its owner id — and every subscription under
	// it — would survive in both owner maps. Because a failed Start is
	// retryable, that leak would grow by one id per abandoned attempt; draining
	// once the stop attempts are done retires the ids regardless. The snapshot
	// entries are still reserved here (clearStarting is deferred until the
	// Start closure returns), so no concurrent StartService can mint a fresh
	// owner this loop would wrongly drain. drainService marks each owner dead,
	// so the abandoned instance's scoped view cannot resubscribe afterwards.
	for _, entry := range entries {
		o.drainService(entry)
	}
	// Stop log-pump: signal it to drain buffered entries and exit.
	if o.logQuit != nil {
		close(o.logQuit)
	}
	return stopErr
}

// setStatus updates the service status (thread-safe).
func (o *Orchestrator) setStatus(entry *serviceEntry, s ServiceStatus) {
	o.setStatusErr(entry, s, nil)
}

// setStatusErr updates the service status and, on a crash, forwards the real
// exit cause to the OnCrash callback (thread-safe).
func (o *Orchestrator) setStatusErr(entry *serviceEntry, s ServiceStatus, err error) {
	o.statusMu.Lock()
	old := entry.status
	entry.status = s
	o.statusMu.Unlock()

	if o.cfg.OnStateChange != nil && old != s {
		callVoid(func() { o.cfg.OnStateChange(entry.name, old, s) })
	}
	if s == StatusCrashed {
		o.metricsCrashes.Add(1)
	}
	if o.cfg.OnCrash != nil && s == StatusCrashed {
		if err == nil {
			err = fmt.Errorf("service %s crashed", entry.name)
		}
		callVoid(func() { o.cfg.OnCrash(entry.name, err) })
	}
}

// stopOneService stops a single service entry with before/after hooks and panic
// recovery, honoring the entry's per-service stop timeout. With no caller
// deadline available it waits for the sequence, so a completed stop is committed
// as StatusStopped; a timeout inside the sequence (there is none when the caller
// deadline is zero, except the per-service WithStopTimeout) leaves the entry
// StatusStopping rather than claiming it stopped.
func (o *Orchestrator) stopOneService(entry *serviceEntry) error {
	wasActive, completed, stopErr := o.stopOneServiceDeadline(entry, time.Time{})
	if completed {
		o.finishStop(entry, wasActive)
	}
	return stopErr
}

// runStopSequence runs the before-stop hook, the service's own Stop() (bounded
// by its per-service WithStopTimeout) and the after-stop hook. It reports
// whether the sequence ran to completion: a hook that overran (ErrHookTimeout)
// or a Stop() capped away leaves it unverified. It never touches the entry's
// status or metrics, so the crash-restart cleanup can reuse it without driving
// the entry back through the teardown state machine.
func (o *Orchestrator) runStopSequence(entry *serviceEntry, hookDeadline time.Time) (error, bool) {
	hookErr := o.callBeforeStopHook(entry, hookDeadline)
	stopErr, stopReturned := o.stopServiceBounded(entry)
	if afterHook := afterStopHook(entry, o); afterHook != nil {
		callVoid(func() { afterHook(entry.name, stopErr) })
	}
	complete := !errors.Is(hookErr, ErrHookTimeout) && stopReturned
	return errors.Join(hookErr, stopErr), complete
}

// restartStopHookDeadline returns the deadline for the before-stop hook in a
// self-heal restart's cleanup sequence. The restart has no caller budget, so it
// reuses the per-service WithStopTimeout when set and falls back to
// defaultRestartStopTimeout otherwise; the hook gets half of it, mirroring how
// stopOneServiceDeadline splits the caller's deadline (the service's own Stop()
// keeps its full per-service timeout through stopServiceBounded). A hook that
// overruns the deadline is abandoned and reported as ErrHookTimeout, and the
// restart proceeds regardless — a failed cleanup is not a reason to leave the
// service dead.
func (o *Orchestrator) restartStopHookDeadline(entry *serviceEntry) time.Time {
	budget := entry.cfg.stopTimeout
	if budget <= 0 {
		budget = defaultRestartStopTimeout
	}
	return time.Now().Add(budget / 2)
}

// beginStop enters the teardown state machine and reports whether the entry was
// active (Running or Starting) when the teardown began. It normally commits the
// transient StatusStopping. A runOnce gate that already reached StatusSucceeded
// is the one exception: "succeeded" is a permanent fact about the gate that a
// later stop must not erase, so the status is left untouched and finishStop
// likewise skips the terminal StatusStopped. The gate has no live instance to
// stop, so reporting it as Stopping would both misrepresent it and let a
// teardown retroactively turn a satisfied dependency into a failed one.
func (o *Orchestrator) beginStop(entry *serviceEntry) (wasActive bool) {
	o.statusMu.Lock()
	s := entry.status
	wasActive = s == StatusRunning || s == StatusStarting
	o.statusMu.Unlock()

	if s != StatusSucceeded {
		o.setStatus(entry, StatusStopping)
	}
	return wasActive
}

// stopOneServiceDeadline is stopOneService with an additional hard caller
// deadline that bounds the whole stop — the before/after hooks and the service's
// own Stop() — not only Stop(). The effective Stop() cap remains the smaller of
// the per-service WithStopTimeout and the time left before the deadline. A zero
// deadline leaves the per-service timeout in charge and runs the sequence
// synchronously.
//
// The before-stop hook gets at most half of the caller budget on its own, so a
// hook that blocks forever cannot consume the whole deadline and starve the
// service's Stop(): the service is always given a chance to release its
// resources, and a hook that overruns is reported as ErrHookTimeout.
//
// The entry is left StatusStopping (a succeeded gate keeps StatusSucceeded);
// only a sequence that ran to completion (no caller-deadline expiry, no
// overrunning hook, no capped-away Stop()) is reported complete. The caller
// commits the terminal status with finishStop once it has also verified the
// instance exited, so a timed-out stop never claims StatusStopped for a service
// that may still be alive.
//
// When the caller deadline wins, the sequence goroutine is abandoned, logged at
// Error level and counted in Metrics().AbandonedGoroutines. An inner hook or
// Stop() abandoned earlier in the same sequence is counted separately, once per
// genuinely-abandoned goroutine.
func (o *Orchestrator) stopOneServiceDeadline(entry *serviceEntry, deadline time.Time) (wasActive, completed bool, stopErr error) {
	wasActive = o.beginStop(entry)

	completed = true
	if deadline.IsZero() {
		stopErr, completed = o.runStopSequence(entry, time.Time{})
	} else {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			// The deadline already passed: bound the sequence to a sliver so a
			// blocking hook or Stop() cannot hang past it.
			remaining = time.Nanosecond
		}
		// Half the remaining budget for the hook, so Stop() is never starved.
		hookDeadline := time.Now().Add(remaining / 2)
		type seqResult struct {
			err       error
			completed bool
		}
		done := make(chan seqResult, 1)
		go func() {
			err, ok := o.runStopSequence(entry, hookDeadline)
			done <- seqResult{err: err, completed: ok}
		}()
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		select {
		case r := <-done:
			stopErr, completed = r.err, r.completed
		case <-timer.C:
			o.recordAbandoned(entry, "abandoning stop sequence: it did not return before the deadline")
			stopErr = fmt.Errorf("stop timeout after %v: %w", remaining, ErrStopTimeout)
			completed = false
		}
	}
	return wasActive, completed, stopErr
}

// finishStop commits the terminal StatusStopped — and, for an entry that was
// Running or Starting when the teardown began, the public Stops accounting. It
// must only be called once the teardown is verified complete: the sequence
// finished and the instance goroutine is known to have exited. A timed-out stop
// leaves the entry StatusStopping instead, so Status() never claims a service
// stopped while it may still be alive.
//
// A runOnce gate already in StatusSucceeded is left there: starting and stopping
// the orchestrator does not undo the fact that the gate did its job, and
// StatusStopped would both erase that distinction and read as a failed
// dependency to a later dependent start. beginStop skipped the StatusStopping
// transition for the same reason, so the gate's status is stable across its
// stop.
func (o *Orchestrator) finishStop(entry *serviceEntry, wasActive bool) {
	if o.statusOf(entry) == StatusSucceeded {
		return
	}
	o.setStatus(entry, StatusStopped)
	if wasActive {
		o.accountStop(entry)
	}
}

// callBeforeStopHook runs the effective before-stop hook. When deadline is zero
// it waits for the hook indefinitely; otherwise the hook is bounded by deadline
// and an overrun is reported as ErrHookTimeout (which also matches
// ErrStopTimeout, so callers that only classify whole-stop timeouts keep
// working). A panic in the hook becomes an error, never an unwind. Running the
// hook in its own goroutine is what lets the caller proceed to Stop() after an
// overrun instead of abandoning the service mid-teardown.
//
// The trade-off is explicit: a hook that ignores the deadline is abandoned in
// its goroutine, logged at Error level and counted in
// Metrics().AbandonedGoroutines. The library cannot force user code to return,
// so the leak is made visible rather than prevented.
func (o *Orchestrator) callBeforeStopHook(entry *serviceEntry, deadline time.Time) error {
	hook := stopHook(entry, o)
	if hook == nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- callErr(func() error { return hook(entry.name) }) }()
	if deadline.IsZero() {
		return <-done
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		o.recordAbandoned(entry, "abandoning before-stop hook: it did not return before the deadline")
		return fmt.Errorf("%w: before-stop hook did not return before the deadline: %w", ErrHookTimeout, ErrStopTimeout)
	}
}

// recordAbandoned notes that a teardown goroutine was abandoned because its
// deadline won: it increments the monotonic Metrics().AbandonedGoroutines
// counter (never decremented, because there is no reliable signal that the
// goroutine later returned) and logs an Error against entry when one is known,
// so the leak is attributable to a named service. The counter is the only
// visibility into a goroutine that is otherwise invisible to Done() and -race.
func (o *Orchestrator) recordAbandoned(entry *serviceEntry, msg string) {
	o.metricsAbandoned.Add(1)
	if entry != nil {
		if lg := entry.getLogger(); lg != nil {
			lg.Error(msg)
		}
	}
}

// stopServiceBounded runs the service's own Stop(), capped by its per-service
// WithStopTimeout. A zero timeout leaves it unbounded; the caller deadline (when
// set) bounds it from the outside. It reports whether Stop() actually returned:
// on the per-service cap it does not, so the teardown is unverified and the
// entry must not be reported StatusStopped.
//
// A Stop() that runs past the cap is abandoned in its goroutine, logged at
// Error level and counted in Metrics().AbandonedGoroutines, like an overrunning
// hook.
func (o *Orchestrator) stopServiceBounded(entry *serviceEntry) (error, bool) {
	timeout := entry.cfg.stopTimeout
	if timeout <= 0 {
		return o.safeStopWithResult(entry), true
	}
	done := make(chan error, 1)
	go func() { done <- o.safeStopWithResult(entry) }()
	select {
	case err := <-done:
		return err, true
	case <-time.After(timeout):
		o.recordAbandoned(entry, "abandoning Stop: it did not return before the per-service timeout")
		return fmt.Errorf("stop timeout after %v", timeout), false
	}
}

// accountStop records one stop for entry at most once per instance. The teardown
// path (stopOneServiceDeadline) and the instance's own exit path
// (handleServiceDone) can both observe the same stop; the latch makes the public
// Stops metric count it exactly once.
func (o *Orchestrator) accountStop(entry *serviceEntry) {
	if entry.stopsCounted.CompareAndSwap(false, true) {
		o.metricsStops.Add(1)
	}
}

// stopHook resolves the effective before-stop hook (per-service over global).
func stopHook(entry *serviceEntry, o *Orchestrator) func(string) error {
	if entry.cfg.onBeforeStop != nil {
		return entry.cfg.onBeforeStop
	}
	return o.cfg.OnBeforeStop
}

// afterStopHook resolves the effective after-stop hook (per-service over global).
func afterStopHook(entry *serviceEntry, o *Orchestrator) func(string, error) {
	if entry.cfg.onAfterStop != nil {
		return entry.cfg.onAfterStop
	}
	return o.cfg.OnAfterStop
}

// safeStopWithResult calls the entry's Stop with panic recovery, returning any
// error. It records the calling goroutine as the entry's stop owner while the
// user callback runs, so a membership op that re-enters from that same Stop can
// be told apart from one colliding on another goroutine.
func (o *Orchestrator) safeStopWithResult(entry *serviceEntry) (err error) {
	id := curGoroutineID()
	entry.stopGoid.Store(id)
	defer entry.stopGoid.CompareAndSwap(id, 0)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("stop panicked: %v", r)
		}
	}()
	return entry.getSvc().Stop()
}

// safeStop calls Stop with panic recovery. Best-effort; errors and panics
// are silently discarded.
func (o *Orchestrator) safeStop(entry *serviceEntry) {
	_ = o.stopOneService(entry)
}

// entriesSnapshot returns a copy of the current entry slice, taken under
// o.mu so callers can iterate it while running user code (hooks, Stop) without
// holding the lock. The copy is what makes the read safe: a concurrent
// membership op replaces o.entries, and the snapshot must not observe that
// mutation partway through an iteration.
func (o *Orchestrator) entriesSnapshot() []*serviceEntry {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]*serviceEntry, len(o.entries))
	copy(out, o.entries)
	return out
}

// persistentEntries returns non-cron, non-runOnce entries.
func (o *Orchestrator) persistentEntries() []*serviceEntry {
	var out []*serviceEntry
	for _, e := range o.entriesSnapshot() {
		if e.cfg.cronSpec == "" && !e.cfg.runOnce {
			out = append(out, e)
		}
	}
	return out
}

// nonPersistentEntries returns cron-only and runOnce entries. Its predicate
// (cronSpec != "" || runOnce) is the exact complement of persistentEntries'
// (cronSpec == "" && !runOnce), so the two lists are disjoint and together
// cover every registry entry. Whole-orchestrator Stop relies on that to stop
// each entry exactly once across its two teardown passes.
func (o *Orchestrator) nonPersistentEntries() []*serviceEntry {
	var out []*serviceEntry
	for _, e := range o.entriesSnapshot() {
		if e.cfg.runOnce || e.cfg.cronSpec != "" {
			out = append(out, e)
		}
	}
	return out
}

// spawnInstance launches one running instance and owns its exit. It is the
// single instance-spawning path — startOneService (Start, StartService,
// StartGroup) and the self-heal restart both call it — so the goroutine body,
// the panic recovery, the wait-group semantics and the fresh owner/messenger
// state a live instance carries cannot diverge between a start and a restart.
// (v0.9.0 consolidated registration the same way.)
//
// entry.getSvc() is read inside the goroutine, so the caller must have installed
// the service the instance runs before calling spawnInstance and must not
// replace it until done is closed.
//
// done is closed once both the Start call and the handleServiceDone decision
// have finished, so a teardown can wait for exactly this run. When startErrCh is
// non-nil the start outcome is reported on it (non-blocking) before the possibly
// blocking handleServiceDone; a self-heal exit reports success so a start
// timeout never aborts Start for a service that comes straight back, while a
// non-self-heal exit reports its real error after the status is committed. A
// restart passes a nil startErrCh.
func (o *Orchestrator) spawnInstance(entry *serviceEntry, sc ServiceContext, done chan struct{}, ownerID uint64, startErrCh chan error) {
	go func() {
		var exitErr error
		defer func() {
			if r := recover(); r != nil {
				sc.Logger.Error("service panicked", "panic", fmt.Sprint(r))
				exitErr = fmt.Errorf("panic: %v", r)
			}
			if entry.cfg.factory != nil {
				// Self-heal: an exit (even an instant error) is handled by
				// handleServiceDone's restart policy. Signal success before the
				// (possibly blocking) restart logic.
				signalStartErr(startErrCh, nil)
				o.handleServiceDone(entry, sc, exitErr, ownerID)
				close(done)
				return
			}
			// No self-heal: update status before signalling so a dependent's
			// status check deterministically sees Crashed/Stopped, not Running.
			o.handleServiceDone(entry, sc, exitErr, ownerID)
			signalStartErr(startErrCh, exitErr)
			close(done)
		}()
		id := curGoroutineID()
		entry.startGoid.Store(id)
		defer entry.startGoid.CompareAndSwap(id, 0)
		exitErr = entry.getSvc().Start(sc)
		if exitErr != nil && exitErr != context.Canceled {
			sc.Logger.Error("service returned error", "error", exitErr.Error())
		}
	}()
}

// signalStartErr reports a start outcome on ch without blocking. A nil ch (a
// self-heal restart) and a full ch are both no-ops, so the reporting never
// stalls the instance's exit.
func signalStartErr(ch chan error, err error) {
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
}

// handleServiceDone is called when a non-cron service exits (normally or via panic).
// For services without self-heal it decrements the waitgroup once.
// For self-heal services it uses the configured backoff and retry policy.
func (o *Orchestrator) handleServiceDone(entry *serviceEntry, sc ServiceContext, exitErr error, ownerID uint64) {
	// Release exactly this instance's subscriptions as it exits; a concurrent
	// restart's fresh owner is never touched.
	defer o.releaseOwner(entry, ownerID)
	if entry.cfg.factory == nil {
		// A non-self-heal instance has no further use for its teardown context;
		// releasing it (deferred, so it runs after the status transition) stops
		// children of o.ctx accumulating across stop/start cycles. Only this
		// instance's context is released, so a concurrent restart's fresh one
		// survives.
		teardown := entry.getTeardown()
		defer entry.releaseTeardownIf(teardown)
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
		switch {
		case entry.removing.Load():
			// A StopService/Unregister teardown owns this entry. It commits the
			// terminal status via finishStop only once the stop is verified
			// complete, so the instance exiting mid-teardown must not claim
			// StatusStopped while Stop() may still be running.
		case entry.teardownActive():
			// A teardown that began before this exit wins: the entry ends
			// StatusStopped, not StatusCrashed. accountStop dedupes against the
			// teardown path's own accounting.
			o.setStatus(entry, StatusStopped)
			o.accountStop(entry)
		case exitErr != nil && !errors.Is(exitErr, context.Canceled):
			o.setStatusErr(entry, StatusCrashed, exitErr)
		default:
			o.setStatus(entry, StatusStopped)
			o.accountStop(entry)
		}
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

	// Check if context was cancelled (orchestrator shutting down), mirroring the
	// non-self-heal branch above: Stop() owns the status transitions through
	// stopOneService (running→stopping→stopped) and accounts the stop, so the
	// instance exiting on shutdown must not commit StatusStopped first. Committing
	// it here would turn a terminal status back into Stopping and skip the stop
	// accounting, making the sequence and Metrics().Stops depend on whether this
	// handler or Stop's loop reached the entry first.
	select {
	case <-o.ctx.Done():
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
		if time.Since(entry.getStableSince()) >= entry.cfg.resetAfter {
			entry.setRetryCount(0)
		}
	}

	// Classify the exit before the restart decision. A real error (not a
	// deliberate context cancellation, which the health threshold uses to
	// trigger a restart) is a crash: report it now, so observers see the
	// Running→Crashed transition, OnCrash and the Crashes metric even when
	// self-heal immediately restarts the service. A teardown owns the terminal
	// status instead, so it must not be pre-empted by a crash report.
	isCrash := exitErr != nil && !errors.Is(exitErr, context.Canceled)
	if isCrash && !entry.teardownActive() {
		o.setStatusErr(entry, StatusCrashed, exitErr)
	}

	// Check maxRetries.
	if entry.cfg.maxRetries > 0 && entry.getRetryCount() >= entry.cfg.maxRetries {
		entry.getLogger().Error("max retries reached, giving up", "retries", entry.getRetryCount())
		switch {
		case entry.removing.Load():
			// A StopService/Unregister teardown owns the terminal status; leave
			// it to finishStop rather than claiming Stopped early.
		case entry.teardownActive():
			// StopService/Unregister cancelled this entry as it reached the
			// retry limit: the teardown wins, so leave it StatusStopped.
			o.setStatus(entry, StatusStopped)
		default:
			// A real crash was already reported above (once); a clean or
			// cancelled exit reaching the retry limit is labelled here, so the
			// crash that ends the retry budget is still reported exactly once.
			if !isCrash {
				o.setStatusErr(entry, StatusCrashed, exitErr)
			}
		}
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

	entry.setRetryCount(entry.getRetryCount() + 1)

	// A clean or cancelled exit that self-heals is not a crash, but the instance
	// is gone until the restart: report it Stopped so the backoff wait does not
	// advertise a dead instance as Running. A crashed exit was already reported
	// Crashed above and keeps that status until the restart.
	if !isCrash && !entry.teardownActive() {
		o.setStatus(entry, StatusStopped)
	}

	// Compute backoff delay.
	backoff := entry.cfg.backoff
	if backoff == nil {
		backoff = ConstantBackoff{Delay: 1 * time.Second}
	}
	delay := backoff.Next(entry.getRetryCount())

	entry.getLogger().Warn("self-heal: restarting service",
		"retry", entry.getRetryCount(), "delay", delay.String())

	// The crashed instance's subscriptions have no further use: release them
	// before the (possibly long) backoff wait, so they do not keep receiving
	// while the restart is pending.
	o.releaseOwner(entry, ownerID)

	// Wait out the backoff, aborting if the entry is torn down or the
	// orchestrator shuts down. A cancelled instance context alone is not a
	// teardown (the health threshold cancels it to trigger a restart), so watch
	// the per-entry teardown context, which only StopService/Unregister or Stop
	// cancel (C4).
	teardown := entry.getTeardown()
	if teardown == nil {
		teardown = sc.Context
	}
	select {
	case <-teardown.Done():
	case <-time.After(delay):
	}
	// Re-read the teardown after the select: when the timer and teardown fire
	// together the select may pick the timer, and spawning the next instance
	// under a cancelled context would let it outlive this teardown.
	if teardown.Err() != nil {
		if !entry.removing.Load() {
			o.setStatus(entry, StatusStopped)
		}
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

	// The dead incarnation is gone for good, so release its per-service context
	// before the cleanup. For a self-heal entry the teardown context is shared
	// across incarnations (only StopService/Unregister/Stop cancel it), so an
	// uncancelled instance context stays registered as one of its children for
	// the entry's whole lifetime — one retained context per restart, unbounded in
	// a zero-backoff crash loop (#109). Cancelling before Stop() mirrors
	// stopEntry, which cancels the live instance before calling its Stop(), and
	// is idempotent: the health-threshold path that already cancelled this
	// context to trigger the exit is unaffected. The teardown context itself is
	// deliberately left live so a later StopService still aborts the next
	// incarnation.
	if cancel := entry.getCancel(); cancel != nil {
		cancel()
	}

	// Best-effort cleanup of the old instance: its Stop() releases resources.
	// The status was already resolved for the exit (Crashed for a real crash,
	// Stopped for a clean or cancelled one) and must not be driven back through
	// the teardown state machine, which would emit a spurious Stopping→Stopped
	// and report a self-heal restart as an ordinary stop.
	//
	// The cleanup runs the before-stop hook and the after-stop hook too: they
	// surround the dead instance's Stop(), exactly as on a caller-initiated
	// teardown, so a hook that releases a lease or deregisters from a load
	// balancer still observes the instance that just died (documented on
	// WithOnBeforeStop/WithOnAfterStop). Unlike a caller stop, a restart has no
	// caller-supplied deadline, so the hook is bounded by the per-service
	// WithStopTimeout (defaultRestartStopTimeout when unset); a hook that
	// overruns is abandoned, logged and counted but never delays the restart.
	_, _ = o.runStopSequence(entry, o.restartStopHookDeadline(entry))
	// The restarted instance gets its own stop-metric latch.
	entry.stopsCounted.Store(false)

	newSvc := entry.cfg.factory()
	entry.setSvc(newSvc)

	// Update logger for the new instance (same name as at registration).
	if o.cfg.Logger != nil {
		entry.setLogger(newServiceLoggerWith(entry.name, o.cfg.Logger))
	} else {
		entry.setLogger(newServiceLogger(entry.name, o.logCh, o.logQuit, o.cfg.LogLevel))
	}
	entry.setStableSince(time.Now())

	// New per-service context, still under the entry's teardown so a later
	// teardown aborts this instance too. The restarted instance gets a fresh
	// Messenger owner, so the crashed instance's subscriptions are not reused.
	svcCtx, svcCancel := context.WithCancel(teardown)
	entry.setCancel(svcCancel)
	restartOwner := o.newOwner(entry)
	newSc := ServiceContext{Context: svcCtx, Logger: entry.getLogger(), Messenger: o.messenger.view(restartOwner)}

	// The restarted instance gets its own exit channel, so a later teardown
	// waits for the current run rather than the crashed one.
	newDone := make(chan struct{})
	entry.setDone(newDone)
	// The new instance is live: re-establish StatusRunning so Status()/Statuses()
	// do not keep reporting the previous crash (or a clean exit's Stopped) while
	// the service is actually up. Set before the goroutine starts, so a
	// concurrent StartService sees a running entry and stays idempotent.
	o.setStatus(entry, StatusRunning)
	o.spawnInstance(entry, newSc, newDone, restartOwner.id, nil)
	o.metricsRestarts.Add(1)
}

// invokeCron executes a cron-triggered service tick. Concurrency policy is
// determined by the entry's cronMode.

// Start begins the orchestrator lifecycle. Returns ErrAlreadyStarted if already started.
// If Start fails, the orchestrator is reset and may be started again (e.g. to retry
// after a transient dependency failure). The rollback is bounded by
// WithFailedStartTimeout (default 30s) and shares one budget across every stop and
// the final wait, so a service that blocks in Stop() cannot hang Start; an overrun
// is reported as ErrStopTimeout and the reset is best-effort.
// A persistent service that returns an error synchronously aborts Start only when a
// start timeout is set; without one its launch is fire-and-forget by construction.
// The whole-orchestrator lifecycle is single-shot: after a successful Stop it cannot
// be restarted, and a subsequent Start returns ErrAlreadyStarted. Registering after
// Stop returns ErrOrchestratorStopped instead.
// Concurrent Start calls are claimed atomically: the caller that wins the claim runs
// the start, and every other caller returns ErrAlreadyStarted immediately without
// waiting for it. A nil return therefore means this call ran the start, not merely
// that some concurrent Start did.
// Thread-safe.
func (o *Orchestrator) Start() error {
	o.ensureInit()
	// Claim the lifecycle before entering startOnce. Without the claim a second
	// caller could pass the o.started pre-check while the winner is still inside
	// the closure (o.started is published there), block on startOnce.Do, and then
	// return the startErr local it never wrote — nil — even though it started
	// nothing (issue #32). startClaimed stays set for the rest of a successful
	// lifecycle; resetAfterStartFailure releases it on failure so a retry can
	// claim again.
	o.mu.Lock()
	if o.started || o.startClaimed {
		o.mu.Unlock()
		return ErrAlreadyStarted
	}
	o.startClaimed = true
	o.mu.Unlock()

	var startErr error
	// rollbackDeadline bounds the failed-Start cleanup. It is captured at the
	// moment of failure (not at Start's beginning) so the synchronous startup
	// work that preceded the failure does not consume the reset budget.
	var rollbackDeadline time.Time
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
		// Register is mutating. membershipMu reserves every snapshotted entry
		// (starting=true) so a StopService/Unregister racing the gap before
		// startOneService is rejected rather than tearing an entry down mid-start
		// (D16). The subscription is released by clearStarting once Start's
		// synchronous work is done.
		o.membershipMu.Lock()
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
		o.cronSched = newCronScheduler()
		entries = make([]*serviceEntry, len(o.entries))
		copy(entries, o.entries)
		nameIndex := make(map[string]*serviceEntry, len(o.nameIndex))
		for name, e := range o.nameIndex {
			nameIndex[name] = e
		}
		for _, e := range entries {
			e.starting.Store(true)
			// o.cronSched is brand new, so any cron id an entry still holds is
			// stale (its scheduler was stopped by a failed Start's rollback).
			// Clear it so setupCron re-schedules every cron entry instead of
			// treating a dead id as live. This is what reschedules a hot-added
			// cron survivor, which resetAfterStartFailure's snapshot does not
			// reach, on the retry.
			e.cronID = 0
		}
		o.mu.Unlock()
		o.membershipMu.Unlock()
		defer o.clearStarting(entries)

		// Assign loggers keyed by the service name (WithName or auto "$N"), so
		// log output correlates with Status/name lookups.
		for _, entry := range entries {
			if o.cfg.Logger != nil {
				entry.setLogger(newServiceLoggerWith(entry.name, o.cfg.Logger))
			} else {
				entry.setLogger(newServiceLogger(entry.name, logCh, logQuit, o.cfg.LogLevel))
			}
		}

		// Spawn log-pump goroutine (default logger only). Capture the log
		// destination here, on the Start caller's goroutine, so the pump never
		// reads the process-global os.Stderr concurrently with a caller that
		// reassigns it to redirect process logging.
		if o.cfg.Logger == nil {
			go o.logPump(os.Stderr, logCh, logQuit, logPumpDone)
		}

		// Set up and start the cron scheduler.
		if err := o.setupCron(entries); err != nil {
			cancel()
			rollbackDeadline = o.failedStartDeadline()
			if logQuit != nil {
				close(logQuit)
				// Bounded wait: the reset below repeats it and surfaces
				// ErrStopTimeout if the log-pump is still stuck.
				_ = o.awaitDone(logPumpDone, rollbackDeadline)
			}
			o.cronSched.Stop()
			startErr = errors.Join(startErr, err)
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
				rollbackDeadline = o.failedStartDeadline()
				startErr = errors.Join(startErr, o.stopStartedServices(entries, rollbackDeadline))
				return
			}
		}

		// Phase 2: persistent services in topological order.
		levels, topoErr := o.topoSort(persistent)
		if topoErr != nil {
			startErr = errors.Join(startErr, topoErr)
			rollbackDeadline = o.failedStartDeadline()
			startErr = errors.Join(startErr, o.stopStartedServices(entries, rollbackDeadline))
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
				rollbackDeadline = o.failedStartDeadline()
				startErr = errors.Join(startErr, o.stopStartedServices(entries, rollbackDeadline))
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
		startErr = errors.Join(startErr, o.resetAfterStartFailure(entries, rollbackDeadline))
	}
	return startErr
}

// failedStartDeadline computes the deadline for a failed-Start rollback from the
// configured failedStartTimeout. It returns the zero time.Time when the
// effective budget is non-positive, meaning the rollback is unbounded.
func (o *Orchestrator) failedStartDeadline() time.Time {
	if o.cfg.failedStartTimeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(o.cfg.failedStartTimeout)
}

// resetAfterStartFailure rolls back all state mutated by a failed Start so the
// orchestrator can be started again. It resets only the entries that Start
// snapshotted: a service hot-added while the failing Start ran keeps its
// registration state and logger. It first waits for all service goroutines and
// the log-pump to fully wind down (they were signalled by stopStartedServices or
// the cron-error path); both waits share deadline, and on expiry it joins
// ErrStopTimeout into the returned error and still performs the reset. The reset
// is therefore best-effort: a goroutine that ignores cancellation and outlives
// the failed Start may keep running, but the orchestrator is left restartable so
// Start can be retried.
//
// Each wait that outlives the deadline abandons a goroutine (the wait's helper
// for the instance group, the log-pump for the log wait); each is logged at
// Error level and counted once in Metrics().AbandonedGoroutines. The log is
// attributed to the first snapshotted entry — the wait is orchestrator-wide, so
// there is no single service to name.
func (o *Orchestrator) resetAfterStartFailure(entries []*serviceEntry, deadline time.Time) error {
	var resetErr error

	// Bound the orchestrator-wide wait for instance goroutines. It is
	// orchestrator-wide, not just the Start snapshot: a StartService that ran
	// during the failing Start also fed o.wg, so one instance ignoring
	// cancellation must not hang the reset.
	var reporter *serviceEntry
	if len(entries) > 0 {
		reporter = entries[0]
	}
	wgDone := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(wgDone)
	}()
	if !o.awaitDone(wgDone, deadline) {
		o.recordAbandoned(reporter, "abandoning failed-Start wait: instance goroutines did not exit before the deadline")
		resetErr = errors.Join(resetErr, ErrStopTimeout)
	}
	// logPumpDone is nil when a custom Logger is set; awaitDone treats that as
	// already done.
	if !o.awaitDone(o.logPumpDone, deadline) {
		o.recordAbandoned(reporter, "abandoning failed-Start wait: log-pump did not exit before the deadline")
		resetErr = errors.Join(resetErr, ErrStopTimeout)
	}

	o.mu.Lock()
	o.started = false
	o.startClaimed = false
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
		// resetEntryLocked is the shared definition of "fresh"; the retry path
		// additionally drops the logger bound to the failed Start's dead log
		// channel, which the next Start rebinds anyway.
		entry.resetEntryLocked()
		entry.setLogger(nil)
	}
	// Reset the once gates in the same critical section as the flags above: a
	// concurrent Start must not observe the released claim and then find a stale
	// consumed startOnce (which would run no closure and return nil).
	o.startOnce = sync.Once{}
	o.stopOnce = sync.Once{}
	o.mu.Unlock()
	return resetErr
}

// Stop shuts down the orchestrator, bounding the whole shutdown by timeout:
// every service's before/after-stop hooks and Stop() call, plus the wait for
// goroutines to exit, share the one budget. Returns aggregated errors from all
// Stop failures, or ErrStopTimeout if the deadline is exceeded.
// Thread-safe. Safe to call on an orchestrator that was never started (no-op).
// The whole-orchestrator lifecycle is single-shot: after a successful Stop it
// cannot be restarted, and a subsequent Start returns ErrAlreadyStarted.
// Registering after Stop returns ErrOrchestratorStopped instead.
func (o *Orchestrator) Stop(timeout time.Duration) error {
	o.ensureInit()
	var stopErr error
	o.stopOnce.Do(func() {
		// Done is a shutdown-completed signal: it closes when this Stop returns,
		// on every path (including the never-started no-op below and a Stop that
		// timed out). It deliberately does not wait on o.wg, which a live hot add
		// or restart reuses and which a timed-out Stop may leave non-zero.
		defer o.signalShutdownDone()

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

		// One deadline for the whole shutdown: the per-service stop sequence and
		// the final wait share it, so a blocking hook cannot outlast the caller.
		var stopDeadline time.Time
		if timeout > 0 {
			stopDeadline = time.Now().Add(timeout)
		}

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

		// 3. Call Stop() on services in reverse topological order. Each stop is
		// recorded so its terminal status can be committed only after the final
		// wait proves every instance goroutine exited.
		type pendingStop struct {
			entry     *serviceEntry
			wasActive bool
			completed bool
		}
		var pending []pendingStop
		stopOne := func(entry *serviceEntry) {
			wasActive, completed, err := o.stopOneServiceDeadline(entry, stopDeadline)
			pending = append(pending, pendingStop{entry: entry, wasActive: wasActive, completed: completed})
			if err != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("%s: %w", entry.name, err))
			}
		}
		// The persistent and non-persistent sets are complements
		// (persistentEntries vs nonPersistentEntries), so the reverse-topological
		// pass and the remaining pass are disjoint and every entry is stopped
		// exactly once. Both take their snapshot under o.mu before iterating, so a
		// concurrent membership op cannot mutate the slice mid-read and no user
		// code (hook or Stop) runs while the lock is held.
		persistent := o.persistentEntries()
		levels, topoErr := o.topoSortForStop(persistent)
		// A cyclic subset still stops every persistent entry (registration-order
		// fallback); surface the ordering failure rather than leaving them running.
		stopErr = errors.Join(stopErr, topoErr)
		for i := len(levels) - 1; i >= 0; i-- {
			for _, entry := range levels[i] {
				stopOne(entry)
			}
		}
		// Cron-only and runOnce entries were excluded from the persistent snapshot
		// above, so this pass completes the teardown without overlapping it.
		for _, entry := range o.nonPersistentEntries() {
			stopOne(entry)
		}

		// 4. Signal log-pump to drain and exit.
		if o.logQuit != nil {
			close(o.logQuit)
		}

		// 5. Clean up messenger subscriptions.
		o.messenger.Drain()

		// 6. Wait for all services + log-pump with whatever budget is left. A
		// non-positive timeout waits indefinitely. Only once every goroutine has
		// exited is the stop verified and its terminal status committed; on a
		// timeout the entries stay StatusStopping rather than falsely claiming
		// they stopped.
		done := make(chan struct{})
		go func() {
			o.wg.Wait()
			if o.logPumpDone != nil {
				<-o.logPumpDone
			}
			close(done)
		}()
		allDone := false
		if stopDeadline.IsZero() {
			<-done
			allDone = true
		} else {
			remaining := max(time.Until(stopDeadline), 0)
			select {
			case <-done:
				allDone = true
			case <-time.After(remaining):
				// The final-wait helper is abandoned: it is neither counted nor
				// logged anywhere else, so account for it here rather than
				// under-reporting the leak this metric exists to expose. The
				// wait is orchestrator-wide, so attribute the log to a stopped
				// entry when one exists. Counted exactly once, monotonic.
				var reporter *serviceEntry
				if len(pending) > 0 {
					reporter = pending[0].entry
				}
				o.recordAbandoned(reporter, "abandoning final wait: instance goroutines or the log-pump did not exit before the stop deadline")
				stopErr = errors.Join(stopErr, ErrStopTimeout)
			}
		}
		if allDone {
			for _, p := range pending {
				if p.completed {
					o.finishStop(p.entry, p.wasActive)
				}
			}
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
