package gorch

import (
	"context"
	"errors"
	"fmt"
	"time"
)

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
				// (possibly blocking) restart logic so a start timeout never
				// aborts Start for a service that self-heals.
				select {
				case startErrCh <- nil:
				default:
				}
				o.handleServiceDone(entry, sc, exitErr, owner.id)
				close(done)
				return
			}
			// No self-heal: update status before signalling so a dependent's
			// status check deterministically sees Crashed/Stopped, not Running.
			o.handleServiceDone(entry, sc, exitErr, owner.id)
			select {
			case startErrCh <- exitErr:
			default:
			}
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
// The entry is left StatusStopping; only a sequence that ran to completion (no
// caller-deadline expiry, no overrunning hook, no capped-away Stop()) is
// reported complete. The caller commits the terminal status with finishStop once
// it has also verified the instance exited, so a timed-out stop never claims
// StatusStopped for a service that may still be alive.
//
// When the caller deadline wins, the sequence goroutine is abandoned, logged at
// Error level and counted in Metrics().AbandonedGoroutines. An inner hook or
// Stop() abandoned earlier in the same sequence is counted separately, once per
// genuinely-abandoned goroutine.
func (o *Orchestrator) stopOneServiceDeadline(entry *serviceEntry, deadline time.Time) (wasActive, completed bool, stopErr error) {
	o.statusMu.RLock()
	wasActive = entry.status == StatusRunning || entry.status == StatusStarting
	o.statusMu.RUnlock()

	o.setStatus(entry, StatusStopping)

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
func (o *Orchestrator) finishStop(entry *serviceEntry, wasActive bool) {
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

// runService runs a persistent service's Start to completion, recovering panics
// and handing the exit to handleServiceDone. done is closed once both the Start
// call and the self-heal decision have finished.
func (o *Orchestrator) runService(entry *serviceEntry, sc ServiceContext, done chan struct{}, ownerID uint64) {
	var exitErr error
	defer func() {
		if r := recover(); r != nil {
			sc.Logger.Error("service panicked", "panic", fmt.Sprint(r))
			exitErr = fmt.Errorf("panic: %v", r)
		}
		o.handleServiceDone(entry, sc, exitErr, ownerID)
		close(done)
	}()

	id := curGoroutineID()
	entry.startGoid.Store(id)
	defer entry.startGoid.CompareAndSwap(id, 0)
	exitErr = entry.getSvc().Start(sc)
	if exitErr != nil && exitErr != context.Canceled {
		sc.Logger.Error("service returned error", "error", exitErr.Error())
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

	// Best-effort cleanup of the old instance: its Stop() releases resources.
	// The status was already resolved for the exit (Crashed for a real crash,
	// Stopped for a clean or cancelled one) and must not be driven back through
	// the teardown state machine, which would emit a spurious Stopping→Stopped
	// and report a self-heal restart as an ordinary stop.
	_, _ = o.runStopSequence(entry, time.Time{})
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
	go o.runService(entry, newSc, newDone, restartOwner.id)
	o.metricsRestarts.Add(1)
}

// invokeCron executes a cron-triggered service tick. Concurrency policy is
// determined by the entry's cronMode.
