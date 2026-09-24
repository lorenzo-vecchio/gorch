package gorch

import (
	"context"
	"fmt"

	"github.com/robfig/cron/v3"
)

func (o *Orchestrator) invokeCron(entry *serviceEntry, gen uint64) {
	// Refuse a tick dispatched while the entry is being torn down, or one that
	// belongs to a superseded schedule. Every tick of a generation derives from
	// the same context, so cancelling it (cronDrain) reaches all in-flight ticks,
	// not only the newest.
	cronParent, ok := entry.cronBegin(gen)
	if !ok {
		return
	}
	defer entry.cronEnd(gen)

	switch entry.cfg.cronMode {
	case CronSkip:
		if !entry.running.CompareAndSwap(false, true) {
			entry.getLogger().Warn("cron tick skipped: previous invocation still running")
			return
		}
		defer entry.running.Store(false)
	case CronQueue:
		entry.cronMu.Lock()
		defer entry.cronMu.Unlock()
	case CronParallel:
	}

	// Each tick gets a fresh Messenger owner, released when the tick returns, so
	// subscriptions from earlier ticks do not accumulate under one id.
	owner := o.newOwner(entry)
	defer o.releaseOwner(entry, owner.id)

	// Per-tick context as a child of the shared schedule context, so teardown
	// cancels this tick along with every other in-flight tick (C8).
	svcCtx, cancel := context.WithCancel(cronParent)
	defer cancel()

	sc := ServiceContext{
		Context:   svcCtx,
		Logger:    entry.getLogger(),
		Messenger: o.messenger.view(owner),
	}

	defer func() {
		if r := recover(); r != nil {
			entry.getLogger().Error("cron service panicked", "panic", fmt.Sprint(r))
		}
	}()

	err := entry.getSvc().Start(sc)
	if err != nil && err != context.Canceled {
		entry.getLogger().Error("cron service returned error", "error", err.Error())
	}
}

type CronMode int

// cronSpecParser mirrors the parser cron.New(WithSeconds()) installs, so a spec
// accepted by validateCronSpec can always be scheduled later.
var cronSpecParser = cron.NewParser(
	cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// validateCronSpec rejects a malformed cron expression without installing a
// schedule. Dynamic Register uses it so a hot-added cron entry can be staged
// (registered now, started later) without ticking while it reports
// StatusRegistered.
func validateCronSpec(spec string) error {
	if _, err := cronSpecParser.Parse(spec); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCron, err)
	}
	return nil
}

const (
	CronParallel CronMode = iota // fire in new goroutine regardless
	// CronQueue serializes ticks on a per-entry mutex: an overlapping tick
	// blocks until the previous one finishes. robfig/cron spawns a goroutine per
	// tick, so a long-running tick makes later ones pile up as blocked goroutines.
	CronQueue
	CronSkip // drop this tick entirely
)

// scheduleEntry registers entry's cron spec with the live scheduler and stores
// the resulting entry ID. It is shared by setupCron (static registration) and
// dynamic Register/StartService. Returns ErrInvalidCron on a bad spec.
func (o *Orchestrator) scheduleEntry(entry *serviceEntry) error {
	// A re-scheduled entry gets a fresh shared context and reset accounting; the
	// generation lets a stale tick from the previous schedule be ignored.
	gen := entry.startCronSchedule(o.ctx)
	id, err := o.cronSched.AddFunc(entry.cfg.cronSpec, func() {
		o.invokeCron(entry, gen)
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCron, err)
	}
	entry.cronID = id
	return nil
}

// removeCronEntry deletes entry's schedule from the live scheduler and clears
// its cron ID, so a later StartService re-schedules it (D14). It is a no-op when
// the entry has no live schedule. Thread-safe.
func (o *Orchestrator) removeCronEntry(entry *serviceEntry) {
	o.mu.Lock()
	id := entry.cronID
	entry.cronID = 0
	o.mu.Unlock()
	if id != 0 && o.cronSched != nil {
		o.cronSched.Remove(id)
	}
}

// setupCron starts the already-published cron scheduler, registering every cron
// service from the Start snapshot and marking them running. Returns ErrInvalidCron
// on a bad spec. It funnels each entry through startCronEntry — the same commit
// path dynamic StartService uses — so a statically and a dynamically scheduled
// entry cannot diverge. The scheduler itself is created by Start under o.mu so a
// concurrent dynamic Register sees it published before the graph goes live.
func (o *Orchestrator) setupCron(entries []*serviceEntry) error {
	for _, entry := range entries {
		if entry.cfg.cronSpec == "" {
			continue
		}
		if err := o.startCronEntry(entry); err != nil {
			return err
		}
	}
	o.cronSched.Start()
	return nil
}
