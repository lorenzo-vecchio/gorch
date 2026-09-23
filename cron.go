package gorch

import (
	"context"
	"fmt"
)

func (o *Orchestrator) invokeCron(entry *serviceEntry) {
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

	// Per-tick context so StopService/Unregister can cancel an in-flight tick by
	// cancelling the entry's current cancel func (C8).
	svcCtx, cancel := context.WithCancel(o.ctx)
	entry.setCancel(cancel)
	defer cancel()

	sc := ServiceContext{
		Context:   svcCtx,
		Logger:    entry.getLogger(),
		Messenger: o.messenger.scoped(entry.owner),
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
	id, err := o.cronSched.AddFunc(entry.cfg.cronSpec, func() {
		o.invokeCron(entry)
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
// on a bad spec. The scheduler itself is created by Start under o.mu so a
// concurrent dynamic Register sees it published before the graph goes live.
func (o *Orchestrator) setupCron(entries []*serviceEntry) error {
	for _, entry := range entries {
		if entry.cfg.cronSpec == "" {
			continue
		}
		if err := o.scheduleEntry(entry); err != nil {
			return err
		}
	}

	o.cronSched.Start()

	// Mark cron services as running now that the scheduler is live.
	for _, entry := range entries {
		if entry.cfg.cronSpec != "" {
			o.setStatus(entry, StatusRunning)
			o.metricsStarts.Add(1)
		}
	}
	return nil
}
