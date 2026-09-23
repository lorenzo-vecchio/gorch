package gorch

import (
	"context"
	"fmt"

	"github.com/robfig/cron/v3"
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

	sc := ServiceContext{
		Context:   o.ctx,
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

// setupCron creates and starts the cron scheduler, registering every cron
// service and marking them running. Returns ErrInvalidCron on a bad spec.
func (o *Orchestrator) setupCron() error {
	o.cronSched = cron.New(cron.WithSeconds())
	for _, entry := range o.entries {
		if entry.cfg.cronSpec == "" {
			continue
		}
		if err := o.scheduleEntry(entry); err != nil {
			return err
		}
	}

	o.cronSched.Start()

	// Mark cron services as running now that the scheduler is live.
	for _, entry := range o.entries {
		if entry.cfg.cronSpec != "" {
			o.setStatus(entry, StatusRunning)
			o.metricsStarts.Add(1)
		}
	}
	return nil
}
