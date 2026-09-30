package gorch

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ctxParkSvc is a persistent service whose Start parks on its context until a
// teardown cancels it, ignoring Stop() entirely. It is the service that exposes
// the StopGroup-then-StartGroup divergence: StopGroup runs Stop() without
// cancelling the context, so the instance is still live when the status already
// reads StatusStopped.
type ctxParkSvc struct {
	starts  atomic.Int32
	stops   atomic.Int32
	entered chan struct{}
	once    sync.Once
}

func newCtxParkSvc() *ctxParkSvc {
	return &ctxParkSvc{entered: make(chan struct{})}
}

func (s *ctxParkSvc) Start(ctx ServiceContext) error {
	s.starts.Add(1)
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return ctx.Err()
}

func (s *ctxParkSvc) Stop() error {
	s.stops.Add(1)
	return nil
}

// waitServiceStarted blocks until the service's Start has been entered, or
// fails after a bounded wait. The timeout is a safety net, not the
// synchronization: the channel close is.
func waitServiceStarted(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the service to start")
	}
}

// TestStartGroup_StoppedCronMember_IsRescheduled pins that StartGroup routes a
// cron member through startCronEntry, exactly like StartService. Before the fix
// it sent every member through startOneService, which launched the cron
// service's Start continuously and left the schedule uninstalled, silently
// turning a scheduled service into an always-on one.
func TestStartGroup_StoppedCronMember_IsRescheduled(t *testing.T) {
	cronSvc := &fuzzCronSvc{}
	o := New(WithHealthChecksDisabled())
	if err := o.Register(cronSvc, WithName("cron"), WithGroup("g"), WithCron("@every 1h", CronParallel)); err != nil {
		t.Fatalf("Register = %v, want nil", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v, want nil", err)
	}

	if err := o.StopService("cron", time.Second); err != nil {
		t.Fatalf("StopService(cron) = %v, want nil", err)
	}
	if err := o.StartGroup("g"); err != nil {
		t.Fatalf("StartGroup(g) = %v, want nil", err)
	}
	if s, _ := o.Status("cron"); s != StatusRunning {
		t.Fatalf("cron status after StartGroup = %v, want StatusRunning", s)
	}
	if e := fuzzEntry(t, o, "cron"); e.cronID == 0 {
		t.Fatal("StartGroup left the cron member unscheduled")
	}
	if v := cronSvc.running.Load(); v != 0 {
		t.Fatalf("StartGroup ran the cron member as a persistent instance: %d live", v)
	}

	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}
}

// TestStartGroup_LiveInstance_IsNotDoubled pins that StartGroup treats a live
// instance channel as already running. A concurrent StopGroup runs Stop()
// without cancelling the entry's context, so the instance can still be exiting
// while the status reads StatusStopped; starting again would spawn a second
// instance and leak the wait group because the wgDone latch releases only one.
func TestStartGroup_LiveInstance_IsNotDoubled(t *testing.T) {
	svc := newCtxParkSvc()
	o := New(WithHealthChecksDisabled())
	if err := o.Register(svc, WithName("s"), WithGroup("g")); err != nil {
		t.Fatalf("Register = %v, want nil", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start = %v, want nil", err)
	}
	waitServiceStarted(t, svc.entered)
	if got := svc.starts.Load(); got != 1 {
		t.Fatalf("starts after Start = %d, want 1", got)
	}

	if err := o.StopGroup("g", time.Second); err != nil {
		t.Fatalf("StopGroup(g) = %v, want nil", err)
	}
	// StopGroup stopped the entry but, by design, did not cancel the context:
	// the instance is still parked in Start.
	if s, _ := o.Status("s"); s != StatusStopped {
		t.Fatalf("status after StopGroup = %v, want StatusStopped", s)
	}
	if err := o.StartGroup("g"); err != nil {
		t.Fatalf("StartGroup(g) = %v, want nil", err)
	}
	if got := svc.starts.Load(); got != 1 {
		t.Fatalf("StartGroup started a second instance of an entry with a live instance: starts = %d, want 1", got)
	}
	if fuzzActive(o, "s") {
		t.Fatal("StartGroup revived an entry whose live instance was not restarted")
	}

	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}
	select {
	case <-o.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("goroutines did not wind down after Stop")
	}
}
