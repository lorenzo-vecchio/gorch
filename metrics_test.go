package gorch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// waitUntil polls cond until it holds or the deadline expires. It delegates to
// the shared waitForCondition (3s) so the suite has one polling implementation.
func waitUntil(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	waitForCondition(t, 3*time.Second, cond, msg)
}

// waitRecv waits for one value on ch, failing on timeout.
func waitRecv(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for synchronization signal")
	}
}

// gatedSvc signals each Start and then blocks until it is told to crash (with a
// fixed error) or its context is cancelled. It lets a metrics test drive a
// self-heal service one instance at a time without sleeping.
type gatedSvc struct {
	started chan<- struct{}
	crash   <-chan struct{}
	err     error
}

func (s *gatedSvc) Start(ctx ServiceContext) error {
	s.started <- struct{}{}
	select {
	case <-s.crash:
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *gatedSvc) Stop() error { return nil }

// metricsCronSvc is a cron service whose tick outcome is set per invocation, so
// CronFailures can be tested deterministically without waiting for a schedule.
type metricsCronSvc struct {
	behavior func() error
}

func (s *metricsCronSvc) Start(ServiceContext) error { return s.behavior() }
func (s *metricsCronSvc) Stop() error                { return nil }

// TestMetrics pins the basic lifecycle counters: a start and a stop of a
// persistent service move Starts/Stops by one each, and nothing else moves.
func TestMetrics(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	svc := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(svc, WithName("m")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}

	if got := o.Metrics(); got.Starts != 1 {
		t.Fatalf("Starts after Start = %d, want 1", got.Starts)
	}
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	got := o.Metrics()
	if got.Stops != 1 {
		t.Errorf("Stops after Stop = %d, want 1", got.Stops)
	}
	for name, v := range map[string]int64{
		"Crashes": got.Crashes, "Restarts": got.Restarts,
		"HealthFails": got.HealthFails, "CronFailures": got.CronFailures,
		"AbandonedGoroutines": got.AbandonedGoroutines,
	} {
		if v != 0 {
			t.Errorf("%s = %d, want 0", name, v)
		}
	}
}

// TestMetrics_CleanSelfExitCountsStop pins the other half of the Stops
// definition: an instance that exits on its own and is committed StatusStopped
// counts as a completed stop, without being mislabelled a crash or a restart.
func TestMetrics_CleanSelfExitCountsStop(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(&testSvc{startFn: func(ctx context.Context) error { return nil }}, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(2 * time.Second)

	waitUntil(t, func() bool { return o.Metrics().Stops == 1 }, "the clean self-exit stop was not counted")
	got := o.Metrics()
	if got.Crashes != 0 {
		t.Errorf("Crashes = %d, want 0: a clean exit is not a crash", got.Crashes)
	}
	if got.Restarts != 0 {
		t.Errorf("Restarts = %d, want 0", got.Restarts)
	}
}

// TestMetrics_StartsCountsInvocationNotSuccess pins that a start that
// immediately fails still counts as a Start (invocations, not successes), while
// the crash it produces counts as a Crash and not a Stop.
func TestMetrics_StartsCountsInvocationNotSuccess(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(&errSvc{err: errors.New("boom")}, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(2 * time.Second)

	waitUntil(t, func() bool { return o.Metrics().Crashes == 1 }, "service crash was not observed")
	got := o.Metrics()
	if got.Starts != 1 {
		t.Errorf("Starts = %d, want 1: a failed start is still a start", got.Starts)
	}
	if got.Restarts != 0 {
		t.Errorf("Restarts = %d, want 0", got.Restarts)
	}
	if got.Stops != 0 {
		t.Errorf("Stops = %d, want 0: a crash is not a stop", got.Stops)
	}
}

// TestMetrics_RestartDoesNotCountStop pins the headline decision: N self-heal
// restarts increment Restarts by N and leave Stops at zero, so Stops never
// conflates a restart's internal cleanup with a caller-initiated stop.
func TestMetrics_RestartDoesNotCountStop(t *testing.T) {
	const restarts = 3

	o := New(WithHealthChecksDisabled())
	started := make(chan struct{}, restarts+1)
	release := make(chan struct{})
	gate := func() Service {
		return &testSvc{startFn: func(ctx context.Context) error {
			started <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
	}
	if err := o.Register(gate(), WithName("s"),
		WithSelfHeal(gate),
		WithBackoff(ConstantBackoff{Delay: 0}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(2 * time.Second)

	waitRecv(t, started) // the initial instance is live
	if got := o.Metrics(); got.Restarts != 0 || got.Stops != 0 {
		t.Fatalf("before restarts: Restarts=%d Stops=%d, want 0/0", got.Restarts, got.Stops)
	}
	for i := 0; i < restarts; i++ {
		release <- struct{}{} // let the current instance exit cleanly
		waitRecv(t, started)  // the next instance is live
	}

	waitUntil(t, func() bool { return o.Metrics().Restarts == restarts }, "restarts were not all counted")
	got := o.Metrics()
	if got.Stops != 0 {
		t.Errorf("Stops = %d, want 0: a self-heal restart must not count as a stop", got.Stops)
	}
	if got.Crashes != 0 {
		t.Errorf("Crashes = %d, want 0: a clean self-heal exit is not a crash", got.Crashes)
	}
}

// TestMetrics_CrashCountsSelfHealCrashes pins that each self-heal crash is
// counted before its restart: N crashes yield N Crashes, N Restarts and N
// OnCrash calls, and still no Stops.
func TestMetrics_CrashCountsSelfHealCrashes(t *testing.T) {
	const crashes = 3
	crashErr := errors.New("self-heal boom")

	started := make(chan struct{}, crashes+2)
	crash := make(chan struct{})
	var onCrash atomic.Int64

	o := New(
		WithHealthChecksDisabled(),
		WithOnCrash(func(string, error) { onCrash.Add(1) }),
	)
	gate := func() Service {
		return &gatedSvc{started: started, crash: crash, err: crashErr}
	}
	if err := o.Register(gate(), WithName("s"),
		WithSelfHeal(gate),
		WithBackoff(ConstantBackoff{Delay: 0}),
	); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(2 * time.Second)

	for i := 0; i < crashes; i++ {
		waitRecv(t, started) // instance i is live
		crash <- struct{}{}  // crash it; a fresh instance follows
	}

	waitUntil(t, func() bool { return o.Metrics().Crashes == crashes }, "crashes were not all counted")
	waitUntil(t, func() bool { return o.Metrics().Restarts == crashes }, "restarts were not all counted")
	if got := onCrash.Load(); got != crashes {
		t.Errorf("OnCrash calls = %d, want %d", got, crashes)
	}
	if got := o.Metrics().Stops; got != 0 {
		t.Errorf("Stops = %d, want 0: crash-restart cleanup is not a stop", got)
	}
}

// TestMetrics_StopTimeoutDoesNotCountStop pins the deadline path: a stop whose
// caller deadline expires does not increment Stops and leaves the entry
// StatusStopping. When the instance finally exits, the completed stop is counted
// exactly once — the counter tracks the completed stop, not the caller's
// deadline.
func TestMetrics_StopTimeoutDoesNotCountStop(t *testing.T) {
	release := make(chan struct{})
	svc := &fuzzStubbornSvc{release: release}

	o := New(WithHealthChecksDisabled())
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(2 * time.Second)
	waitUntil(t, func() bool { return svc.running.Load() == 1 }, "instance did not start")

	before := o.Metrics().Stops
	if err := o.StopService("s", 50*time.Millisecond); !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("StopService = %v, want ErrStopTimeout", err)
	}
	if s, _ := o.Status("s"); s != StatusStopping {
		t.Errorf("status = %v, want StatusStopping", s)
	}
	if got := o.Metrics().Stops; got != before {
		t.Fatalf("Stops = %d immediately after a timed-out stop, want %d (unchanged)", got, before)
	}

	// Release the instance: the teardown that the caller's deadline abandoned
	// completes on its own, and only then is the stop counted.
	close(release)
	waitUntil(t, func() bool { return o.Metrics().Stops == before+1 }, "the completed late stop was not counted once")
	if err := o.Stop(2 * time.Second); err != nil {
		t.Fatalf("final Stop: %v", err)
	}
	if got := o.Metrics().Stops; got != before+1 {
		t.Errorf("Stops = %d after the late stop completed, want %d", got, before+1)
	}
}

// TestMetrics_CronTicks_Semantics pins cron accounting: installing a schedule
// counts one Start, a tick never counts as a Start, and a tick's Start returning
// an error or panicking counts one CronFailures while a context.Canceled return
// (the teardown signal) does not.
func TestMetrics_CronTicks_Semantics(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	svc := &metricsCronSvc{behavior: func() error { return nil }}
	// A yearly spec keeps the scheduler from firing during the test; ticks are
	// driven directly below.
	if err := o.Register(svc, WithName("tick"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(2 * time.Second)

	if got := o.Metrics().Starts; got != 1 {
		t.Fatalf("Starts after cron start = %d, want 1 (the schedule installation)", got)
	}

	o.mu.RLock()
	e := o.nameIndex["tick"]
	o.mu.RUnlock()
	gen := e.cronGeneration()

	tests := []struct {
		name         string
		behavior     func() error
		wantFailures int64
	}{
		{"success", func() error { return nil }, 0},
		{"error", func() error { return errors.New("tick failed") }, 1},
		{"canceled_is_teardown", func() error { return context.Canceled }, 0},
		{"panic", func() error { panic("tick panic") }, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc.behavior = tt.behavior
			before := o.Metrics()
			o.invokeCron(e, gen)
			got := o.Metrics()
			if d := got.Starts - before.Starts; d != 0 {
				t.Errorf("Starts delta = %d, want 0: a cron tick is not a Start", d)
			}
			if d := got.CronFailures - before.CronFailures; d != tt.wantFailures {
				t.Errorf("CronFailures delta = %d, want %d", d, tt.wantFailures)
			}
		})
	}
}

// TestMetrics_HealthFails_IsProbesOrIncidents pins that HealthFails counts
// failed probes, not incidents: every failed periodic probe increments it (even
// while below the threshold and past it), successful probes and on-demand
// Health() calls do not.
func TestMetrics_HealthFails_IsProbesOrIncidents(t *testing.T) {
	t.Run("counts_each_failed_probe_not_each_incident", func(t *testing.T) {
		var failing atomic.Bool
		o := New(
			WithLogLevel(LogLevelError),
			WithHealthChecks(time.Hour, WithFailureThreshold(10)),
		)
		svc := &healthSvc{
			testSvc: testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
			healthFn: func(context.Context) error {
				if failing.Load() {
					return errors.New("unhealthy")
				}
				return nil
			},
		}
		if err := o.Register(svc, WithName("s")); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)

		// A healthy probe is not a failure.
		o.runHealthChecks()
		if got := o.Metrics().HealthFails; got != 0 {
			t.Fatalf("HealthFails after a healthy probe = %d, want 0", got)
		}

		// Each failing probe counts, one per probe rather than one per outage.
		failing.Store(true)
		for i := int64(1); i <= 3; i++ {
			o.runHealthChecks()
			if got := o.Metrics().HealthFails; got != i {
				t.Fatalf("HealthFails after %d failing probes = %d, want %d", i, got, i)
			}
		}

		// A recovering probe does not decrement the counter, and a later failure
		// counts again.
		failing.Store(false)
		o.runHealthChecks()
		failing.Store(true)
		o.runHealthChecks()
		if got := o.Metrics().HealthFails; got != 4 {
			t.Fatalf("HealthFails after recovery + failure = %d, want 4", got)
		}

		// On-demand probes instrument nothing.
		for i := 0; i < 3; i++ {
			o.Health()
		}
		if got := o.Metrics().HealthFails; got != 4 {
			t.Errorf("HealthFails after on-demand Health() = %d, want 4 (unchanged)", got)
		}
	})

	t.Run("counts_each_probe_past_the_threshold", func(t *testing.T) {
		o := New(
			WithLogLevel(LogLevelError),
			WithHealthChecks(time.Hour, WithFailureThreshold(2)),
		)
		svc := &healthSvc{
			testSvc:  testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
			healthFn: func(context.Context) error { return errors.New("always unhealthy") },
		}
		if err := o.Register(svc, WithName("s")); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)

		// The threshold is reached on the second probe; with no factory the
		// service is not restarted, and every probe still counts.
		for i := int64(1); i <= 3; i++ {
			o.runHealthChecks()
			if got := o.Metrics().HealthFails; got != i {
				t.Fatalf("HealthFails after %d probes past threshold = %d, want %d", i, got, i)
			}
		}
	})
}
