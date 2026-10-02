package gorch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// This file holds the deterministic, time-driven measurements that cannot be
// expressed as -bench numbers without benchmarking time.Sleep: the health-loop
// tick, the cron tick, and the self-heal restart backoff. They run under
// testing/synctest, whose fake clock advances only when every goroutine in the
// bubble is durably blocked, so the assertions pin the *schedule* exactly and
// never depend on wall-clock timing.

// scheduleTickSvc is a cron double that records how many ticks ran and the peak
// concurrency. When block is set, each tick parks until its tick context is
// cancelled, so overlapping ticks are observable.
type scheduleTickSvc struct {
	mu         sync.Mutex
	calls      int
	running    int
	maxRunning int
	block      bool
}

func (s *scheduleTickSvc) Start(ctx ServiceContext) error {
	s.mu.Lock()
	s.calls++
	s.running++
	if s.running > s.maxRunning {
		s.maxRunning = s.running
	}
	s.mu.Unlock()
	if s.block {
		<-ctx.Done()
	}
	s.mu.Lock()
	s.running--
	s.mu.Unlock()
	return nil
}

func (s *scheduleTickSvc) Stop() error { return nil }

func (s *scheduleTickSvc) counts() (calls, maxRunning int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.maxRunning
}

// TestBenchHealthTickSchedule pins the periodic probe cadence: no probe before
// the first interval, exactly one per interval after that.
func TestBenchHealthTickSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = time.Hour
		hc := newControllableHealthChecker()
		o := New(WithHealthChecks(interval, WithProbeTimeout(time.Second)), WithLogger(&testLogger{}))
		if err := o.Register(hc, WithName("svc")); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if got := hc.healthCalls(); got != 0 {
			t.Fatalf("probes before the first interval = %d, want 0", got)
		}
		for tick := 1; tick <= 3; tick++ {
			time.Sleep(interval)
			synctest.Wait()
			if got := hc.healthCalls(); got != tick {
				t.Fatalf("probes after %v = %d, want %d", time.Duration(tick)*interval, got, tick)
			}
		}
		if err := o.Stop(time.Second); err != nil {
			t.Fatal(err)
		}
	})
}

// TestBenchHealthProbeNotSerialized pins the latency guard deterministically: a
// probe parked in user code must not hold an orchestration lock that serializes
// a concurrent Health() call. Two Health() calls run in their own goroutines; if
// either probe took a shared lock across the callback, the second would never
// reach the gate and synctest would report a deadlock.
func TestBenchHealthProbeNotSerialized(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		hc := newControllableHealthChecker()
		hc.gate = gate
		o := New(WithHealthChecksDisabled(), WithLogger(&testLogger{}))
		if err := o.Register(hc, WithName("svc")); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}

		go o.Health()
		synctest.Wait()
		if got := hc.healthCalls(); got != 1 {
			t.Fatalf("probes while the first call is parked = %d, want 1", got)
		}
		go o.Health()
		synctest.Wait()
		if got := hc.healthCalls(); got != 2 {
			t.Fatalf("probes after a second call = %d, want 2 (the probe is serialized)", got)
		}
		close(gate)
		synctest.Wait()
		if err := o.Stop(time.Second); err != nil {
			t.Fatal(err)
		}
	})
}

// TestBenchCronTickSchedule pins the cron tick cadence for each concurrency
// mode when ticks do not overlap: exactly one invocation per interval.
func TestBenchCronTickSchedule(t *testing.T) {
	for _, mode := range []CronMode{CronParallel, CronQueue, CronSkip} {
		t.Run(cronModeName(mode), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				svc := &scheduleTickSvc{}
				o := New(WithHealthChecksDisabled(), WithLogger(&testLogger{}))
				if err := o.Register(svc, WithName("cron"), WithCron("@every 1s", mode)); err != nil {
					t.Fatal(err)
				}
				if err := o.Start(); err != nil {
					t.Fatal(err)
				}
				for tick := 1; tick <= 2; tick++ {
					time.Sleep(time.Second)
					synctest.Wait()
					calls, _ := svc.counts()
					if calls != tick {
						t.Fatalf("ticks after %ds = %d, want %d", tick, calls, tick)
					}
				}
				if err := o.Stop(time.Second); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

// TestBenchCronOverlapModes pins the two modes whose behaviour is visible when a
// tick outlives its interval: CronParallel runs the overlapping tick
// concurrently, CronSkip drops it. CronQueue is covered by
// TestBenchCronTickSchedule because a tick blocked on the queue mutex cannot be
// made durably blocked under synctest.
func TestBenchCronOverlapModes(t *testing.T) {
	for _, tc := range []struct {
		mode       CronMode
		wantCalls  int
		wantMaxRun int
	}{
		{mode: CronParallel, wantCalls: 2, wantMaxRun: 2},
		{mode: CronSkip, wantCalls: 1, wantMaxRun: 1},
	} {
		t.Run(cronModeName(tc.mode), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				svc := &scheduleTickSvc{block: true}
				o := New(WithHealthChecksDisabled(), WithLogger(&testLogger{}))
				if err := o.Register(svc, WithName("cron"), WithCron("@every 1s", tc.mode)); err != nil {
					t.Fatal(err)
				}
				if err := o.Start(); err != nil {
					t.Fatal(err)
				}
				time.Sleep(2 * time.Second)
				synctest.Wait()
				calls, maxRunning := svc.counts()
				if calls != tc.wantCalls || maxRunning != tc.wantMaxRun {
					t.Fatalf("calls=%d maxRunning=%d, want calls=%d maxRunning=%d",
						calls, maxRunning, tc.wantCalls, tc.wantMaxRun)
				}
				if err := o.Stop(time.Second); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

// TestBenchRestartBackoffSchedule pins the self-heal restart schedule under an
// exponential backoff: a crash at each attempt, then a wait of
// Initial*Factor^(retry-1) before the next instance. With Initial=1s, Factor=2,
// attempts land at t = 0, 1, 3, 7 — the schedule, not the wall-clock duration.
func TestBenchRestartBackoffSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts atomic.Int32
		crash := func() Service {
			return &testSvc{startFn: func(context.Context) error {
				attempts.Add(1)
				return errors.New("boom")
			}}
		}
		o := New(WithHealthChecksDisabled(), WithLogger(&testLogger{}))
		if err := o.Register(crash(), WithName("svc"), WithSelfHeal(crash),
			WithBackoff(ExponentialBackoff{Initial: time.Second, Factor: 2, Max: time.Minute})); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		assertAttempts := func(at time.Duration, want int) {
			t.Helper()
			if got := int(attempts.Load()); got != want {
				t.Fatalf("restart attempts at %v = %d, want %d", at, got, want)
			}
		}

		// Retry 1 fires after Initial (1s): t=1.
		time.Sleep(time.Second)
		synctest.Wait()
		assertAttempts(time.Second, 2)
		// Retry 2 waits Initial*Factor (2s) from t=1, so nothing at t=2.
		time.Sleep(time.Second)
		synctest.Wait()
		assertAttempts(2*time.Second, 2)
		// It fires at t=3.
		time.Sleep(time.Second)
		synctest.Wait()
		assertAttempts(3*time.Second, 3)
		// Retry 3 waits Initial*Factor² (4s) from t=3, so nothing at t=4 or t=6.
		time.Sleep(3 * time.Second)
		synctest.Wait()
		assertAttempts(6*time.Second, 3)
		// It fires at t=7.
		time.Sleep(time.Second)
		synctest.Wait()
		assertAttempts(7*time.Second, 4)

		if err := o.Stop(time.Second); err != nil {
			t.Fatal(err)
		}
	})
}

func cronModeName(mode CronMode) string {
	switch mode {
	case CronParallel:
		return "parallel"
	case CronQueue:
		return "queue"
	case CronSkip:
		return "skip"
	default:
		return "unknown"
	}
}
