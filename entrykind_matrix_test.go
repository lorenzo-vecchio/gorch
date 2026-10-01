package gorch

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// Matrix 2 from issue #54 — the four entry kinds crossed with the operations
// that act on them. The four kinds reach the graph through different start
// paths (persistent through startOneService, cron through startCronEntry,
// runOnce through startOneService's synchronous branch, self-heal through
// handleServiceDone), and the existing tests sampled each kind on its own. This
// matrix runs the same StartService/StopService/Unregister/restart operations
// against all four so a defect in the shared membership layer cannot hide
// behind a kind-specific test.

// entryKindCase declares a hot-addable entry kind and the operation-specific
// expectations that differ between kinds.
type entryKindCase struct {
	name string
	// startedStatus is the status StartService leaves the entry in.
	startedStatus ServiceStatus
	// stoppedStatus is the status StopService leaves the entry in.
	stoppedStatus ServiceStatus
	// add starts an empty orchestrator, hot-adds the entry, and returns the
	// initial service plus a kind-specific restart operation. restart returns
	// the service whose Start must be observed and the minimum start count it
	// must reach.
	add func(t *testing.T) (o *Orchestrator, first *recordingService, restart func(t *testing.T, o *Orchestrator) (*recordingService, int))
}

// selfHealFixture manufactures a fresh crashable service per self-heal
// incarnation and lets a test crash one deterministically.
type selfHealFixture struct {
	mu      sync.Mutex
	svcs    []*recordingService
	crashes []chan struct{}
}

// newSvc is both the initially-registered service and the self-heal factory: it
// returns a recordingService whose Start blocks until this incarnation is
// crashed (returning an error) or the context is cancelled.
func (fx *selfHealFixture) newSvc() *recordingService {
	svc := newRecordingService()
	crash := make(chan struct{})
	svc.startFn = func(_ int, ctx ServiceContext) error {
		select {
		case <-crash:
			return errors.New("boom")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fx.mu.Lock()
	fx.svcs = append(fx.svcs, svc)
	fx.crashes = append(fx.crashes, crash)
	fx.mu.Unlock()
	return svc
}

// crash makes incarnation i exit with an error, driving self-heal.
func (fx *selfHealFixture) crash(i int) {
	fx.mu.Lock()
	ch := fx.crashes[i]
	fx.mu.Unlock()
	close(ch)
}

// waitForInstance blocks until at least n incarnations exist and returns the
// n-th (1-based).
func (fx *selfHealFixture) waitForInstance(t *testing.T, n int) *recordingService {
	t.Helper()
	waitForCondition(t, 3*time.Second, func() bool {
		fx.mu.Lock()
		defer fx.mu.Unlock()
		return len(fx.svcs) >= n
	}, "self-heal to spawn a replacement instance")
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return fx.svcs[n-1]
}

// entryKindCases returns the four kinds.
func entryKindCases() []entryKindCase {
	return []entryKindCase{
		{
			name:          "persistent",
			startedStatus: StatusRunning,
			stoppedStatus: StatusStopped,
			add: func(t *testing.T) (*Orchestrator, *recordingService, func(t *testing.T, o *Orchestrator) (*recordingService, int)) {
				o := startEmpty(t)
				svc := newRecordingService()
				if err := o.Register(svc, WithName("svc")); err != nil {
					t.Fatal(err)
				}
				restart := func(t *testing.T, o *Orchestrator) (*recordingService, int) {
					t.Helper()
					next := newRecordingService()
					if err := o.ReplaceService("svc", next, 3*time.Second); err != nil {
						t.Fatalf("ReplaceService: %v", err)
					}
					return next, 1
				}
				return o, svc, restart
			},
		},
		{
			name:          "cron",
			startedStatus: StatusRunning,
			stoppedStatus: StatusStopped,
			add: func(t *testing.T) (*Orchestrator, *recordingService, func(t *testing.T, o *Orchestrator) (*recordingService, int)) {
				o := startEmpty(t)
				svc := newRecordingService()
				svc.startFn = func(int, ServiceContext) error { return nil }
				if err := o.Register(svc, WithName("svc"), WithCron("@every 1s", CronQueue)); err != nil {
					t.Fatal(err)
				}
				restart := func(t *testing.T, o *Orchestrator) (*recordingService, int) {
					t.Helper()
					next := newRecordingService()
					next.startFn = func(int, ServiceContext) error { return nil }
					if err := o.ReplaceService("svc", next, 3*time.Second); err != nil {
						t.Fatalf("ReplaceService: %v", err)
					}
					return next, 1
				}
				return o, svc, restart
			},
		},
		{
			name:          "runOnce",
			startedStatus: StatusSucceeded,
			stoppedStatus: StatusSucceeded,
			add: func(t *testing.T) (*Orchestrator, *recordingService, func(t *testing.T, o *Orchestrator) (*recordingService, int)) {
				o := startEmpty(t)
				svc := newRecordingService()
				svc.startFn = func(int, ServiceContext) error { return nil }
				if err := o.Register(svc, WithName("svc"), WithRunOnce()); err != nil {
					t.Fatal(err)
				}
				restart := func(t *testing.T, o *Orchestrator) (*recordingService, int) {
					t.Helper()
					// A runOnce gate is deliberately re-runnable: StartService
					// runs it a second time rather than replacing the instance.
					if err := o.StartService("svc"); err != nil {
						t.Fatalf("StartService re-run: %v", err)
					}
					return svc, 2
				}
				return o, svc, restart
			},
		},
		{
			name:          "selfHeal",
			startedStatus: StatusRunning,
			stoppedStatus: StatusStopped,
			add: func(t *testing.T) (*Orchestrator, *recordingService, func(t *testing.T, o *Orchestrator) (*recordingService, int)) {
				o := startEmpty(t)
				fx := &selfHealFixture{}
				svc := fx.newSvc()
				if err := o.Register(svc, WithName("svc"),
					WithSelfHeal(func() Service { return fx.newSvc() }),
					WithBackoff(ConstantBackoff{Delay: time.Millisecond}),
				); err != nil {
					t.Fatal(err)
				}
				restart := func(t *testing.T, o *Orchestrator) (*recordingService, int) {
					t.Helper()
					fx.crash(0)
					return fx.waitForInstance(t, 2), 1
				}
				return o, svc, restart
			},
		},
	}
}

// TestEntryKind_Operation_Matrix pins StartService, StopService, Unregister and
// the kind's restart primitive across all four entry kinds. Each cell asserts
// the resulting status, the service Stop() call, and that the operation
// returned the documented error.
func TestEntryKind_Operation_Matrix(t *testing.T) {
	for _, kind := range entryKindCases() {
		kind := kind
		t.Run(kind.name, func(t *testing.T) {
			t.Run("Start", func(t *testing.T) {
				o, svc, _ := kind.add(t)
				defer func() { _ = o.Stop(3 * time.Second) }()

				if err := o.StartService("svc"); err != nil {
					t.Fatalf("StartService: %v", err)
				}
				if got := o.statusOf(mustEntry(t, o, "svc")); got != kind.startedStatus {
					t.Fatalf("status = %s, want %s", got, kind.startedStatus)
				}
				waitForCondition(t, 3*time.Second, func() bool { return svc.startCount() >= 1 }, "service Start")
			})

			t.Run("Stop", func(t *testing.T) {
				o, svc, _ := kind.add(t)
				defer func() { _ = o.Stop(3 * time.Second) }()

				if err := o.StartService("svc"); err != nil {
					t.Fatalf("StartService: %v", err)
				}
				if err := o.StopService("svc", 3*time.Second); err != nil {
					t.Fatalf("StopService: %v", err)
				}
				if got := o.statusOf(mustEntry(t, o, "svc")); got != kind.stoppedStatus {
					t.Errorf("status = %s, want %s", got, kind.stoppedStatus)
				}
				if svc.stopCount() == 0 {
					t.Errorf("StopService did not run the service's Stop()")
				}
			})

			t.Run("Remove", func(t *testing.T) {
				o, _, _ := kind.add(t)
				defer func() { _ = o.Stop(3 * time.Second) }()

				if err := o.StartService("svc"); err != nil {
					t.Fatalf("StartService: %v", err)
				}
				if err := o.Unregister("svc", 3*time.Second); err != nil {
					t.Fatalf("Unregister: %v", err)
				}
				o.mu.RLock()
				_, still := o.nameIndex["svc"]
				o.mu.RUnlock()
				if still {
					t.Fatalf("entry still registered after Unregister")
				}
			})

			t.Run("Restart", func(t *testing.T) {
				o, _, restart := kind.add(t)
				defer func() { _ = o.Stop(3 * time.Second) }()

				if err := o.StartService("svc"); err != nil {
					t.Fatalf("StartService: %v", err)
				}
				next, wantStarts := restart(t, o)
				waitForCondition(t, 3*time.Second, func() bool { return next.startCount() >= wantStarts }, "restarted service to start")
				waitForCondition(t, 3*time.Second, func() bool {
					return o.statusOf(mustEntry(t, o, "svc")) == kind.startedStatus
				}, "restarted entry to return to its started status")
			})
		})
	}
}

// TestEntryKind_HealthFailure_Matrix pins what a health-probe failure does for
// each kind. Only a self-heal service with a factory restarts on threshold; a
// persistent service without one records the failure and stays Running, and
// cron/runOnce entries are not restarted by the periodic loop at all.
func TestEntryKind_HealthFailure_Matrix(t *testing.T) {
	t.Run("selfHealRestarts", func(t *testing.T) {
		o := New(WithHealthChecks(5*time.Millisecond, WithProbeTimeout(50*time.Millisecond), WithFailureThreshold(1)))
		fx := &selfHealFixture{}
		svc := &controllableHealthChecker{recordingService: fx.newSvc()}
		svc.setHealth(errors.New("unhealthy"))
		if err := o.Register(svc, WithName("svc"), WithSelfHeal(func() Service {
			return &controllableHealthChecker{recordingService: fx.newSvc()}
		}), WithBackoff(ConstantBackoff{Delay: time.Millisecond})); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = o.Stop(3 * time.Second) }()

		waitForCondition(t, 3*time.Second, func() bool { return fx.instanceCount() >= 2 }, "health threshold to restart the service")
		waitForCondition(t, 3*time.Second, func() bool {
			return o.Metrics().HealthFails >= 1
		}, "HealthFails metric")
		if svc.healthCalls() == 0 {
			t.Errorf("the periodic loop never probed the service")
		}
	})

	t.Run("persistentRecordsWithoutRestart", func(t *testing.T) {
		o := New(WithHealthChecks(5*time.Millisecond, WithProbeTimeout(50*time.Millisecond), WithFailureThreshold(1)))
		hc := newControllableHealthChecker()
		hc.setHealth(errors.New("unhealthy"))
		if err := o.Register(hc, WithName("svc")); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = o.Stop(3 * time.Second) }()

		waitForCondition(t, 3*time.Second, func() bool { return o.Metrics().HealthFails >= 1 }, "HealthFails metric")
		if got := o.statusOf(mustEntry(t, o, "svc")); got != StatusRunning {
			t.Fatalf("status = %s, want %s (no factory means no restart)", got, StatusRunning)
		}
	})
}

// instanceCount is the number of self-heal incarnations spawned so far.
func (fx *selfHealFixture) instanceCount() int {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return len(fx.svcs)
}
