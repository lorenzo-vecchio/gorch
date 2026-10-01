package gorch

import (
	"errors"
	"testing"
	"time"
)

// This file holds the behaviour matrices from issue #54. A matrix pins the
// cross-product of two contract axes instead of the happy path: the existing
// suite sampled each axis separately, which is why defects living only in the
// intersection (a status handled by only one teardown path, a kind of entry
// reached by only one operation) kept shipping.
//
// Matrix 1 — every ServiceStatus crossed with every teardown path. Two of the
// seven statuses are transient and cannot be observed through the public API:
// StatusStarting lasts only for the synchronous window inside startOneService,
// and StatusStopping is committed while a teardown is already in flight. The
// matrix primes those two directly on the entry (the tests are in-package) so
// each teardown path is exercised against a stable state rather than racing to
// catch it. End-to-end transitions are covered by the named contract tests.

// teardownPath is one of the four ways a caller can tear the graph down.
type teardownPath struct {
	name   string
	invoke func(o *Orchestrator) error
}

// teardownPaths is the teardown axis of the matrix.
func teardownPaths() []teardownPath {
	return []teardownPath{
		{"Stop", func(o *Orchestrator) error { return o.Stop(3 * time.Second) }},
		{"StopService", func(o *Orchestrator) error { return o.StopService("svc", 3*time.Second) }},
		{"Unregister", func(o *Orchestrator) error { return o.Unregister("svc", 3*time.Second) }},
		{"StopGroup", func(o *Orchestrator) error { return o.StopGroup("g", 3*time.Second) }},
	}
}

// statusCase builds an orchestrator whose target entry ("svc", group "g") is in
// the named status. It returns the recording service so the cell can assert the
// Stop() delta.
type statusCase struct {
	name  string
	build func(t *testing.T) (*Orchestrator, *recordingService)
}

// startEmpty returns a started orchestrator with no services, the base for a
// hot-added entry that must stay StatusRegistered or be primed into a transient
// status.
func startEmpty(t *testing.T) *Orchestrator {
	t.Helper()
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return o
}

// hotAdd registers svc into the running orchestrator under the matrix names.
func hotAdd(t *testing.T, o *Orchestrator, svc Service) {
	t.Helper()
	if err := o.Register(svc, WithName("svc"), WithGroup("g")); err != nil {
		t.Fatalf("hot Register: %v", err)
	}
}

// primeStatus forces the target entry into a transient status the public API
// cannot hold. starting/removing are set to the reservation the status implies,
// so the teardown paths see the same state a real transition would expose.
func primeStatus(t *testing.T, o *Orchestrator, status ServiceStatus) {
	t.Helper()
	e := mustEntry(t, o, "svc")
	switch status {
	case StatusStarting:
		e.starting.Store(true)
	case StatusStopping:
		e.removing.Store(true)
	}
	o.statusMu.Lock()
	e.status = status
	o.statusMu.Unlock()
}

// startRegistered returns a running orchestrator plus a statically registered
// persistent recording service.
func startRegistered(t *testing.T) (*Orchestrator, *recordingService) {
	t.Helper()
	svc := newRecordingService()
	o := newGraph(t).service("svc", svc, WithGroup("g")).start()
	return o, svc
}

// statusCases is the status axis of the matrix, in the order the statuses are
// declared.
func statusCases() []statusCase {
	return []statusCase{
		{"Registered", func(t *testing.T) (*Orchestrator, *recordingService) {
			o := startEmpty(t)
			svc := newRecordingService()
			hotAdd(t, o, svc)
			return o, svc
		}},
		{"Starting", func(t *testing.T) (*Orchestrator, *recordingService) {
			o := startEmpty(t)
			svc := newRecordingService()
			hotAdd(t, o, svc)
			primeStatus(t, o, StatusStarting)
			return o, svc
		}},
		{"Running", func(t *testing.T) (*Orchestrator, *recordingService) {
			return startRegistered(t)
		}},
		{"Stopping", func(t *testing.T) (*Orchestrator, *recordingService) {
			o := startEmpty(t)
			svc := newRecordingService()
			hotAdd(t, o, svc)
			primeStatus(t, o, StatusStopping)
			return o, svc
		}},
		{"Stopped", func(t *testing.T) (*Orchestrator, *recordingService) {
			o, svc := startRegistered(t)
			if err := o.StopService("svc", 3*time.Second); err != nil {
				t.Fatalf("setup StopService: %v", err)
			}
			return o, svc
		}},
		{"Crashed", func(t *testing.T) (*Orchestrator, *recordingService) {
			svc := newRecordingService()
			svc.startFn = func(int, ServiceContext) error { return errors.New("boom") }
			o := newGraph(t).service("svc", svc, WithGroup("g")).start()
			waitForCondition(t, 3*time.Second, func() bool {
				return o.statusOf(mustEntry(t, o, "svc")) == StatusCrashed
			}, "service to reach StatusCrashed")
			return o, svc
		}},
		{"Succeeded", func(t *testing.T) (*Orchestrator, *recordingService) {
			svc := newRecordingService()
			svc.startFn = func(int, ServiceContext) error { return nil }
			o := newGraph(t).service("svc", svc, WithGroup("g"), WithRunOnce()).start()
			waitForCondition(t, 3*time.Second, func() bool {
				return o.statusOf(mustEntry(t, o, "svc")) == StatusSucceeded
			}, "runOnce gate to reach StatusSucceeded")
			return o, svc
		}},
	}
}

// matrixCell is the expected outcome of tearing a status down one way.
type matrixCell struct {
	// wantErr is the sentinel the path must return, or nil for success.
	wantErr error
	// stopDelta is how many times the service's Stop() must be called.
	stopDelta int
	// removed reports that the entry must be gone from the registry.
	removed bool
	// final is the status the entry must hold when it is not removed.
	final ServiceStatus
}

// statusMatrix is the expected outcome for every status × teardown path.
func statusMatrix() map[string]map[string]matrixCell {
	success := func(stopDelta int, final ServiceStatus) matrixCell {
		return matrixCell{stopDelta: stopDelta, final: final}
	}
	removed := func() matrixCell { return matrixCell{stopDelta: 1, removed: true} }
	busy := func(final ServiceStatus) matrixCell {
		return matrixCell{wantErr: ErrMembershipBusy, final: final}
	}
	skipped := func(final ServiceStatus) matrixCell { return matrixCell{final: final} }

	return map[string]map[string]matrixCell{
		"Registered": {
			"Stop":        success(1, StatusStopped),
			"StopService": success(1, StatusStopped),
			"Unregister":  removed(),
			"StopGroup":   success(1, StatusStopped),
		},
		"Starting": {
			"Stop":        success(1, StatusStopped),
			"StopService": busy(StatusStarting),
			"Unregister":  busy(StatusStarting),
			"StopGroup":   skipped(StatusStarting),
		},
		"Running": {
			"Stop":        success(1, StatusStopped),
			"StopService": success(1, StatusStopped),
			"Unregister":  removed(),
			"StopGroup":   success(1, StatusStopped),
		},
		"Stopping": {
			"Stop":        success(1, StatusStopped),
			"StopService": busy(StatusStopping),
			"Unregister":  busy(StatusStopping),
			"StopGroup":   skipped(StatusStopping),
		},
		"Stopped": {
			"Stop":        success(1, StatusStopped),
			"StopService": success(1, StatusStopped),
			"Unregister":  removed(),
			"StopGroup":   success(1, StatusStopped),
		},
		"Crashed": {
			"Stop":        success(1, StatusStopped),
			"StopService": success(1, StatusStopped),
			"Unregister":  removed(),
			"StopGroup":   success(1, StatusStopped),
		},
		"Succeeded": {
			"Stop":        success(1, StatusSucceeded),
			"StopService": success(1, StatusSucceeded),
			"Unregister":  removed(),
			// StopGroup deliberately excludes runOnce gates, so it selects
			// nothing and the gate keeps both its Stop() and its status.
			"StopGroup": skipped(StatusSucceeded),
		},
	}
}

// TestStatus_TeardownPath_Matrix pins the seven-statuses × four-teardown-paths
// contract: what each teardown returns, whether it ran the service's Stop(), and
// the status the entry is left in. It is the matrix that would have caught the
// #11 class of defect (a teardown claiming a status it did not verify) at the
// intersection of a status and a path, not on the happy path.
func TestStatus_TeardownPath_Matrix(t *testing.T) {
	for _, status := range statusCases() {
		for _, path := range teardownPaths() {
			want, ok := statusMatrix()[status.name][path.name]
			if !ok {
				t.Fatalf("matrix missing cell %s × %s", status.name, path.name)
			}
			t.Run(status.name+"_"+path.name, func(t *testing.T) {
				o, svc := status.build(t)
				// The whole-orchestrator Stop cannot be called twice, but each
				// cell has its own orchestrator, so the cleanup only matters for
				// the membership paths.
				defer func() { _ = o.Stop(3 * time.Second) }()

				before := svc.stopCount()
				err := runBounded(t, 3*time.Second, func() error { return path.invoke(o) })

				switch {
				case want.wantErr == nil && err != nil:
					t.Fatalf("%s returned %v, want nil", path.name, err)
				case want.wantErr != nil && !errors.Is(err, want.wantErr):
					t.Fatalf("%s returned %v, want %v", path.name, err, want.wantErr)
				}

				if got := svc.stopCount() - before; got != want.stopDelta {
					t.Errorf("Stop() delta = %d, want %d", got, want.stopDelta)
				}

				o.mu.RLock()
				e := o.nameIndex["svc"]
				o.mu.RUnlock()
				if want.removed {
					if e != nil {
						t.Errorf("entry still registered after %s, want removed", path.name)
					}
					return
				}
				if e == nil {
					t.Fatalf("entry missing after %s", path.name)
				}
				if got := o.statusOf(e); got != want.final {
					t.Errorf("final status = %s, want %s", got, want.final)
				}
			})
		}
	}
}

// runBounded runs fn in a goroutine and fails the test if it does not return
// within d, so a teardown that deadlocks fails fast and reproducibly instead of
// hanging until the package test timeout.
func runBounded(t *testing.T, d time.Duration, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("operation did not return within %v", d)
		return nil
	}
}
