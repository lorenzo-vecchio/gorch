package gorch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins the lock discipline around user code (issue #54): a callback
// the library invokes must never run while an orchestration lock is held, or a
// service that calls a membership operation from inside the callback — a
// documented, supported thing to do — would deadlock. The test runs, from
// inside every documented call site, a probe that takes the orchestrator's
// write lock (`StopService` on an unknown name). A held lock would block the
// probe forever, and the bounded trigger would fail by timeout rather than by a
// `-race` report, which is the failure mode the issue calls out.

// validatingSvc is a Service whose Validate runs the probe at Register time.
type validatingSvc struct{ validate func() error }

func (s *validatingSvc) Start(ServiceContext) error { return nil }
func (s *validatingSvc) Stop() error                { return nil }
func (s *validatingSvc) Validate() error            { return s.validate() }

// healthProbeSvc is a Service whose Health runs the probe.
type healthProbeSvc struct{ health func() error }

func (s *healthProbeSvc) Start(ServiceContext) error { return nil }
func (s *healthProbeSvc) Stop() error                { return nil }
func (s *healthProbeSvc) Health(context.Context) error {
	return s.health()
}

// readyProbeSvc is a Service whose Ready runs the probe.
type readyProbeSvc struct{ ready func() error }

func (s *readyProbeSvc) Start(ServiceContext) error { return nil }
func (s *readyProbeSvc) Stop() error                { return nil }
func (s *readyProbeSvc) Ready(context.Context) error {
	return s.ready()
}

// TestUserCodeCallSites_DoNotHoldLocks drives each documented user-code call
// site while, from inside it, performing a write-lock membership op. Every
// trigger is bounded, so a lock held across user code fails fast and
// deterministically.
func TestUserCodeCallSites_DoNotHoldLocks(t *testing.T) {
	// probe must never deadlock; its result is irrelevant, only that it returns.
	probe := func(o *Orchestrator) { _ = o.StopService("__probe__", 0) }

	sites := []struct {
		name string
		run  func(t *testing.T) error
	}{
		{"Validator", func(t *testing.T) error {
			var fired atomic.Int32
			o := New(WithHealthChecksDisabled())
			svc := &validatingSvc{validate: func() error {
				probe(o)
				fired.Add(1)
				return nil
			}}
			if err := o.Register(svc, WithName("svc")); err != nil {
				return err
			}
			assertFired(t, &fired, "Validate")
			return nil
		}},
		{"startCondition", func(t *testing.T) error {
			var fired atomic.Int32
			o := New(WithHealthChecksDisabled())
			if err := o.Register(newRecordingService(), WithName("svc"), WithStartCondition(func() bool {
				probe(o)
				fired.Add(1)
				return true
			})); err != nil {
				return err
			}
			if err := o.Start(); err != nil {
				return err
			}
			assertFired(t, &fired, "startCondition")
			return o.Stop(3 * time.Second)
		}},
		{"OnBeforeStart", func(t *testing.T) error {
			var fired atomic.Int32
			var o *Orchestrator
			o = New(WithHealthChecksDisabled(), WithGlobalOnBeforeStart(func(string) error {
				probe(o)
				fired.Add(1)
				return nil
			}))
			if err := o.Register(newRecordingService(), WithName("svc")); err != nil {
				return err
			}
			if err := o.Start(); err != nil {
				return err
			}
			assertFired(t, &fired, "OnBeforeStart")
			return o.Stop(3 * time.Second)
		}},
		{"OnAfterStart", func(t *testing.T) error {
			var fired atomic.Int32
			var o *Orchestrator
			o = New(WithHealthChecksDisabled(), WithGlobalOnAfterStart(func(string, error) {
				probe(o)
				fired.Add(1)
			}))
			if err := o.Register(newRecordingService(), WithName("svc")); err != nil {
				return err
			}
			if err := o.Start(); err != nil {
				return err
			}
			assertFired(t, &fired, "OnAfterStart")
			return o.Stop(3 * time.Second)
		}},
		{"OnBeforeStop", func(t *testing.T) error {
			var fired atomic.Int32
			var o *Orchestrator
			o = New(WithHealthChecksDisabled(), WithGlobalOnBeforeStop(func(string) error {
				probe(o)
				fired.Add(1)
				return nil
			}))
			if err := o.Register(newRecordingService(), WithName("svc")); err != nil {
				return err
			}
			if err := o.Start(); err != nil {
				return err
			}
			err := o.Stop(3 * time.Second)
			assertFired(t, &fired, "OnBeforeStop")
			return err
		}},
		{"OnAfterStop", func(t *testing.T) error {
			var fired atomic.Int32
			var o *Orchestrator
			o = New(WithHealthChecksDisabled(), WithGlobalOnAfterStop(func(string, error) {
				probe(o)
				fired.Add(1)
			}))
			if err := o.Register(newRecordingService(), WithName("svc")); err != nil {
				return err
			}
			if err := o.Start(); err != nil {
				return err
			}
			err := o.Stop(3 * time.Second)
			assertFired(t, &fired, "OnAfterStop")
			return err
		}},
		{"OnStateChange", func(t *testing.T) error {
			var fired atomic.Int32
			var o *Orchestrator
			o = New(WithHealthChecksDisabled(), WithOnStateChange(func(string, ServiceStatus, ServiceStatus) {
				probe(o)
				fired.Add(1)
			}))
			if err := o.Register(newRecordingService(), WithName("svc")); err != nil {
				return err
			}
			if err := o.Start(); err != nil {
				return err
			}
			assertFired(t, &fired, "OnStateChange")
			return o.Stop(3 * time.Second)
		}},
		{"OnCrash", func(t *testing.T) error {
			var fired atomic.Int32
			var o *Orchestrator
			o = New(WithHealthChecksDisabled(), WithOnCrash(func(string, error) {
				probe(o)
				fired.Add(1)
			}))
			svc := newRecordingService()
			svc.startFn = func(int, ServiceContext) error { return errors.New("boom") }
			if err := o.Register(svc, WithName("svc")); err != nil {
				return err
			}
			if err := o.Start(); err != nil {
				return err
			}
			waitForCondition(t, 3*time.Second, func() bool { return fired.Load() >= 1 }, "OnCrash")
			return o.Stop(3 * time.Second)
		}},
		{"HealthChecker", func(t *testing.T) error {
			var fired atomic.Int32
			o := New(WithHealthChecksDisabled())
			svc := &healthProbeSvc{health: func() error {
				probe(o)
				fired.Add(1)
				return nil
			}}
			if err := o.Register(svc, WithName("svc")); err != nil {
				return err
			}
			if err := o.Start(); err != nil {
				return err
			}
			_ = o.Health()
			assertFired(t, &fired, "HealthChecker")
			return o.Stop(3 * time.Second)
		}},
		{"ReadinessChecker", func(t *testing.T) error {
			var fired atomic.Int32
			o := New(WithHealthChecksDisabled())
			svc := &readyProbeSvc{ready: func() error {
				probe(o)
				fired.Add(1)
				return nil
			}}
			if err := o.Register(svc, WithName("svc")); err != nil {
				return err
			}
			if err := o.Start(); err != nil {
				return err
			}
			_ = o.IsReady(context.Background(), "svc")
			assertFired(t, &fired, "ReadinessChecker")
			return o.Stop(3 * time.Second)
		}},
	}

	for _, site := range sites {
		site := site
		t.Run(site.name, func(t *testing.T) {
			if err := runBounded(t, 3*time.Second, func() error { return site.run(t) }); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("lifecycle returned %v", err)
			}
		})
	}
}

// assertFired fails if the callback did not run, so a test cannot pass
// vacuously because the site it names was never reached.
func assertFired(t *testing.T, fired *atomic.Int32, site string) {
	t.Helper()
	if fired.Load() < 1 {
		t.Fatalf("%s callback never fired", site)
	}
}
