package gorch

import (
	"context"
	"errors"
	"runtime"
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
//
// The callbacks run inside a goroutine the bounded runner owns, so they must
// not call t.Fatal (which would Goexit that goroutine and never signal the
// runner). Each site returns an error instead and the assertions happen on the
// test goroutine.

// validatingSvc is a Service whose Validate runs the probe at Register time.
type validatingSvc struct{ validate func() error }

func (s *validatingSvc) Start(ctx ServiceContext) error { <-ctx.Done(); return ctx.Err() }
func (s *validatingSvc) Stop() error                    { return nil }
func (s *validatingSvc) Validate() error                { return s.validate() }

// healthProbeSvc is a Service whose Health runs the probe. It blocks in Start
// so it is Running when Health probes it.
type healthProbeSvc struct{ health func() error }

func (s *healthProbeSvc) Start(ctx ServiceContext) error { <-ctx.Done(); return ctx.Err() }
func (s *healthProbeSvc) Stop() error                    { return nil }
func (s *healthProbeSvc) Health(context.Context) error   { return s.health() }

// readyProbeSvc is a Service whose Ready runs the probe.
type readyProbeSvc struct{ ready func() error }

func (s *readyProbeSvc) Start(ctx ServiceContext) error { <-ctx.Done(); return ctx.Err() }
func (s *readyProbeSvc) Stop() error                    { return nil }
func (s *readyProbeSvc) Ready(context.Context) error    { return s.ready() }

// TestUserCodeCallSites_DoNotHoldLocks drives each documented user-code call
// site while, from inside it, performing a write-lock membership op. Every
// trigger is bounded, so a lock held across user code fails fast and
// deterministically.
func TestUserCodeCallSites_DoNotHoldLocks(t *testing.T) {
	// probe must never deadlock; its result is irrelevant, only that it returns.
	probe := func(o *Orchestrator) { _ = o.StopService("__probe__", 0) }

	sites := []struct {
		name string
		run  func() error
	}{
		{"Validator", func() error {
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
			return expectFired(&fired)
		}},
		{"startCondition", func() error {
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
			_ = o.Stop(3 * time.Second)
			return expectFired(&fired)
		}},
		{"OnBeforeStart", func() error {
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
			_ = o.Stop(3 * time.Second)
			return expectFired(&fired)
		}},
		{"OnAfterStart", func() error {
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
			_ = o.Stop(3 * time.Second)
			return expectFired(&fired)
		}},
		{"OnBeforeStop", func() error {
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
			if ferr := expectFired(&fired); ferr != nil {
				return ferr
			}
			return err
		}},
		{"OnAfterStop", func() error {
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
			if ferr := expectFired(&fired); ferr != nil {
				return ferr
			}
			return err
		}},
		{"OnStateChange", func() error {
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
			_ = o.Stop(3 * time.Second)
			return expectFired(&fired)
		}},
		{"OnCrash", func() error {
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
			waitNonFatal(&fired, 2*time.Second)
			err := o.Stop(3 * time.Second)
			if ferr := expectFired(&fired); ferr != nil {
				return ferr
			}
			return err
		}},
		{"HealthChecker", func() error {
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
			err := o.Stop(3 * time.Second)
			if ferr := expectFired(&fired); ferr != nil {
				return ferr
			}
			return err
		}},
		{"ReadinessChecker", func() error {
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
			err := o.Stop(3 * time.Second)
			if ferr := expectFired(&fired); ferr != nil {
				return ferr
			}
			return err
		}},
	}

	for _, site := range sites {
		site := site
		t.Run(site.name, func(t *testing.T) {
			res := make(chan error, 1)
			go func() { res <- site.run() }()
			select {
			case err := <-res:
				if err != nil {
					t.Fatalf("%s: %v", site.name, err)
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("%s: trigger did not return within 3s (a lock held across user code?)", site.name)
			}
		})
	}
}

// expectFired returns an error if the callback never ran, so a site cannot pass
// vacuously because it was never reached.
func expectFired(fired *atomic.Int32) error {
	if fired.Load() < 1 {
		return errors.New("callback never fired")
	}
	return nil
}

// waitNonFatal waits for fired to become non-zero without failing the test, so
// it is safe on a goroutine that owns only a channel, not the test.
func waitNonFatal(fired *atomic.Int32, d time.Duration) {
	deadline := time.Now().Add(d)
	for fired.Load() == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
}
