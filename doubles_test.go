package gorch

import (
	"context"
	"sync/atomic"
)

// This file collects the service doubles shared across the gorch suite: a
// plain recording service and named, erroring, panicking and stop-panicking
// variants, plus the HealthChecker, ReadinessChecker and Validator decorators.
// They are ordinary package-level types so every other test file can use them
// without redeclaring one.

type testSvc struct {
	startFn    func(ctx context.Context) error
	stopFn     func() error
	stopCalls  atomic.Int32
	startCalls atomic.Int32
}

func (s *testSvc) Start(ctx ServiceContext) error {
	s.startCalls.Add(1)
	if s.startFn != nil {
		return s.startFn(ctx)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *testSvc) Stop() error {
	s.stopCalls.Add(1)
	if s.stopFn != nil {
		return s.stopFn()
	}
	return nil
}

type namedSvc struct{ name string }

func (s *namedSvc) Start(ctx ServiceContext) error { <-ctx.Done(); return ctx.Err() }

func (s *namedSvc) Stop() error { return nil }

type errSvc struct{ err error }

func (s *errSvc) Start(ctx ServiceContext) error { return s.err }

func (s *errSvc) Stop() error { return nil }

type panicSvc struct{ msg string }

func (s *panicSvc) Start(ctx ServiceContext) error { panic(s.msg) }

func (s *panicSvc) Stop() error { return nil }

type stopPanicSvc struct{}

func (s *stopPanicSvc) Start(ctx ServiceContext) error { <-ctx.Done(); return ctx.Err() }

func (s *stopPanicSvc) Stop() error { panic("stop boom") }

// healthSvc is a test service that implements HealthChecker.
type healthSvc struct {
	testSvc
	healthFn    func(ctx context.Context) error
	healthCalls atomic.Int32
}

func (s *healthSvc) Health(ctx context.Context) error {
	s.healthCalls.Add(1)
	if s.healthFn != nil {
		return s.healthFn(ctx)
	}
	return nil
}

// readySvc is a test service that implements ReadinessChecker.
type readySvc struct {
	testSvc
	readyFn func(ctx context.Context) error
}

func (s *readySvc) Ready(ctx context.Context) error {
	if s.readyFn != nil {
		return s.readyFn(ctx)
	}
	return nil
}

// validSvc is a test service that implements Validator.
type validSvc struct {
	testSvc
	validateErr error
}

func (s *validSvc) Validate() error { return s.validateErr }
