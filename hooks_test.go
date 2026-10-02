package gorch

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins the lifecycle callbacks: OnStateChange (including duplicate
// suppression), OnCrash (with and without self-heal), and the global after-start
// hook when set and when absent.

func TestLifecycleHooks(t *testing.T) {
	t.Run("global_before_start_hook", func(t *testing.T) {
		var called []string
		o := New(
			WithLogLevel(LogLevelWarn),
			WithGlobalOnBeforeStart(func(name string) error {
				called = append(called, "before:"+name)
				return nil
			}),
			WithGlobalOnAfterStart(func(name string, err error) {
				called = append(called, "after:"+name)
			}),
		)
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("a"))
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		_ = o.Stop(time.Second)

		if len(called) < 1 || called[0] != "before:a" {
			t.Errorf("expected [before:a ...], got %v", called)
		}
	})

	t.Run("before_start_hook_error_aborts", func(t *testing.T) {
		hookErr := errors.New("hook denied")
		o := New(
			WithLogLevel(LogLevelWarn),
			WithGlobalOnBeforeStart(func(name string) error { return hookErr }),
		)
		_ = o.Register(&testSvc{}, WithName("a"))
		err := o.Start()
		if err == nil {
			t.Fatal("expected error from before-start hook")
		}
		// ponytail: logCh already closed by stopStartedServices.
	})

	t.Run("per_service_after_start_overrides_global", func(t *testing.T) {
		var globalCalls, perSvcCalls []string

		o := New(
			WithLogLevel(LogLevelWarn),
			WithGlobalOnAfterStart(func(name string, err error) {
				globalCalls = append(globalCalls, name)
			}),
		)

		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("a"), WithOnAfterStart(func(name string, err error) {
			perSvcCalls = append(perSvcCalls, name)
		}))

		_ = o.Start()
		defer o.Stop(time.Second)
		time.Sleep(50 * time.Millisecond)

		if len(perSvcCalls) != 1 {
			t.Errorf("per-service hook should be called, got %v", perSvcCalls)
		}
		if len(globalCalls) != 0 {
			t.Errorf("global hook should not be called when per-service overrides, got %v", globalCalls)
		}
	})

	t.Run("all_four_hooks_for_stop", func(t *testing.T) {
		var events []string
		o := New(WithLogLevel(LogLevelWarn))

		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("a"),
			WithOnBeforeStop(func(name string) error {
				events = append(events, "per-before-stop:"+name)
				return nil
			}),
			WithOnAfterStop(func(name string, err error) {
				events = append(events, "per-after-stop:"+name)
			}),
		)

		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		_ = o.Stop(time.Second)

		foundBefore := false
		foundAfter := false
		for _, e := range events {
			if e == "per-before-stop:a" {
				foundBefore = true
			}
			if e == "per-after-stop:a" {
				foundAfter = true
			}
		}
		if !foundBefore || !foundAfter {
			t.Errorf("per-service stop hooks not all called: %v", events)
		}
	})

	t.Run("before_stop_hook_error_not_fatal", func(t *testing.T) {
		o := New(
			WithLogLevel(LogLevelWarn),
			WithGlobalOnBeforeStop(func(name string) error {
				return errors.New("before-stop issue")
			}),
		)
		svc := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		_ = o.Register(svc, WithName("a"))
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		err := o.Stop(time.Second)
		// Hook error is logged but Stop still completes.
		if err == nil {
			t.Error("expected error from before-stop hook")
		}
		if svc.stopCalls.Load() < 1 {
			t.Error("service Stop should still be called despite hook error")
		}
	})
}

func TestCallAfterStartHook_Global(t *testing.T) {
	var called []string
	o := New(
		WithLogLevel(LogLevelWarn),
		WithGlobalOnAfterStart(func(name string, err error) {
			called = append(called, name)
		}),
	)

	entry := &serviceEntry{name: "test", cfg: registerConfig{name: "test"}}
	o.callAfterStartHook(entry, nil)

	if len(called) != 1 || called[0] != "test" {
		t.Errorf("expected [test], got %v", called)
	}
}

func TestCallAfterStartHook_None(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	entry := &serviceEntry{name: "test", cfg: registerConfig{name: "test"}}
	// No global, no per-service hook. Must not panic.
	o.callAfterStartHook(entry, nil)
}

func TestOnStateChange(t *testing.T) {
	var events []struct{ name, from, to string }
	var mu sync.Mutex

	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnStateChange(func(name string, from, to ServiceStatus) {
			mu.Lock()
			events = append(events, struct{ name, from, to string }{
				name, from.String(), to.String(),
			})
			mu.Unlock()
		}),
	)

	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}
	_ = o.Register(svc, WithName("tracked"))
	_ = o.Start()
	time.Sleep(50 * time.Millisecond)
	_ = o.Stop(time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(events) < 4 {
		t.Fatalf("expected at least 4 events, got %d: %v", len(events), events)
	}

	// registered -> starting
	if events[0].from != "registered" || events[0].to != "starting" {
		t.Errorf("event 0: expected registered->starting, got %s->%s", events[0].from, events[0].to)
	}
	// starting -> running
	if events[1].from != "starting" || events[1].to != "running" {
		t.Errorf("event 1: expected starting->running, got %s->%s", events[1].from, events[1].to)
	}
	// running -> stopping
	foundStopping := false
	foundStopped := false
	for _, e := range events {
		if e.from == "running" && e.to == "stopping" {
			foundStopping = true
		}
		if e.from == "stopping" && e.to == "stopped" {
			foundStopped = true
		}
	}
	if !foundStopping {
		t.Error("missing running->stopping transition")
	}
	if !foundStopped {
		t.Error("missing stopping->stopped transition")
	}
}

func TestOnCrash(t *testing.T) {
	t.Run("hook_fires_on_crash", func(t *testing.T) {
		var crashName string
		var crashErr error
		var mu sync.Mutex

		o := New(
			WithLogLevel(LogLevelWarn),
			WithOnCrash(func(name string, err error) {
				mu.Lock()
				crashName = name
				crashErr = err
				mu.Unlock()
			}),
		)

		// StatusCrashed is set when a runOnce service returns a non-Canceled error.
		_ = o.Register(&errSvc{err: errors.New("boom")}, WithName("crasher"), WithRunOnce())
		o.Start() // will fail because runOnce error aborts startup
		// logCh is already closed by stopStartedServices, so no Stop() call.

		mu.Lock()
		defer mu.Unlock()
		if crashName != "crasher" {
			t.Errorf("expected crash on 'crasher', got %q", crashName)
		}
		if crashErr == nil {
			t.Error("expected non-nil crash error")
		}
		if !strings.Contains(crashErr.Error(), "boom") {
			t.Errorf("expected real error to reach OnCrash, got %v", crashErr)
		}
	})

	t.Run("no_hook_no_panic", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(&errSvc{err: errors.New("boom")}, WithName("nocb"))
		_ = o.Start()
		time.Sleep(100 * time.Millisecond)
		_ = o.Stop(500 * time.Millisecond)
		// Just verifying no panic.
	})
}

func TestOnStateChange_NoDuplicateTransitions(t *testing.T) {
	var events int
	var mu sync.Mutex

	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnStateChange(func(name string, from, to ServiceStatus) {
			mu.Lock()
			events++
			mu.Unlock()
		}),
	)

	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("nodup"))

	_ = o.Start()
	time.Sleep(100 * time.Millisecond)
	_ = o.Stop(time.Second)

	mu.Lock()
	defer mu.Unlock()

	// We expect registered->starting, starting->running, running->stopping, stopping->stopped (4 unique).
	if events != 4 {
		t.Errorf("expected 4 unique state transitions, got %d", events)
	}
}

func TestOnCrash_WithSelfHeal(t *testing.T) {
	// OnCrash fires with the real error once a self-heal service exhausts its
	// maxRetries and transitions to StatusCrashed.

	var crashes []string
	var mu sync.Mutex

	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnCrash(func(name string, err error) {
			mu.Lock()
			crashes = append(crashes, name)
			mu.Unlock()
		}),
	)

	var factoryCalls atomic.Int32
	factory := func() Service {
		factoryCalls.Add(1)
		return &errSvc{err: errors.New("recurring crash")}
	}
	_ = o.Register(&errSvc{err: errors.New("initial crash")},
		WithName("healer"),
		WithSelfHeal(factory),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Millisecond}),
		WithMaxRetries(3),
	)
	_ = o.Start()
	defer o.Stop(time.Second)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if factoryCalls.Load() < 1 {
		t.Error("self-heal should have created new instance")
	}
	if len(crashes) == 0 {
		t.Error("OnCrash should fire when self-heal exhausts maxRetries")
	}
}
