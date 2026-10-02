package gorch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// This file pins runOnce gates: synchronous execution with timeout, panic
// recovery, cancellation via context, and the general one-shot lifecycle.

func TestOneShot(t *testing.T) {
	t.Run("runs_before_persistent", func(t *testing.T) {
		var order []string
		var mu sync.Mutex
		started := make(chan struct{}, 2)

		oneShot := &testSvc{
			startFn: func(ctx context.Context) error {
				mu.Lock()
				order = append(order, "init")
				mu.Unlock()
				started <- struct{}{}
				return nil
			},
		}
		persistent := &testSvc{
			startFn: func(ctx context.Context) error {
				mu.Lock()
				order = append(order, "main")
				mu.Unlock()
				started <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			},
		}

		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(oneShot, WithName("init"), WithRunOnce())
		_ = o.Register(persistent, WithName("main"))
		_ = o.Start()
		defer o.Stop(time.Second)

		// Wait for both to start.
		for i := 0; i < 2; i++ {
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for service to start")
			}
		}

		mu.Lock()
		defer mu.Unlock()
		if len(order) < 2 || order[0] != "init" || order[1] != "main" {
			t.Errorf("expected [init main], got %v", order)
		}
	})

	t.Run("error_aborts_startup", func(t *testing.T) {
		oneShot := &testSvc{
			startFn: func(ctx context.Context) error {
				return errors.New("init failed")
			},
		}
		persistent := &testSvc{}

		o := New()
		_ = o.Register(oneShot, WithName("init"), WithRunOnce())
		_ = o.Register(persistent, WithName("main"))

		err := o.Start()
		if err == nil {
			o.Stop(time.Second)
			t.Fatal("expected error from failed one-shot")
		}
		// ponytail: logCh already closed by stopStartedServices.

		if persistent.startCalls.Load() > 0 {
			t.Error("persistent service should not start when one-shot fails")
		}
	})

	t.Run("transitions_to_succeeded", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		oneShot := &testSvc{
			startFn: func(ctx context.Context) error { return nil },
		}
		_ = o.Register(oneShot, WithName("init"), WithRunOnce())
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("main"))
		_ = o.Start()

		s, ok := o.Status("init")
		if !ok {
			t.Fatal("expected init to be found")
		}
		if s != StatusSucceeded {
			t.Errorf("one-shot should be StatusSucceeded, got %v", s)
		}
		if s.String() != "succeeded" {
			t.Errorf("expected String() == %q, got %q", "succeeded", s.String())
		}
		_ = o.Stop(time.Second)
	})

	t.Run("hard_dep_on_runonce_is_allowed", func(t *testing.T) {
		// A runOnce gate runs in an earlier phase than persistent services, so a
		// hard DependsOn on one is satisfiable: the gate is outside the persistent
		// topo subset, and its edge must be ignored rather than treated as a cycle.
		o := New(WithLogLevel(LogLevelWarn))
		var mu sync.Mutex
		var order []string
		gate := &testSvc{startFn: func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "gate")
			mu.Unlock()
			return nil
		}}
		workerStarted := make(chan struct{})
		worker := &testSvc{startFn: func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "worker")
			mu.Unlock()
			close(workerStarted)
			<-ctx.Done()
			return ctx.Err()
		}}
		_ = o.Register(gate, WithName("gate"), WithRunOnce())
		_ = o.Register(worker, WithName("worker"), DependsOn("gate"))
		if err := o.Start(); err != nil {
			t.Fatalf("hard dep on a runOnce gate must not fail Start: %v", err)
		}
		defer o.Stop(time.Second)

		select {
		case <-workerStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("worker was never started")
		}
		mu.Lock()
		got := append([]string(nil), order...)
		mu.Unlock()
		if len(got) != 2 || got[0] != "gate" || got[1] != "worker" {
			t.Errorf("expected gate to run before worker, got %v", got)
		}
	})

	t.Run("context_canceled_is_not_an_error", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		oneShot := &testSvc{
			startFn: func(ctx context.Context) error {
				return context.Canceled
			},
		}
		_ = o.Register(oneShot, WithName("init"), WithRunOnce())
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("main"))
		err := o.Start()
		if err != nil {
			t.Fatalf("context.Canceled from one-shot should not be an error: %v", err)
		}
		defer o.Stop(time.Second)
	})
}

func TestRunOnce_WithTimeout(t *testing.T) {
	// Cover runOnce with timeout goroutine paths.
	t.Run("success_within_timeout", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		svc := &testSvc{
			startFn: func(ctx context.Context) error { return nil },
		}
		_ = o.Register(svc, WithName("quick"), WithRunOnce(), WithStartTimeout(time.Second))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("svc"))
		err := o.Start()
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		_ = o.Stop(time.Second)
	})

	t.Run("timeout_exceeded", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(500 * time.Millisecond):
					return nil
				}
			},
		}
		_ = o.Register(svc, WithName("slow"), WithRunOnce(), WithStartTimeout(50*time.Millisecond))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("svc"))
		err := o.Start()
		if err == nil {
			o.Stop(time.Second)
			t.Fatal("expected timeout error from runOnce")
		}
	})
}

func TestRunOnce_PanicRecovery(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(&panicSvc{msg: "runonce panic"}, WithRunOnce(), WithStartTimeout(time.Second))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("svc"))

	o.Start() // will fail — don't call Stop; logCh already closed by stopStartedServices

	// Give logPump time to process the panic log entry.
	time.Sleep(50 * time.Millisecond)

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	output := buf.String()
	if !strings.Contains(output, "service panicked") {
		t.Errorf("expected 'service panicked' in log for runOnce panic: %s", output)
	}
}

func TestRunOnce_CtxDone(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.logCh = make(chan logEntry, 1)
	o.logQuit = make(chan struct{})
	o.logPumpDone = make(chan struct{})
	o.started = true
	o.nameIndex = make(map[string]*serviceEntry)

	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			select {
			case <-time.After(time.Minute):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	entry := &serviceEntry{
		name:   "runonce",
		svc:    svc,
		cfg:    registerConfig{name: "runonce", runOnce: true, startTimeout: time.Second},
		status: StatusRegistered,
		logger: newServiceLogger("runonce", o.logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, entry)
	o.nameIndex["runonce"] = entry

	go func() {
		time.Sleep(50 * time.Millisecond)
		o.cancel()
	}()

	_ = o.startOneService(entry) // ctx.Canceled is not treated as an error for runOnce
}
