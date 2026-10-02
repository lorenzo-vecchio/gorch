package gorch

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

// This file pins Run: signal handling (default and custom signals),
// start-error propagation, and that a concurrent Run/Start does not silently
// succeed.

// TestRun_Concurrent_DoesNotSilentlySucceed pins that Run inherits the
// concurrent-Start decision: a second Run returns ErrAlreadyStarted instead of
// blocking on a shutdown signal after a Start it did not run.
func TestRun_Concurrent_DoesNotSilentlySucceed(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn), WithHealthChecksDisabled())

	boom := errors.New("run winner boom")
	if err := o.Register(&errSvc{err: boom}, WithName("gate"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}

	unlockMembership := sync.OnceFunc(o.membershipMu.Unlock)
	defer unlockMembership()
	o.membershipMu.Lock()

	winner := make(chan error, 1)
	go func() { winner <- o.Run(time.Second) }()
	waitUntil(t, func() bool {
		o.mu.RLock()
		claimed := o.startClaimed
		o.mu.RUnlock()
		return claimed
	}, "winning Run never claimed the lifecycle")

	loser := make(chan error, 1)
	go func() { loser <- o.Run(time.Second) }()
	select {
	case err := <-loser:
		if err == nil {
			t.Fatal("concurrent Run returned nil while the winner had not finished")
		}
		if !errors.Is(err, ErrAlreadyStarted) {
			t.Fatalf("loser Run = %v, want ErrAlreadyStarted", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("loser Run blocked instead of returning ErrAlreadyStarted")
	}

	unlockMembership()
	if err := <-winner; !errors.Is(err, boom) {
		t.Fatalf("winning Run = %v, want %v", err, boom)
	}
}

func TestRun_StartError(t *testing.T) {
	o := New()
	if err := o.Register(&namedSvc{name: "bad"}, WithCron("* * * * * *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	// Register validates the spec, so corrupt it after registration: Run calls
	// Start, whose setupCron re-check must surface ErrInvalidCron.
	o.mu.Lock()
	o.entries[0].cfg.cronSpec = "invalid"
	o.mu.Unlock()
	err := o.Run(time.Second)
	if !errors.Is(err, ErrInvalidCron) {
		t.Errorf("expected ErrInvalidCron, got %v", err)
	}
}

func TestRun_DefaultSignal(t *testing.T) {
	// Cover the default signal path (len(sigSet) == 0 → SIGINT + SIGTERM).
	o := New(WithLogLevel(LogLevelWarn))
	done := make(chan error, 1)
	go func() {
		done <- o.Run(time.Second) // no signals → defaults to SIGINT + SIGTERM
	}()

	time.Sleep(100 * time.Millisecond)
	p, _ := os.FindProcess(os.Getpid())
	p.Signal(os.Interrupt)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after signal")
	}
}

func TestRun_DefaultSignal_SIGTERM(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	done := make(chan error, 1)
	go func() {
		done <- o.Run(time.Second) // defaults to SIGINT + SIGTERM
	}()

	time.Sleep(100 * time.Millisecond)
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after SIGTERM")
	}
}

func TestRun_CustomSignal(t *testing.T) {
	// Cover the custom signal path.
	o := New(WithLogLevel(LogLevelWarn))

	done := make(chan error, 1)
	go func() {
		done <- o.Run(time.Second, syscall.SIGUSR1)
	}()

	time.Sleep(100 * time.Millisecond)
	syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after signal")
	}
}
