package gorch

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"
)

// nilCtx is a nil context.Context used to exercise the nil-context guards.
// Declaring it as a variable rather than passing a literal nil keeps
// staticcheck's SA1012 (which targets the literal) from flagging the deliberate
// hostile input.
var nilCtx context.Context

// This file audits the public entry points for silent-misconfiguration traps and
// panic-free behaviour (issue #33). The named tests pin the fixes; the
// documented-but-intentional behaviours each get a test so the contract in
// README.md and doc.go cannot drift from the code.

// TestWithCron_EmptySpec_IsRejected pins that WithCron("") is no longer a
// silent no-op: the empty string is the "not a cron service" marker, so a flag
// tracks whether WithCron was called and Register rejects the empty spec on both
// the static and the hot-add path.
func TestWithCron_EmptySpec_IsRejected(t *testing.T) {
	t.Run("static", func(t *testing.T) {
		o := New()
		err := o.Register(&namedSvc{}, WithName("c"), WithCron("", CronParallel))
		if !errors.Is(err, ErrInvalidCron) {
			t.Fatalf("static Register WithCron(\"\") = %v, want ErrInvalidCron", err)
		}
		if o.Count() != 0 {
			t.Fatalf("rejected cron service must not enter the graph, count = %d", o.Count())
		}
	})

	t.Run("dynamic", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)
		err := o.Register(&namedSvc{}, WithName("c"), WithCron("", CronParallel))
		if !errors.Is(err, ErrInvalidCron) {
			t.Fatalf("dynamic Register WithCron(\"\") = %v, want ErrInvalidCron", err)
		}
		if _, ok := o.Status("c"); ok {
			t.Fatal("rejected cron service must not be registered")
		}
	})
}

// TestRegister_BadCronSpec_RejectedAtRegisterTime_OnBothPaths pins the timing
// reconciliation: a malformed spec is rejected by Register whether it is added
// statically (before Start) or hot-added, so the same spec is never accepted on
// one path and rejected on the other.
func TestRegister_BadCronSpec_RejectedAtRegisterTime_OnBothPaths(t *testing.T) {
	t.Run("static", func(t *testing.T) {
		o := New()
		err := o.Register(&namedSvc{}, WithName("c"), WithCron("not a spec", CronParallel))
		if !errors.Is(err, ErrInvalidCron) {
			t.Fatalf("static Register bad cron = %v, want ErrInvalidCron", err)
		}
		if o.Count() != 0 {
			t.Fatalf("rejected cron service must not enter the graph, count = %d", o.Count())
		}
	})

	t.Run("dynamic", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)
		err := o.Register(&namedSvc{}, WithName("c"), WithCron("not a spec", CronParallel))
		if !errors.Is(err, ErrInvalidCron) {
			t.Fatalf("dynamic Register bad cron = %v, want ErrInvalidCron", err)
		}
		if _, ok := o.Status("c"); ok {
			t.Fatal("rejected cron service must not be registered")
		}
	})
}

// TestSubscribeWithBuffer_NegativeCapacity_ReturnsError pins that a negative
// buffer capacity is a typed error instead of a make(chan, negative) panic.
func TestSubscribeWithBuffer_NegativeCapacity_ReturnsError(t *testing.T) {
	m := newMessenger()

	ch, unsub, err := m.SubscribeWithBuffer("topic", -1)
	if !errors.Is(err, ErrInvalidBufferSize) {
		t.Fatalf("SubscribeWithBuffer(-1) = %v, want ErrInvalidBufferSize", err)
	}
	if ch != nil || unsub != nil {
		t.Fatal("a rejected SubscribeWithBuffer must return a nil channel and unsubscribe")
	}

	// Zero and positive capacities stay valid.
	for _, size := range []int{0, 1, 4} {
		ch, unsub, err := m.SubscribeWithBuffer("topic", size)
		if err != nil {
			t.Fatalf("SubscribeWithBuffer(%d) = %v, want nil", size, err)
		}
		if ch == nil || unsub == nil {
			t.Fatalf("SubscribeWithBuffer(%d) returned a nil channel or unsubscribe", size)
		}
		unsub()
	}
}

// TestTypedRequest_NilContext_ReturnsErrNilContext pins the nil-context guard
// on the typed request path (the untyped path is exercised through Request in
// the no-panic audit).
func TestTypedRequest_NilContext_ReturnsErrNilContext(t *testing.T) {
	m := newMessenger()
	if _, err := TypedRequest[int, int](m, nilCtx, 1, "topic"); !errors.Is(err, ErrNilContext) {
		t.Fatalf("TypedRequest(nil ctx) = %v, want ErrNilContext", err)
	}
}

// TestStartGroup_BeforeStart_ReturnsErrOrchestratorNotStarted pins the lifecycle
// gate: a non-empty group before Start would build a member context from a nil
// parent and panic, so it is refused like StartService is.
func TestStartGroup_BeforeStart_ReturnsErrOrchestratorNotStarted(t *testing.T) {
	o := New()
	if err := o.Register(&namedSvc{}, WithName("s"), WithGroup("g")); err != nil {
		t.Fatal(err)
	}
	if err := o.StartGroup("g"); !errors.Is(err, ErrOrchestratorNotStarted) {
		t.Fatalf("StartGroup before Start = %v, want ErrOrchestratorNotStarted", err)
	}
	// The gate must run before selection: nothing started, nothing reserved.
	if s, _ := o.Status("s"); s != StatusRegistered {
		t.Fatalf("status after gated StartGroup = %v, want StatusRegistered", s)
	}
	if o.Busy("s") {
		t.Fatal("gated StartGroup must not leave a start reservation")
	}
}

// TestStopGroup_BeforeStart_IsNoop pins the documented asymmetry: stopping
// nothing is a no-op returning nil, consistent with StopService/Unregister,
// while starting needs the live context and is gated.
func TestStopGroup_BeforeStart_IsNoop(t *testing.T) {
	o := New()
	if err := o.Register(&namedSvc{}, WithName("s"), WithGroup("g")); err != nil {
		t.Fatal(err)
	}
	if err := o.StopGroup("g", time.Second); err != nil {
		t.Fatalf("StopGroup before Start = %v, want nil (nothing is running)", err)
	}
	if err := o.StopGroup("ghost-group", time.Second); err != nil {
		t.Fatalf("StopGroup on an unknown group = %v, want nil (a group is a tag)", err)
	}
}

// TestStartGroup_UnknownGroup_IsNoop pins that an unknown/empty group on a
// started orchestrator selects nothing and returns nil rather than an invented
// not-found error: a group is a filter tag, not a registered entity.
func TestStartGroup_UnknownGroup_IsNoop(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)
	if err := o.StartGroup("ghost-group"); err != nil {
		t.Fatalf("StartGroup unknown group = %v, want nil", err)
	}
}

// TestWithName_Empty_IsAutoAssigned pins that an explicit empty name is
// equivalent to omitting WithName: the auto-name sequence applies.
func TestWithName_Empty_IsAutoAssigned(t *testing.T) {
	o := New()
	if err := o.Register(&namedSvc{}, WithName("")); err != nil {
		t.Fatalf("Register WithName(\"\") = %v, want nil (auto-named)", err)
	}
	names := o.Names()
	if len(names) != 1 || names[0] != "$1" {
		t.Fatalf("Names() = %v, want [$1]", names)
	}
}

// TestWaitFor_NegativeTimeout_ReturnsImmediately pins the documented behaviour:
// a non-positive timeout fires at once, it is not a hang and not a panic.
func TestWaitFor_NegativeTimeout_ReturnsImmediately(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(&namedSvc{}, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	start := time.Now()
	err := o.WaitFor("s", StatusStopped, -time.Second)
	if err == nil {
		t.Fatal("WaitFor with a negative timeout on a non-matching status must time out")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("negative timeout waited %v; it must fire immediately", elapsed)
	}
}

// TestRun_NilSignalIsInert pins the documented Run behaviour: a nil element in
// the signal list is inert (it can never match), and a real signal alongside it
// still shuts down cleanly. It also proves the nil element is not a panic.
func TestRun_NilSignalIsInert(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	done := make(chan error, 1)
	go func() { done <- o.Run(time.Second, nil, syscall.SIGUSR1) }()

	time.Sleep(100 * time.Millisecond)
	syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the real signal in the list")
	}
}

// TestDependsOnRecursive_SoftEdgeDepthError covers the soft-edge error branch of
// the cycle walk: a depth error raised below a soft edge must propagate out, not
// be swallowed. The graph is built white-box because Register would reject the
// over-deep soft edge before the walk.
func TestDependsOnRecursive_SoftEdgeDepthError(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	top := insertDepChain(o, maxDependencyDepth+16)

	mid := &serviceEntry{name: "mid", svc: &namedSvc{}}
	mid.cfg.name = "mid"
	mid.cfg.softDependsOn = []string{top}
	start := &serviceEntry{name: "start", svc: &namedSvc{}}
	start.cfg.name = "start"
	start.cfg.dependsOn = []string{"mid"}
	o.entries = append(o.entries, mid, start)
	o.nameIndex["mid"] = mid
	o.nameIndex["start"] = start

	_, err := o.dependsOnRecursive(start, "unreachable", make(map[string]struct{}), 0)
	if !errors.Is(err, ErrDependencyDepthExceeded) {
		t.Fatalf("walk start -> mid (hard) -> top (soft) = %v, want ErrDependencyDepthExceeded", err)
	}
}
