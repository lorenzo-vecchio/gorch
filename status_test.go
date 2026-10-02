package gorch

import (
	"context"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins introspection: Status/Statuses/Names/Count/CountRunning
// semantics, the reservation window, cron-staged entries, status transitions,
// and the internal status setter.

func TestStatus_Found(t *testing.T) {
	o := New()
	_ = o.Register(&namedSvc{}, WithName("alpha"))
	_ = o.Register(&namedSvc{}, WithName("beta"))

	s, ok := o.Status("alpha")
	if !ok {
		t.Fatal("expected alpha to be found")
	}
	if s != StatusRegistered {
		t.Errorf("expected StatusRegistered, got %v", s)
	}
}

func TestStatus_NotFound(t *testing.T) {
	o := New()
	_, ok := o.Status("nope")
	if ok {
		t.Error("expected nope to not be found")
	}
}

func TestStatuses(t *testing.T) {
	o := New()
	_ = o.Register(&namedSvc{}, WithName("a"))
	_ = o.Register(&namedSvc{}, WithName("b"))

	m := o.Statuses()
	if len(m) != 2 {
		t.Errorf("expected 2 entries, got %d", len(m))
	}
	if m["a"] != StatusRegistered || m["b"] != StatusRegistered {
		t.Errorf("unexpected statuses: %v", m)
	}
}

func TestNames(t *testing.T) {
	o := New()
	_ = o.Register(&namedSvc{}, WithName("first"))
	_ = o.Register(&namedSvc{}, WithName("second"))

	names := o.Names()
	if len(names) != 2 {
		t.Fatalf("expected 2 names, got %d", len(names))
	}
	if names[0] != "first" || names[1] != "second" {
		t.Errorf("expected [first second], got %v", names)
	}
}

func TestCount(t *testing.T) {
	o := New()
	if c := o.Count(); c != 0 {
		t.Errorf("expected 0, got %d", c)
	}
	_ = o.Register(&namedSvc{})
	if c := o.Count(); c != 1 {
		t.Errorf("expected 1, got %d", c)
	}
	_ = o.Register(&namedSvc{})
	if c := o.Count(); c != 2 {
		t.Errorf("expected 2, got %d", c)
	}
}

// TestCount_And_Names_SemanticsAreDocumented pins the introspection contract for
// dynamic membership across the four states a consumer must tell apart:
//
//   - running: StatusRunning, in CountRunning/RunningNames.
//   - hot-added not started: registered (counted by Count/Names, StatusRegistered)
//     but not running.
//   - staged cron: same registered reading as a persistent hot add, unscheduled.
//   - reserved: an in-flight start claimed but not yet committed, so it still
//     reads as the prior status while Busy reports the reservation.
func TestCount_And_Names_SemanticsAreDocumented(t *testing.T) {
	if o := New(); o.CountRunning() != 0 || len(o.RunningNames()) != 0 {
		t.Errorf("empty orchestrator: CountRunning=%d RunningNames=%v, want 0/[]", o.CountRunning(), o.RunningNames())
	}

	running := func() *testSvc {
		return &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	}

	cases := []struct {
		name        string
		setup       func(t *testing.T) (o *Orchestrator, subject string, cleanup func())
		wantStatus  ServiceStatus
		wantRunning bool
		wantBusy    bool
	}{
		{
			name: "running",
			setup: func(t *testing.T) (*Orchestrator, string, func()) {
				o := New(WithHealthChecksDisabled())
				if err := o.Register(running(), WithName("subject")); err != nil {
					t.Fatal(err)
				}
				if err := o.Start(); err != nil {
					t.Fatal(err)
				}
				waitForStatus(t, o, "subject", StatusRunning)
				return o, "subject", func() { _ = o.Stop(time.Second) }
			},
			wantStatus:  StatusRunning,
			wantRunning: true,
		},
		{
			name: "hot-added_not_started",
			setup: func(t *testing.T) (*Orchestrator, string, func()) {
				o := New(WithHealthChecksDisabled())
				if err := o.Register(running(), WithName("base")); err != nil {
					t.Fatal(err)
				}
				if err := o.Start(); err != nil {
					t.Fatal(err)
				}
				waitForStatus(t, o, "base", StatusRunning)
				if err := o.Register(running(), WithName("subject")); err != nil {
					t.Fatal(err)
				}
				return o, "subject", func() { _ = o.Stop(time.Second) }
			},
			wantStatus:  StatusRegistered,
			wantRunning: false,
		},
		{
			name: "staged_cron",
			setup: func(t *testing.T) (*Orchestrator, string, func()) {
				o := New(WithHealthChecksDisabled())
				if err := o.Register(running(), WithName("base")); err != nil {
					t.Fatal(err)
				}
				if err := o.Start(); err != nil {
					t.Fatal(err)
				}
				waitForStatus(t, o, "base", StatusRunning)
				if err := o.Register(running(), WithName("subject"), WithCron("0 0 0 1 1 *", CronParallel)); err != nil {
					t.Fatal(err)
				}
				return o, "subject", func() { _ = o.Stop(time.Second) }
			},
			wantStatus:  StatusRegistered,
			wantRunning: false,
		},
		{
			name: "reserved",
			setup: func(t *testing.T) (*Orchestrator, string, func()) {
				o := New(WithHealthChecksDisabled())
				if err := o.Register(running(), WithName("base")); err != nil {
					t.Fatal(err)
				}
				if err := o.Start(); err != nil {
					t.Fatal(err)
				}
				waitForStatus(t, o, "base", StatusRunning)

				entered := make(chan struct{})
				release := make(chan struct{})
				var enterOnce, releaseOnce sync.Once
				if err := o.Register(running(), WithName("subject"),
					WithOnBeforeStart(func(string) error {
						enterOnce.Do(func() { close(entered) })
						<-release
						return nil
					})); err != nil {
					t.Fatal(err)
				}
				go func() { _ = o.StartService("subject") }()
				<-entered
				return o, "subject", func() {
					releaseOnce.Do(func() { close(release) })
					_ = o.Stop(time.Second)
				}
			},
			wantStatus:  StatusRegistered,
			wantRunning: false,
			wantBusy:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o, subject, cleanup := tc.setup(t)
			defer cleanup()

			statuses := o.Statuses()
			if got := statuses[subject]; got != tc.wantStatus {
				t.Errorf("Statuses()[%s] = %v, want %v", subject, got, tc.wantStatus)
			}
			names := o.Names()
			if o.Count() != len(names) {
				t.Errorf("Count() = %d, len(Names()) = %d, want equal", o.Count(), len(names))
			}
			if !slices.Contains(names, subject) {
				t.Errorf("Names() = %v, want it to contain %q (Count/Names report registered, not running)", names, subject)
			}
			runningNames := o.RunningNames()
			if o.CountRunning() != len(runningNames) {
				t.Errorf("CountRunning() = %d, len(RunningNames()) = %d, want equal", o.CountRunning(), len(runningNames))
			}
			if got := slices.Contains(runningNames, subject); got != tc.wantRunning {
				t.Errorf("RunningNames() contains %q = %v, want %v (names=%v)", subject, got, tc.wantRunning, runningNames)
			}
			if got := o.Busy(subject); got != tc.wantBusy {
				t.Errorf("Busy(%s) = %v, want %v", subject, got, tc.wantBusy)
			}
			// CountRunning is exactly the StatusRunning subset of Statuses.
			wantRunning := 0
			for _, s := range statuses {
				if s == StatusRunning {
					wantRunning++
				}
			}
			if o.CountRunning() != wantRunning {
				t.Errorf("CountRunning() = %d, but Statuses has %d StatusRunning, want equal", o.CountRunning(), wantRunning)
			}
		})
	}
}

// TestStatus_DuringReservation pins that the reservation window is reported by
// Busy(name), not by Status/Statuses: an entry whose start has been claimed but
// not yet committed to StatusStarting still reads as its prior lifecycle status.
func TestStatus_DuringReservation(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(&testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)
	waitForStatus(t, o, "base", StatusRunning)

	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	svc := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(svc, WithName("reserved"), WithOnBeforeStart(func(string) error {
		enterOnce.Do(func() { close(entered) })
		<-release
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	startErr := make(chan error, 1)
	go func() { startErr <- o.StartService("reserved") }()
	<-entered

	// Reservation window: the start is claimed, but startOneService has not
	// committed StatusStarting yet, so the entry still reports the prior status.
	if !o.Busy("reserved") {
		t.Error("Busy(reserved) = false during the reservation, want true")
	}
	if s, ok := o.Status("reserved"); !ok || s != StatusRegistered {
		t.Errorf("Status(reserved) = %v (ok=%v), want StatusRegistered", s, ok)
	}
	if got := o.Statuses()["reserved"]; got != StatusRegistered {
		t.Errorf("Statuses()[reserved] = %v, want StatusRegistered (the reservation is not a status)", got)
	}
	if o.CountRunning() != 1 {
		t.Errorf("CountRunning during reservation = %d, want 1 (only base)", o.CountRunning())
	}
	if names := o.RunningNames(); len(names) != 1 || names[0] != "base" {
		t.Errorf("RunningNames during reservation = %v, want [base]", names)
	}

	close(release)
	if err := <-startErr; err != nil {
		t.Fatalf("StartService(reserved) = %v", err)
	}
	waitForStatus(t, o, "reserved", StatusRunning)
	if o.Busy("reserved") {
		t.Error("Busy(reserved) = true after the start committed, want false")
	}
	if got := o.Statuses()["reserved"]; got != StatusRunning {
		t.Errorf("Statuses()[reserved] = %v, want StatusRunning", got)
	}
}

// TestStatuses_CronStaged_NotRunning pins the staged-cron contract: a hot-added
// cron entry is registered (so counted by Count/Names and present in Statuses)
// but not scheduled, so it reports StatusRegistered, is absent from the running
// subset, and never ticks until StartService schedules it.
func TestStatuses_CronStaged_NotRunning(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Register(&testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)
	waitForStatus(t, o, "base", StatusRunning)

	var ticks atomic.Int32
	staged := &testSvc{startFn: func(ctx context.Context) error {
		ticks.Add(1)
		return nil
	}}
	if err := o.Register(staged, WithName("staged"), WithCron("* * * * * *", CronParallel)); err != nil {
		t.Fatal(err)
	}

	if got := o.Statuses()["staged"]; got != StatusRegistered {
		t.Fatalf("staged cron Statuses() = %v, want StatusRegistered", got)
	}
	if !slices.Contains(o.Names(), "staged") || o.Count() != 2 {
		t.Errorf("staged cron must be registered: Count=%d Names=%v", o.Count(), o.Names())
	}
	if o.CountRunning() != 1 {
		t.Errorf("CountRunning = %d, want 1 (only base)", o.CountRunning())
	}
	if names := o.RunningNames(); len(names) != 1 || names[0] != "base" {
		t.Errorf("RunningNames = %v, want [base]", names)
	}

	// No schedule is installed, so a full second passes with no tick.
	time.Sleep(1100 * time.Millisecond)
	if got := ticks.Load(); got != 0 {
		t.Fatalf("staged cron ticked %d times, want 0", got)
	}

	// Scheduling it marks it running and it joins the running subset.
	if err := o.StartService("staged"); err != nil {
		t.Fatal(err)
	}
	if got := o.Statuses()["staged"]; got != StatusRunning {
		t.Fatalf("scheduled cron Statuses() = %v, want StatusRunning", got)
	}
	if o.CountRunning() != 2 {
		t.Errorf("CountRunning after scheduling = %d, want 2", o.CountRunning())
	}
}

func TestStatusTransitions(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}
	_ = o.Register(svc, WithName("lifecycle"))
	_ = o.Start()

	// After Start, status should be Running.
	time.Sleep(50 * time.Millisecond)
	s, ok := o.Status("lifecycle")
	if !ok || s != StatusRunning {
		t.Errorf("expected Running after Start, got %v (ok=%v)", s, ok)
	}

	_ = o.Stop(time.Second)

	// After Stop, status should be Stopped.
	s, ok = o.Status("lifecycle")
	if !ok || s != StatusStopped {
		t.Errorf("expected Stopped after Stop, got %v (ok=%v)", s, ok)
	}
}

func TestSetStatusErr_NilError_FabricatesMessage(t *testing.T) {
	var got error
	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnCrash(func(name string, err error) { got = err }),
	)
	entry := &serviceEntry{name: "x", status: StatusRunning}
	o.setStatusErr(entry, StatusCrashed, nil)
	if got == nil {
		t.Fatal("expected fabricated crash error")
	}
	if !strings.Contains(got.Error(), "crashed") {
		t.Errorf("expected fabricated message, got %v", got)
	}
}
