package gorch

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// This file pins the group operations: StartGroup/StopGroup/UnregisterGroup/
// StatusesByGroup and label lookups, including cross-group hard dependencies,
// partial-failure rollback, and per-group state-change hooks.

// TestStartGroup_CrossGroupHardDep pins that StartGroup does not report a hard
// dependency on a service outside the started group as a cycle.
func TestStartGroup_CrossGroupHardDep(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	infra := &testSvc{}
	app := &testSvc{}
	_ = o.Register(infra, WithName("infra"), WithGroup("infra"))
	_ = o.Register(app, WithName("app"), WithGroup("app"), DependsOn("infra"))
	if err := o.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer o.Stop(time.Second)

	if err := o.StartGroup("app"); err != nil {
		t.Fatalf("StartGroup must not report a cross-group hard dependency as a cycle, got %v", err)
	}
}

// TestStopGroup_CrossGroupHardDep pins that StopGroup stops the members it
// selected even when one of them hard-depends on a service in another group.
// Before the fix the discarded topoSort error left levels nil and nothing was
// stopped.
func TestStopGroup_CrossGroupHardDep(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	infra := &testSvc{}
	app := &testSvc{}
	_ = o.Register(infra, WithName("infra"), WithGroup("infra"))
	_ = o.Register(app, WithName("app"), WithGroup("app"), DependsOn("infra"))
	if err := o.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer o.Stop(time.Second)

	if err := o.StopGroup("app", time.Second); err != nil {
		t.Fatalf("StopGroup failed: %v", err)
	}
	if app.stopCalls.Load() < 1 {
		t.Error("StopGroup must stop the selected member")
	}
	if infra.stopCalls.Load() != 0 {
		t.Error("StopGroup must not stop services outside the group")
	}
}

func TestGroup(t *testing.T) {
	t.Run("group_isolation", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("a"), WithGroup("alpha"))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("b"), WithGroup("beta"))

		_ = o.Start()
		defer o.Stop(time.Second)

		// StatusesByGroup isolates.
		alphaMap := o.StatusesByGroup("alpha")
		betaMap := o.StatusesByGroup("beta")
		if len(alphaMap) != 1 || alphaMap["a"] != StatusRunning {
			t.Errorf("expected alpha={a:running}, got %v", alphaMap)
		}
		if len(betaMap) != 1 || betaMap["b"] != StatusRunning {
			t.Errorf("expected beta={b:running}, got %v", betaMap)
		}
	})

	t.Run("start_group_and_stop_group", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("s1"), WithGroup("workers"))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("s2"), WithGroup("workers"))
		_ = o.Register(&testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}, WithName("other"))

		_ = o.Start()
		defer o.Stop(time.Second)

		// Verify all started.
		all := o.Statuses()
		if all["s1"] != StatusRunning || all["s2"] != StatusRunning || all["other"] != StatusRunning {
			t.Fatalf("all services should be running: %v", all)
		}

		// Stop just the workers group.
		err := o.StopGroup("workers", time.Second)
		if err != nil {
			t.Errorf("StopGroup failed: %v", err)
		}

		// Workers should be stopped; "other" still running.
		s1, _ := o.Status("s1")
		s2, _ := o.Status("s2")
		other, _ := o.Status("other")
		if s1 != StatusStopped || s2 != StatusStopped {
			t.Errorf("workers should be stopped: s1=%v s2=%v", s1, s2)
		}
		if other != StatusRunning {
			t.Errorf("other should still be running: %v", other)
		}
	})

	t.Run("statuses_by_group_empty", func(t *testing.T) {
		o := New()
		m := o.StatusesByGroup("nonexistent")
		if len(m) != 0 {
			t.Errorf("expected empty map, got %v", m)
		}
	})
}

func TestLabel(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("web"), WithLabel("tier", "frontend"), WithLabel("env", "prod"))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("api"), WithLabel("tier", "backend"))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("db"), WithLabel("tier", "backend"), WithLabel("critical", "true"))

	_ = o.Start()
	defer o.Stop(time.Second)

	t.Run("filter_by_label", func(t *testing.T) {
		frontend := o.StatusesByLabel("tier", "frontend")
		if len(frontend) != 1 || frontend["web"] != StatusRunning {
			t.Errorf("expected one frontend, got %v", frontend)
		}
		backend := o.StatusesByLabel("tier", "backend")
		if len(backend) != 2 {
			t.Errorf("expected two backends, got %v", backend)
		}
	})

	t.Run("no_match", func(t *testing.T) {
		m := o.StatusesByLabel("env", "staging")
		if len(m) != 0 {
			t.Errorf("expected empty map, got %v", m)
		}
	})
}

func TestStateChangeHooksWithGroups(t *testing.T) {
	var events []struct {
		name string
		from string
		to   string
	}
	var mu sync.Mutex

	o := New(
		WithLogLevel(LogLevelWarn),
		WithOnStateChange(func(name string, from, to ServiceStatus) {
			mu.Lock()
			events = append(events, struct {
				name string
				from string
				to   string
			}{name, from.String(), to.String()})
			mu.Unlock()
		}),
	)

	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("alpha"), WithGroup("grp1"))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}, WithName("beta"), WithGroup("grp2"))

	_ = o.Start()

	// Wait for services to start.
	time.Sleep(100 * time.Millisecond)

	// Stop just grp1.
	err := o.StopGroup("grp1", time.Second)
	if err != nil {
		t.Fatalf("StopGroup failed: %v", err)
	}

	// Wait for events to settle.
	time.Sleep(50 * time.Millisecond)

	// Verify group filtering after StopGroup but before full Stop.
	grp1Map := o.StatusesByGroup("grp1")
	grp2Map := o.StatusesByGroup("grp2")
	if grp1Map["alpha"] != StatusStopped {
		t.Errorf("alpha should be stopped, got %v", grp1Map["alpha"])
	}
	if grp2Map["beta"] != StatusRunning {
		t.Errorf("beta should still be running after stopping grp1, got %v", grp2Map["beta"])
	}

	_ = o.Stop(time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Verify alpha went through full lifecycle.
	alphaEvents := 0
	betaEvents := 0
	for _, e := range events {
		if e.name == "alpha" {
			alphaEvents++
		}
		if e.name == "beta" {
			betaEvents++
		}
	}

	if alphaEvents < 4 {
		t.Errorf("alpha should have at least 4 events (registered->starting->running->stopping->stopped), got %d", alphaEvents)
	}
	if betaEvents < 2 {
		t.Errorf("beta should have at least 2 events (started), got %d", betaEvents)
	}
}

func TestStartGroup_Complete(t *testing.T) {
	t.Run("start_group_successfully", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.started = true // StartGroup requires a started orchestrator (issue #33)
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		go o.logPump(os.Stderr, o.logCh, o.logQuit, o.logPumpDone)

		s1 := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		s2 := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		e1 := &serviceEntry{
			name:   "s1",
			svc:    s1,
			cfg:    registerConfig{name: "s1", group: "workers"},
			status: StatusRegistered,
			logger: newServiceLogger("s1", o.logCh, nil, LogLevelDebug),
		}
		e2 := &serviceEntry{
			name:   "s2",
			svc:    s2,
			cfg:    registerConfig{name: "s2", group: "workers"},
			status: StatusRegistered,
			logger: newServiceLogger("s2", o.logCh, nil, LogLevelDebug),
		}
		o.entries = append(o.entries, e1, e2)
		o.nameIndex = map[string]*serviceEntry{"s1": e1, "s2": e2}

		err := o.StartGroup("workers")
		if err != nil {
			t.Fatalf("StartGroup failed: %v", err)
		}
		defer o.Stop(time.Second)
		time.Sleep(50 * time.Millisecond)

		if s1.startCalls.Load() < 1 {
			t.Error("s1 should be started")
		}
		if s2.startCalls.Load() < 1 {
			t.Error("s2 should be started")
		}
	})

	t.Run("start_group_toposort_error", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.started = true // StartGroup requires a started orchestrator (issue #33)
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})

		eA := &serviceEntry{
			name:   "cycle-a",
			svc:    &testSvc{},
			cfg:    registerConfig{name: "cycle-a", dependsOn: []string{"cycle-b"}, group: "cyclers"},
			status: StatusRegistered,
			logger: newServiceLogger("cycle-a", o.logCh, nil, LogLevelDebug),
		}
		eB := &serviceEntry{
			name:   "cycle-b",
			svc:    &testSvc{},
			cfg:    registerConfig{name: "cycle-b", dependsOn: []string{"cycle-a"}, group: "cyclers"},
			status: StatusRegistered,
			logger: newServiceLogger("cycle-b", o.logCh, nil, LogLevelDebug),
		}
		o.entries = append(o.entries, eA, eB)
		o.nameIndex = map[string]*serviceEntry{"cycle-a": eA, "cycle-b": eB}

		err := o.StartGroup("cyclers")
		if err == nil {
			t.Fatal("expected cycle error from StartGroup")
		}
		if !errors.Is(err, ErrDependencyCycle) {
			t.Errorf("expected ErrDependencyCycle, got %v", err)
		}
	})

	t.Run("start_group_start_failure", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.started = true // StartGroup requires a started orchestrator (issue #33)
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})

		e := &serviceEntry{
			name:   "bad",
			svc:    &errSvc{err: errors.New("init crash")},
			cfg:    registerConfig{name: "bad", group: "doomed", runOnce: true},
			status: StatusRegistered,
			logger: newServiceLogger("bad", o.logCh, nil, LogLevelDebug),
		}
		o.entries = append(o.entries, e)
		o.nameIndex = map[string]*serviceEntry{"bad": e}

		err := o.StartGroup("doomed")
		if err == nil {
			t.Fatal("expected error from StartGroup when service fails to start")
		}
		if !strings.Contains(err.Error(), "init crash") {
			t.Errorf("expected 'init crash' in error, got: %v", err)
		}
	})

	t.Run("start_group_empty", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		o.started = true // an empty group on a started orchestrator is a no-op
		err := o.StartGroup("nonexistent")
		if err != nil {
			t.Fatalf("StartGroup with empty group should not error: %v", err)
		}
	})
}

// TestStartGroup_PartialFailureRollsBack pins that a mid-group StartGroup failure
// stops the members already started in earlier levels, so the group is never
// left partially started.
func TestStartGroup_PartialFailureRollsBack(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.started = true // StartGroup requires a started orchestrator (issue #33)
	o.logCh = make(chan logEntry, 1)
	o.logQuit = make(chan struct{})
	o.logPumpDone = make(chan struct{})
	go o.logPump(os.Stderr, o.logCh, o.logQuit, o.logPumpDone)
	defer func() {
		o.cancel()
		close(o.logQuit)
		<-o.logPumpDone
	}()

	good := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	eGood := &serviceEntry{
		name: "a-ok", svc: good,
		cfg: registerConfig{name: "a-ok", group: "g"}, status: StatusRegistered,
		logger: newServiceLogger("a-ok", o.logCh, nil, LogLevelDebug),
	}
	eBad := &serviceEntry{
		name: "z-bad", svc: &errSvc{err: errors.New("boom")},
		cfg: registerConfig{name: "z-bad", group: "g", runOnce: true}, status: StatusRegistered,
		logger: newServiceLogger("z-bad", o.logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, eGood, eBad)
	o.nameIndex = map[string]*serviceEntry{"a-ok": eGood, "z-bad": eBad}

	if err := o.StartGroup("g"); err == nil {
		t.Fatal("expected StartGroup to fail on the second member")
	}
	if s := o.statusOf(eGood); s != StatusStopped {
		t.Errorf("rolled-back member status = %v, want StatusStopped", s)
	}
	if good.stopCalls.Load() == 0 {
		t.Error("rollback must call Stop() on the already-started member")
	}
}

func TestStopGroup_StopError(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.logCh = make(chan logEntry, 1)
	o.logQuit = make(chan struct{})
	o.logPumpDone = make(chan struct{})
	go o.logPump(os.Stderr, o.logCh, o.logQuit, o.logPumpDone)
	o.statusMu = sync.RWMutex{}

	e := &serviceEntry{
		name:   "flaky",
		svc:    &testSvc{stopFn: func() error { return errors.New("stop failure") }},
		cfg:    registerConfig{name: "flaky", group: "err-group"},
		status: StatusRunning,
		logger: newServiceLogger("flaky", o.logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, e)
	o.nameIndex = map[string]*serviceEntry{"flaky": e}

	err := o.StopGroup("err-group", time.Second)
	if err == nil {
		t.Fatal("expected error from StopGroup when service Stop fails")
	}
	if !strings.Contains(err.Error(), "stop failure") {
		t.Errorf("expected 'stop failure' in error, got: %v", err)
	}
}
