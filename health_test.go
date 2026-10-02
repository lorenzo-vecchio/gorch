package gorch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins health supervision: Health() probing, the periodic loop and
// its failure accounting, the restart threshold, per-probe timeouts, the
// before/after health hooks, and the default/disabled interval.

func TestHealth_NonHealthChecker_ReportsNil(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	_ = o.Register(&namedSvc{}, WithName("plain"))
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)
	results := o.Health()
	if _, ok := results["plain"]; !ok {
		t.Fatal("running non-HealthChecker must be reported")
	}
	if results["plain"] != nil {
		t.Error("running non-HealthChecker should report nil")
	}
}

func TestHealth_HealthChecker_ReportsError(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	healthErr := errors.New("unhealthy")
	svc := &healthSvc{healthFn: func(ctx context.Context) error { return healthErr }}
	_ = o.Register(svc, WithName("sick"))
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)
	results := o.Health()
	if !errors.Is(results["sick"], healthErr) {
		t.Errorf("expected health error, got %v", results["sick"])
	}
}

func TestHealth_HealthyService_ReportsNil(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	svc := &healthSvc{healthFn: func(ctx context.Context) error { return nil }}
	_ = o.Register(svc, WithName("fine"))
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)
	results := o.Health()
	if _, ok := results["fine"]; !ok {
		t.Fatal("running healthy service must be reported")
	}
	if results["fine"] != nil {
		t.Errorf("expected nil for healthy service, got %v", results["fine"])
	}
}

func TestHealth_NoEntries(t *testing.T) {
	o := New()
	results := o.Health()
	if len(results) != 0 {
		t.Errorf("expected empty map, got %v", results)
	}
}

// TestHealth_SkipsNonRunning pins that Health() omits every non-running entry
// instead of reporting its probe result as a live health signal: a stopped
// service whose Health() returns nil must not surface as "healthy".
func TestHealth_SkipsNonRunning(t *testing.T) {
	nonRunning := []ServiceStatus{
		StatusRegistered,
		StatusStarting,
		StatusStopping,
		StatusStopped,
		StatusCrashed,
		StatusSucceeded,
	}
	for _, st := range nonRunning {
		t.Run(st.String(), func(t *testing.T) {
			o := New(WithHealthChecks(0))
			svc := &healthSvc{healthFn: func(ctx context.Context) error { return nil }}
			entry := &serviceEntry{
				name:   "svc",
				svc:    svc,
				cfg:    registerConfig{name: "svc"},
				status: st,
				logger: newServiceLogger("svc", make(chan logEntry, 8), nil, LogLevelError),
			}
			o.entries = append(o.entries, entry)
			o.nameIndex[entry.name] = entry

			results := o.Health()
			if got, ok := results["svc"]; ok {
				t.Errorf("non-running (%s) entry must be omitted, got %v", st, got)
			}
			if n := svc.healthCalls.Load(); n != 0 {
				t.Errorf("non-running (%s) entry must not be probed, got %d calls", st, n)
			}
		})
	}
}

// TestHealth_And_RunHealthChecks_AgreeOnWhichEntriesAreProbed pins that the
// public method and the periodic loop select exactly the same entries — only
// StatusRunning ones — across all seven statuses. Each entry's checker records
// that it ran; both paths must probe only the running entry.
func TestHealth_And_RunHealthChecks_AgreeOnWhichEntriesAreProbed(t *testing.T) {
	statuses := []ServiceStatus{
		StatusRegistered,
		StatusStarting,
		StatusRunning,
		StatusStopping,
		StatusStopped,
		StatusCrashed,
		StatusSucceeded,
	}

	setup := func(t *testing.T) (*Orchestrator, []*healthSvc) {
		t.Helper()
		o := New(WithHealthChecks(0, WithFailureThreshold(100)))
		svcs := make([]*healthSvc, len(statuses))
		for i, st := range statuses {
			svc := &healthSvc{}
			svcs[i] = svc
			entry := &serviceEntry{
				name:   st.String(),
				svc:    svc,
				cfg:    registerConfig{name: st.String()},
				status: st,
				logger: newServiceLogger(st.String(), make(chan logEntry, 8), nil, LogLevelError),
			}
			o.entries = append(o.entries, entry)
			o.nameIndex[entry.name] = entry
		}
		return o, svcs
	}

	probed := func(svcs []*healthSvc) []ServiceStatus {
		var got []ServiceStatus
		for i, svc := range svcs {
			if svc.healthCalls.Load() > 0 {
				got = append(got, statuses[i])
			}
		}
		return got
	}

	o, svcs := setup(t)
	o.Health()
	fromHealth := probed(svcs)

	oLoop, svcsLoop := setup(t)
	oLoop.runHealthChecks()
	fromLoop := probed(svcsLoop)

	if !reflect.DeepEqual(fromHealth, fromLoop) {
		t.Fatalf("Health probed %v, runHealthChecks probed %v; they must agree", fromHealth, fromLoop)
	}
	want := []ServiceStatus{StatusRunning}
	if !reflect.DeepEqual(fromHealth, want) {
		t.Fatalf("only StatusRunning must be probed, got %v", fromHealth)
	}
}

// TestHealth_ConcurrentRemoval covers the matrix row "Health vs removal": an
// Unregister that removes an entry while a probe is in flight must not leave
// the removed name in the result.
func TestHealth_ConcurrentRemoval(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	entered := make(chan struct{})
	release := make(chan struct{})
	svc := &healthSvc{healthFn: func(ctx context.Context) error {
		close(entered)
		<-release
		return nil
	}}
	if err := o.Register(svc, WithName("h")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	results := make(chan map[string]error, 1)
	go func() { results <- o.Health() }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("health probe never ran")
	}

	if err := o.Unregister("h", time.Second); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	close(release)

	select {
	case res := <-results:
		if _, ok := res["h"]; ok {
			t.Error("removed entry must not appear in the Health result")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Health did not return")
	}
}

func TestRunHealthChecks_FailuresTracked(t *testing.T) {
	// ponytail: set up entry manually to avoid the health-check loop.
	o := New(WithHealthChecks(0, WithFailureThreshold(3)))
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "sick",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return errors.New("bad") }},
		cfg:    registerConfig{name: "sick"},
		status: StatusRunning,
		logger: newServiceLogger("sick", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, entry)
	o.nameIndex[entry.name] = entry

	o.runHealthChecks()
	o.runHealthChecks()
	o.runHealthChecks()

	if entry.healthFailures < 3 {
		t.Errorf("expected at least 3 health failures, got %d", entry.healthFailures)
	}
}

func TestRunHealthChecks_HealthyResetsCounter(t *testing.T) {
	o := New(WithHealthChecks(0, WithFailureThreshold(3)))
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "healthy",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return nil }},
		cfg:    registerConfig{name: "healthy"},
		status: StatusRunning,
		logger: newServiceLogger("healthy", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, entry)
	o.nameIndex[entry.name] = entry

	entry.healthFailures = 5
	o.runHealthChecks()

	if entry.healthFailures != 0 {
		t.Errorf("expected health failures reset to 0, got %d", entry.healthFailures)
	}
}

func TestRunHealthChecks_PerProbeTimeout(t *testing.T) {
	// A slow checker that consumes its whole deadline must not fail a later
	// instant checker: each probe gets a fresh per-service timeout.
	o := New(WithHealthChecks(0, WithProbeTimeout(50*time.Millisecond)))
	logCh := make(chan logEntry, 1)
	slow := &serviceEntry{
		name: "slow",
		svc: &healthSvc{healthFn: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
				return errors.New("slow")
			}
		}},
		cfg:    registerConfig{name: "slow"},
		status: StatusRunning,
		logger: newServiceLogger("slow", logCh, nil, LogLevelDebug),
	}
	fast := &serviceEntry{
		name: "fast",
		svc: &healthSvc{healthFn: func(ctx context.Context) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}},
		cfg:    registerConfig{name: "fast"},
		status: StatusRunning,
		logger: newServiceLogger("fast", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, slow, fast)
	o.nameIndex[slow.name] = slow
	o.nameIndex[fast.name] = fast

	o.runHealthChecks()

	if fast.healthFailures != 0 {
		t.Errorf("fast should be healthy with a fresh per-probe timeout, got %d failures", fast.healthFailures)
	}
	if slow.healthFailures < 1 {
		t.Errorf("slow should have failed its probe (timed out), got %d failures", slow.healthFailures)
	}
}

func TestHealth_PerProbeTimeout(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn), WithHealthChecks(0, WithProbeTimeout(50*time.Millisecond)))
	slow := &healthSvc{healthFn: func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return errors.New("slow")
		}
	}}
	fast := &healthSvc{healthFn: func(ctx context.Context) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}}
	_ = o.Register(slow, WithName("slow"))
	_ = o.Register(fast, WithName("fast"))
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	results := o.Health()

	if results["fast"] != nil {
		t.Errorf("fast should be healthy with a fresh per-probe timeout, got %v", results["fast"])
	}
	if results["slow"] == nil {
		t.Error("slow should be unhealthy (probe deadline expired)")
	}
}

func TestRunHealthChecks_NonRunningSkipped(t *testing.T) {
	o := New(WithHealthChecks(0, WithFailureThreshold(3)))
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "registered",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return errors.New("bad") }},
		cfg:    registerConfig{name: "registered"},
		status: StatusRegistered, // not running — should be skipped
		logger: newServiceLogger("registered", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, entry)
	o.nameIndex[entry.name] = entry

	o.runHealthChecks()

	if entry.healthFailures != 0 {
		t.Errorf("non-running service should not be health-checked, got %d failures", entry.healthFailures)
	}
}

func TestRunHealthChecks_ThresholdTriggersRestart(t *testing.T) {
	o := New(
		WithLogLevel(LogLevelWarn),
		WithHealthChecks(50*time.Millisecond, WithProbeTimeout(500*time.Millisecond), WithFailureThreshold(2)),
	)
	var factoryCalls atomic.Int32

	factory := func() Service {
		factoryCalls.Add(1)
		return &healthSvc{
			testSvc: testSvc{
				startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			},
			healthFn: func(ctx context.Context) error { return errors.New("still bad") },
		}
	}

	svc := &healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
		healthFn: func(ctx context.Context) error { return errors.New("bad") },
	}
	_ = o.Register(svc, WithName("sick"),
		WithSelfHeal(factory),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Millisecond}),
	)
	_ = o.Start()
	defer o.Stop(time.Second)

	// Health loop fires every 50ms, threshold=2, backoff=10ms.
	// After ~100ms (2 ticks) threshold is reached → restart.
	time.Sleep(300 * time.Millisecond)

	if factoryCalls.Load() < 1 {
		t.Error("expected self-heal restart triggered by health threshold")
	}
}

func TestRunHealthChecks_ThresholdWithoutSelfHeal(t *testing.T) {
	o := New(WithHealthChecks(0, WithFailureThreshold(2)))
	logCh := make(chan logEntry, 1)
	entry := &serviceEntry{
		name:   "sick",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return errors.New("bad") }},
		cfg:    registerConfig{name: "sick"},
		status: StatusRunning,
		logger: newServiceLogger("sick", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, entry)
	o.nameIndex[entry.name] = entry

	entry.healthFailures = 2
	o.runHealthChecks()

	// Health check fails → healthFailures increments to 3.
	// 3 >= threshold(2) but factory is nil → no reset, no restart.
	// healthFailures is NOT reset without self-heal.
	if entry.healthFailures != 3 {
		t.Errorf("expected healthFailures to be 3 (2 + 1 failed check, no reset without self-heal), got %d", entry.healthFailures)
	}
}

func TestHealthLoop_DefaultInterval(t *testing.T) {
	// Health checks are enabled by default (30s interval). Use
	// WithHealthChecksDisabled to turn them off.
	o := New()
	_ = o.Register(&healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
	}, WithName("hc"))
	_ = o.Start()
	defer o.Stop(time.Second)

	if o.healthCancel == nil {
		t.Error("health loop should be started with default interval")
	}
}

func TestHealthLoop_Disabled(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	_ = o.Register(&healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
	}, WithName("hc"))
	_ = o.Start()
	defer o.Stop(time.Second)

	if o.healthCancel != nil {
		t.Error("health loop should not be started when disabled")
	}
}

func TestRunHealthChecks_NonHealthCheckerSkipped(t *testing.T) {
	o := New(WithHealthChecks(0, WithFailureThreshold(3)))
	logCh := make(chan logEntry, 1)
	// Add a non-HealthChecker entry alongside a HealthChecker entry.
	e1 := &serviceEntry{
		name:   "plain",
		svc:    &namedSvc{}, // does NOT implement HealthChecker
		cfg:    registerConfig{name: "plain"},
		status: StatusRunning,
		logger: newServiceLogger("plain", logCh, nil, LogLevelDebug),
	}
	e2 := &serviceEntry{
		name:   "hc",
		svc:    &healthSvc{healthFn: func(ctx context.Context) error { return nil }},
		cfg:    registerConfig{name: "hc"},
		status: StatusRunning,
		logger: newServiceLogger("hc", logCh, nil, LogLevelDebug),
	}
	o.entries = append(o.entries, e1, e2)

	o.runHealthChecks() // should skip e1 without panic
}

// ── BeforeHealthCheck / AfterHealthCheck ──
func TestHealthCheckHooks(t *testing.T) {
	o := New(
		WithHealthChecks(50*time.Millisecond, WithProbeTimeout(500*time.Millisecond), WithFailureThreshold(3)),
		WithBeforeHealthCheck(func(name string) error {
			return nil
		}),
		WithAfterHealthCheck(func(name string, err error) {}),
	)
	svc := &healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
		healthFn: func(ctx context.Context) error { return nil },
	}
	_ = o.Register(svc, WithName("hc"))
	_ = o.Start()

	// Let health checks fire at least a couple times.
	time.Sleep(150 * time.Millisecond)
	_ = o.Stop(time.Second)

	// No crash means hooks ran.
	if svc.healthCalls.Load() < 1 {
		t.Error("health check should have been called")
	}
}

func TestBeforeHealthCheckHookError(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(
		WithHealthChecks(50*time.Millisecond, WithProbeTimeout(500*time.Millisecond), WithFailureThreshold(3)),
		WithBeforeHealthCheck(func(name string) error {
			return errors.New("before-health error")
		}),
	)
	svc := &healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
		healthFn: func(ctx context.Context) error { return nil },
	}
	_ = o.Register(svc, WithName("hc"))
	_ = o.Start()

	time.Sleep(150 * time.Millisecond)
	_ = o.Stop(time.Second)

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	output := buf.String()
	if !strings.Contains(output, "before-health-check hook failed") {
		t.Errorf("expected 'before-health-check hook failed' in log: %s", output)
	}
}

func TestAfterHealthCheckHook(t *testing.T) {
	var lastErr error
	o := New(
		WithHealthChecks(50*time.Millisecond, WithProbeTimeout(500*time.Millisecond), WithFailureThreshold(3)),
		WithAfterHealthCheck(func(name string, err error) {
			lastErr = err
		}),
	)
	svc := &healthSvc{
		testSvc: testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		},
		healthFn: func(ctx context.Context) error { return errors.New("unhealthy") },
	}
	_ = o.Register(svc, WithName("hc"))
	_ = o.Start()

	time.Sleep(150 * time.Millisecond)
	_ = o.Stop(time.Second)

	if lastErr == nil {
		t.Error("AfterHealthCheck should receive health error")
	}
	if !strings.Contains(lastErr.Error(), "unhealthy") {
		t.Errorf("unexpected error in AfterHealthCheck: %v", lastErr)
	}
}
