// Advanced example: groups, labels, soft dependencies, RegisterFunc,
// Validator, ReadinessChecker, HealthChecker, state-change hooks,
// health-check hooks, WithStartCondition, WaitFor, Metrics, and Done().
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/lorenzo-vecchio/gorch/gorch"
)

// ── Config holder (implements Validator) ──────────────────────────────────

type Config struct {
	loaded  atomic.Bool
	version string
}

// Validate rejects config at Register time if the version is empty (never
// happens here, but shows the interface). Called synchronously in Register.
func (c *Config) Validate() error {
	if c.version == "" {
		return fmt.Errorf("config version is required")
	}
	return nil
}

// ── ConfigLoader (one-shot, group "infra") ────────────────────────────────

type ConfigLoader struct {
	config *Config
}

func (l *ConfigLoader) Start(ctx context.Context) error {
	sc := ctx.(gorch.ServiceContext)
	sc.Logger.Info("loading configuration")
	time.Sleep(100 * time.Millisecond) // simulate I/O
	l.config.loaded.Store(true)
	return nil
}

func (l *ConfigLoader) Stop() error { return nil }

// ── MetricsCollector (persistent, group "infra", HealthChecker) ───────────

type MetricsCollector struct {
	healthy atomic.Bool
}

func (m *MetricsCollector) Start(ctx context.Context) error {
	m.healthy.Store(true)
	<-ctx.Done()
	return nil
}

func (m *MetricsCollector) Stop() error {
	m.healthy.Store(false)
	return nil
}

// Health implements gorch.HealthChecker — probed periodically by the
// orchestrator. A non-nil error means unhealthy.
func (m *MetricsCollector) Health(ctx context.Context) error {
	if !m.healthy.Load() {
		return fmt.Errorf("metrics-collector is not healthy")
	}
	return nil
}

// ── APIServer (group "app", label tier=frontend) ──────────────────────────
// Implements ReadinessChecker so WaitFor can block until truly ready.
// Implements HealthChecker for periodic health probes.
// Soft-depends on "cache" — starts fine when cache is missing.

type APIServer struct {
	ready   atomic.Bool
	healthy atomic.Bool
}

func (a *APIServer) Start(ctx context.Context) error {
	sc := ctx.(gorch.ServiceContext)
	sc.Logger.Info("api-server booting up…")
	time.Sleep(200 * time.Millisecond) // simulated warm-up
	a.ready.Store(true)
	a.healthy.Store(true)
	sc.Logger.Info("api-server ready to serve")
	<-ctx.Done()
	return nil
}

func (a *APIServer) Stop() error {
	a.ready.Store(false)
	return nil
}

// Ready implements gorch.ReadinessChecker. Returns nil when warm-up is done.
func (a *APIServer) Ready(ctx context.Context) error {
	if !a.ready.Load() {
		return fmt.Errorf("api-server not ready yet")
	}
	return nil
}

// Health implements gorch.HealthChecker.
func (a *APIServer) Health(ctx context.Context) error {
	if !a.healthy.Load() {
		return fmt.Errorf("api-server unhealthy")
	}
	return nil
}

// ── BackgroundWorker (group "app", label tier=backend) ────────────────────

type BackgroundWorker struct {
	ticks atomic.Int64
}

func (w *BackgroundWorker) Start(ctx context.Context) error {
	sc := ctx.(gorch.ServiceContext)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			n := w.ticks.Add(1)
			sc.Logger.Debug("worker tick", "count", n)
		}
	}
}

func (w *BackgroundWorker) Stop() error { return nil }

// ── main ──────────────────────────────────────────────────────────────────

func main() {
	// Shared config — passed to services that need it.
	cfg := &Config{version: "1.0.0"}

	// Orchestrator with hooks. Health checks run every 2s (demo pace).
	orch := gorch.New(gorch.Config{
		LogLevel:          gorch.LogLevelDebug,
		HealthInterval:  2 * time.Second,
		HealthTimeout:   1 * time.Second,
		HealthThreshold: 2,
		// No global start timeout — persistent services block in Start().
		// Use WithStartTimeout per-service for one-shot init tasks.

		// OnStateChange fires on every status transition.
		OnStateChange: func(name string, from, to gorch.ServiceStatus) {
			fmt.Printf("[hook] %s: %s → %s\n", name, from, to)
		},

		// OnCrash fires when a service reaches StatusCrashed.
		OnCrash: func(name string, err error) {
			fmt.Printf("[hook] CRASH: %s — %v\n", name, err)
		},

		// BeforeHealthCheck fires before each service health probe.
		BeforeHealthCheck: func(name string) error {
			fmt.Printf("[health] probing %s\n", name)
			return nil
		},

		// AfterHealthCheck fires after the probe (err is the probe result).
		AfterHealthCheck: func(name string, err error) {
			if err != nil {
				fmt.Printf("[health] %s: UNHEALTHY — %v\n", name, err)
			}
		},
	})

	// ── Register infra-group services ──

	orch.Register(&ConfigLoader{config: cfg},
		gorch.WithName("config-loader"),
		gorch.WithGroup("infra"),
		gorch.WithRunOnce(),                    // one-shot init: runs before persistent services
		gorch.WithStartTimeout(2*time.Second), // guard against stuck init
	)

	orch.Register(&MetricsCollector{},
		gorch.WithName("metrics-collector"),
		gorch.WithGroup("infra"),
		gorch.WithLabel("tier", "infrastructure"),
	)

	// ── Register app-group services ──

	// api-server soft-depends on "cache": starts after it if registered,
	// but tolerates its absence.
	orch.Register(&APIServer{},
		gorch.WithName("api-server"),
		gorch.WithGroup("app"),
		gorch.WithLabel("tier", "frontend"),
		gorch.DependsOnSoft("cache"), // "cache" is never registered — no error
		// Hard dependency on metrics-collector (same group, persistent).
		gorch.DependsOn("metrics-collector"),
		// Note: config-loader is runOnce, so it's already guaranteed to
		// finish before any persistent service starts — no DependsOn needed.
	)

	// RegisterFunc: closure-based service without a dedicated struct.
	// Validates that we have the required env var; skips itself if missing.
	orch.RegisterFunc("env-check",
		func(ctx gorch.ServiceContext) error {
			ctx.Logger.Info("env-check running")
			time.Sleep(50 * time.Millisecond)
			return nil
		},
		func() error {
			fmt.Println("[env-check] cleaning up")
			return nil
		},
		gorch.WithRunOnce(),
		gorch.WithGroup("infra"),
		gorch.WithStartCondition(func() bool {
			// Always start in this demo; swap to os.Getenv("ENABLE_CHECK")=="1"
			// to see start conditions in action.
			return os.Getenv("SKIP_ENV_CHECK") != "1"
		}),
	)

	// BackgroundWorker with label filtering demo.
	orch.Register(&BackgroundWorker{},
		gorch.WithName("bg-worker"),
		gorch.WithGroup("app"),
		gorch.WithLabel("tier", "backend"),
		gorch.DependsOn("api-server"),
	)

	// ── Start ──

	fmt.Println("=== starting orchestrator ===")
	if err := orch.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start error: %v\n", err)
		os.Exit(1)
	}

	// WaitFor blocks until api-server reaches StatusRunning.
	fmt.Println("=== waiting for api-server ===")
	if err := orch.WaitFor("api-server", gorch.StatusRunning, 5*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "WaitFor failed: %v\n", err)
	}

	// Readiness is separate from running status: a service can be Running
	// but still warming up. Poll IsReady until it reports ready.
	fmt.Println("=== waiting for api-server readiness ===")
	for !orch.IsReady("api-server") {
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println("api-server reports ready")

	// ── Inspect state ──

	fmt.Println("\n=== statuses by group ===")
	for _, group := range []string{"infra", "app"} {
		fmt.Printf("[%s]\n", group)
		for name, status := range orch.StatusesByGroup(group) {
			fmt.Printf("  %-20s %s\n", name, status)
		}
	}

	fmt.Println("\n=== frontend services (label tier=frontend) ===")
	for name, status := range orch.StatusesByLabel("tier", "frontend") {
		fmt.Printf("  %-20s %s\n", name, status)
	}

	// Metrics snapshot: counters for orchestrator-level events.
	m := orch.Metrics()
	fmt.Printf("\n=== metrics snapshot ===\n")
	fmt.Printf("  starts=%d stops=%d crashes=%d restarts=%d health-fails=%d\n",
		m.Starts, m.Stops, m.Crashes, m.Restarts, m.HealthFails)

	// Let health checks fire a couple of times.
	time.Sleep(4 * time.Second)

	// ── Stop ──

	fmt.Println("\n=== stopping orchestrator ===")
	if err := orch.Stop(10 * time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "stop error: %v\n", err)
	}

	// Done() closes when all goroutines have exited.
	<-orch.Done()
	fmt.Println("orchestrator fully shut down")
}
