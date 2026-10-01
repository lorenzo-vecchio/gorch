// Nested example: reloading a subsystem at runtime with a sub-orchestrator.
//
// A gorch Orchestrator has a single-shot lifecycle: once Stop returns, it cannot
// be started again. The supported way to "restart" a running program graph is
// therefore not to restart the orchestrator, but to wrap the reloadable part in
// an adapter service that owns a sub-orchestrator. Stopping and starting the
// adapter (or ReplaceService-ing it) builds a brand-new inner orchestrator, so
// every orchestrator's single-shot lifecycle stays intact.
//
// This is the AppOrchestrator pattern: the outer orchestrator manages the
// process for its whole lifetime, while the inner one can be rebuilt as often as
// configuration changes.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lorenzo-vecchio/gorch"
)

// ── Reloadable configuration ──────────────────────────────────────────────

// Config is the part of the program that changes on reload.
type Config struct {
	version string
	workers int
}

// ConfigStore holds the live configuration. A reload publishes a new, immutable
// *Config; in-flight workers keep the snapshot they started with, so the store
// can be updated without locking a running subsystem.
type ConfigStore struct {
	cur atomic.Pointer[Config]
}

func (s *ConfigStore) Load() *Config   { return s.cur.Load() }
func (s *ConfigStore) Store(c *Config) { s.cur.Store(c) }

// ── Inner service ─────────────────────────────────────────────────────────

// Worker is a sub-service of the inner orchestrator. It is built fresh on every
// reload, so it reads the Config snapshot captured at construction.
type Worker struct {
	id  int
	cfg *Config
}

func (w *Worker) Start(ctx gorch.ServiceContext) error {
	ctx.Logger.Info("worker up", "id", w.id, "version", w.cfg.version)
	<-ctx.Done()
	return nil
}

func (w *Worker) Stop() error { return nil }

// ── AppOrchestrator adapter ───────────────────────────────────────────────

// App is the AppOrchestrator: a gorch service whose job is to own and rebuild a
// sub-orchestrator. Because Start always constructs a new inner orchestrator,
// the single-shot lifecycle of each inner one is never violated.
type App struct {
	store *ConfigStore

	mu    sync.Mutex
	inner *gorch.Orchestrator
}

func (a *App) Start(ctx gorch.ServiceContext) error {
	cfg := a.store.Load()

	inner := gorch.New()
	for i := 0; i < cfg.workers; i++ {
		if err := inner.Register(&Worker{id: i, cfg: cfg},
			gorch.WithName(fmt.Sprintf("worker-%d", i)),
		); err != nil {
			return err
		}
	}
	if err := inner.Start(); err != nil {
		return err
	}

	a.mu.Lock()
	a.inner = inner
	a.mu.Unlock()

	ctx.Logger.Info("subsystem started", "version", cfg.version, "workers", cfg.workers)

	// A persistent service blocks until its instance context is cancelled.
	<-ctx.Done()
	return nil
}

func (a *App) Stop() error {
	a.mu.Lock()
	inner := a.inner
	a.inner = nil
	a.mu.Unlock()

	if inner == nil {
		return nil
	}
	return inner.Stop(5 * time.Second)
}

// ── main ──────────────────────────────────────────────────────────────────

func must(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "gorch: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	store := &ConfigStore{}
	store.Store(&Config{version: "v1", workers: 2})

	outer := gorch.New(gorch.WithLogLevel(gorch.LogLevelInfo))
	must(outer.Register(&App{store: store}, gorch.WithName("app")))
	must(outer.Start())

	fmt.Println("=== v1 running; changing config and reloading in 3s ===")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	select {
	case <-time.After(3 * time.Second):
	case <-sig:
	}

	// Reload: stop the adapter, publish the new config, start the adapter again.
	// The outer orchestrator never stops, and the replacement inner orchestrator
	// is built from scratch — no orchestrator is ever restarted.
	fmt.Println("=== reloading subsystem to v2 ===")
	must(outer.StopService("app", 5*time.Second))
	store.Store(&Config{version: "v2", workers: 3})
	must(outer.StartService("app"))

	fmt.Println("=== v2 running; Ctrl+C to shut down ===")
	select {
	case <-time.After(3 * time.Second):
	case <-sig:
	}

	must(outer.Stop(10 * time.Second))
	<-outer.Done()
	fmt.Println("=== fully shut down ===")
}
