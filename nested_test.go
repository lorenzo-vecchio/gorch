package gorch

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// nestedConfig is the reloadable configuration of the AppOrchestrator harness.
type nestedConfig struct {
	version string
	workers int
}

// nestedWorker is a sub-service of the inner orchestrator. It has no state: the
// harness only cares that the inner orchestrator starts and stops cleanly.
type nestedWorker struct{}

func (nestedWorker) Start(ctx ServiceContext) error {
	<-ctx.Done()
	return nil
}

func (nestedWorker) Stop() error { return nil }

// innerBuild records one construction of the sub-orchestrator, so a test can
// observe each rebuild deterministically instead of sleeping.
type innerBuild struct {
	orch    *Orchestrator
	version string
	workers int
}

// nestedApp is the AppOrchestrator harness: it owns a sub-orchestrator and
// rebuilds it from the current config on every Start. This is the pattern
// documented in doc.go and demonstrated in examples/nested.
type nestedApp struct {
	cfg   atomic.Pointer[nestedConfig]
	built chan innerBuild

	mu    sync.Mutex
	inner *Orchestrator
}

func (a *nestedApp) Start(ctx ServiceContext) error {
	cfg := a.cfg.Load()

	inner := New()
	for i := 0; i < cfg.workers; i++ {
		if err := inner.Register(nestedWorker{}, WithName(fmt.Sprintf("worker-%d", i))); err != nil {
			return err
		}
	}
	if err := inner.Start(); err != nil {
		return err
	}

	a.mu.Lock()
	a.inner = inner
	a.mu.Unlock()

	a.built <- innerBuild{orch: inner, version: cfg.version, workers: cfg.workers}
	<-ctx.Done()
	return nil
}

func (a *nestedApp) Stop() error {
	a.mu.Lock()
	inner := a.inner
	a.inner = nil
	a.mu.Unlock()
	if inner == nil {
		return nil
	}
	return inner.Stop(5 * time.Second)
}

// receiveBuild blocks until the adapter reports a rebuilt sub-orchestrator. The
// bounded timeout keeps a broken implementation from hanging the suite.
func receiveBuild(t *testing.T, ch <-chan innerBuild) innerBuild {
	t.Helper()
	select {
	case b := <-ch:
		return b
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the sub-orchestrator to be built")
		return innerBuild{}
	}
}

// TestNestedAppOrchestrator_ReloadBuildsFreshInner proves the recipe behind
// doc.go's Restarting section: stopping and starting the adapter rebuilds the
// sub-orchestrator from scratch while the outer orchestrator keeps running, so
// no orchestrator's single-shot lifecycle is ever abused.
func TestNestedAppOrchestrator_ReloadBuildsFreshInner(t *testing.T) {
	app := &nestedApp{built: make(chan innerBuild, 4)}
	app.cfg.Store(&nestedConfig{version: "v1", workers: 2})

	outer := New()
	if err := outer.Register(app, WithName("app")); err != nil {
		t.Fatalf("Register = %v, want nil", err)
	}
	if err := outer.Start(); err != nil {
		t.Fatalf("Start = %v, want nil", err)
	}

	first := receiveBuild(t, app.built)
	if first.version != "v1" || first.workers != 2 {
		t.Fatalf("first build = {version:%q workers:%d}, want {v1 2}", first.version, first.workers)
	}

	// Reload: publish the new config, then rebuild the adapter's sub-orchestrator.
	app.cfg.Store(&nestedConfig{version: "v2", workers: 3})
	if err := outer.StopService("app", 5*time.Second); err != nil {
		t.Fatalf("StopService = %v, want nil", err)
	}
	select {
	case <-first.orch.Done():
	default:
		t.Fatal("StopService returned but the old sub-orchestrator was not shut down")
	}
	// The old sub-orchestrator is terminal: the reload must never restart it.
	if err := first.orch.Start(); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("restarting the reloaded sub-orchestrator = %v, want ErrAlreadyStarted", err)
	}

	if err := outer.StartService("app"); err != nil {
		t.Fatalf("StartService = %v, want nil", err)
	}
	second := receiveBuild(t, app.built)

	if second.orch == first.orch {
		t.Fatal("reload reused the old sub-orchestrator, want a fresh one")
	}
	if second.version != "v2" || second.workers != 3 {
		t.Fatalf("second build = {version:%q workers:%d}, want {v2 3}", second.version, second.workers)
	}
	if got, ok := outer.Status("app"); !ok || got != StatusRunning {
		t.Fatalf("outer app status = %v (ok=%v), want StatusRunning", got, ok)
	}

	if err := outer.Stop(5 * time.Second); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}
	select {
	case <-second.orch.Done():
	default:
		t.Fatal("outer Stop returned but the sub-orchestrator was not shut down")
	}
}

// TestNestedAppOrchestrator_ReplaceRebuildsInner covers the other documented
// path: ReplaceService swaps in an adapter that owns a fresh sub-orchestrator
// without the outer orchestrator ever stopping.
func TestNestedAppOrchestrator_ReplaceRebuildsInner(t *testing.T) {
	built := make(chan innerBuild, 4)
	app := &nestedApp{built: built}
	app.cfg.Store(&nestedConfig{version: "v1", workers: 1})

	outer := New()
	if err := outer.Register(app, WithName("app")); err != nil {
		t.Fatalf("Register = %v, want nil", err)
	}
	if err := outer.Start(); err != nil {
		t.Fatalf("Start = %v, want nil", err)
	}
	first := receiveBuild(t, built)

	replacement := &nestedApp{built: built}
	replacement.cfg.Store(&nestedConfig{version: "v2", workers: 4})
	if err := outer.ReplaceService("app", replacement, 5*time.Second); err != nil {
		t.Fatalf("ReplaceService = %v, want nil", err)
	}

	second := receiveBuild(t, built)
	if second.orch == first.orch {
		t.Fatal("ReplaceService reused the old sub-orchestrator, want a fresh one")
	}
	if second.version != "v2" || second.workers != 4 {
		t.Fatalf("replacement build = {version:%q workers:%d}, want {v2 4}", second.version, second.workers)
	}
	select {
	case <-first.orch.Done():
	default:
		t.Fatal("ReplaceService returned but the old sub-orchestrator was not shut down")
	}

	if err := outer.Stop(5 * time.Second); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}
	select {
	case <-second.orch.Done():
	default:
		t.Fatal("outer Stop returned but the sub-orchestrator was not shut down")
	}
}
