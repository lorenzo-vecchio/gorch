package gorch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ── Benchmark regression gate ──
//
// The numbers in testdata/bench/baseline.txt are the checked-in reference point
// for the benchmarks in this file. The "Benchmark" CI workflow (bench.yml) uses
// them two ways:
//
//   - allocations (B/op, allocs/op) are compared directly against the baseline,
//     since they are machine-independent; and
//   - sec/op is compared with a large tripwire budget, so a callback that starts
//     holding an orchestration lock — an order-of-magnitude slowdown — fails CI
//     even though the baseline was recorded on a different machine. Fine-grained
//     time regressions are compared against the pull request's base revision in
//     the same CI job, where the machine is fixed.
//
// Regenerate the baseline with the CI toolchain and the exact CI command when a
// change intentionally moves allocations or the tripwire numbers:
//
//	GOTOOLCHAIN=go1.25.0 go test . -run '^$' -bench . -benchmem -cpu=4 -count=6 > testdata/bench/baseline.txt
//
// -cpu=4 pins GOMAXPROCS so the "-4" benchmark-name suffix matches the CI run;
// -ignore cpu on the benchstat side absorbs the machine description. Without
// both, benchstat silently emits no comparison at all.

// BenchmarkStartStop50Services measures the full lifecycle cost of starting and
// gracefully stopping 50 trivial persistent services.
func BenchmarkStartStop50Services(b *testing.B) {
	for i := 0; i < b.N; i++ {
		o := New(WithHealthChecksDisabled())
		for j := 0; j < 50; j++ {
			if err := o.Register(&namedSvc{name: fmt.Sprintf("svc-%02d", j)}, WithName(fmt.Sprintf("svc-%02d", j))); err != nil {
				b.Fatal(err)
			}
		}
		if err := o.Start(); err != nil {
			b.Fatal(err)
		}
		if err := o.Stop(10 * time.Second); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMessengerPublish measures hot-path publish throughput to a single
// subscriber, then drains the buffered messages outside the timed loop.
func BenchmarkMessengerPublish(b *testing.B) {
	m := newMessenger()
	ch, _, err := m.SubscribeWithBuffer("bench", b.N+1)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Publish(i, "bench")
	}
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		<-ch
	}
}

// BenchmarkTopoSort measures topological sorting of a 200-node chain, the shape
// the start/stop paths sort on every orchestrator run.
func BenchmarkTopoSort(b *testing.B) {
	o := New()
	const n = 200
	entries := make([]*serviceEntry, n)
	for i := 0; i < n; i++ {
		entries[i] = &serviceEntry{name: fmt.Sprintf("n%03d", i)}
		if i > 0 {
			entries[i].cfg.dependsOn = []string{fmt.Sprintf("n%03d", i-1)}
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := o.topoSort(entries); err != nil {
			b.Fatal(err)
		}
	}
}

// churnOrchestrator builds and starts an orchestrator with n trivial services,
// then spawns one background goroutine per service that repeatedly stops and
// restarts it, so registry reads contend with live membership churn. The
// returned cleanup quiesces the churn and shuts the orchestrator down.
func churnOrchestrator(b *testing.B, n int) (*Orchestrator, func()) {
	b.Helper()
	o := New(WithHealthChecksDisabled())
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("svc-%02d", i)
		if err := o.Register(&namedSvc{}, WithName(name)); err != nil {
			b.Fatal(err)
		}
	}
	if err := o.Start(); err != nil {
		b.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("svc-%02d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = o.StopService(name, 50*time.Millisecond)
				_ = o.StartService(name)
			}
		}()
	}
	return o, func() {
		close(stop)
		wg.Wait()
		_ = o.Stop(5 * time.Second)
	}
}

// BenchmarkStatusesChurn measures Statuses throughput while background
// membership operations stop and restart services concurrently.
func BenchmarkStatusesChurn(b *testing.B) {
	o, cleanup := churnOrchestrator(b, 8)
	defer cleanup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = o.Statuses()
	}
}

// BenchmarkNamesChurn measures Names throughput while background membership
// operations stop and restart services concurrently.
func BenchmarkNamesChurn(b *testing.B) {
	o, cleanup := churnOrchestrator(b, 8)
	defer cleanup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = o.Names()
	}
}

// benchSlowHealthProbe is how long the latency guard's health callback holds
// user code. It is orders of magnitude larger than a Statuses call, so a
// Statuses that blocked on a lock the probe held would report roughly this per
// operation.
const benchSlowHealthProbe = 20 * time.Millisecond

// BenchmarkStatusesDuringSlowHealthProbe is the lock-across-user-code latency
// guard: one service's Health callback sleeps while a background goroutine calls
// Health() in a loop, and the main goroutine times Statuses(). It pins the
// contract that Health runs user code with no orchestration lock held, so a slow
// probe cannot delay introspection on another goroutine. A regression that took
// a shared lock across the callback would make each Statuses() wait for
// benchSlowHealthProbe and inflate ns/op by roughly four orders of magnitude —
// the failure mode race detection cannot see and the CI regression gate catches.
func BenchmarkStatusesDuringSlowHealthProbe(b *testing.B) {
	o := New(WithHealthChecksDisabled())
	slow := &healthSvc{healthFn: func(context.Context) error {
		time.Sleep(benchSlowHealthProbe)
		return nil
	}}
	if err := o.Register(slow, WithName("slow-health")); err != nil {
		b.Fatal(err)
	}
	if err := o.Start(); err != nil {
		b.Fatal(err)
	}
	defer o.Stop(5 * time.Second)

	stop := make(chan struct{})
	probed := make(chan struct{})
	go func() {
		defer close(probed)
		for {
			select {
			case <-stop:
				return
			default:
				o.Health()
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = o.Statuses()
	}
	b.StopTimer()
	close(stop)
	<-probed
}

// BenchmarkRegister_ReloadChurn measures Register on a live graph under
// register/unregister churn — the shape a supervisor reload loop produces. Each
// churn registration names the top of a pre-built 500-entry chain, so it pays a
// dependency walk over that chain; with the visited set the walk is O(V+E).
func BenchmarkRegister_ReloadChurn(b *testing.B) {
	o := New(WithHealthChecksDisabled())
	top := insertDepChain(o, 500)
	if err := o.Start(); err != nil {
		b.Fatal(err)
	}
	defer o.Stop(5 * time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("churn-%d", i)
		if err := o.Register(&namedSvc{}, WithName(name), DependsOn(top)); err != nil {
			b.Fatal(err)
		}
		if err := o.Unregister(name, time.Second); err != nil {
			b.Fatal(err)
		}
	}
}
