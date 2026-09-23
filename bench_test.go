package gorch

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

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
	ch, _ := m.SubscribeWithBuffer("bench", b.N+1)
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
// membership operations stop and restart services concurrently (C20).
func BenchmarkStatusesChurn(b *testing.B) {
	o, cleanup := churnOrchestrator(b, 8)
	defer cleanup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = o.Statuses()
	}
}

// BenchmarkNamesChurn measures Names throughput while background membership
// operations stop and restart services concurrently (C20).
func BenchmarkNamesChurn(b *testing.B) {
	o, cleanup := churnOrchestrator(b, 8)
	defer cleanup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = o.Names()
	}
}
