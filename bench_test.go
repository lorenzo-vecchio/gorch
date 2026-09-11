package gorch

import (
	"fmt"
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
