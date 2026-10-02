package gorch

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ── Benchmark regression gate ──
//
// The numbers in testdata/bench/baseline.txt are the checked-in reference point
// for the benchmarks in this file. The "Benchmark" workflow (bench.yml) uses
// them two ways:
//
//   - allocations (B/op, allocs/op) are compared directly against the baseline,
//     since they are machine-independent; and
//   - sec/op is compared with a large tripwire budget, so a callback that starts
//     holding an orchestration lock — an order-of-magnitude slowdown — fails CI
//     even though the baseline was recorded on a different machine.
//
// bench.yml is a reusable workflow called by test-and-publish.yml, whose
// `publish` job waits on it. The comparison therefore runs only when a version
// tag drives a release, never on every push, and a failure blocks the release.
//
// Regenerate the baseline with the CI toolchain and the exact CI command when a
// change intentionally moves allocations or the tripwire numbers:
//
//	GOTOOLCHAIN=go1.25.0 go test . -run '^$' -bench . -benchmem -cpu=4 -count=6 > testdata/bench/baseline.txt
//
// -cpu=4 pins GOMAXPROCS so the "-4" benchmark-name suffix matches the CI run;
// -ignore cpu on the benchstat side absorbs the machine description. Without
// both, benchstat silently emits no comparison at all.
//
// Deterministic, time-driven measurements (health tick, cron tick, restart
// backoff) do not live here because they cannot be expressed as -bench numbers
// without benchmarking time.Sleep; they are in bench_schedule_test.go, under
// testing/synctest.

// benchPayload is the request/reply payload used by the Request and typed
// benchmarks. Registering it with gob (once, below) lets the untyped Request
// path carry it through the interface-valued encode.
type benchPayload struct {
	Value int
}

var benchPayloadOnce sync.Once

func registerBenchPayload() {
	benchPayloadOnce.Do(func() { gob.Register(benchPayload{}) })
}

// ── Lifecycle ──

// BenchmarkLifecycleScaling measures one full start+stop cycle per service and
// reports the derived ns/svc metric, so "what does one more service cost" has a
// direct answer and a superlinear term is visible. The flat graph isolates the
// per-service overhead; the chain graph adds the dependency walk, isolating the
// topoSort level scan (O(V²) today, tracked on #23/#39).
func BenchmarkLifecycleScaling(b *testing.B) {
	for _, shape := range []string{"flat", "chain"} {
		for _, n := range []int{1, 10, 100, 1000} {
			b.Run(fmt.Sprintf("%s/n=%d", shape, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					o := New(WithHealthChecksDisabled())
					registerTopology(b, o, shape, n)
					if err := o.Start(); err != nil {
						b.Fatal(err)
					}
					if err := o.Stop(30 * time.Second); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/svc")
			})
		}
	}
}

// registerTopology registers n named services on o: independent (flat) or each
// depending on the previous one (chain). It fails the benchmark on error.
func registerTopology(b *testing.B, o *Orchestrator, shape string, n int) {
	b.Helper()
	prev := ""
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("svc-%04d", i)
		opts := []RegisterOption{WithName(name)}
		if shape == "chain" && prev != "" {
			opts = append(opts, DependsOn(prev))
		}
		if err := o.Register(&namedSvc{}, opts...); err != nil {
			b.Fatal(err)
		}
		prev = name
	}
}

// BenchmarkGoroutineBaseline is the primitive gorch replaces: N goroutines that
// park on a context and are torn down by cancel+WaitGroup. It gives the README a
// calibration point — the orchestration overhead over plain goroutines — rather
// than an unanchored absolute number.
func BenchmarkGoroutineBaseline(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				var wg sync.WaitGroup
				for j := 0; j < n; j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-ctx.Done()
					}()
				}
				cancel()
				wg.Wait()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/svc")
		})
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
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := o.topoSort(entries); err != nil {
			b.Fatal(err)
		}
	}
}

// ── Publish ──

// BenchmarkMessengerPublish is the publish matrix: subscribers ∈ {0,1,8,64} ×
// consumer ∈ {draining, stalled} × publisher ∈ {single, parallel} × delivery ∈
// {targeted, broadcast}. The 0 case is the early-exit path; stalled never reads,
// so the buffer fills and every Publish takes the documented drop branch;
// draining makes a consumer keep up; parallel is the RWMutex contention case;
// broadcast iterates every topic × owner. The subscriber buffer is the real
// default (16), not the b.N-sized buffer the old benchmark allocated.
func BenchmarkMessengerPublish(b *testing.B) {
	for _, broadcast := range []bool{false, true} {
		for _, subs := range []int{0, 1, 8, 64} {
			for _, consumer := range []string{"draining", "stalled"} {
				for _, parallel := range []bool{false, true} {
					name := fmt.Sprintf("delivery=%s/subs=%d/%s/publisher=%s",
						broadcastLabel(broadcast), subs, consumer, publisherLabel(parallel))
					b.Run(name, func(b *testing.B) {
						benchMessengerPublish(b, subs, consumer == "draining", broadcast, parallel)
					})
				}
			}
		}
	}
}

func broadcastLabel(broadcast bool) string {
	if broadcast {
		return "broadcast"
	}
	return "targeted"
}

func publisherLabel(parallel bool) string {
	if parallel {
		return "parallel"
	}
	return "single"
}

func benchMessengerPublish(b *testing.B, subs int, draining, broadcast, parallel bool) {
	b.Helper()
	b.ReportAllocs()

	m := newMessenger()
	chans := make([]<-chan any, 0, subs)
	unsubs := make([]func(), 0, subs)
	for i := 0; i < subs; i++ {
		ch, unsub := m.Subscribe("bench")
		chans = append(chans, ch)
		unsubs = append(unsubs, unsub)
	}

	var consumerWG sync.WaitGroup
	if draining {
		for _, ch := range chans {
			consumerWG.Add(1)
			go func(ch <-chan any) {
				defer consumerWG.Done()
				for range ch {
				}
			}(ch)
		}
	}

	publish := func(i int) {
		if broadcast {
			m.Publish(i)
			return
		}
		m.Publish(i, "bench")
	}

	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				publish(i)
				i++
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			publish(i)
		}
	}
	b.StopTimer()

	// Drain closes every subscriber channel, releasing the draining goroutines;
	// the unsubscribes are then no-ops.
	m.Drain()
	consumerWG.Wait()
	for _, unsub := range unsubs {
		unsub()
	}
}

// ── Request / reply ──

// benchEcho responds to every request on topic by publishing the same payload
// back to the request's ReplyTopic. It lives until m.Drain closes its channel.
func benchEcho(m *Messenger, topic string) {
	ch, _ := m.Subscribe(topic)
	go func() {
		for v := range ch {
			msg, ok := v.(Message)
			if !ok {
				continue
			}
			m.Publish(Message{Payload: msg.Payload, Topic: msg.Topic}, msg.ReplyTopic)
		}
	}()
}

// BenchmarkRequest measures the full untyped round trip against an echo
// responder: gob encode, reply-topic mint, subscribe, publish, and decode.
func BenchmarkRequest(b *testing.B) {
	registerBenchPayload()
	b.ReportAllocs()
	m := newMessenger()
	benchEcho(m, "bench")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.Request(ctx, benchPayload{Value: i}, "bench"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	m.Drain()
}

// BenchmarkRequestAsync measures the full async round trip: the same fixed
// per-call cost as Request plus the forwarding goroutine and the extra
// receive/send hop.
func BenchmarkRequestAsync(b *testing.B) {
	registerBenchPayload()
	b.ReportAllocs()
	m := newMessenger()
	benchEcho(m, "bench")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch, err := m.RequestAsync(ctx, benchPayload{Value: i}, "bench")
		if err != nil {
			b.Fatal(err)
		}
		<-ch
	}
	b.StopTimer()
	m.Drain()
}

// BenchmarkRequestEncode prices the gob encode half of the Request fixed cost,
// so the reporter can attribute the round trip to encode vs routing.
func BenchmarkRequestEncode(b *testing.B) {
	payload := benchPayload{Value: 42}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(&payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReplyTopicMint prices the reply-topic mint (crypto/rand read plus
// hex formatting) — the other fixed cost a Request pays before it routes.
func BenchmarkReplyTopicMint(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newUUID(rand.Reader)
	}
}

// BenchmarkSubscribeRouting prices the subscribe/unsubscribe of a reply topic
// under the root lock, isolating the routing cost from encode and mint.
func BenchmarkSubscribeRouting(b *testing.B) {
	b.ReportAllocs()
	m := newMessenger()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, unsub := m.Subscribe("reply")
		unsub()
	}
}

// ── Typed messaging ──

// BenchmarkTypedPublish prices the gob encode plus routing of TypedPublish,
// separated from the raw (non-serializing) Publish path.
func BenchmarkTypedPublish(b *testing.B) {
	b.ReportAllocs()
	m := newMessenger()
	if err := RegisterType[benchPayload](m); err != nil {
		b.Fatal(err)
	}
	ch, _ := TypedSubscribe[benchPayload](m, "bench")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TypedPublish(m, benchPayload{Value: i}, "bench")
	}
	b.StopTimer()
	m.Drain()
	<-done
}

// BenchmarkTypedRequest measures a typed round trip: encode request, route,
// decode request, encode response, route, decode response.
func BenchmarkTypedRequest(b *testing.B) {
	b.ReportAllocs()
	m := newMessenger()
	if err := RegisterType[benchPayload](m); err != nil {
		b.Fatal(err)
	}
	reqCh, _ := TypedSubscribeRequest[benchPayload](m, "bench")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for env := range reqCh {
			TypedRespond(m, benchPayload{Value: env.Value.Value + 1}, env.ReplyTopic)
		}
	}()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := TypedRequest[benchPayload, benchPayload](m, ctx, benchPayload{Value: i}, "bench"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	m.Drain()
	<-done
}

// ── Contended introspection ──

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
	b.ReportAllocs()
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
	b.ReportAllocs()
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
	b.ReportAllocs()
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

// ── Hook pipeline and construction ──

func benchNoopBefore(string) error { return nil }
func benchNoopAfter(string, error) {}

// BenchmarkStartStopHooks prices the hook machinery itself rather than a hook
// body, by running a one-service start+stop with zero hooks versus all four
// global and per-service hooks registered.
func BenchmarkStartStopHooks(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("hooks=%v", enabled), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				opts := []Option{WithHealthChecksDisabled()}
				ropts := []RegisterOption{WithName("svc")}
				if enabled {
					opts = append(opts,
						WithGlobalOnBeforeStart(benchNoopBefore),
						WithGlobalOnAfterStart(benchNoopAfter),
						WithGlobalOnBeforeStop(benchNoopBefore),
						WithGlobalOnAfterStop(benchNoopAfter),
					)
					ropts = append(ropts,
						WithOnBeforeStart(benchNoopBefore),
						WithOnAfterStart(benchNoopAfter),
						WithOnBeforeStop(benchNoopBefore),
						WithOnAfterStop(benchNoopAfter),
					)
				}
				o := New(opts...)
				if err := o.Register(&namedSvc{}, ropts...); err != nil {
					b.Fatal(err)
				}
				if err := o.Start(); err != nil {
					b.Fatal(err)
				}
				if err := o.Stop(5 * time.Second); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkNew prices constructing an orchestrator with many functional
// options, so option-application cost is visible next to construction itself.
func BenchmarkNew(b *testing.B) {
	opts := []Option{
		WithHealthChecks(time.Second, WithProbeTimeout(time.Second), WithFailureThreshold(5)),
		WithDefaultStartTimeout(time.Second),
		WithFailedStartTimeout(time.Minute),
		WithLogLevel(LogLevelWarn),
		WithGlobalOnBeforeStart(benchNoopBefore),
		WithGlobalOnAfterStart(benchNoopAfter),
		WithGlobalOnBeforeStop(benchNoopBefore),
		WithGlobalOnAfterStop(benchNoopAfter),
		WithOnStateChange(func(string, ServiceStatus, ServiceStatus) {}),
		WithOnCrash(func(string, error) {}),
		WithBeforeHealthCheck(func(string) error { return nil }),
		WithAfterHealthCheck(func(string, error) {}),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = New(opts...)
	}
}
