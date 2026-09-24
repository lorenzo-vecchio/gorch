package gorch

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// fuzzPayload is the decode target for FuzzTypedSubscribeDecode. It only needs
// to be a gob-decodable struct; the point is the byte path, not the type.
type fuzzPayload struct {
	A int
	B string
}

// FuzzTypedSubscribeDecode feeds arbitrary bytes through the Messenger's typed
// decode path (TypedSubscribe), the one surface that consumes bytes published
// by arbitrary services. It asserts the decoder never panics on malformed input.
func FuzzTypedSubscribeDecode(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte{})
	f.Add([]byte{0x0e, 0x00, 0x01, 0xff})
	f.Fuzz(func(t *testing.T, payload []byte) {
		m := newMessenger()
		ch, _ := TypedSubscribe[fuzzPayload](m, "fuzz")
		m.Publish(Message{Payload: payload, TypeName: "fuzz"}, "fuzz")
		m.Drain() // close raw channel so the decode goroutine exits
		for range ch {
		}
	})
}

// fuzzCronSvc blocks in a cron tick until cancelled and counts the ticks
// currently in flight, so the fuzz can assert that none survives a stop.
type fuzzCronSvc struct {
	running atomic.Int32
}

func (s *fuzzCronSvc) Start(ctx ServiceContext) error {
	s.running.Add(1)
	<-ctx.Done()
	s.running.Add(-1)
	return ctx.Err()
}

func (s *fuzzCronSvc) Stop() error { return nil }

// fuzzStubbornSvc ignores context cancellation and blocks in Start until
// released, while Stop() returns immediately. It is the service that exposes a
// dishonest status: a stop that times out with the instance goroutine still live
// must never be reported stopped.
type fuzzStubbornSvc struct {
	release chan struct{}
	running atomic.Int32
}

func (s *fuzzStubbornSvc) Start(ctx ServiceContext) error {
	s.running.Add(1)
	<-s.release
	s.running.Add(-1)
	return nil
}

func (s *fuzzStubbornSvc) Stop() error { return nil }

// FuzzMembershipTransitions drives a random add/start/stop/remove/crash/restart
// sequence against a small dependency graph. Besides asserting no panic or
// deadlock and no goroutine leak, it checks semantic properties grep-style
// invariants miss: a successful stop leaves an honest status, never counts one
// stop twice, and no cron tick survives a teardown.
func FuzzMembershipTransitions(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{5, 5, 4, 3, 2, 1, 0, 2, 2, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		o := New(WithHealthChecksDisabled())
		_ = o.Register(&namedSvc{}, WithName("a"))
		_ = o.Register(&namedSvc{}, WithName("b"), DependsOn("a"))
		_ = o.Register(&crashSignalSvc{sig: make(chan struct{})}, WithName("c"),
			DependsOn("b"),
			WithSelfHeal(func() Service { return &crashSignalSvc{sig: make(chan struct{})} }),
			WithBackoff(ConstantBackoff{Delay: 200 * time.Millisecond}),
		)
		cronSvc := &fuzzCronSvc{}
		_ = o.Register(cronSvc, WithName("cron"), WithCron("* * * * * *", CronParallel))
		_ = o.Start()

		// assertNoReservationLeak pins the reservation lifecycle: once every
		// membership op of an iteration has returned, no entry may still be
		// flagged starting. A leaked reservation is what would leave an entry
		// stuck in StatusStarting and reject every later membership op.
		assertNoReservationLeak := func() {
			o.mu.RLock()
			entries := make([]*serviceEntry, len(o.entries))
			copy(entries, o.entries)
			o.mu.RUnlock()
			for _, e := range entries {
				if e.starting.Load() {
					t.Fatalf("entry %s left a start reservation in flight", e.name)
				}
			}
		}

		// stopChecked runs a stop op and, when it succeeds, asserts the public
		// status is honest and the Stops metric moved by at most maxDelta, so a
		// teardown/done race cannot double-count one stop.
		stopChecked := func(name string, maxDelta int64, stop func() error) {
			before := o.Metrics().Stops
			if err := stop(); err != nil {
				return
			}
			if delta := o.Metrics().Stops - before; delta > maxDelta {
				t.Fatalf("stop %s counted %d stops, want <= %d", name, delta, maxDelta)
			}
			if s, ok := o.Status(name); ok && (s == StatusRunning || s == StatusStarting) {
				t.Fatalf("stop %s returned nil but status is %v", name, s)
			}
		}

		const maxLate = 8
		late := 0
		for _, b := range data {
			switch b % 9 {
			case 0:
				_ = o.StartService("a")
			case 1:
				stopChecked("a", 1, func() error { return o.StopService("a", 50*time.Millisecond) })
			case 2:
				stopChecked("b", 3, func() error { return o.StopService("b", 50*time.Millisecond, WithCascadeStop()) })
			case 3:
				before := o.Metrics().Stops
				if err := o.Unregister("c", 50*time.Millisecond); err == nil {
					if _, ok := o.Status("c"); ok {
						t.Fatal("Unregister returned nil but c is still registered")
					}
					// c self-heals, so a background restart may account a stop in
					// the same window; a systematic double count would add more.
					if o.Metrics().Stops-before > 2 {
						t.Fatal("Unregister double-counted a stop")
					}
				}
			case 4:
				// Hot-add a fresh dependent of a and try to start it. The cap
				// keeps a large input from exploding the registry, and unique
				// names keep each registration accepted.
				if late < maxLate {
					late++
					name := fmt.Sprintf("late-%d", late)
					_ = o.Register(&namedSvc{}, WithName(name), DependsOn("a"))
					_ = o.StartService(name)
				}
			case 5:
				_ = o.StartService("c")
			case 6:
				// c self-heals, so allow one background restart's accounting.
				stopChecked("c", 2, func() error { return o.StopService("c", 50*time.Millisecond) })
			case 7:
				stopChecked("b", 1, func() error { return o.StopService("b", 50*time.Millisecond) })
			case 8:
				// Fire a cron tick directly (the scheduler's second cadence is too
				// slow for a fuzz iteration), tear the entry down, and require the
				// in-flight tick to be gone: no tick may survive the stop.
				o.mu.RLock()
				e := o.nameIndex["cron"]
				o.mu.RUnlock()
				if e == nil {
					break
				}
				go o.invokeCron(e, e.cronGeneration())
				time.Sleep(2 * time.Millisecond)
				if err := o.StopService("cron", 50*time.Millisecond); err == nil {
					if v := cronSvc.running.Load(); v != 0 {
						t.Fatalf("cron tick survived StopService: %d still running", v)
					}
					if s, _ := o.Status("cron"); s != StatusStopped {
						t.Fatalf("cron status after stop = %v, want StatusStopped", s)
					}
				}
				_ = o.StartService("cron")
			}
			assertNoReservationLeak()
		}
		_ = o.Stop(2 * time.Second)
		assertNoReservationLeak()
		for name, s := range o.Statuses() {
			if s == StatusStarting {
				t.Fatalf("entry %s still StatusStarting after shutdown", name)
			}
		}
		select {
		case <-o.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("goroutines did not wind down after Stop")
		}
	})
}

// FuzzStopTimeoutStatus drives a stop against a service that ignores context
// cancellation, so the stop times out with the instance goroutine still live. It
// asserts the semantic obligation the deadline path must never break: while the
// instance is live, the entry is never reported StatusStopped. The first input
// byte picks a bounded timeout and the second picks whole-orchestrator Stop
// versus StopService.
func FuzzStopTimeoutStatus(f *testing.F) {
	f.Add([]byte{1})
	f.Add([]byte{50, 0})
	f.Add([]byte{200, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		timeout := time.Duration(int(data[0])%200+1) * time.Millisecond
		whole := len(data) > 1 && data[1]%2 == 1

		release := make(chan struct{})
		svc := &fuzzStubbornSvc{release: release}
		o := New(WithHealthChecksDisabled())
		_ = o.Register(svc, WithName("s"))
		_ = o.Start()

		// Wait until the instance goroutine is actually live before stopping.
		live := time.After(time.Second)
		for svc.running.Load() == 0 {
			select {
			case <-live:
				t.Fatal("service did not start")
			case <-time.After(time.Millisecond):
			}
		}

		if whole {
			_ = o.Stop(timeout)
		} else {
			_ = o.StopService("s", timeout)
		}
		if s, ok := o.Status("s"); ok && s == StatusStopped && svc.running.Load() != 0 {
			t.Fatalf("entry reported Stopped while its instance goroutine is still live")
		}

		// Release the instance and let everything wind down.
		close(release)
		_ = o.Stop(2 * time.Second)
		select {
		case <-o.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("goroutines did not wind down after Stop")
		}
	})
}
