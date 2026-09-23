package gorch

import (
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

// FuzzMembershipTransitions drives a random add/start/stop/remove/crash/restart
// sequence against a small dependency graph, asserting no panic or deadlock.
// The per-iteration orchestrator is always stopped, so it also guards against
// goroutine leaks.
func FuzzMembershipTransitions(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5})
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
		_ = o.Start()

		for _, b := range data {
			switch b % 6 {
			case 0:
				_ = o.StartService("a")
			case 1:
				_ = o.StopService("a", 50*time.Millisecond)
			case 2:
				_ = o.StopService("b", 50*time.Millisecond, WithCascadeStop())
			case 3:
				_ = o.Unregister("c", 50*time.Millisecond)
			case 4:
				_ = o.Register(&namedSvc{}, WithName("late"))
			case 5:
				_ = o.StartService("c")
			}
		}
		_ = o.Stop(2 * time.Second)
	})
}
