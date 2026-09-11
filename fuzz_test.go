package gorch

import "testing"

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
