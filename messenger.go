package gorch

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
)

// Messenger is a pub-sub registry. A Messenger obtained from a ServiceContext
// is a scoped view: subscriptions it creates are tagged with the owning
// service's id so they can be released individually via drainOwner, while
// Publish and Drain keep their orchestrator-wide semantics.
type Messenger struct {
	mu    sync.RWMutex
	subs  map[string]map[uint64][]chan any
	types map[string]reflect.Type // registered typed-message types

	// base points at the root registry shared by scoped views. It is nil for a
	// root Messenger (including the zero value); root() resolves it.
	base  *Messenger
	owner uint64 // ownership id tagged on subscriptions from this view
}

func newMessenger() *Messenger {
	// Register the Message envelope so it can be gob-encoded as an interface
	// value (e.g. passed through Request's untyped `any` payload).
	gob.Register(Message{})
	return &Messenger{subs: make(map[string]map[uint64][]chan any)}
}

// root returns the root registry this view shares, or the receiver itself for a
// root Messenger.
func (m *Messenger) root() *Messenger {
	if m.base != nil {
		return m.base
	}
	return m
}

// scoped returns a view that tags every subscription it creates with owner. It
// shares the root registry, so Publish, Drain and typed registration stay
// global across views.
func (m *Messenger) scoped(owner uint64) *Messenger {
	return &Messenger{base: m.root(), owner: owner}
}

// Subscribe registers interest in a topic. Returns a receive-only channel and an
// unsubscribe function. The channel is buffered (cap 16). Thread-safe.
func (m *Messenger) Subscribe(topic string) (<-chan any, func()) {
	return m.subscribeFor(m.owner, topic, 16)
}

// SubscribeWithBuffer registers interest in a topic with a caller-specified
// buffer size. Returns a receive-only channel and an unsubscribe function.
// Safe to call after Drain (subscriptions are lazily re-initialized).
// Thread-safe.
func (m *Messenger) SubscribeWithBuffer(topic string, bufSize int) (<-chan any, func()) {
	return m.subscribeFor(m.owner, topic, bufSize)
}

// subscribeFor registers interest in topic under owner. Returns a receive-only
// channel and an unsubscribe function. Thread-safe, and safe to call after
// Drain or drainOwner (the registry is lazily re-initialized).
func (m *Messenger) subscribeFor(owner uint64, topic string, bufSize int) (<-chan any, func()) {
	r := m.root()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.subs == nil {
		r.subs = make(map[string]map[uint64][]chan any)
	}
	byOwner := r.subs[topic]
	if byOwner == nil {
		byOwner = make(map[uint64][]chan any)
		r.subs[topic] = byOwner
	}
	ch := make(chan any, bufSize)
	byOwner[owner] = append(byOwner[owner], ch)
	unsubscribe := sync.OnceValue(func() struct{} {
		r.mu.Lock()
		defer r.mu.Unlock()
		// r.subs can be nil after Drain, and the topic/owner can be absent after
		// drainOwner; both lookups below then yield an empty slice.
		subs := r.subs[topic][owner]
		for i, existing := range subs {
			if existing == ch {
				remaining := append(subs[:i], subs[i+1:]...)
				if len(remaining) == 0 {
					delete(r.subs[topic], owner)
					if len(r.subs[topic]) == 0 {
						delete(r.subs, topic)
					}
				} else {
					r.subs[topic][owner] = remaining
				}
				return struct{}{}
			}
		}
		return struct{}{}
	})
	return ch, func() { unsubscribe() }
}

// Publish sends msg to subscribers. Non-blocking: if a subscriber's channel is full,
// the message is dropped for that subscriber. Thread-safe.
// If topics is empty or nil, broadcasts to ALL subscribers on every topic.
func (m *Messenger) Publish(msg any, topics ...string) {
	r := m.root()
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(topics) == 0 {
		for _, byOwner := range r.subs {
			for _, subs := range byOwner {
				for _, ch := range subs {
					select {
					case ch <- msg:
					default:
					}
				}
			}
		}
		return
	}
	for _, topic := range topics {
		for _, subs := range r.subs[topic] {
			for _, ch := range subs {
				select {
				case ch <- msg:
				default:
				}
			}
		}
	}
}

// Request publishes a request message and waits for a single reply.
// It creates a temporary reply topic, subscribes to it, publishes the
// request, and returns the first response (or an error if ctx expires).
// The responding service receives a Message on its channel; it should
// Publish the response on msg.ReplyTopic.
// Thread-safe.
func (m *Messenger) Request(ctx context.Context, msg any, topic string) (any, error) {
	ch, err := m.RequestAsync(ctx, msg, topic)
	if err != nil {
		return nil, err
	}
	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// requestMessage publishes an already-built request Message and returns the
// reply channel plus an unsubscribe func. It assigns a unique ReplyTopic and
// subscribes before publishing to avoid a reply race. Thread-safe.
func (m *Messenger) requestMessage(wrapper Message, topic string) (<-chan any, func()) {
	replyTopic := "_reply." + newUUID(rand.Reader)
	wrapper.ReplyTopic = replyTopic
	replyCh, unsub := m.Subscribe(replyTopic)
	m.Publish(wrapper, topic)
	return replyCh, unsub
}

// RequestAsync is like Request but returns immediately with a response
// channel. The caller must select on the channel and ctx.Done().
// The returned channel is delivered to exactly once on reply, and the
// forwarding goroutine exits on either a reply or context cancellation.
// Thread-safe.
func (m *Messenger) RequestAsync(ctx context.Context, msg any, topic string) (<-chan any, error) {
	// encode payload with gob
	var payload []byte
	if msg != nil {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(&msg); err != nil {
			return nil, fmt.Errorf("gorch: failed to encode request: %w", err)
		}
		payload = buf.Bytes()
	}

	wrapper := Message{
		Payload: payload,
		Topic:   topic,
	}

	replyCh, unsub := m.requestMessage(wrapper, topic)

	out := make(chan any, 1)
	go func() {
		defer unsub()
		select {
		case resp := <-replyCh:
			out <- resp
		case <-ctx.Done():
		}
	}()

	return out, nil
}

// Drain closes all subscriber channels and clears all subscriptions. Buffered
// messages are delivered to receivers before they observe the close. After
// Drain, the Messenger is empty, Publish is a no-op, and a subsequent
// Subscribe re-initializes the subscription map. Thread-safe.
func (m *Messenger) Drain() {
	r := m.root()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, byOwner := range r.subs {
		for _, subs := range byOwner {
			closeSubs(subs)
		}
	}
	r.subs = nil
}

// drainOwner closes and removes every subscription tagged with owner, leaving
// all other owners (and the global Drain contract) untouched. Repeated calls
// are a no-op, and the owner's registry entry is deleted entirely so it retains
// no memory. Thread-safe.
func (m *Messenger) drainOwner(owner uint64) {
	r := m.root()
	r.mu.Lock()
	defer r.mu.Unlock()
	for topic, byOwner := range r.subs {
		subs, ok := byOwner[owner]
		if !ok {
			continue
		}
		closeSubs(subs)
		delete(byOwner, owner)
		if len(byOwner) == 0 {
			delete(r.subs, topic)
		}
	}
}

// closeSubs is the single close path shared by Drain and drainOwner so a
// channel can never be closed twice: both callers hold the root lock and
// remove a closed slice from the registry before releasing it.
func closeSubs(subs []chan any) {
	for _, ch := range subs {
		close(ch)
	}
}

// newUUID generates a short random ID for reply topics.
// ponytail: crypto/rand hex. It does not fail in practice, but if the reader
// errors we fall back to a process-unique counter so the reply topic stays
// unique instead of panicking or returning an empty topic.
func newUUID(r io.Reader) string {
	b := make([]byte, 8)
	if _, err := io.ReadFull(r, b); err != nil {
		return fmt.Sprintf("fallback-%d", fallbackUUIDSeq.Add(1))
	}
	return fmt.Sprintf("%x", b)
}

var fallbackUUIDSeq atomic.Uint64
