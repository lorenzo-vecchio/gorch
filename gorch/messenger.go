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
)

type Messenger struct {
	mu    sync.RWMutex
	subs  map[string][]chan any
	types map[string]reflect.Type // registered typed-message types
}

func newMessenger() *Messenger {
	// Register the Message envelope so it can be gob-encoded as an interface
	// value (e.g. passed through Request's untyped `any` payload).
	gob.Register(Message{})
	return &Messenger{subs: make(map[string][]chan any)}
}

// Subscribe registers interest in a topic. Returns a receive-only channel and an
// unsubscribe function. The channel is buffered (cap 16). Thread-safe.
func (m *Messenger) Subscribe(topic string) (<-chan any, func()) {
	return m.SubscribeWithBuffer(topic, 16)
}

// SubscribeWithBuffer registers interest in a topic with a caller-specified
// buffer size. Returns a receive-only channel and an unsubscribe function.
// Safe to call after Drain (subscriptions are lazily re-initialized).
// Thread-safe.
func (m *Messenger) SubscribeWithBuffer(topic string, bufSize int) (<-chan any, func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.subs == nil {
		m.subs = make(map[string][]chan any)
	}
	ch := make(chan any, bufSize)
	m.subs[topic] = append(m.subs[topic], ch)
	unsubscribe := sync.OnceValue(func() struct{} {
		m.mu.Lock()
		defer m.mu.Unlock()
		subs := m.subs[topic]
		for i, c := range subs {
			if c == ch {
				m.subs[topic] = append(subs[:i], subs[i+1:]...)
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
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(topics) == 0 {
		for _, subs := range m.subs {
			for _, ch := range subs {
				select {
				case ch <- msg:
				default:
				}
			}
		}
		return
	}
	for _, topic := range topics {
		for _, ch := range m.subs[topic] {
			select {
			case ch <- msg:
			default:
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
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, subs := range m.subs {
		for _, ch := range subs {
			close(ch)
		}
	}
	m.subs = nil
}

// newUUID generates a short random ID for reply topics.
// ponytail: crypto/rand hex, error path removed — rand.Read never fails on Linux.
func newUUID(r io.Reader) string {
	b := make([]byte, 8)
	io.ReadFull(r, b) // never fails with crypto/rand.Reader
	return fmt.Sprintf("%x", b)
}
