package gorch

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"reflect"
)

// Message is the envelope for typed pub-sub and request-reply messaging.
// Publishers encode their payload into Payload; subscribers decode it.
// ReplyTopic is set automatically by Request/RequestAsync so responders
// know where to send the reply.
type Message struct {
	Payload    []byte
	Topic      string
	ReplyTopic string
	TypeName   string
}

// RegisterType registers T with encoding/gob so it can be used with
// TypedPublish and TypedSubscribe. Must be called before any typed
// operations for the type. Recovers from gob panics and returns an
// error if the type is not gob-compatible. Thread-safe.
// ponytail: standalone func (not method) because Go does not support
// generic methods on non-generic types.
func RegisterType[T any](m *Messenger) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var zero T
	// ponytail: gob.Register no longer panics in Go 1.25+; recover removed.
	gob.Register(zero)
	if m.types == nil {
		m.types = make(map[string]reflect.Type)
	}
	t := reflect.TypeOf(zero)
	name := t.String()
	m.types[name] = t
	return nil
}

// TypedPublish gob-encodes msg and publishes it as a Message to the
// given topics. Silently drops the message if the type has not been
// registered via RegisterType. Thread-safe.
func TypedPublish[T any](m *Messenger, msg T, topics ...string) {
	t := reflect.TypeOf(msg)
	name := t.String()

	m.mu.RLock()
	_, ok := m.types[name]
	m.mu.RUnlock()
	if !ok {
		return
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&msg); err != nil {
		return
	}

	wrapper := Message{
		Payload:  buf.Bytes(),
		TypeName: name,
	}

	var topicList []string
	if len(topics) > 0 {
		topicList = topics
	}
	m.Publish(wrapper, topicList...)
}

// TypedSubscribe subscribes to topic and returns a typed receive-only
// channel and an unsubscribe function. Messages published via TypedPublish
// are gob-decoded into T before delivery. Non-Message values and
// unrecognized types are silently dropped. Thread-safe.
func TypedSubscribe[T any](m *Messenger, topic string) (<-chan T, func()) {
	rawCh, unsub := m.Subscribe(topic)
	typedCh := make(chan T, 16)

	go func() {
		defer close(typedCh)
		for val := range rawCh {
			msg, ok := val.(Message)
			if !ok {
				continue
			}
			var result T
			if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&result); err != nil {
				continue
			}
			select {
			case typedCh <- result:
			default:
			}
		}
	}()

	return typedCh, unsub
}

// TypedRequest sends a typed request and waits for a typed response.
// It gob-encodes the request, publishes it via Request, and gob-decodes
// the response. Returns the decoded response or an error.
func TypedRequest[TReq, TResp any](m *Messenger, ctx context.Context, req TReq, topic string) (TResp, error) {
	var zero TResp
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&req); err != nil {
		return zero, fmt.Errorf("gorch: failed to encode request: %w", err)
	}
	wrapper := Message{Payload: buf.Bytes(), Topic: topic, TypeName: reflect.TypeOf(req).String()}
	raw, err := m.Request(ctx, wrapper, topic)
	if err != nil {
		return zero, err
	}
	respMsg, ok := raw.(Message)
	if !ok {
		return zero, fmt.Errorf("gorch: expected Message response, got %T", raw)
	}
	var result TResp
	if err := gob.NewDecoder(bytes.NewReader(respMsg.Payload)).Decode(&result); err != nil {
		return zero, fmt.Errorf("gorch: failed to decode response: %w", err)
	}
	return result, nil
}
