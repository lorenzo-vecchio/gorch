package gorch

import (
	"sync"
	"testing"
	"time"
)

// This file pins Messenger integration with the orchestrator: a service can
// subscribe and publish, Stop closes subscriber channels, and subscribing after
// Stop does not panic.

// TestStop_ClosesMessengerSubscriberChannels verifies that a subscriber blocked
// on receive observes a channel close when the orchestrator stops.
func TestStop_ClosesMessengerSubscriberChannels(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Start()
	ch, _ := o.messenger.Subscribe("topic")
	_ = o.Stop(time.Second)

	_, ok := <-ch
	if ok {
		t.Error("expected subscriber channel to be closed on Stop")
	}
}

// TestStop_SubscribeAfterStopDoesNotPanic verifies the messenger re-initializes
// its subscription map lazily so a late Subscribe does not panic.
func TestStop_SubscribeAfterStopDoesNotPanic(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	_ = o.Start()
	_ = o.Stop(time.Second)

	ch, _ := o.messenger.Subscribe("topic")
	o.messenger.Publish("x", "topic")
	select {
	case <-ch:
	default:
	}
}

func TestMessenger_SubscribePublish(t *testing.T) {
	t.Run("subscribe_and_receive", func(t *testing.T) {
		m := newMessenger()
		ch, unsub := m.Subscribe("topic1")
		defer unsub()
		m.Publish("hello", "topic1")
		select {
		case msg := <-ch:
			if msg != "hello" {
				t.Errorf("expected 'hello', got %v", msg)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected to receive message")
		}
	})

	t.Run("publish_no_topics_broadcasts_to_all", func(t *testing.T) {
		m := newMessenger()
		ch1, _ := m.Subscribe("a")
		ch2, _ := m.Subscribe("b")
		m.Publish("broadcast")
		for _, ch := range []<-chan any{ch1, ch2} {
			select {
			case msg := <-ch:
				if msg != "broadcast" {
					t.Errorf("expected 'broadcast', got %v", msg)
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatal("expected broadcast to reach subscriber")
			}
		}
	})

	t.Run("publish_nil_topics_broadcasts_to_all", func(t *testing.T) {
		m := newMessenger()
		ch, _ := m.Subscribe("x")
		m.Publish("nil broadcast")
		select {
		case msg := <-ch:
			if msg != "nil broadcast" {
				t.Errorf("expected 'nil broadcast', got %v", msg)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected broadcast to reach subscriber")
		}
	})

	t.Run("publish_non_blocking_when_full", func(t *testing.T) {
		m := newMessenger()
		ch, _ := m.Subscribe("t")
		for i := 0; i < 16; i++ {
			m.Publish(i, "t")
		}
		done := make(chan struct{})
		go func() {
			m.Publish("overflow", "t")
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Publish blocked when channel was full")
		}
		for i := 0; i < 16; i++ {
			<-ch
		}
	})

	t.Run("subscribe_unsubscribe_not_found_path", func(t *testing.T) {
		m := newMessenger()
		_, unsub := m.Subscribe("t")
		m.mu.Lock()
		m.subs = nil
		m.mu.Unlock()
		unsub()
	})

	t.Run("unsubscribe_stops_receiving", func(t *testing.T) {
		m := newMessenger()
		ch, unsub := m.Subscribe("t")
		unsub()
		m.Publish("after unsub", "t")
		select {
		case <-ch:
			t.Error("received message after unsubscribe")
		default:
		}
	})

	t.Run("publish_after_messenger_cleanup_no_panic", func(t *testing.T) {
		m := newMessenger()
		_, _ = m.Subscribe("t")
		m.mu.Lock()
		m.subs = nil
		m.mu.Unlock()
		m.Publish("msg", "t")
		m.Publish("broadcast")
	})

	t.Run("unsubscribe_during_publish_no_deadlock", func(t *testing.T) {
		m := newMessenger()
		var wg sync.WaitGroup

		unsubs := make([]func(), 100)
		for i := 0; i < 100; i++ {
			_, unsub := m.Subscribe("t")
			unsubs[i] = unsub
		}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					m.Publish("x", "t")
				}
			}()
		}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				unsubs[idx]()
			}(i)
		}
		wg.Wait()
	})
}
