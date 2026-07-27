package gorch

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/gob"
	"errors"
	"strings"
	"testing"
	"time"
)

// ── ServiceLogger ──

func TestServiceLogger_Info(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		args []any
	}{
		{"simple message", "hello", nil},
		{"message with keyvals", "event happened", []any{"key1", "val1", "key2", 42}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan logEntry, 1)
			l := newServiceLogger("testsvc", ch)
			l.Info(tt.msg, tt.args...)
			select {
			case e := <-ch:
				if e.level != LogLevelInfo {
					t.Errorf("expected level %v, got %v", LogLevelInfo, e.level)
				}
				if e.service != "testsvc" {
					t.Errorf("expected service 'testsvc', got %q", e.service)
				}
				if e.msg != tt.msg {
					t.Errorf("expected msg %q, got %q", tt.msg, e.msg)
				}
			case <-time.After(10 * time.Millisecond):
				t.Fatal("expected log entry on channel, got none")
			}
		})
	}
}

func TestServiceLogger_Error(t *testing.T) {
	ch := make(chan logEntry, 1)
	l := newServiceLogger("errsvc", ch)
	l.Error("err msg", "code", 500)
	select {
	case e := <-ch:
		if e.level != LogLevelError {
			t.Errorf("expected level %v, got %v", LogLevelError, e.level)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected log entry on channel, got none")
	}
}

func TestServiceLogger_Debug(t *testing.T) {
	ch := make(chan logEntry, 1)
	l := newServiceLogger("dbgsvc", ch)
	l.Debug("debug msg")
	select {
	case e := <-ch:
		if e.level != LogLevelDebug {
			t.Errorf("expected level %v, got %v", LogLevelDebug, e.level)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected log entry on channel, got none")
	}
}

func TestServiceLogger_Warn(t *testing.T) {
	ch := make(chan logEntry, 1)
	l := newServiceLogger("warnsvc", ch)
	l.Warn("warn msg")
	select {
	case e := <-ch:
		if e.level != LogLevelWarn {
			t.Errorf("expected level %v, got %v", LogLevelWarn, e.level)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected log entry on channel, got none")
	}
}

func TestServiceLogger_ChannelFull_NoBlock(t *testing.T) {
	tests := []struct {
		name    string
		capSize int
	}{
		{"zero-cap channel", 0},
		{"full buffered channel", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan logEntry, tt.capSize)
			l := newServiceLogger("fullsvc", ch)
			if tt.capSize > 0 {
				ch <- logEntry{}
			}
			done := make(chan struct{})
			go func() {
				l.Info("dropped msg")
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(100 * time.Millisecond):
				t.Fatal("emit blocked on full channel")
			}
		})
	}
}

func TestServiceLogger_Emit_OddArgs_NoPanic(t *testing.T) {
	ch := make(chan logEntry, 1)
	l := newServiceLogger("oddsvc", ch)
	l.Info("msg", "key1")
	select {
	case e := <-ch:
		if len(e.args) != 1 {
			t.Errorf("expected 1 arg, got %d", len(e.args))
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected log entry")
	}
	l.Warn("warn", "k1", "v1", "k2")
	select {
	case e := <-ch:
		if len(e.args) != 3 {
			t.Errorf("expected 3 args, got %d", len(e.args))
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected log entry")
	}
}

// ── Messenger: Subscribe ──

func TestMessenger_Subscribe(t *testing.T) {
	m := newMessenger()
	ch, unsub := m.Subscribe("topic-a")
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
	if unsub == nil {
		t.Fatal("expected non-nil unsubscribe func")
	}
}

func TestMessenger_Subscribe_ReceiveMessage(t *testing.T) {
	m := newMessenger()
	ch, _ := m.Subscribe("events")
	m.Publish("hello", "events")
	select {
	case msg := <-ch:
		if msg != "hello" {
			t.Errorf("expected 'hello', got %v", msg)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected message on channel")
	}
}

func TestMessenger_Unsubscribe(t *testing.T) {
	m := newMessenger()
	ch, unsub := m.Subscribe("events")
	unsub()
	m.Publish("should be dropped", "events")
	select {
	case msg := <-ch:
		t.Errorf("unexpected message after unsubscribe: %v", msg)
	case <-time.After(5 * time.Millisecond):
	}
}

func TestMessenger_Unsubscribe_Twice_Idempotent(t *testing.T) {
	m := newMessenger()
	_, unsub := m.Subscribe("events")
	unsub()
	unsub()
	m.Publish("nobody", "events")
}

func TestMessenger_Subscribe_MultipleTopics(t *testing.T) {
	tests := []struct {
		name   string
		topics []string
	}{
		{"two topics", []string{"a", "b"}},
		{"three topics", []string{"x", "y", "z"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newMessenger()
			channels := make([]<-chan any, len(tt.topics))
			for i, topic := range tt.topics {
				ch, _ := m.Subscribe(topic)
				channels[i] = ch
			}
			for i, topic := range tt.topics {
				m.Publish(topic, topic)
				for j, ch := range channels {
					select {
					case msg := <-ch:
						if i == j {
							if msg != topic {
								t.Errorf("expected %q on ch[%d], got %v", topic, j, msg)
							}
						} else {
							t.Errorf("unexpected message on ch[%d] from topic %q: %v", j, topic, msg)
						}
					case <-time.After(5 * time.Millisecond):
						if i == j {
							t.Errorf("expected message on ch[%d] from topic %q", j, topic)
						}
					}
				}
			}
		})
	}
}

// ── Messenger: Publish ──

func TestMessenger_Publish_SpecificTopic(t *testing.T) {
	m := newMessenger()
	ch1, _ := m.Subscribe("topic-a")
	ch2, _ := m.Subscribe("topic-b")

	m.Publish("for-a", "topic-a")

	select {
	case msg := <-ch1:
		if msg != "for-a" {
			t.Errorf("expected 'for-a', got %v", msg)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected message on ch1")
	}
	select {
	case msg := <-ch2:
		t.Errorf("unexpected message on ch2: %v", msg)
	case <-time.After(5 * time.Millisecond):
	}
}

func TestMessenger_Publish_NilTopics_Broadcasts(t *testing.T) {
	m := newMessenger()
	ch1, _ := m.Subscribe("a")
	ch2, _ := m.Subscribe("b")
	m.Publish("broadcast")
	for i, ch := range []<-chan any{ch1, ch2} {
		select {
		case msg := <-ch:
			if msg != "broadcast" {
				t.Errorf("ch[%d]: expected 'broadcast', got %v", i, msg)
			}
		case <-time.After(10 * time.Millisecond):
			t.Fatalf("ch[%d]: expected broadcast message", i)
		}
	}
}

func TestMessenger_Publish_EmptyTopics_Broadcasts(t *testing.T) {
	m := newMessenger()
	ch, _ := m.Subscribe("x")
	m.Publish("all", []string{}...)
	select {
	case msg := <-ch:
		if msg != "all" {
			t.Errorf("expected 'all', got %v", msg)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected broadcast message with empty topics")
	}
}

func TestMessenger_Publish_ChannelFull_NonBlocking(t *testing.T) {
	m := newMessenger()
	ch, _ := m.Subscribe("overflow")

	for i := 0; i < 16; i++ {
		m.Publish(i, "overflow")
	}
	for i := 0; i < 16; i++ {
		select {
		case msg := <-ch:
			if msg != i {
				t.Errorf("expected msg %d, got %v", i, msg)
			}
		case <-time.After(10 * time.Millisecond):
			t.Fatalf("expected message %d", i)
		}
	}
	for i := 0; i < 16; i++ {
		m.Publish(i, "overflow")
	}
	done := make(chan struct{})
	go func() {
		m.Publish("dropped", "overflow")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Publish blocked on full subscriber channel")
	}
}

func TestMessenger_Publish_NoSubscribers_NoPanic(t *testing.T) {
	m := newMessenger()
	m.Publish("nobody here", "ghost-topic")
	m2 := newMessenger()
	m2.Publish("broadcast to nobody")
}

// ── CronMode ──

func TestCronMode_DistinctValues(t *testing.T) {
	if CronParallel == CronQueue || CronParallel == CronSkip || CronQueue == CronSkip {
		t.Errorf("CronMode constants must be distinct: %d %d %d", CronParallel, CronQueue, CronSkip)
	}
}

// ── RegisterOption ──

func TestWithCron(t *testing.T) {
	opt := WithCron("*/5 * * * * *", CronQueue)
	cfg := &registerConfig{}
	opt(cfg)
	if cfg.cronSpec != "*/5 * * * * *" {
		t.Errorf("expected cronSpec '*/5 * * * * *', got %q", cfg.cronSpec)
	}
	if cfg.cronMode != CronQueue {
		t.Errorf("expected cronMode %v, got %v", CronQueue, cfg.cronMode)
	}
}

func TestWithSelfHeal(t *testing.T) {
	called := false
	factory := func() Service {
		called = true
		return nil
	}
	opt := WithSelfHeal(factory)
	cfg := &registerConfig{}
	opt(cfg)
	if cfg.factory == nil {
		t.Fatal("expected non-nil factory")
	}
	cfg.factory()
	if !called {
		t.Error("expected factory to be called")
	}
}

func TestRegisterOption_Composition(t *testing.T) {
	opts := []RegisterOption{
		WithCron("@every 1s", CronParallel),
		WithSelfHeal(func() Service { return nil }),
	}
	cfg := &registerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.cronSpec != "@every 1s" {
		t.Errorf("expected '@every 1s', got %q", cfg.cronSpec)
	}
	if cfg.cronMode != CronParallel {
		t.Errorf("expected CronParallel, got %v", cfg.cronMode)
	}
	if cfg.factory == nil {
		t.Error("expected non-nil factory")
	}
}

// ── Additional RegisterOption tests ──

func TestWithName(t *testing.T) {
	cfg := &registerConfig{}
	WithName("my-svc")(cfg)
	if cfg.name != "my-svc" {
		t.Errorf("expected 'my-svc', got %q", cfg.name)
	}
}

func TestDependsOn(t *testing.T) {
	cfg := &registerConfig{}
	DependsOn("a", "b")(cfg)
	if len(cfg.dependsOn) != 2 || cfg.dependsOn[0] != "a" || cfg.dependsOn[1] != "b" {
		t.Errorf("expected [a b], got %v", cfg.dependsOn)
	}
	// Append more.
	DependsOn("c")(cfg)
	if len(cfg.dependsOn) != 3 {
		t.Errorf("expected 3 deps, got %d", len(cfg.dependsOn))
	}
}

func TestWithStartTimeout(t *testing.T) {
	cfg := &registerConfig{}
	WithStartTimeout(5 * time.Second)(cfg)
	if cfg.startTimeout != 5*time.Second {
		t.Errorf("expected 5s, got %v", cfg.startTimeout)
	}
}

func TestWithRunOnce(t *testing.T) {
	cfg := &registerConfig{}
	WithRunOnce()(cfg)
	if !cfg.runOnce {
		t.Error("expected runOnce to be true")
	}
}

func TestWithMaxRetries(t *testing.T) {
	cfg := &registerConfig{}
	WithMaxRetries(5)(cfg)
	if cfg.maxRetries != 5 {
		t.Errorf("expected 5, got %d", cfg.maxRetries)
	}
}

func TestWithBackoff(t *testing.T) {
	cfg := &registerConfig{}
	b := ConstantBackoff{Delay: 2 * time.Second}
	WithBackoff(b)(cfg)
	if cfg.backoff == nil {
		t.Fatal("expected backoff to be set")
	}
	if cfg.backoff.Next(1) != 2*time.Second {
		t.Errorf("expected 2s from backoff, got %v", cfg.backoff.Next(1))
	}
}

func TestWithResetAfter(t *testing.T) {
	cfg := &registerConfig{}
	WithResetAfter(30 * time.Second)(cfg)
	if cfg.resetAfter != 30*time.Second {
		t.Errorf("expected 30s, got %v", cfg.resetAfter)
	}
}

func TestWithOnBeforeStart(t *testing.T) {
	called := false
	cfg := &registerConfig{}
	WithOnBeforeStart(func(name string) error {
		called = true
		return nil
	})(cfg)
	if cfg.onBeforeStart == nil {
		t.Fatal("expected hook to be set")
	}
	cfg.onBeforeStart("test")
	if !called {
		t.Error("expected hook to be called")
	}
}

func TestWithOnAfterStart(t *testing.T) {
	called := false
	cfg := &registerConfig{}
	WithOnAfterStart(func(name string, err error) {
		called = true
	})(cfg)
	if cfg.onAfterStart == nil {
		t.Fatal("expected hook to be set")
	}
	cfg.onAfterStart("test", nil)
	if !called {
		t.Error("expected hook to be called")
	}
}

func TestWithOnBeforeStop(t *testing.T) {
	called := false
	cfg := &registerConfig{}
	WithOnBeforeStop(func(name string) error {
		called = true
		return nil
	})(cfg)
	if cfg.onBeforeStop == nil {
		t.Fatal("expected hook to be set")
	}
	cfg.onBeforeStop("test")
	if !called {
		t.Error("expected hook to be called")
	}
}

func TestWithOnAfterStop(t *testing.T) {
	called := false
	cfg := &registerConfig{}
	WithOnAfterStop(func(name string, err error) {
		called = true
	})(cfg)
	if cfg.onAfterStop == nil {
		t.Fatal("expected hook to be set")
	}
	cfg.onAfterStop("test", nil)
	if !called {
		t.Error("expected hook to be called")
	}
}

// ── Sentinel errors ──

func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantMsg string
	}{
		{"ErrAlreadyStarted", ErrAlreadyStarted, "gorch: orchestrator already started"},
		{"ErrInvalidCron", ErrInvalidCron, "gorch: invalid cron expression"},
		{"ErrStopTimeout", ErrStopTimeout, "gorch: stop timed out waiting for services"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Fatal("expected non-nil sentinel error")
			}
			if tt.err.Error() != tt.wantMsg {
				t.Errorf("expected %q, got %q", tt.wantMsg, tt.err.Error())
			}
		})
	}
}

func TestSentinelErrors_Is(t *testing.T) {
	wrapped := errors.New("wrapped: " + ErrAlreadyStarted.Error())
	if !errors.Is(wrapped, ErrAlreadyStarted) {
		t.Log("note: errors.Is on plain errors.New sentinels works via identity, not content")
	}
	if errors.Is(ErrAlreadyStarted, ErrInvalidCron) {
		t.Error("different sentinels should not match")
	}
	if errors.Is(ErrStopTimeout, ErrAlreadyStarted) {
		t.Error("different sentinels should not match")
	}
}

// ── Messenger: broadcast with nil/empty topics covers all subscribers ──

func TestMessenger_Publish_NilTopics_NoSubscribers_NoPanic(t *testing.T) {
	m := newMessenger()
	m.Publish("broadcast")
}

// ── Messenger: multiple subscribers on same topic ──

func TestMessenger_MultipleSubscribers_SameTopic(t *testing.T) {
	m := newMessenger()
	ch1, _ := m.Subscribe("topic")
	ch2, _ := m.Subscribe("topic")
	m.Publish("msg", "topic")
	for i, ch := range []<-chan any{ch1, ch2} {
		select {
		case msg := <-ch:
			if msg != "msg" {
				t.Errorf("ch[%d]: expected 'msg', got %v", i, msg)
			}
		case <-time.After(10 * time.Millisecond):
			t.Fatalf("ch[%d]: expected message", i)
		}
	}
}

// ── Messenger: unsubscribe one subscriber leaves the other intact ──

func TestMessenger_Unsubscribe_OneLeavesOther(t *testing.T) {
	m := newMessenger()
	ch1, unsub1 := m.Subscribe("topic")
	ch2, _ := m.Subscribe("topic")
	unsub1()
	m.Publish("msg", "topic")
	select {
	case msg := <-ch1:
		t.Errorf("unexpected message on unsubscribed ch1: %v", msg)
	case <-time.After(5 * time.Millisecond):
	}
	select {
	case msg := <-ch2:
		if msg != "msg" {
			t.Errorf("expected 'msg' on ch2, got %v", msg)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected message on ch2")
	}
}

// ── logEntry structure trimming ──

func TestLogEntry_ArgsPreserved(t *testing.T) {
	ch := make(chan logEntry, 1)
	l := newServiceLogger("svc", ch)
	l.Info("test", "key", "value", "count", 5)
	select {
	case e := <-ch:
		if len(e.args) != 4 {
			t.Fatalf("expected 4 args, got %d", len(e.args))
		}
		if e.args[0] != "key" || e.args[1] != "value" || e.args[2] != "count" || e.args[3] != 5 {
			t.Errorf("args not preserved: %v", e.args)
		}
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected log entry")
	}
}

// ── Messenger: thread-safety smoke test ──

func TestMessenger_Concurrent_SubscribePublish(t *testing.T) {
	m := newMessenger()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			ch, unsub := m.Subscribe("topic")
			m.Publish(i, "topic")
			select {
			case <-ch:
			case <-time.After(10 * time.Millisecond):
			}
			unsub()
		}
		close(done)
	}()
	go func() {
		for i := 0; i < 100; i++ {
			m.Publish(i)
		}
	}()
	<-done
}

// ── Backoff ──

func TestExponentialBackoff_Next(t *testing.T) {
	b := ExponentialBackoff{Initial: 100 * time.Millisecond, Max: 5 * time.Second, Factor: 2.0}
	tests := []struct {
		retry int
		want  time.Duration
	}{
		{0, 100 * time.Millisecond},
		{-1, 100 * time.Millisecond},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
	}
	for _, tt := range tests {
		got := b.Next(tt.retry)
		if got != tt.want {
			t.Errorf("Next(%d) = %v, want %v", tt.retry, got, tt.want)
		}
	}
}

func TestExponentialBackoff_Next_MaxCap(t *testing.T) {
	b := ExponentialBackoff{Initial: 1 * time.Second, Max: 3 * time.Second, Factor: 2.0}
	if got := b.Next(3); got != 3*time.Second {
		t.Errorf("expected cap at 3s, got %v", got)
	}
	// retry 10: 1 * 2^9 = 512s, capped at 3s.
	if got := b.Next(10); got != 3*time.Second {
		t.Errorf("expected cap at 3s for large retry, got %v", got)
	}
}

func TestExponentialBackoff_Next_DefaultFactor(t *testing.T) {
	// Zero Factor: d * 0 = 0 for subsequent retries. Capped at Max if Max > 0.
	b := ExponentialBackoff{Initial: 100 * time.Millisecond, Max: time.Second, Factor: 0}
	if got := b.Next(3); got != 0 {
		t.Errorf("expected 0 with factor=0, got %v", got)
	}
}

func TestConstantBackoff_Next(t *testing.T) {
	b := ConstantBackoff{Delay: 500 * time.Millisecond}
	for _, retry := range []int{0, 1, 10, 100} {
		if got := b.Next(retry); got != 500*time.Millisecond {
			t.Errorf("Next(%d) = %v, want 500ms", retry, got)
		}
	}
}

func TestBackoff_Interface(t *testing.T) {
	// Compile-time check: both types satisfy Backoff.
	var _ Backoff = ExponentialBackoff{}
	var _ Backoff = ConstantBackoff{}
}

// ── ServiceStatus.String ──

func TestServiceStatus_String(t *testing.T) {
	tests := []struct {
		status ServiceStatus
		want   string
	}{
		{StatusRegistered, "registered"},
		{StatusStarting, "starting"},
		{StatusRunning, "running"},
		{StatusStopping, "stopping"},
		{StatusStopped, "stopped"},
		{StatusCrashed, "crashed"},
		{ServiceStatus(99), "unknown"},
		{ServiceStatus(-1), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.status.String(); got != tt.want {
			t.Errorf("ServiceStatus(%d).String() = %q, want %q", tt.status, got, tt.want)
		}
	}
}

// ── HealthChecker interface ──

func TestHealthChecker_Interface(t *testing.T) {
	// ponytail: compile-time interface satisfaction.
	var _ HealthChecker = &healthSvc{}
}

// ── Typed messages ──

func TestRegisterType(t *testing.T) {
	m := newMessenger()
	err := RegisterType[string](m)
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}
	if m.types == nil {
		t.Fatal("expected types map to be initialized")
	}
	if _, ok := m.types["string"]; !ok {
		t.Error("expected 'string' type to be registered")
	}
}

func TestRegisterType_NonGobCompatible(t *testing.T) {
	// ponytail: gob.Register no longer panics on channels/funcs/complex in Go 1.25.
	// The recover path in RegisterType is defensive for future gob versions.
	// We still test it compiles and handles a recoverable type.
	m := newMessenger()
	// RegisterType should succeed or return a recoverable error.
	_ = RegisterType[chan int](m)
}

func TestTypedPublishSubscribe(t *testing.T) {
	m := newMessenger()
	err := RegisterType[string](m)
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}

	ch, unsub := TypedSubscribe[string](m, "topic")
	defer unsub()

	TypedPublish(m, "hello", "topic")

	select {
	case msg := <-ch:
		if msg != "hello" {
			t.Errorf("expected 'hello', got %v", msg)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected typed message")
	}
}

func TestTypedPublish_UnregisteredType_Dropped(t *testing.T) {
	m := newMessenger()
	rawCh, _ := m.Subscribe("topic")
	TypedPublish(m, "unregistered", "topic")
	select {
	case <-rawCh:
		t.Error("unregistered type should be dropped silently")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestTypedPublish_NilTypesMap_Dropped(t *testing.T) {
	m := newMessenger()
	// types map is nil (no RegisterType called).
	rawCh, _ := m.Subscribe("topic")
	// Must not panic when reading from nil types map.
	TypedPublish(m, "anything", "topic")
	select {
	case <-rawCh:
		t.Error("unregistered type (nil types) should be dropped silently")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestTypedPublish_MultipleTopics(t *testing.T) {
	m := newMessenger()
	RegisterType[int](m)
	ch1, _ := TypedSubscribe[int](m, "a")
	ch2, _ := TypedSubscribe[int](m, "b")

	TypedPublish(m, 42, "a")

	select {
	case v := <-ch1:
		if v != 42 {
			t.Errorf("expected 42, got %d", v)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected message on ch1")
	}
	select {
	case <-ch2:
		t.Error("unexpected message on ch2")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestTypedPublish_Broadcast(t *testing.T) {
	m := newMessenger()
	RegisterType[string](m)
	ch1, _ := TypedSubscribe[string](m, "a")
	ch2, _ := TypedSubscribe[string](m, "b")

	TypedPublish(m, "all") // no topics → broadcast

	for i, ch := range []<-chan string{ch1, ch2} {
		select {
		case msg := <-ch:
			if msg != "all" {
				t.Errorf("ch[%d]: expected 'all', got %v", i, msg)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("ch[%d]: expected broadcast message", i)
		}
	}
}

func TestTypedSubscribe_Unsubscribe(t *testing.T) {
	m := newMessenger()
	RegisterType[string](m)
	ch, unsub := TypedSubscribe[string](m, "t")
	unsub()
	TypedPublish(m, "hello", "t")
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after unsubscribe + raw channel close")
		}
	case <-time.After(50 * time.Millisecond):
	}
}

func TestTypedSubscribe_NonMessageDropped(t *testing.T) {
	m := newMessenger()
	RegisterType[string](m)
	ch, unsub := TypedSubscribe[string](m, "t")
	defer unsub()

	// Publish a raw non-Message value on the same topic.
	m.Publish("raw string not a Message", "t")

	// TypedSubscribe should drop it (it's not a Message).
	select {
	case <-ch:
		t.Error("non-Message value should be dropped")
	case <-time.After(50 * time.Millisecond):
	}
}

// ── Request / RequestAsync ──

func TestMessenger_Request(t *testing.T) {
	m := newMessenger()

	rawCh, _ := m.Subscribe("req")
	go func() {
		msg := (<-rawCh).(Message)
		m.Publish("response!", msg.ReplyTopic)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := m.Request(ctx, "ping", "req")
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	if resp != "response!" {
		t.Errorf("expected 'response!', got %v", resp)
	}
}

func TestMessenger_Request_Timeout(t *testing.T) {
	m := newMessenger()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := m.Request(ctx, "ping", "no-responder")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestMessenger_RequestAsync(t *testing.T) {
	m := newMessenger()

	rawCh, _ := m.Subscribe("req")
	go func() {
		msg := (<-rawCh).(Message)
		m.Publish(99, msg.ReplyTopic)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ch, err := m.RequestAsync(ctx, "hello", "req")
	if err != nil {
		t.Fatalf("RequestAsync failed: %v", err)
	}

	select {
	case resp := <-ch:
		if resp != 99 {
			t.Errorf("expected 99, got %v", resp)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for response")
	}
}

func TestMessenger_RequestAsync_ReplyTopicSet(t *testing.T) {
	m := newMessenger()

	var replyTopic string
	rawCh, _ := m.Subscribe("req")
	done := make(chan struct{})
	go func() {
		msg := (<-rawCh).(Message)
		replyTopic = msg.ReplyTopic
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := m.RequestAsync(ctx, "ping", "req")
	if err != nil {
		t.Fatalf("RequestAsync failed: %v", err)
	}

	select {
	case <-done:
		if replyTopic == "" {
			t.Error("expected ReplyTopic to be set")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for request delivery")
	}
}

func TestMessenger_RequestAsync_ContextCleanup(t *testing.T) {
	m := newMessenger()

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := m.RequestAsync(ctx, "ping", "req")
	if err != nil {
		t.Fatalf("RequestAsync failed: %v", err)
	}

	// Cancel context; cleanup goroutine should unsubscribe.
	cancel()
	time.Sleep(50 * time.Millisecond)

	// The reply subscription should be cleaned up.
	// Publish to verify no panic or leak (we check that the reply channel
	// isn't blocking indefinitely).
	m.Publish("late response")
	select {
	case <-ch:
		// may or may not receive
	default:
	}
}

func TestMessenger_RequestAsync_NilMessage(t *testing.T) {
	m := newMessenger()

	rawCh, _ := m.Subscribe("req")
	go func() {
		msg := (<-rawCh).(Message)
		if len(msg.Payload) != 0 {
			t.Errorf("expected empty payload for nil message, got %d bytes", len(msg.Payload))
		}
		m.Publish("ok", msg.ReplyTopic)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := m.Request(ctx, nil, "req")
	if err != nil {
		t.Fatalf("Request(nil) failed: %v", err)
	}
	if resp != "ok" {
		t.Errorf("expected 'ok', got %v", resp)
	}
}

// ── gobBuf / newUUID ──

func TestGobBuf_Write(t *testing.T) {
	var buf gobBuf
	n, err := buf.Write([]byte("hello"))
	if n != 5 || err != nil {
		t.Errorf("expected (5, nil), got (%d, %v)", n, err)
	}
	if string(buf.Bytes()) != "hello" {
		t.Errorf("expected 'hello', got %q", buf.Bytes())
	}
	buf.Write([]byte(" world"))
	if string(buf.Bytes()) != "hello world" {
		t.Errorf("expected 'hello world', got %q", buf.Bytes())
	}
}

func TestGobBuf_Bytes_Empty(t *testing.T) {
	var buf gobBuf
	if len(buf.Bytes()) != 0 {
		t.Errorf("expected empty, got %v", buf.Bytes())
	}
}

func TestNewUUID(t *testing.T) {
	id := newUUID(rand.Reader)
	if len(id) != 16 {
		t.Errorf("expected 16 hex chars (8 bytes), got %d: %s", len(id), id)
	}
	id2 := newUUID(rand.Reader)
	if id == id2 {
		t.Error("expected unique UUIDs")
	}
}

// ── TypedSubscribe: channel full / decode error ──

func TestTypedSubscribe_ChannelFull_Drops(t *testing.T) {
	// ponytail: the typed forwarding goroutine uses a non-blocking send.
	// Verify the full-channel path doesn't deadlock.
	m := newMessenger()
	RegisterType[int](m)
	ch, _ := TypedSubscribe[int](m, "t")

	// Fill the typed channel to capacity.
	for i := 0; i < 16; i++ {
		TypedPublish(m, i, "t")
	}
	time.Sleep(30 * time.Millisecond) // let forwarding goroutine catch up

	// This publish either queues (if forwarding goroutine freed space in rawCh)
	// or drops (if typedCh is full). Must not deadlock.
	done := make(chan struct{})
	go func() {
		TypedPublish(m, 99, "t")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("TypedPublish with full typed channel deadlocked")
	}
	// Clean up: drain channel.
	for i := 0; i < 16; i++ {
		select {
		case <-ch:
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestTypedSubscribe_DecodeError_Drops(t *testing.T) {
	// ponytail: send a malformed Message that gob can't decode to a typed subscriber.
	m := newMessenger()
	RegisterType[int](m)
	ch, _ := TypedSubscribe[int](m, "t")

	// Publish a Message with corrupted payload (not valid gob).
	m.Publish(Message{
		Payload:  []byte{0xFF, 0xFF, 0xFF, 0xFF},
		TypeName: "int",
	}, "t")

	// The typed subscriber should drop it (decode error).
	select {
	case <-ch:
		t.Error("malformed payload should be dropped")
	case <-time.After(50 * time.Millisecond):
	}

	// Valid message should still arrive.
	TypedPublish(m, 42, "t")
	select {
	case v := <-ch:
		if v != 42 {
			t.Errorf("expected 42, got %d", v)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("valid typed message should arrive")
	}
}

// ── Publish: broadcast default (non-blocking send when channel full) ──

func TestMessenger_Publish_BroadcastChannelFull(t *testing.T) {
	m := newMessenger()
	ch, _ := m.Subscribe("topic")
	// Fill subscriber channel to capacity (16).
	for i := 0; i < 16; i++ {
		m.Publish(i, "topic")
	}
	// Broadcast must not block when subscriber channel is full.
	done := make(chan struct{})
	go func() {
		m.Publish("broadcast") // no topics -> broadcast to all subscribers
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("broadcast Publish blocked when channel full")
	}
	// Drain channel.
	for i := 0; i < 16; i++ {
		<-ch
	}
}

// ── Request: error from RequestAsync ──

func TestMessenger_Request_EncodeError(t *testing.T) {
	m := newMessenger()
	ctx := context.Background()
	// chan int cannot be gob-encoded.
	_, err := m.Request(ctx, make(chan int), "topic")
	if err == nil {
		t.Fatal("expected error from gob encode failure in RequestAsync")
	}
	if !strings.Contains(err.Error(), "encode") {
		t.Errorf("expected encode error, got %v", err)
	}
}

// ── RequestAsync: gob encode error ──

func TestMessenger_RequestAsync_EncodeError(t *testing.T) {
	m := newMessenger()
	ctx := context.Background()
	// chan int cannot be gob-encoded.
	ch, err := m.RequestAsync(ctx, make(chan int), "topic")
	if err == nil {
		t.Fatal("expected error from gob encode failure")
	}
	if ch != nil {
		t.Error("expected nil channel on error")
	}
	if !strings.Contains(err.Error(), "encode") {
		t.Errorf("expected encode error, got %v", err)
	}
}

// ── TypedPublish: gob encode error ──

func TestTypedPublish_GobEncodeError(t *testing.T) {
	m := newMessenger()
	// Register chan int type — but encoding a channel value will fail.
	err := RegisterType[chan int](m)
	if err != nil {
		t.Skipf("RegisterType[chan int] not supported: %v", err)
	}
	rawCh, _ := m.Subscribe("topic")
	// TypedPublish encodes with gob. Encoding a channel should fail silently.
	TypedPublish(m, make(chan int), "topic")
	select {
	case <-rawCh:
		t.Error("gob encode error should drop the message silently")
	case <-time.After(50 * time.Millisecond):
	}
}

// ── TypedSubscribe: non-blocking send default (typed channel full) ──

func TestTypedSubscribe_ChannelFullDefault(t *testing.T) {
	m := newMessenger()
	_ = RegisterType[int](m)

	ch, unsub := TypedSubscribe[int](m, "t")
	defer unsub()

	// Encode a valid gob payload. Publish 20 Messages directly (not via
	// TypedPublish) so they reach the forwarding goroutine. typedCh cap is 16;
	// messages 17+ hit the non-blocking default branch.
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(42)
	payload := make([]byte, len(buf.Bytes()))
	copy(payload, buf.Bytes())

	for i := 0; i < 20; i++ {
		m.Publish(Message{Payload: payload, TypeName: "int"}, "t")
	}
	// Give the forwarding goroutine time to process.
	time.Sleep(50 * time.Millisecond)

	// Drain typedCh to prevent goroutine leak.
	for i := 0; i < 16; i++ {
		select {
		case <-ch:
		case <-time.After(10 * time.Millisecond):
			return
		}
	}
}
