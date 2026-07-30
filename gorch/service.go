// Package gorch is a composable Go orchestrator for managing goroutine lifecycles.
// It handles start, stop, cron scheduling, and inter-service pub-sub messaging
// with self-healing restart. Orchestrators can be nested — a service may create
// and manage its own gorch instance for sub-services.
package gorch

import (
	"context"
	"crypto/rand"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"
)

// Service — every managed goroutine implements this.
type Service interface {
	Start(ctx context.Context) error // blocks; for cron: runs per-tick; for non-cron: runs until ctx cancelled
	Stop() error                     // cleanup signal beyond context cancellation
}

// ServiceContext — what the orchestrator hands each service.
// Embeds context.Context so it satisfies the context.Context interface and can be
// passed directly to Service.Start. Carries the orchestrator's cancellation context.
type ServiceContext struct {
	context.Context
	Logger    *ServiceLogger
	Messenger *Messenger
}

// Logger is the logging interface used by gorch. Services receive a *ServiceLogger
// which satisfies this interface. Inject a custom implementation via Config.Logger.
// The standard library's *slog.Logger satisfies this interface directly.
type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
	Warn(msg string, args ...any)
}

// ServiceLogger — a logger that doesn't log; it sends entries to gorch's log channel.
// gorch consumes the channel and does the actual output (formatting, writing to stderr).
// When a custom Logger is set via Config.Logger, ServiceLogger delegates to it
// instead of the channel, prepending "service"=<name> to the key-value pairs.
type ServiceLogger struct {
	svcName string
	ch      chan<- logEntry // used by default channel-based logging
	logger  Logger          // custom logger (bypasses channel)
}

// newServiceLogger creates a channel-based ServiceLogger (internal).
func newServiceLogger(svcName string, ch chan<- logEntry) *ServiceLogger {
	return &ServiceLogger{svcName: svcName, ch: ch}
}

// newServiceLoggerWith creates a ServiceLogger that delegates to a custom Logger.
func newServiceLoggerWith(svcName string, logger Logger) *ServiceLogger {
	return &ServiceLogger{svcName: svcName, logger: logger}
}

func (l *ServiceLogger) Info(msg string, args ...any)  { l.emit(LogLevelInfo, msg, args) }
func (l *ServiceLogger) Error(msg string, args ...any) { l.emit(LogLevelError, msg, args) }
func (l *ServiceLogger) Debug(msg string, args ...any) { l.emit(LogLevelDebug, msg, args) }
func (l *ServiceLogger) Warn(msg string, args ...any)  { l.emit(LogLevelWarn, msg, args) }

func (l *ServiceLogger) emit(level LogLevel, msg string, args []any) {
	if l.logger != nil {
		fullArgs := make([]any, 0, len(args)+2)
		fullArgs = append(fullArgs, "service", l.svcName)
		fullArgs = append(fullArgs, args...)
		switch level {
		case LogLevelInfo:
			l.logger.Info(msg, fullArgs...)
		case LogLevelError:
			l.logger.Error(msg, fullArgs...)
		case LogLevelDebug:
			l.logger.Debug(msg, fullArgs...)
		case LogLevelWarn:
			l.logger.Warn(msg, fullArgs...)
		}
		return
	}
	select {
	case l.ch <- logEntry{time: time.Now(), level: level, service: l.svcName, msg: msg, args: args}:
	default: // drop if channel full
	}
}

// CronMode — concurrency policy when cron fires while previous invocation still runs.
type CronMode int

const (
	CronParallel CronMode = iota // fire in new goroutine regardless
	CronQueue                    // serialize: wait for previous to finish
	CronSkip                     // drop this tick entirely
)

// registerConfig holds options set via RegisterOption functions.
type registerConfig struct {
	cronSpec string
	cronMode CronMode
	factory  func() Service // non-nil means self-heal is enabled

	// dependency ordering
	name      string
	dependsOn []string

	// timeouts
	startTimeout time.Duration

	// one-shot
	runOnce bool

	// backoff / retry for self-heal
	maxRetries int           // 0 = unlimited
	backoff    Backoff       // nil = default (1s constant)
	resetAfter time.Duration // 0 = never reset retry counter

	// lifecycle hooks (per-service overrides)
	onBeforeStart func(name string) error
	onAfterStart  func(name string, err error)
	onBeforeStop  func(name string) error
	onAfterStop   func(name string, err error)

	// group / labels for filtering
	group  string
	labels map[string]string

	// soft dependencies: start after if present, ignore if missing
	softDependsOn []string

	// per-service stop timeout
	stopTimeout time.Duration

	// startCondition: if set and returns false, service is skipped at startup
	startCondition func() bool
}

// RegisterOption — functional options for Register.
type RegisterOption func(*registerConfig)

// WithCron registers the service to run on a 6-field cron schedule (seconds included).
func WithCron(spec string, mode CronMode) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.cronSpec = spec
		cfg.cronMode = mode
	}
}

// WithSelfHeal enables auto-restart: when the service crashes (returns error or panics),
// the orchestrator calls factory() for a fresh instance and restarts it.
func WithSelfHeal(factory func() Service) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.factory = factory
	}
}

// WithName assigns a human-readable name used for dependency ordering,
// status queries, and lifecycle hooks. Names must be unique across all
// registered services.
func WithName(name string) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.name = name
	}
}

// DependsOn declares that this service must start after the named services
// and stop before them. Cycles are detected at registration time.
func DependsOn(names ...string) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.dependsOn = append(cfg.dependsOn, names...)
	}
}

// WithStartTimeout sets the maximum time to wait for this service's Start
// to return. Overrides Config.DefaultStartTimeout. A zero duration means no
// timeout (use with caution).
func WithStartTimeout(d time.Duration) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.startTimeout = d
	}
}

// WithRunOnce marks a service as a one-shot init task. It runs before
// persistent services, never receives Stop(), and transitions to
// StatusStopped when Start returns. If Start returns an error, startup aborts.
func WithRunOnce() RegisterOption {
	return func(cfg *registerConfig) {
		cfg.runOnce = true
	}
}

// WithMaxRetries sets the maximum number of self-heal restarts.
// 0 means unlimited (up to context cancellation). After the limit
// is reached, the service transitions to StatusStopped.
func WithMaxRetries(max int) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.maxRetries = max
	}
}

// WithBackoff sets the backoff strategy for self-heal restarts.
// If nil or not set, the default is 1s constant backoff.
func WithBackoff(b Backoff) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.backoff = b
	}
}

// WithResetAfter sets a stability window. If the service runs continuously
// for this duration without crashing, the retry counter resets to zero.
func WithResetAfter(d time.Duration) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.resetAfter = d
	}
}

// WithOnBeforeStart sets a per-service hook called just before Start().
// If the hook returns an error, Start() is aborted for this service.
func WithOnBeforeStart(fn func(name string) error) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.onBeforeStart = fn
	}
}

// WithOnAfterStart sets a per-service hook called after Start() returns.
func WithOnAfterStart(fn func(name string, err error)) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.onAfterStart = fn
	}
}

// WithOnBeforeStop sets a per-service hook called just before Stop().
// If the hook returns an error, Stop() is still called.
func WithOnBeforeStop(fn func(name string) error) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.onBeforeStop = fn
	}
}

// WithOnAfterStop sets a per-service hook called after Stop() returns.
func WithOnAfterStop(fn func(name string, err error)) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.onAfterStop = fn
	}
}

// WithGroup assigns the service to a named group for filtering.
func WithGroup(name string) RegisterOption {
	return func(cfg *registerConfig) { cfg.group = name }
}

// WithLabel attaches a key-value label to the service for filtering.
func WithLabel(key, value string) RegisterOption {
	return func(cfg *registerConfig) {
		if cfg.labels == nil {
			cfg.labels = make(map[string]string)
		}
		cfg.labels[key] = value
	}
}

// DependsOnSoft declares soft dependencies: start after the named services
// if they are present, but ignore any that are not registered.
func DependsOnSoft(names ...string) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.softDependsOn = append(cfg.softDependsOn, names...)
	}
}

// WithStopTimeout sets a per-service timeout on Stop(). If Stop() does not
// return within this duration, the orchestrator proceeds with shutdown.
func WithStopTimeout(d time.Duration) RegisterOption {
	return func(cfg *registerConfig) { cfg.stopTimeout = d }
}

// WithStartCondition sets a function called at startup. If it returns false,
// the service is skipped (not started). nil or not set means always start.
func WithStartCondition(fn func() bool) RegisterOption {
	return func(cfg *registerConfig) { cfg.startCondition = fn }
}

// Sentinel errors
var (
	ErrAlreadyStarted  = errors.New("gorch: orchestrator already started")
	ErrInvalidCron     = errors.New("gorch: invalid cron expression")
	ErrStopTimeout     = errors.New("gorch: stop timed out waiting for services")
	ErrDuplicateName   = errors.New("gorch: duplicate service name")
	ErrDependencyCycle = errors.New("gorch: dependency cycle detected")
	ErrStartAborted    = errors.New("gorch: start aborted due to dependency failure")
)

// funcService wraps closures as a Service. Used by RegisterFunc.
type funcService struct {
	startFn func(ctx context.Context) error
	stopFn  func() error
}

func (f *funcService) Start(ctx context.Context) error { return f.startFn(ctx) }
func (f *funcService) Stop() error {
	if f.stopFn != nil {
		return f.stopFn()
	}
	return nil
}

// Messenger — pub-sub with topics (Socket.IO rooms style).
// A nil or empty topics slice in Publish broadcasts to ALL subscribers.
type Messenger struct {
	mu    sync.RWMutex
	subs  map[string][]chan any
	types map[string]reflect.Type // registered typed-message types
}

func newMessenger() *Messenger {
	return &Messenger{subs: make(map[string][]chan any)}
}

// Subscribe registers interest in a topic. Returns a receive-only channel and an
// unsubscribe function. The channel is buffered (cap 16). Thread-safe.
func (m *Messenger) Subscribe(topic string) (<-chan any, func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan any, 16)
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

// SubscribeWithBuffer registers interest in a topic with a caller-specified
// buffer size. Returns a receive-only channel and an unsubscribe function.
// Thread-safe.
func (m *Messenger) SubscribeWithBuffer(topic string, bufSize int) (<-chan any, func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
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

// RequestAsync is like Request but returns immediately with a response
// channel. The caller must select on the channel and ctx.Done().
// Thread-safe.
func (m *Messenger) RequestAsync(ctx context.Context, msg any, topic string) (<-chan any, error) {
	// generate unique reply topic
	replyTopic := "_reply." + newUUID(rand.Reader)

	// subscribe before publishing to avoid race
	replyCh, unsub := m.Subscribe(replyTopic)

	// encode payload with gob
	var payload []byte
	if msg != nil {
		var buf gobBuf
		if err := gob.NewEncoder(&buf).Encode(&msg); err != nil {
			unsub()
			return nil, fmt.Errorf("gorch: failed to encode request: %w", err)
		}
		payload = buf.Bytes()
	}

	wrapper := Message{
		Payload:    payload,
		Topic:      topic,
		ReplyTopic: replyTopic,
	}

	m.Publish(wrapper, topic)

	// spawn cleanup goroutine that waits for context done, then unsubs
	go func() {
		<-ctx.Done()
		unsub()
	}()

	return replyCh, nil
}

// Drain closes all subscriber channels and clears all subscriptions.
// After Drain, the Messenger is empty and no new publishes will be
// received by prior subscribers. Thread-safe.
func (m *Messenger) Drain() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for topic, subs := range m.subs {
		for _, ch := range subs {
			select {
			case <-ch:
			default:
			}
			close(ch)
		}
		delete(m.subs, topic)
	}
}

// gobBuf is a simple bytes.Buffer wrapper for gob encoding.
// ponytail: stdlib bytes.Buffer already implements io.Writer; used directly.
type gobBuf struct {
	buf []byte
}

func (b *gobBuf) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *gobBuf) Bytes() []byte { return b.buf }

// newUUID generates a short random ID for reply topics.
// ponytail: crypto/rand hex, error path removed — rand.Read never fails on Linux.
func newUUID(r io.Reader) string {
	b := make([]byte, 8)
	io.ReadFull(r, b) // never fails with crypto/rand.Reader
	return fmt.Sprintf("%x", b)
}
