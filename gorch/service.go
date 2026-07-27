// Package gorch is a composable Go orchestrator for managing goroutine lifecycles.
// It handles start, stop, cron scheduling, and inter-service pub-sub messaging
// with self-healing restart. Orchestrators can be nested — a service may create
// and manage its own gorch instance for sub-services.
package gorch

import (
	"context"
	"errors"
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

// ServiceLogger — a logger that doesn't log; it sends entries to gorch's log channel.
// gorch consumes the channel and does the actual output (formatting, writing to stderr).
type ServiceLogger struct {
	svcName string
	ch      chan<- logEntry // send-only, shared across all services
}

// newServiceLogger creates a ServiceLogger (internal, called by orchestrator).
func newServiceLogger(svcName string, ch chan<- logEntry) *ServiceLogger {
	return &ServiceLogger{svcName: svcName, ch: ch}
}

func (l *ServiceLogger) Info(msg string, args ...any)  { l.emit(LogLevelInfo, msg, args) }
func (l *ServiceLogger) Error(msg string, args ...any) { l.emit(LogLevelError, msg, args) }
func (l *ServiceLogger) Debug(msg string, args ...any) { l.emit(LogLevelDebug, msg, args) }
func (l *ServiceLogger) Warn(msg string, args ...any)  { l.emit(LogLevelWarn, msg, args) }

func (l *ServiceLogger) emit(level LogLevel, msg string, args []any) {
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

// Sentinel errors
var (
	ErrAlreadyStarted = errors.New("gorch: orchestrator already started")
	ErrInvalidCron    = errors.New("gorch: invalid cron expression")
	ErrStopTimeout    = errors.New("gorch: stop timed out waiting for services")
)

// Messenger — pub-sub with topics (Socket.IO rooms style).
// A nil or empty topics slice in Publish broadcasts to ALL subscribers.
type Messenger struct {
	mu   sync.RWMutex
	subs map[string][]chan any
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

// Publish sends msg to subscribers. Non-blocking: if a subscriber's channel is full,
// the message is dropped for that subscriber. Thread-safe.
// If topics is empty or nil, broadcasts to ALL subscribers on every topic.
func (m *Messenger) Publish(msg any, topics ...string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(topics) == 0 {
		// broadcast to all
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
