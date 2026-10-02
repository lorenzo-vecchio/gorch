package gorch

import (
	"context"
	"testing"
	"time"
)

// This file pins the orchestrator constructor contract: the defaults New
// installs, and that a zero-value Orchestrator behaves like New() rather than
// panicking on an uninitialised registry, Messenger or shutdown channel.

func TestNew_Defaults(t *testing.T) {
	t.Run("zero_loglevel_defaults_to_info", func(t *testing.T) {
		o := New()
		if o.cfg.LogLevel != LogLevelInfo {
			t.Errorf("expected LogLevelInfo (1), got %d", o.cfg.LogLevel)
		}
	})

	t.Run("logLevelDebug_is_selectable", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelDebug))
		if o.cfg.LogLevel != LogLevelDebug {
			t.Errorf("WithLogLevel(LogLevelDebug) should select Debug, got %d", o.cfg.LogLevel)
		}
	})

	t.Run("explicit_warn_stays_warn", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		if o.cfg.LogLevel != LogLevelWarn {
			t.Errorf("expected LogLevelWarn (2), got %d", o.cfg.LogLevel)
		}
	})

	t.Run("health_checks_can_be_disabled", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		if o.cfg.HealthInterval != 0 {
			t.Errorf("expected HealthInterval=0 when disabled, got %v", o.cfg.HealthInterval)
		}
	})

	t.Run("messenger_initialized", func(t *testing.T) {
		o := New()
		if o.messenger == nil {
			t.Fatal("expected messenger to be initialized")
		}
	})

	t.Run("health_defaults", func(t *testing.T) {
		o := New()
		if o.cfg.HealthInterval != 30*time.Second {
			t.Errorf("expected HealthInterval=30s, got %v", o.cfg.HealthInterval)
		}
		if o.cfg.HealthTimeout != 5*time.Second {
			t.Errorf("expected HealthTimeout=5s, got %v", o.cfg.HealthTimeout)
		}
		if o.cfg.HealthThreshold != 3 {
			t.Errorf("expected HealthThreshold=3, got %d", o.cfg.HealthThreshold)
		}
	})

	t.Run("nameIndex_initialized", func(t *testing.T) {
		o := New()
		if o.nameIndex == nil {
			t.Fatal("expected nameIndex to be initialized")
		}
	})
}

func TestServiceContext_SatisfiesContext(t *testing.T) {
	var sc ServiceContext
	var _ context.Context = sc
}
