package gorch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// This file pins the logging surface: LogLevel.String, the default channel log
// pump (including buffered drain on quit and level filtering), ServiceLogger's
// emit path, the service name attached to each line, and that logging after
// cancellation never panics.

func TestLogLevel_String(t *testing.T) {
	tests := []struct {
		level LogLevel
		want  string
	}{
		{LogLevelDebug, "DEBUG"},
		{LogLevelInfo, "INFO"},
		{LogLevelWarn, "WARN"},
		{LogLevelError, "ERROR"},
		{LogLevel(99), "????"},
		{LogLevel(-1), "????"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.level.String(); got != tt.want {
				t.Errorf("LogLevel(%d).String() = %q, want %q", tt.level, got, tt.want)
			}
		})
	}
}

// TestStop_ServiceLogsAfterCancel_NoPanic is a regression test for the
// "send on closed channel" crash: a service that keeps logging after ctx
// cancellation while Stop() runs must not panic.
func TestStop_ServiceLogsAfterCancel_NoPanic(t *testing.T) {
	o := New(WithLogLevel(LogLevelError))
	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			sc := ctx.(ServiceContext)
			<-ctx.Done()
			for i := 0; i < 1000; i++ {
				sc.Logger.Error("post-cancel log", "i", i)
			}
			return ctx.Err()
		},
	}
	_ = o.Register(svc)
	_ = o.Start()
	time.Sleep(20 * time.Millisecond)
	if err := o.Stop(time.Second); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

// TestStop_Timeout_LogPumpExits verifies that when a service ignores ctx and
// Stop() times out, the log-pump still exits and nothing panics.
func TestStop_Timeout_LogPumpExits(t *testing.T) {
	o := New(WithLogLevel(LogLevelError))
	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			sc := ctx.(ServiceContext)
			for i := 0; i < 5; i++ {
				sc.Logger.Error("still running", "i", i)
				time.Sleep(time.Millisecond)
			}
			never := make(chan struct{})
			<-never
			return nil
		},
	}
	_ = o.Register(svc)
	_ = o.Start()
	time.Sleep(20 * time.Millisecond)
	err := o.Stop(50 * time.Millisecond)
	if !errors.Is(err, ErrStopTimeout) {
		t.Fatalf("expected ErrStopTimeout, got %v", err)
	}
}

func TestLogPump(t *testing.T) {
	t.Run("format_includes_timestamp_level_service_msg_args", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(WithLogLevel(LogLevelDebug))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		go o.logPump(os.Stderr, o.logCh, o.logQuit, o.logPumpDone)

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 123456789, time.UTC),
			level:   LogLevelInfo,
			service: "*testSvc",
			msg:     "hello world",
			args:    []any{"k1", "v1", "k2", 42},
		}
		close(o.logQuit)
		<-o.logPumpDone

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		checks := []string{
			"2026-01-02 15:04:05.123",
			"INFO",
			"*testSvc",
			"hello world",
			"k1=v1",
			"k2=42",
		}
		for _, c := range checks {
			if !strings.Contains(output, c) {
				t.Errorf("output missing %q:\n%s", c, output)
			}
		}
	})

	t.Run("odd_args_single_append_missing", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(WithLogLevel(LogLevelDebug))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		go o.logPump(os.Stderr, o.logCh, o.logQuit, o.logPumpDone)

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level:   LogLevelError,
			service: "svc",
			msg:     "odd",
			args:    []any{"lonely"},
		}
		close(o.logQuit)
		<-o.logPumpDone

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "lonely=(missing)") {
			t.Errorf("expected 'lonely=(missing)', got: %s", output)
		}
	})

	t.Run("odd_args_three_elements", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(WithLogLevel(LogLevelDebug))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		go o.logPump(os.Stderr, o.logCh, o.logQuit, o.logPumpDone)

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level:   LogLevelError,
			service: "svc",
			msg:     "odd3",
			args:    []any{"a", 1, "b"},
		}
		close(o.logQuit)
		<-o.logPumpDone

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "a=1") {
			t.Errorf("expected 'a=1', got: %s", output)
		}
		if !strings.Contains(output, "b=(missing)") {
			t.Errorf("expected 'b=(missing)', got: %s", output)
		}
	})

	t.Run("empty_args", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(WithLogLevel(LogLevelDebug))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		go o.logPump(os.Stderr, o.logCh, o.logQuit, o.logPumpDone)

		o.logCh <- logEntry{
			time:    time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level:   LogLevelInfo,
			service: "svc",
			msg:     "no args",
			args:    nil,
		}
		close(o.logQuit)
		<-o.logPumpDone

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "no args") {
			t.Errorf("expected 'no args', got: %s", output)
		}
	})

	t.Run("filter_below_loglevel", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(WithLogLevel(LogLevelWarn))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 256)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		go o.logPump(os.Stderr, o.logCh, o.logQuit, o.logPumpDone)

		o.logCh <- logEntry{
			time:  time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level: LogLevelDebug, service: "svc", msg: "debug msg",
		}
		o.logCh <- logEntry{
			time:  time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level: LogLevelInfo, service: "svc", msg: "info msg",
		}
		o.logCh <- logEntry{
			time:  time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level: LogLevelWarn, service: "svc", msg: "warn msg",
		}
		o.logCh <- logEntry{
			time:  time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
			level: LogLevelError, service: "svc", msg: "error msg",
		}
		close(o.logQuit)
		<-o.logPumpDone

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if strings.Contains(output, "debug msg") {
			t.Error("debug message should have been filtered out")
		}
		if strings.Contains(output, "info msg") {
			t.Error("info message should have been filtered out")
		}
		if !strings.Contains(output, "warn msg") {
			t.Error("warn message should have been included")
		}
		if !strings.Contains(output, "error msg") {
			t.Error("error message should have been included")
		}
	})

	t.Run("logPump_exits_on_quit_signal", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelDebug))
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.logCh = make(chan logEntry, 1)
		o.logQuit = make(chan struct{})
		o.logPumpDone = make(chan struct{})
		done := make(chan struct{})
		go func() {
			o.logPump(os.Stderr, o.logCh, o.logQuit, o.logPumpDone)
			close(done)
		}()
		close(o.logQuit)
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("logPump did not exit after channel close")
		}
	})
}

// TestLogPump_DrainsBufferedOnQuit verifies the pump flushes buffered entries
// to stderr before exiting on the quit signal.
func TestLogPump_DrainsBufferedOnQuit(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(WithLogLevel(LogLevelDebug))
	o.logCh = make(chan logEntry, 256)
	o.logQuit = make(chan struct{})
	o.logPumpDone = make(chan struct{})

	// Pre-fill the buffer before starting the pump; on quit it must drain all.
	for i := 0; i < 3; i++ {
		o.logCh <- logEntry{time: time.Now(), level: LogLevelInfo, service: "svc", msg: "buffered"}
	}
	close(o.logQuit)
	go o.logPump(os.Stderr, o.logCh, o.logQuit, o.logPumpDone)
	<-o.logPumpDone

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	if count := strings.Count(buf.String(), "buffered"); count != 3 {
		t.Errorf("expected 3 buffered entries flushed, got %d", count)
	}
}

// TestLogLevelFiltering_AtEmit verifies that below-minimum entries are dropped
// at emit (not the pump), so a Debug flood cannot starve the buffer and drop
// an Error entry.
func TestLogLevelFiltering_AtEmit(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(WithLogLevel(LogLevelInfo))
	svc := &testSvc{
		startFn: func(ctx context.Context) error {
			sc := ctx.(ServiceContext)
			for i := 0; i < 1000; i++ {
				sc.Logger.Debug("debug spam", "i", i)
			}
			sc.Logger.Error("important error", "code", 500)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	_ = o.Register(svc)
	_ = o.Start()
	time.Sleep(100 * time.Millisecond)
	_ = o.Stop(time.Second)

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	output := buf.String()
	if !strings.Contains(output, "important error") {
		t.Errorf("Error entry should reach stderr despite Debug flood, got:\n%s", output)
	}
	if strings.Contains(output, "debug spam") {
		t.Error("Debug entries should be filtered at emit (level=Info)")
	}
}

func TestServiceLogger_Emit(t *testing.T) {
	t.Run("non_blocking_send_when_channel_full", func(t *testing.T) {
		ch := make(chan logEntry, 1)
		ch <- logEntry{}
		logger := newServiceLogger("test", ch, nil, LogLevelDebug)
		logger.Info("dropped")
	})

	t.Run("methods_use_correct_levels", func(t *testing.T) {
		ch := make(chan logEntry, 16)
		logger := newServiceLogger("svc", ch, nil, LogLevelDebug)

		logger.Debug("d")
		logger.Info("i")
		logger.Warn("w")
		logger.Error("e")

		levels := map[string]LogLevel{}
		for i := 0; i < 4; i++ {
			e := <-ch
			levels[e.msg] = e.level
		}
		if levels["d"] != LogLevelDebug {
			t.Errorf("Debug level got %d", levels["d"])
		}
		if levels["i"] != LogLevelInfo {
			t.Errorf("Info level got %d", levels["i"])
		}
		if levels["w"] != LogLevelWarn {
			t.Errorf("Warn level got %d", levels["w"])
		}
		if levels["e"] != LogLevelError {
			t.Errorf("Error level got %d", levels["e"])
		}
	})
}

func TestServiceNameInLogger(t *testing.T) {
	t.Run("auto_name_matches_status_name", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(WithLogLevel(LogLevelWarn))
		_ = o.Register(&panicSvc{msg: "namecheck"})
		_ = o.Start()
		time.Sleep(100 * time.Millisecond)
		_ = o.Stop(200 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		// An unnamed service logs under its auto-assigned "$N" name, the same
		// name Status()/Names() report, not its reflect type.
		if !strings.Contains(output, "$1 ---") {
			t.Errorf("expected '$1' log prefix matching the status name, got: %s", output)
		}
	})
}
