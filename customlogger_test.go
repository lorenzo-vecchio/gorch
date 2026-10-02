package gorch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins the Logger option: a caller-supplied logger receives every
// level, no default stderr output is produced, and the custom logger is used on
// the self-heal and start-error paths too.

// testLogger records log calls for assertions in tests.
type testLogger struct {
	mu    sync.Mutex
	calls []testLogCall
}

type testLogCall struct {
	level string
	msg   string
	args  []any
}

func (tl *testLogger) Info(msg string, args ...any) { tl.record("INFO", msg, args) }

func (tl *testLogger) Error(msg string, args ...any) { tl.record("ERROR", msg, args) }

func (tl *testLogger) Debug(msg string, args ...any) { tl.record("DEBUG", msg, args) }

func (tl *testLogger) Warn(msg string, args ...any) { tl.record("WARN", msg, args) }

func (tl *testLogger) record(level, msg string, args []any) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	copied := make([]any, len(args))
	copy(copied, args)
	tl.calls = append(tl.calls, testLogCall{level: level, msg: msg, args: copied})
}

func (tl *testLogger) callsMatching(msg string) []testLogCall {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	var out []testLogCall
	for _, c := range tl.calls {
		if c.msg == msg {
			out = append(out, c)
		}
	}
	return out
}

func TestCustomLogger_BasicDelegation(t *testing.T) {
	tl := &testLogger{}
	o := New(WithLogger(tl), WithLogLevel(LogLevelError)) // LogLevel should be ignored
	o.Register(&testSvc{}, WithName("alpha"))
	o.Start()

	// Use the service logger directly.
	o.mu.Lock()
	entry := o.nameIndex["alpha"]
	o.mu.Unlock()

	entry.logger.Info("hello", "k", "v")
	entry.logger.Debug("debug msg", "a", 1)
	entry.logger.Warn("watch out", "severity", "high")
	entry.logger.Error("boom", "code", 500)

	o.Stop(100 * time.Millisecond)

	if len(tl.calls) != 4 {
		t.Fatalf("expected 4 log calls, got %d", len(tl.calls))
	}
	for _, c := range tl.calls {
		found := false
		for i := 0; i < len(c.args)-1; i += 2 {
			if c.args[i] == "service" && c.args[i+1] == "alpha" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("log call %q missing service=alpha in args: %v", c.msg, c.args)
		}
	}
	// Verify Debug was passed through despite LogLevelError.
	debugCalls := tl.callsMatching("debug msg")
	if len(debugCalls) != 1 {
		t.Errorf("expected Debug to be passed through when custom logger is set, got %d calls", len(debugCalls))
	}
}

func TestCustomLogger_NoStderrOutput(t *testing.T) {
	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	tl := &testLogger{}
	o := New(WithLogger(tl))
	o.Register(&testSvc{}, WithName("silent"))
	o.Start()

	o.mu.Lock()
	entry := o.nameIndex["silent"]
	o.mu.Unlock()
	entry.logger.Info("should go to custom logger only")

	o.Stop(100 * time.Millisecond)
	w.Close()
	os.Stderr = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	if strings.Contains(buf.String(), "should go to custom logger only") {
		t.Error("log message leaked to stderr when custom logger is set")
	}
}

func TestCustomLogger_SelfHeal(t *testing.T) {
	tl := &testLogger{}
	var factoryCalls atomic.Int32
	o := New(WithLogger(tl), WithLogLevel(LogLevelWarn))
	_ = o.Register(&testSvc{
		startFn: func(ctx context.Context) error {
			return errors.New("crash")
		},
		stopFn: func() error { return nil },
	}, WithName("rebound"), WithSelfHeal(func() Service {
		factoryCalls.Add(1)
		return &testSvc{
			startFn: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}
	}))
	_ = o.Start()

	deadline := time.After(3 * time.Second)
	for factoryCalls.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for factory call")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
	_ = o.Stop(500 * time.Millisecond)

	// After self-heal, the new logger should still use the custom logger.
	// The restart warning and error should have "service"="rebound".
	foundService := false
	for _, c := range tl.calls {
		for i := 0; i < len(c.args)-1; i += 2 {
			if c.args[i] == "service" && c.args[i+1] == "rebound" {
				foundService = true
				break
			}
		}
	}
	if !foundService {
		t.Errorf("no log call from self-heal contained service=rebound")
	}
}

func TestCustomLogger_StartErrorPath(t *testing.T) {
	// Verify that the cron error path in Start doesn't panic when a custom
	// logger is set (logCh and logQuit are nil).
	tl := &testLogger{}
	o := New(WithLogger(tl))
	if err := o.Register(&testSvc{}, WithName("x"), WithCron("* * * * * *", CronParallel)); err != nil {
		t.Fatal(err)
	}
	// Register validates the spec, so corrupt it afterwards to force Start to
	// fail at setupCron with a custom logger (nil logCh/logQuit).
	o.mu.Lock()
	o.entries[0].cfg.cronSpec = "invalid cron spec"
	o.mu.Unlock()
	if err := o.Start(); !errors.Is(err, ErrInvalidCron) {
		t.Fatalf("Start = %v, want ErrInvalidCron", err)
	}
	if o.logCh != nil {
		t.Error("expected logCh to be nil when custom logger is set")
	}
}

func TestCustomLogger_HandlesAllLevels(t *testing.T) {
	tl := &testLogger{}
	o := New(WithLogger(tl))

	o.Register(&testSvc{}, WithName("levels"))
	o.Start()

	o.mu.Lock()
	entry := o.nameIndex["levels"]
	o.mu.Unlock()

	entry.logger.Info("i", "x", 1)
	entry.logger.Error("e", "y", 2)
	entry.logger.Debug("d", "z", 3)
	entry.logger.Warn("w", "q", 4)

	o.Stop(100 * time.Millisecond)

	levelSet := make(map[string]bool)
	for _, c := range tl.calls {
		levelSet[c.level] = true
	}
	for _, lvl := range []string{"INFO", "ERROR", "DEBUG", "WARN"} {
		if !levelSet[lvl] {
			t.Errorf("expected %s level to be passed to custom logger", lvl)
		}
	}
}
