package gorch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins the internal runService path: the normal completion path,
// error logging, the self-heal exit path, panic recovery, and that the log pump
// captures stderr at start.

// TestRunService_LogPumpCapturesStderrAtStart pins the log-pump's destination
// to the os.Stderr value captured when the pump started: reassigning os.Stderr
// afterwards must not redirect an already-running pump (which would also race
// the reassignment, since the pump writes from its own goroutine).
func TestRunService_LogPumpCapturesStderrAtStart(t *testing.T) {
	firstR, firstW, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = firstW
	defer func() { os.Stderr = old }()

	ready := make(chan struct{})
	release := make(chan struct{})
	emitted := make(chan struct{})

	o := New(WithLogLevel(LogLevelError))
	if err := o.RegisterFunc("svc", func(ctx ServiceContext) error {
		close(ready)
		<-release
		ctx.Logger.Error("after-reassignment")
		close(emitted)
		return nil
	}, nil); err != nil {
		t.Fatalf("RegisterFunc: %v", err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-ready

	secondR, secondW, _ := os.Pipe()
	os.Stderr = secondW

	close(release)
	<-emitted
	if err := o.Stop(2 * time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	_ = firstW.Close()
	_ = secondW.Close()
	firstOut, _ := io.ReadAll(firstR)
	secondOut, _ := io.ReadAll(secondR)

	if !strings.Contains(string(firstOut), "after-reassignment") {
		t.Fatalf("pump did not write to the stderr captured at start; first pipe got %q", firstOut)
	}
	if strings.Contains(string(secondOut), "after-reassignment") {
		t.Fatalf("pump followed a later os.Stderr reassignment; second pipe got %q", secondOut)
	}
}

func TestRunService_ErrorLogging(t *testing.T) {
	t.Run("service_error_logged", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(WithLogLevel(LogLevelError))
		_ = o.Register(&errSvc{err: errors.New("test failure")})
		_ = o.Start()
		time.Sleep(100 * time.Millisecond)
		_ = o.Stop(500 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "service returned error") {
			t.Errorf("expected 'service returned error' in log, got: %s", output)
		}
		if !strings.Contains(output, "test failure") {
			t.Errorf("expected error message in log, got: %s", output)
		}
	})

	t.Run("context_canceled_not_logged_as_error", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(WithLogLevel(LogLevelError))
		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				<-ctx.Done()
				return context.Canceled
			},
		}
		_ = o.Register(svc)
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		_ = o.Stop(500 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if strings.Contains(output, "service returned error") {
			t.Errorf("context.Canceled should not be logged as error, got: %s", output)
		}
	})

	t.Run("cron_error_logged", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(WithLogLevel(LogLevelError))
		_ = o.Register(&errSvc{err: errors.New("cron fail")}, WithCron("* * * * * *", CronParallel))
		_ = o.Start()
		time.Sleep(1500 * time.Millisecond)
		_ = o.Stop(500 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if !strings.Contains(output, "cron service returned error") {
			t.Errorf("expected 'cron service returned error' in log, got: %s", output)
		}
	})
}

func TestRunService_Normal(t *testing.T) {
	t.Run("normal_return_no_error_logged", func(t *testing.T) {
		r, w, _ := os.Pipe()
		old := os.Stderr
		os.Stderr = w

		o := New(WithLogLevel(LogLevelError))
		svc := &testSvc{
			startFn: func(ctx context.Context) error {
				return nil
			},
		}
		_ = o.Register(svc)
		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		_ = o.Stop(500 * time.Millisecond)

		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stderr = old

		output := buf.String()
		if strings.Contains(output, "service returned error") {
			t.Errorf("normal return should not log error: %s", output)
		}
	})
}

func TestRunService_ErrorViaSelfHeal(t *testing.T) {
	// Cover runService logging when service returns non-Canceled error.
	// This is triggered via self-heal: factory creates a service that returns an error.
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(WithLogLevel(LogLevelError))
	var factoryCalls atomic.Int32
	factory := func() Service {
		factoryCalls.Add(1)
		return &errSvc{err: errors.New("factory err")}
	}
	_ = o.Register(&errSvc{err: errors.New("init")},
		WithSelfHeal(factory),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Millisecond}),
	)
	_ = o.Start()
	time.Sleep(200 * time.Millisecond)
	_ = o.Stop(500 * time.Millisecond)

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	output := buf.String()
	if !strings.Contains(output, "service returned error") {
		t.Errorf("expected 'service returned error' for factory-created service: %s", output)
	}
	if factoryCalls.Load() < 1 {
		t.Error("expected factory to be called")
	}
}

func TestRunService_PanicRecovery(t *testing.T) {
	r, w, _ := os.Pipe()
	old := os.Stderr
	os.Stderr = w

	o := New(WithLogLevel(LogLevelError))
	var factoryCalls atomic.Int32
	factory := func() Service {
		factoryCalls.Add(1)
		return &panicSvc{msg: "factory panic"}
	}
	_ = o.Register(&errSvc{err: errors.New("init err")},
		WithSelfHeal(factory),
		WithBackoff(ConstantBackoff{Delay: 10 * time.Millisecond}),
		WithMaxRetries(2),
	)
	_ = o.Start()
	time.Sleep(200 * time.Millisecond)
	_ = o.Stop(500 * time.Millisecond)

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stderr = old

	output := buf.String()
	if !strings.Contains(output, "service panicked") {
		t.Errorf("expected 'service panicked' from runService recovery: %s", output)
	}
	if factoryCalls.Load() < 1 {
		t.Error("expected factory to be called")
	}
}
