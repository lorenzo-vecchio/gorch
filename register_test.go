package gorch

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins registration: RegisterOption application exactly once and
// before validation, structural validation (duplicates, missing dependencies,
// transitive cycles, depth caps), auto-naming, the Validator protocol, the
// identical treatment of static and dynamic paths, and RegisterFunc.

func TestRegister(t *testing.T) {
	t.Run("register_before_start", func(t *testing.T) {
		o := New()
		err := o.Register(&namedSvc{name: "a"})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(o.entries) != 1 {
			t.Errorf("expected 1 entry, got %d", len(o.entries))
		}
	})

	t.Run("register_after_start_hot_adds_without_starting", func(t *testing.T) {
		o := New()
		_ = o.Register(&namedSvc{name: "a"}, WithName("a"))
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer o.Stop(1 * time.Second)
		svc := &testSvc{}
		err := o.Register(svc, WithName("b"))
		if err != nil {
			t.Fatalf("hot Register returned %v", err)
		}
		s, ok := o.Status("b")
		if !ok || s != StatusRegistered {
			t.Errorf("hot-added service status = %v, want StatusRegistered", s)
		}
		names := o.Names()
		if len(names) != 2 {
			t.Errorf("Names() = %v, want 2 entries", names)
		}
		if got := svc.startCalls.Load(); got != 0 {
			t.Errorf("hot Register must not start the service, Start called %d times", got)
		}
	})

	t.Run("register_with_cron_option", func(t *testing.T) {
		o := New()
		err := o.Register(&namedSvc{name: "c"}, WithCron("* * * * * *", CronParallel))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if o.entries[0].cfg.cronSpec != "* * * * * *" {
			t.Errorf("expected cron spec, got %q", o.entries[0].cfg.cronSpec)
		}
		if o.entries[0].cfg.cronMode != CronParallel {
			t.Errorf("expected CronParallel, got %v", o.entries[0].cfg.cronMode)
		}
	})

	t.Run("register_with_selfheal_option", func(t *testing.T) {
		o := New()
		factory := func() Service { return &namedSvc{name: "healed"} }
		err := o.Register(&namedSvc{name: "a"}, WithSelfHeal(factory))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if o.entries[0].cfg.factory == nil {
			t.Fatal("expected factory to be set")
		}
	})
}

func TestRegister_UnsupportedSelfHealCombination(t *testing.T) {
	factory := func() Service { return &namedSvc{name: "healed"} }

	t.Run("cron", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		err := o.Register(&namedSvc{name: "a"}, WithCron("* * * * * *", CronParallel), WithSelfHeal(factory))
		if !errors.Is(err, ErrUnsupportedOption) {
			t.Fatalf("expected ErrUnsupportedOption, got %v", err)
		}
		if o.Count() != 0 {
			t.Errorf("rejected service must not be registered, got count %d", o.Count())
		}
	})

	t.Run("runOnce", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		err := o.Register(&namedSvc{name: "a"}, WithRunOnce(), WithSelfHeal(factory))
		if !errors.Is(err, ErrUnsupportedOption) {
			t.Fatalf("expected ErrUnsupportedOption, got %v", err)
		}
		if o.Count() != 0 {
			t.Errorf("rejected service must not be registered, got count %d", o.Count())
		}
	})
}

func TestRegister_DuplicateName(t *testing.T) {
	o := New()
	_ = o.Register(&namedSvc{}, WithName("dup"))
	err := o.Register(&namedSvc{}, WithName("dup"))
	if !errors.Is(err, ErrDuplicateName) {
		t.Errorf("expected ErrDuplicateName, got %v", err)
	}
}

func TestRegister_SelfDependency(t *testing.T) {
	o := New()
	err := o.Register(&namedSvc{}, WithName("self"), DependsOn("self"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Errorf("expected ErrDependencyCycle, got %v", err)
	}
}

func TestRegister_DependencyNotFound(t *testing.T) {
	o := New()
	err := o.Register(&namedSvc{}, WithName("orphan"), DependsOn("nobody"))
	if !errors.Is(err, ErrDependencyNotFound) {
		t.Fatalf("static Register with an unknown hard dependency = %v, want ErrDependencyNotFound", err)
	}
}

func TestRegisterOptions(t *testing.T) {
	t.Run("WithCron_sets_spec_and_mode", func(t *testing.T) {
		cfg := registerConfig{}
		WithCron("*/5 * * * * *", CronQueue)(&cfg)
		if cfg.cronSpec != "*/5 * * * * *" {
			t.Errorf("expected spec, got %q", cfg.cronSpec)
		}
		if cfg.cronMode != CronQueue {
			t.Errorf("expected CronQueue, got %v", cfg.cronMode)
		}
	})

	t.Run("WithSelfHeal_sets_factory", func(t *testing.T) {
		cfg := registerConfig{}
		f := func() Service { return &namedSvc{name: "x"} }
		WithSelfHeal(f)(&cfg)
		if cfg.factory == nil {
			t.Fatal("expected factory to be set")
		}
	})
}

func TestRegister_DepFoundInEntries(t *testing.T) {
	// ponytail: the entries-backup check in Register is for batch operations.
	// Since Register is called sequentially, nameIndex always has previous entries.
	// We verify the path exists by testing indirectly: register two services
	// where the second depends on the first.
	o := New()
	_ = o.Register(&namedSvc{}, WithName("base"))
	err := o.Register(&namedSvc{}, WithName("child"), DependsOn("base"))
	if err != nil {
		t.Errorf("child should be able to depend on already-registered base: %v", err)
	}
}

func TestErrDuplicateName_MessageHasName(t *testing.T) {
	o := New()
	_ = o.Register(&namedSvc{}, WithName("a"))
	err := o.Register(&namedSvc{}, WithName("a"))
	if err == nil || !errors.Is(err, ErrDuplicateName) {
		t.Fatalf("expected ErrDuplicateName, got %v", err)
	}
	if !strings.Contains(err.Error(), "a") {
		t.Errorf("error message should contain name: %v", err)
	}
}

func TestErrDependencyCycle_MessageHasNames(t *testing.T) {
	o := New()
	err := o.Register(&namedSvc{}, WithName("self"), DependsOn("self"))
	if err == nil || !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("expected ErrDependencyCycle, got %v", err)
	}
	if !strings.Contains(err.Error(), "self") {
		t.Errorf("error message should contain name: %v", err)
	}
}

func TestRegister_AutoName(t *testing.T) {
	o := New()
	_ = o.Register(&namedSvc{}) // no WithName
	if o.entries[0].name != "$1" {
		t.Errorf("expected auto-name '$1', got %q", o.entries[0].name)
	}
	_ = o.Register(&namedSvc{})
	if o.entries[1].name != "$2" {
		t.Errorf("expected auto-name '$2', got %q", o.entries[1].name)
	}
}

func TestRegister_TransitiveCycle(t *testing.T) {
	// White-box: manually add an entry to o.entries (not nameIndex) that
	// depends on the service we're about to register. Then register that
	// service with a dep on the entry → cycle detected.
	o := New()
	existing := &serviceEntry{
		name: "intermediate",
		cfg:  registerConfig{name: "intermediate", dependsOn: []string{"target"}},
	}
	o.entries = append(o.entries, existing)
	// Not in nameIndex — tests the entries fallback + transitive cycle path.

	err := o.Register(&namedSvc{}, WithName("target"), DependsOn("intermediate"))
	if !errors.Is(err, ErrDependencyCycle) {
		t.Errorf("expected ErrDependencyCycle, got %v", err)
	}
}

func TestRegister_DepInEntriesNotNameIndex(t *testing.T) {
	// White-box: cover the path where a dependency is found in o.entries
	// but not in o.nameIndex (batch-register scenario).
	o := New()
	entry := &serviceEntry{name: "base", cfg: registerConfig{name: "base"}, status: StatusRegistered}
	o.entries = append(o.entries, entry)
	// nameIndex does NOT have "base".

	err := o.Register(&namedSvc{}, WithName("child"), DependsOn("base"))
	if err != nil {
		t.Errorf("should find dep in entries (not nameIndex): %v", err)
	}
	// "child" is added to nameIndex; "base" remains only in entries.
	if _, ok := o.nameIndex["child"]; !ok {
		t.Error("child should be in nameIndex")
	}
}

func TestRegisterFunc_v3(t *testing.T) {
	t.Run("start_stop_lifecycle", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		var started atomic.Int32
		var stopped atomic.Int32

		err := o.RegisterFunc("fn", func(ctx ServiceContext) error {
			started.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}, func() error {
			stopped.Add(1)
			return nil
		})
		if err != nil {
			t.Fatalf("RegisterFunc failed: %v", err)
		}

		_ = o.Start()
		time.Sleep(50 * time.Millisecond)
		if started.Load() != 1 {
			t.Error("started should be 1")
		}
		_ = o.Stop(time.Second)
		if stopped.Load() != 1 {
			t.Error("stopped should be 1")
		}
	})

	t.Run("with_options", func(t *testing.T) {
		o := New(WithLogLevel(LogLevelWarn))
		err := o.RegisterFunc("opt-fn",
			func(ctx ServiceContext) error { <-ctx.Done(); return ctx.Err() },
			func() error { return nil },
			WithGroup("test-grp"),
			WithLabel("env", "test"),
		)
		if err != nil {
			t.Fatalf("RegisterFunc with options failed: %v", err)
		}
		if o.Count() != 1 {
			t.Fatalf("expected 1 service, got %d", o.Count())
		}
		_ = o.Start()
		_ = o.Stop(time.Second)
	})
}

// TestRegister_DeepChain_ReturnsDepthExceeded verifies a real Register whose
// hard-dependency walk exceeds the cap surfaces ErrDependencyDepthExceeded
// rather than overflowing the stack.
func TestRegister_DeepChain_ReturnsDepthExceeded(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	top := insertDepChain(o, maxDependencyDepth+16)
	err := o.Register(&testSvc{}, WithName("leaf"), DependsOn(top))
	if !errors.Is(err, ErrDependencyDepthExceeded) {
		t.Fatalf("Register over-deep hard chain = %v, want ErrDependencyDepthExceeded", err)
	}
	if _, ok := o.nameIndex["leaf"]; ok {
		t.Fatal("rejected Register must not leave the entry in the registry")
	}
}

// TestRegister_DeepSoftChain_ReturnsDepthExceeded covers the soft-edge path:
// the depth error from a soft dependency's walk is propagated, not swallowed.
func TestRegister_DeepSoftChain_ReturnsDepthExceeded(t *testing.T) {
	o := New(WithLogLevel(LogLevelWarn))
	top := insertDepChain(o, maxDependencyDepth+16)
	err := o.Register(&testSvc{}, WithName("leaf"), DependsOnSoft(top))
	if !errors.Is(err, ErrDependencyDepthExceeded) {
		t.Fatalf("Register over-deep soft chain = %v, want ErrDependencyDepthExceeded", err)
	}
}

func TestValidate(t *testing.T) {
	t.Run("validation_passes", func(t *testing.T) {
		o := New()
		err := o.Register(&validSvc{}, WithName("pass"))
		if err != nil {
			t.Errorf("valid service should register: %v", err)
		}
	})

	t.Run("validation_fails", func(t *testing.T) {
		o := New()
		wantErr := errors.New("invalid config")
		err := o.Register(&validSvc{validateErr: wantErr}, WithName("fail"))
		if err == nil {
			t.Error("expected validation error")
		}
		if !strings.Contains(err.Error(), "invalid config") {
			t.Errorf("expected 'invalid config' in error: %v", err)
		}
	})

	t.Run("validation_no_validator_interface", func(t *testing.T) {
		o := New()
		err := o.Register(&namedSvc{}, WithName("plain"))
		if err != nil {
			t.Errorf("service without Validator should register: %v", err)
		}
	})
}

// blockingValidator blocks inside Validate (outside the orchestrator lock) until
// released, so a test can race Start/Stop against a static Validator.
type blockingValidator struct {
	testSvc
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingValidator) Validate() error {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return nil
}

// racingValidator registers a conflicting name during Validate (outside the
// lock), then blocks until released.
type racingValidator struct {
	testSvc
	o       *Orchestrator
	inner   Service
	name    string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *racingValidator) Validate() error {
	_ = r.o.Register(r.inner, WithName(r.name))
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return nil
}

// TestRegister_StaticValidator pins that a pre-Start Validator runs outside the
// orchestrator lock: it can block or reenter without deadlocking, and a Start or
// Stop that races it is resolved at commit time.
func TestRegister_StaticValidator(t *testing.T) {
	t.Run("duplicate resolved at commit", func(t *testing.T) {
		o := New()
		if err := o.Register(&namedSvc{}, WithName("dup")); err != nil {
			t.Fatal(err)
		}
		err := o.Register(&validSvc{testSvc: testSvc{}}, WithName("dup"))
		if !errors.Is(err, ErrDuplicateName) {
			t.Fatalf("got %v, want ErrDuplicateName", err)
		}
	})

	t.Run("duplicate raced by Start", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		v := &racingValidator{
			o:       o,
			inner:   &namedSvc{},
			name:    "raced",
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		regDone := make(chan error, 1)
		go func() { regDone <- o.Register(v, WithName("raced")) }()
		<-v.entered // inner "raced" registered while the outer Validate blocks
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		close(v.release)
		if err := <-regDone; !errors.Is(err, ErrDuplicateName) {
			t.Fatalf("Register = %v, want ErrDuplicateName", err)
		}
		if err := o.Stop(time.Second); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})

	t.Run("start wins the race", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		v := &blockingValidator{entered: make(chan struct{}), release: make(chan struct{})}
		regDone := make(chan error, 1)
		go func() { regDone <- o.Register(v, WithName("raced")) }()
		<-v.entered
		if err := o.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		close(v.release)
		if err := <-regDone; err != nil {
			t.Fatalf("Register: %v", err)
		}
		if s, _ := o.Status("raced"); s != StatusRegistered {
			t.Errorf("raced hot-add status = %v, want StatusRegistered", s)
		}
		if err := o.Stop(time.Second); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})

	t.Run("stop wins the race", func(t *testing.T) {
		o := New(WithHealthChecksDisabled())
		stopEntered := make(chan struct{})
		stopRelease := make(chan struct{})
		blocker := &testSvc{
			startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			stopFn: func() error {
				close(stopEntered)
				<-stopRelease
				return nil
			},
		}
		if err := o.Register(blocker, WithName("blocker")); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}

		v := &blockingValidator{entered: make(chan struct{}), release: make(chan struct{})}
		regDone := make(chan error, 1)
		go func() { regDone <- o.Register(v, WithName("raced")) }()
		<-v.entered

		stopDone := make(chan error, 1)
		go func() { stopDone <- o.Stop(5 * time.Second) }()
		<-stopEntered // stopping is set while Validate still blocks
		close(v.release)
		err := <-regDone
		if !errors.Is(err, ErrOrchestratorStopping) && !errors.Is(err, ErrOrchestratorStopped) {
			t.Fatalf("Register racing Stop = %v, want a shutdown sentinel", err)
		}
		close(stopRelease)
		if err := <-stopDone; err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})
}

// countingRegisterOption returns a RegisterOption that increments calls each
// time it is applied. It is non-idempotent on purpose: Register must apply it
// exactly once.
func countingRegisterOption(calls *atomic.Int32) RegisterOption {
	return func(*registerConfig) { calls.Add(1) }
}

// TestRegister_OptionsAppliedExactlyOnce pins that RegisterOption closures run
// once per Register call, on the static and hot-add paths alike and whether or
// not the service implements Validator.
func TestRegister_OptionsAppliedExactlyOnce(t *testing.T) {
	t.Run("plain static", func(t *testing.T) {
		var calls atomic.Int32
		o := New()
		if err := o.Register(&namedSvc{}, countingRegisterOption(&calls), WithName("plain")); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("option applied %d times, want 1", got)
		}
	})

	t.Run("validator static", func(t *testing.T) {
		var calls atomic.Int32
		o := New()
		if err := o.Register(&validSvc{}, countingRegisterOption(&calls), WithName("valid")); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("option applied %d times, want 1", got)
		}
	})

	t.Run("plain dynamic", func(t *testing.T) {
		var calls atomic.Int32
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)
		if err := o.Register(&namedSvc{}, countingRegisterOption(&calls), WithName("plain")); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("option applied %d times, want 1", got)
		}
	})

	t.Run("validator dynamic", func(t *testing.T) {
		var calls atomic.Int32
		o := New()
		if err := o.Start(); err != nil {
			t.Fatal(err)
		}
		defer o.Stop(time.Second)
		if err := o.Register(&validSvc{}, countingRegisterOption(&calls), WithName("valid")); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("option applied %d times, want 1", got)
		}
	})
}

// sameRegisterConfig reports whether two configs built from the same options
// are equivalent. factory holds a function value, which reflect.DeepEqual never
// compares equal, so it is checked for presence and then cleared.
func sameRegisterConfig(a, b registerConfig) bool {
	if (a.factory == nil) != (b.factory == nil) {
		return false
	}
	a.factory, b.factory = nil, nil
	return reflect.DeepEqual(a, b)
}

// TestRegister_ValidatorPath_And_PlainPath_ProduceSameConfig pins that the same
// options yield the same registerConfig and entry whether the service
// implements Validator or not, across static option sets.
func TestRegister_ValidatorPath_And_PlainPath_ProduceSameConfig(t *testing.T) {
	optionSets := []struct {
		name string
		opts func() []RegisterOption
	}{
		{"name only", func() []RegisterOption {
			return []RegisterOption{WithName("target")}
		}},
		{"hard dependency", func() []RegisterOption {
			return []RegisterOption{WithName("target"), DependsOn("dep")}
		}},
		{"soft dependency", func() []RegisterOption {
			return []RegisterOption{WithName("target"), DependsOnSoft("ghost")}
		}},
		{"group", func() []RegisterOption {
			return []RegisterOption{WithName("target"), WithGroup("g")}
		}},
		{"cron", func() []RegisterOption {
			return []RegisterOption{WithName("target"), WithCron("* * * * * *", CronParallel)}
		}},
		{"run once", func() []RegisterOption {
			return []RegisterOption{WithName("target"), WithRunOnce()}
		}},
		{"self heal", func() []RegisterOption {
			return []RegisterOption{WithName("target"), WithSelfHeal(func() Service { return &namedSvc{} })}
		}},
	}

	for _, tc := range optionSets {
		t.Run(tc.name, func(t *testing.T) {
			opts := tc.opts()

			po := New()
			if err := po.Register(&namedSvc{name: "dep"}, WithName("dep")); err != nil {
				t.Fatal(err)
			}
			if err := po.Register(&namedSvc{}, opts...); err != nil {
				t.Fatalf("plain Register: %v", err)
			}
			po.mu.Lock()
			plainEntry := po.nameIndex["target"]
			plainCfg, plainStatus := plainEntry.cfg, plainEntry.status
			po.mu.Unlock()

			vo := New()
			if err := vo.Register(&namedSvc{name: "dep"}, WithName("dep")); err != nil {
				t.Fatal(err)
			}
			if err := vo.Register(&validSvc{}, opts...); err != nil {
				t.Fatalf("validator Register: %v", err)
			}
			vo.mu.Lock()
			validEntry := vo.nameIndex["target"]
			validCfg, validStatus := validEntry.cfg, validEntry.status
			vo.mu.Unlock()

			if plainEntry.name != validEntry.name {
				t.Errorf("entry names differ: plain %q, validator %q", plainEntry.name, validEntry.name)
			}
			if plainStatus != validStatus {
				t.Errorf("statuses differ: plain %v, validator %v", plainStatus, validStatus)
			}
			if !sameRegisterConfig(plainCfg, validCfg) {
				t.Errorf("configs differ:\nplain:     %+v\nvalidator: %+v", plainCfg, validCfg)
			}
		})
	}
}

// TestRegister_OptionAppliedBeforeValidation pins that the name set by a
// RegisterOption is visible in the Validator failure message: the option runs
// once, before Validate.
func TestRegister_OptionAppliedBeforeValidation(t *testing.T) {
	o := New()
	err := o.Register(&validSvc{validateErr: errors.New("rejected")}, WithName("boom"))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("validation error %q does not contain the option-set name %q", err, "boom")
	}
}
