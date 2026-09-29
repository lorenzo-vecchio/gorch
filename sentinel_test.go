package gorch

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"
)

// ── Sentinel taxonomy (issue #29) ──
//
// Every exported sentinel is classified along two axes: a class and whether it
// is retryable. Only a transient sentinel is retryable. This table is the
// checked-in taxonomy; TestSentinelsTable_EverySentinelHasATest fails when a new
// exported sentinel is added without an entry here, and
// TestSentinelsTable_ProducibleFromDocumentedEntry fails when an entry cannot be
// produced from its documented public entry point.

type sentinelClass string

const (
	// classPermanent: a bug in the caller's code or configuration; an identical
	// call keeps failing until code or configuration changes.
	classPermanent sentinelClass = "permanent"
	// classTransient: a condition that may clear on its own or after another
	// operation; retry the same call once it clears.
	classTransient sentinelClass = "transient"
	// classTerminal: the whole-orchestrator lifecycle has already begun or ended;
	// by design the operation can never succeed.
	classTerminal sentinelClass = "terminal"
	// classEnvironmental: user code or teardown overran a deadline and the
	// outcome is unverified; the timed-out teardown is not retried.
	classEnvironmental sentinelClass = "environmental"
)

// retryable reports whether the class may be retried unchanged. By the
// taxonomy's rule, only transient conditions are retryable.
func (c sentinelClass) retryable() bool { return c == classTransient }

// sentinelSpec is one classified sentinel: its class, the public entry point
// that produces it, and a deterministic producer for the reachability test.
type sentinelSpec struct {
	name    string
	err     error
	class   sentinelClass
	entry   string
	produce func(t *testing.T) error
}

// documentedEntryPoints is the set of public entry points a sentinel may name
// as its producer; it keeps the entry column from drifting to a typo.
var documentedEntryPoints = map[string]struct{}{
	"Start":               {},
	"Stop":                {},
	"Register":            {},
	"RegisterFunc":        {},
	"StartService":        {},
	"StopService":         {},
	"Unregister":          {},
	"StartGroup":          {},
	"StopGroup":           {},
	"SubscribeWithBuffer": {},
	"Request":             {},
}

var sentinelTaxonomy = []sentinelSpec{
	{name: "ErrAlreadyStarted", err: ErrAlreadyStarted, class: classTerminal, entry: "Start", produce: produceErrAlreadyStarted},
	{name: "ErrInvalidCron", err: ErrInvalidCron, class: classPermanent, entry: "Register", produce: produceErrInvalidCron},
	{name: "ErrStopTimeout", err: ErrStopTimeout, class: classEnvironmental, entry: "StopService", produce: produceErrStopTimeout},
	{name: "ErrDuplicateName", err: ErrDuplicateName, class: classPermanent, entry: "Register", produce: produceErrDuplicateName},
	{name: "ErrNilService", err: ErrNilService, class: classPermanent, entry: "Register", produce: produceErrNilService},
	{name: "ErrHookTimeout", err: ErrHookTimeout, class: classEnvironmental, entry: "StopService", produce: produceErrHookTimeout},
	{name: "ErrDependencyCycle", err: ErrDependencyCycle, class: classPermanent, entry: "Register", produce: produceErrDependencyCycle},
	{name: "ErrStartAborted", err: ErrStartAborted, class: classTransient, entry: "Start", produce: produceErrStartAborted},
	{name: "ErrUnsupportedOption", err: ErrUnsupportedOption, class: classPermanent, entry: "Register", produce: produceErrUnsupportedOption},
	{name: "ErrServiceNotFound", err: ErrServiceNotFound, class: classPermanent, entry: "StopService", produce: produceErrServiceNotFound},
	{name: "ErrHasDependents", err: ErrHasDependents, class: classTransient, entry: "StopService", produce: produceErrHasDependents},
	{name: "ErrOrchestratorStopping", err: ErrOrchestratorStopping, class: classTransient, entry: "Register", produce: produceErrOrchestratorStopping},
	{name: "ErrOrchestratorStopped", err: ErrOrchestratorStopped, class: classTerminal, entry: "Register", produce: produceErrOrchestratorStopped},
	{name: "ErrOrchestratorNotStarted", err: ErrOrchestratorNotStarted, class: classTransient, entry: "StartService", produce: produceErrOrchestratorNotStarted},
	{name: "ErrDependencyNotRunning", err: ErrDependencyNotRunning, class: classTransient, entry: "StartService", produce: produceErrDependencyNotRunning},
	{name: "ErrDependencyNotFound", err: ErrDependencyNotFound, class: classPermanent, entry: "Register", produce: produceErrDependencyNotFound},
	{name: "ErrDependencyRemoving", err: ErrDependencyRemoving, class: classTransient, entry: "Register", produce: produceErrDependencyRemoving},
	{name: "ErrDependencyDepthExceeded", err: ErrDependencyDepthExceeded, class: classPermanent, entry: "Register", produce: produceErrDependencyDepthExceeded},
	{name: "ErrReentrantMembership", err: ErrReentrantMembership, class: classPermanent, entry: "StartService", produce: produceErrReentrantMembership},
	{name: "ErrMembershipBusy", err: ErrMembershipBusy, class: classTransient, entry: "StartService", produce: produceErrMembershipBusy},
	{name: "ErrInvalidBufferSize", err: ErrInvalidBufferSize, class: classPermanent, entry: "SubscribeWithBuffer", produce: produceErrInvalidBufferSize},
	{name: "ErrNilContext", err: ErrNilContext, class: classPermanent, entry: "Request", produce: produceErrNilContext},
}

// TestSentinelsTable_EverySentinelHasATest fails when a new exported sentinel
// (an exported package-level var initialized with errors.New) is added without
// being classified and given a producer, or when the taxonomy names a sentinel
// that no longer exists. It parses the package sources because Go cannot
// enumerate package-level variables at runtime.
func TestSentinelsTable_EverySentinelHasATest(t *testing.T) {
	found := sentinelVarDocs(t)
	if len(found) == 0 {
		t.Fatal("source scan found no exported sentinels; the parser is broken")
	}

	listed := make(map[string]struct{}, len(sentinelTaxonomy))
	for _, spec := range sentinelTaxonomy {
		if spec.err == nil {
			t.Fatalf("%s: nil sentinel", spec.name)
		}
		if _, dup := listed[spec.name]; dup {
			t.Fatalf("%s: duplicate taxonomy entry", spec.name)
		}
		listed[spec.name] = struct{}{}
		switch spec.class {
		case classPermanent, classTransient, classTerminal, classEnvironmental:
		default:
			t.Errorf("%s: unknown class %q", spec.name, spec.class)
		}
		if _, ok := documentedEntryPoints[spec.entry]; !ok {
			t.Errorf("%s: entry %q is not a documented public entry point", spec.name, spec.entry)
		}
		if spec.produce == nil {
			t.Errorf("%s: no producer (every classification must be produced by a test)", spec.name)
		}
	}

	for name := range found {
		if _, ok := listed[name]; !ok {
			t.Errorf("exported sentinel %s is not classified: add a sentinelSpec with a class, retryable answer, documented entry point, and producer", name)
		}
	}
	for name := range listed {
		if _, ok := found[name]; !ok {
			t.Errorf("sentinelTaxonomy lists %s, but no exported errors.New var of that name exists in the package", name)
		}
	}

	// The retryable answer is a function of the class alone; pin that so a class
	// cannot silently acquire a different retry policy.
	for _, spec := range sentinelTaxonomy {
		if got, want := spec.class.retryable(), spec.class == classTransient; got != want {
			t.Errorf("%s: retryable(%s) = %v, want %v", spec.name, spec.class, got, want)
		}
	}
}

// sentinelVarDocs parses the non-test sources of this package and returns every
// exported package-level variable whose initializer is a call to errors.New,
// mapped to its doc comment (empty when it has none). Import aliases are not
// used for errors in this package.
func sentinelVarDocs(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()
	found := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			decl, ok := n.(*ast.GenDecl)
			if !ok || decl.Tok != token.VAR {
				return true
			}
			for _, spec := range decl.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, ident := range vs.Names {
					if !ast.IsExported(ident.Name) || i >= len(vs.Values) {
						continue
					}
					if isErrorsNewCall(vs.Values[i]) {
						doc := ""
						if vs.Doc != nil {
							doc = vs.Doc.Text()
						}
						found[ident.Name] = doc
					}
				}
			}
			return true
		})
	}
	return found
}

// TestSentinelTaxonomy_DocCommentsMatch pins the source doc comments to the
// taxonomy: every sentinel's comment must state the class the table assigns it,
// so the code a caller reads and the classification the tests enforce cannot
// drift.
func TestSentinelTaxonomy_DocCommentsMatch(t *testing.T) {
	docs := sentinelVarDocs(t)
	for _, spec := range sentinelTaxonomy {
		doc, ok := docs[spec.name]
		if !ok {
			t.Errorf("%s: no doc comment found", spec.name)
			continue
		}
		if !strings.Contains(doc, "is "+string(spec.class)+":") {
			t.Errorf("%s: doc comment does not state class %q: %q", spec.name, spec.class, doc)
		}
	}
}

func isErrorsNewCall(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == "errors" && sel.Sel.Name == "New"
}

// TestSentinelsTable_ProducibleFromDocumentedEntry produces every classified
// sentinel from its documented entry point and asserts the returned error
// matches it with errors.Is. A sentinel with no reachable path is dead surface
// and must be removed before the freeze.
func TestSentinelsTable_ProducibleFromDocumentedEntry(t *testing.T) {
	for _, spec := range sentinelTaxonomy {
		t.Run(spec.name, func(t *testing.T) {
			if spec.produce == nil {
				t.Fatalf("%s has no producer", spec.name)
			}
			err := spec.produce(t)
			if err == nil {
				t.Fatalf("%s: producer from %s returned nil, want the sentinel", spec.name, spec.entry)
			}
			if !errors.Is(err, spec.err) {
				t.Fatalf("%s: producer from %s returned %v, want it to match the sentinel", spec.name, spec.entry, err)
			}
		})
	}
}

// TestSentinelClassification_Example asserts the documented caller pattern:
// match the sentinel with errors.Is and read its class off the taxonomy. The
// README's classification example is this switch.
func TestSentinelClassification_Example(t *testing.T) {
	byName := make(map[string]sentinelSpec, len(sentinelTaxonomy))
	for _, spec := range sentinelTaxonomy {
		byName[spec.name] = spec
	}

	cases := []struct {
		name      string
		class     sentinelClass
		retryable bool
	}{
		{"ErrMembershipBusy", classTransient, true},
		{"ErrDuplicateName", classPermanent, false},
		{"ErrAlreadyStarted", classTerminal, false},
		{"ErrStopTimeout", classEnvironmental, false},
	}
	for _, tc := range cases {
		spec, ok := byName[tc.name]
		if !ok {
			t.Fatalf("taxonomy is missing %s", tc.name)
		}
		if spec.class != tc.class {
			t.Errorf("%s: class = %q, want %q", tc.name, spec.class, tc.class)
		}
		if got := spec.class.retryable(); got != tc.retryable {
			t.Errorf("%s: retryable = %v, want %v", tc.name, got, tc.retryable)
		}
	}

	// A sentinel produced by a real call classifies through errors.Is.
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	err := o.StopService("missing", time.Second)
	if got := classifySentinel(err); got != classPermanent {
		t.Fatalf("StopService(unknown) classified %q, want permanent (err=%v)", got, err)
	}

	// A non-sentinel error classifies as the empty class.
	if got := classifySentinel(errors.New("not a gorch sentinel")); got != "" {
		t.Fatalf("classifySentinel(unrelated) = %q, want empty", got)
	}
}

// classifySentinel returns the class of the first taxonomy sentinel err matches,
// or "" when err matches none. A joined error can match several sentinels; the
// first match in taxonomy order wins.
func classifySentinel(err error) sentinelClass {
	for _, spec := range sentinelTaxonomy {
		if errors.Is(err, spec.err) {
			return spec.class
		}
	}
	return ""
}

// ── Deterministic producers, one per sentinel ──

// startedOrchestrator returns a started orchestrator; the caller registers its
// own Stop cleanup.
func startedOrchestrator(t *testing.T) *Orchestrator {
	t.Helper()
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return o
}

func produceErrAlreadyStarted(t *testing.T) error {
	t.Helper()
	o := New()
	if err := o.Start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	return o.Start()
}

func produceErrInvalidCron(t *testing.T) error {
	t.Helper()
	// Register validates the cron spec on both the static and the hot-add path.
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	return o.Register(&namedSvc{}, WithName("bad"), WithCron("invalid", CronParallel))
}

func produceErrStopTimeout(t *testing.T) error {
	t.Helper()
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	svc := &testSvc{
		startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		stopFn:  func() error { <-release; return nil },
	}
	if err := o.Register(svc, WithName("s")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		_ = o.Stop(time.Second)
	})
	return o.StopService("s", 50*time.Millisecond)
}

func produceErrDuplicateName(t *testing.T) error {
	t.Helper()
	o := New()
	if err := o.Register(&namedSvc{}, WithName("dup")); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	return o.Register(&namedSvc{}, WithName("dup"))
}

func produceErrNilService(t *testing.T) error {
	t.Helper()
	return New().Register(nil, WithName("nil"))
}

func produceErrHookTimeout(t *testing.T) error {
	t.Helper()
	o := New(WithHealthChecksDisabled())
	release := make(chan struct{})
	svc := &testSvc{startFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := o.Register(svc, WithName("s"), WithOnBeforeStop(func(string) error {
		<-release
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		_ = o.Stop(time.Second)
	})
	return o.StopService("s", 100*time.Millisecond)
}

func produceErrDependencyCycle(t *testing.T) error {
	t.Helper()
	return New().Register(&namedSvc{}, WithName("a"), DependsOn("a"))
}

func produceErrStartAborted(t *testing.T) error {
	t.Helper()
	o := New(WithLogLevel(LogLevelWarn))
	clean := &testSvc{startFn: func(context.Context) error { return nil }}
	if err := o.Register(clean, WithName("a"), WithStartTimeout(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&testSvc{}, WithName("b"), DependsOn("a")); err != nil {
		t.Fatal(err)
	}
	return o.Start()
}

func produceErrUnsupportedOption(t *testing.T) error {
	t.Helper()
	return New().Register(&namedSvc{}, WithName("x"),
		WithSelfHeal(func() Service { return &namedSvc{} }),
		WithCron("* * * * * *", CronParallel))
}

func produceErrServiceNotFound(t *testing.T) error {
	t.Helper()
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	return o.StopService("missing", time.Second)
}

func produceErrHasDependents(t *testing.T) error {
	t.Helper()
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	if err := o.Register(&namedSvc{}, WithName("dep")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("child"), DependsOn("dep")); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("dep"); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("child"); err != nil {
		t.Fatal(err)
	}
	return o.StopService("dep", time.Second)
}

func produceErrOrchestratorStopping(t *testing.T) error {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	svc := &testSvc{stopFn: func() error {
		close(entered)
		<-release
		return nil
	}}
	o := New()
	if err := o.Register(svc, WithName("blocker")); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- o.Stop(5 * time.Second) }()
	<-entered // Stop is under way: the stopping flag is set.

	err := o.Register(&namedSvc{}, WithName("during"))
	close(release)
	if serr := <-stopDone; serr != nil {
		t.Fatalf("Stop: %v", serr)
	}
	return err
}

func produceErrOrchestratorStopped(t *testing.T) error {
	t.Helper()
	o := New()
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	if err := o.Stop(time.Second); err != nil {
		t.Fatal(err)
	}
	return o.Register(&namedSvc{}, WithName("after"))
}

func produceErrOrchestratorNotStarted(t *testing.T) error {
	t.Helper()
	o := New()
	if err := o.Register(&namedSvc{}, WithName("n")); err != nil {
		t.Fatal(err)
	}
	return o.StartService("n")
}

func produceErrDependencyNotRunning(t *testing.T) error {
	t.Helper()
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	if err := o.Register(&namedSvc{}, WithName("dep")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("child"), DependsOn("dep")); err != nil {
		t.Fatal(err)
	}
	return o.StartService("child")
}

func produceErrDependencyNotFound(t *testing.T) error {
	t.Helper()
	return New().Register(&namedSvc{}, WithName("child"), DependsOn("missing"))
}

func produceErrDependencyRemoving(t *testing.T) error {
	t.Helper()
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = o.Stop(5 * time.Second) })

	stopEntered := make(chan struct{})
	releaseStop := make(chan struct{})
	dep := &testSvc{stopFn: func() error {
		close(stopEntered)
		<-releaseStop
		return nil
	}}
	if err := o.Register(dep, WithName("dep")); err != nil {
		t.Fatal(err)
	}
	if err := o.StartService("dep"); err != nil {
		t.Fatal(err)
	}

	unregDone := make(chan error, 1)
	go func() { unregDone <- o.Unregister("dep", 5*time.Second) }()
	// dep.Stop() only runs after tearDown set the entry's removing flag, so
	// waiting for it guarantees the flag is observable.
	<-stopEntered

	err := o.Register(&namedSvc{}, WithName("child"), DependsOn("dep"))
	close(releaseStop)
	if uerr := <-unregDone; uerr != nil {
		t.Fatalf("Unregister: %v", uerr)
	}
	return err
}

func produceErrDependencyDepthExceeded(t *testing.T) error {
	t.Helper()
	o := New(WithLogLevel(LogLevelWarn))
	top := insertDepChain(o, maxDependencyDepth+16)
	return o.Register(&testSvc{}, WithName("leaf"), DependsOn(top))
}

func produceErrReentrantMembership(t *testing.T) error {
	t.Helper()
	o := New()
	var innerErr error
	svc := &testSvc{startFn: func(context.Context) error {
		innerErr = o.StartService("gate")
		return nil
	}}
	if err := o.Register(svc, WithName("gate"), WithRunOnce()); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	return innerErr
}

func produceErrMembershipBusy(t *testing.T) error {
	t.Helper()
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })
	if err := o.Register(&namedSvc{}, WithName("r")); err != nil {
		t.Fatal(err)
	}
	entry := entryNamed(t, o, "r")
	entry.starting.Store(true)
	t.Cleanup(func() { entry.starting.Store(false) })
	return o.StartService("r")
}

func produceErrInvalidBufferSize(t *testing.T) error {
	t.Helper()
	m := newMessenger()
	_, _, err := m.SubscribeWithBuffer("topic", -1)
	return err
}

func produceErrNilContext(t *testing.T) error {
	t.Helper()
	m := newMessenger()
	_, err := m.Request(nilCtx, "req", "topic")
	return err
}
