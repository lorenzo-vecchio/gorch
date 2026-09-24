package gorch

import "fmt"

// panicToError converts a panic raised by caller-supplied code into an error so
// it cannot unwind through a public entry point of the orchestrator. Every
// exported entry point that invokes user code (lifecycle hooks, validators,
// start conditions, health and readiness probes) routes it through one of the
// safe call helpers below.
func panicToError(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("gorch: user code panicked: %v", r)
	}
}

// callErr invokes fn, turning a panic into an error.
func callErr(fn func() error) (err error) {
	defer panicToError(&err)
	return fn()
}

// callBool invokes fn, turning a panic into an error with ok false.
func callBool(fn func() bool) (ok bool, err error) {
	defer panicToError(&err)
	return fn(), nil
}

// callVoid invokes fn, discarding a panic: callers of a notification hook have
// no error channel to report it through.
func callVoid(fn func()) {
	defer func() { _ = recover() }()
	fn()
}
