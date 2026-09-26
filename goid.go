package gorch

import "runtime"

// curGoroutineID returns the id of the calling goroutine. gorch uses it to tell
// a genuine same-goroutine re-entry (a service's own Start/Stop callback calling
// a membership op on itself) from a concurrent reservation collision on another
// goroutine: the atomic reservation flags say an operation is in flight, but
// only the goroutine identity says who owns it.
//
// The id is parsed from the first line of runtime.Stack's output, which is
// "goroutine <id> [...]", without allocating a string or an error path.
func curGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	var id uint64
	for _, b := range buf[:n] {
		if b < '0' || b > '9' {
			if id != 0 {
				break
			}
			continue
		}
		id = id*10 + uint64(b-'0')
	}
	return id
}
