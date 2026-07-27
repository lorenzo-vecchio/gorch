package gorch

import "time"

// Backoff computes the delay before the next retry attempt.
// retry is 1-based (first retry = 1).
type Backoff interface {
	Next(retry int) time.Duration
}

// ExponentialBackoff produces delays: initial * factor^(retry-1), capped at max.
// ponytail: no jitter; add WithJitter(bool) if thundering-herd becomes a problem.
type ExponentialBackoff struct {
	Initial time.Duration
	Max     time.Duration
	Factor  float64
}

// Next returns initial * factor^(retry-1), capped at Max.
func (b ExponentialBackoff) Next(retry int) time.Duration {
	if retry <= 0 {
		retry = 1
	}
	d := b.Initial
	for i := 1; i < retry; i++ {
		d = time.Duration(float64(d) * b.Factor)
	}
	if d > b.Max {
		d = b.Max
	}
	return d
}

// ConstantBackoff always returns the same delay regardless of retry count.
type ConstantBackoff struct {
	Delay time.Duration
}

// Next returns Delay (ignores retry count).
func (b ConstantBackoff) Next(retry int) time.Duration {
	return b.Delay
}
