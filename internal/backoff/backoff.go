// Package backoff computes the delay between a failed attempt and the next one.
package backoff

import (
	"math"
	"math/rand/v2"
	"time"
)

// Policy is an exponential backoff with full jitter.
//
// The jitter is not decoration. Without it, every job that failed during the
// same downstream outage retries at the same instant when that outage ends, and
// the recovering service is knocked over by the recovery. Spreading each delay
// uniformly across [0, exponential] breaks up the herd at the cost of some
// retries happening sooner than the curve alone would suggest.
type Policy struct {
	// Base is the delay after the first failure, before jitter.
	Base time.Duration
	// Cap bounds the exponential growth.
	Cap time.Duration

	// rand is injectable so tests can pin the jitter. Nil means the global
	// source, which is safe for concurrent use.
	rand func() float64
}

// New builds a policy. A non-positive base or cap falls back to values that
// keep the curve meaningful rather than producing instant retry storms.
func New(base, capped time.Duration) Policy {
	if base <= 0 {
		base = time.Second
	}
	if capped < base {
		capped = base
	}
	return Policy{Base: base, Cap: capped}
}

// Delay returns how long to wait before attempt number attempt+1, given that
// attempt attempts have now failed. The first failure produces a delay in
// [0, Base], the second [0, 2*Base], and so on up to Cap.
func (p Policy) Delay(attempts int32) time.Duration {
	if attempts < 1 {
		attempts = 1
	}

	ceiling := p.ceiling(attempts)
	jitter := p.random()
	return time.Duration(jitter * float64(ceiling))
}

// RetryAt is Delay applied to a point in time, which is what the store wants.
func (p Policy) RetryAt(now time.Time, attempts int32) time.Time {
	return now.Add(p.Delay(attempts))
}

// ceiling is the un-jittered exponential delay: Base * 2^(attempts-1), bounded
// by Cap. The shift is computed in float64 so a large attempt count saturates
// at Cap instead of overflowing back to a small number.
func (p Policy) ceiling(attempts int32) time.Duration {
	exponent := float64(attempts - 1)

	// Past this point the result exceeds any sane Cap anyway, and the
	// multiplication would overflow int64.
	const maxExponent = 62
	if exponent > maxExponent {
		return p.Cap
	}

	scaled := float64(p.Base) * math.Pow(2, exponent)
	if scaled > float64(p.Cap) || scaled > math.MaxInt64 {
		return p.Cap
	}
	return time.Duration(scaled)
}

func (p Policy) random() float64 {
	if p.rand != nil {
		return p.rand()
	}
	return rand.Float64()
}
