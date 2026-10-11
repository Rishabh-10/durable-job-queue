package backoff

import (
	"math"
	"testing"
	"time"
)

// fixed pins the jitter so the exponential curve underneath can be asserted.
func fixed(p Policy, at float64) Policy {
	p.rand = func() float64 { return at }
	return p
}

func TestDelayDoublesUntilCap(t *testing.T) {
	// Jitter at its maximum exposes the un-jittered ceiling.
	p := fixed(New(time.Second, time.Minute), 1)

	tests := []struct {
		attempts int32
		want     time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
		{6, 32 * time.Second},
		{7, time.Minute}, // 64s would exceed the cap
		{8, time.Minute},
	}

	for _, tc := range tests {
		if got := p.Delay(tc.attempts); got != tc.want {
			t.Errorf("Delay(%d) = %s, want %s", tc.attempts, got, tc.want)
		}
	}
}

func TestDelayNeverExceedsCap(t *testing.T) {
	p := New(time.Second, 30*time.Second)

	for attempts := int32(1); attempts < 200; attempts++ {
		if got := p.Delay(attempts); got > p.Cap {
			t.Fatalf("Delay(%d) = %s, want at most the cap %s", attempts, got, p.Cap)
		}
	}
}

// A huge attempt count must saturate at the cap rather than overflowing int64
// and wrapping round to a tiny delay.
func TestDelayDoesNotOverflow(t *testing.T) {
	p := fixed(New(time.Hour, 24*time.Hour), 1)

	for _, attempts := range []int32{62, 63, 64, 1000, math.MaxInt32} {
		got := p.Delay(attempts)
		if got != 24*time.Hour {
			t.Errorf("Delay(%d) = %s, want the cap %s", attempts, got, 24*time.Hour)
		}
	}
}

func TestDelayIsNeverNegative(t *testing.T) {
	p := New(time.Second, time.Hour)

	for attempts := int32(-5); attempts < 50; attempts++ {
		if got := p.Delay(attempts); got < 0 {
			t.Errorf("Delay(%d) = %s, want a non-negative delay", attempts, got)
		}
	}
}

// Full jitter spreads each delay across [0, ceiling]. Both ends of that range
// have to be reachable, or the herd is not actually broken up.
func TestJitterSpansTheWholeWindow(t *testing.T) {
	base := time.Second
	p := New(base, time.Hour)

	if got := fixed(p, 0).Delay(1); got != 0 {
		t.Errorf("Delay with minimum jitter = %s, want 0", got)
	}
	if got := fixed(p, 1).Delay(1); got != base {
		t.Errorf("Delay with maximum jitter = %s, want %s", got, base)
	}
}

// Two jobs that failed in the same outage must not retry at the same instant.
func TestJitterSeparatesConcurrentFailures(t *testing.T) {
	p := New(time.Minute, time.Hour)

	const samples = 200
	seen := make(map[time.Duration]int, samples)
	for range samples {
		seen[p.Delay(5)]++
	}

	// With a 16-minute window and nanosecond resolution, collisions mean the
	// jitter is not being applied at all.
	if len(seen) < samples/2 {
		t.Errorf("%d distinct delays across %d samples, want them spread out", len(seen), samples)
	}
}

func TestNewRepairsNonsenseSettings(t *testing.T) {
	tests := []struct {
		name           string
		base, capped   time.Duration
		wantBase       time.Duration
		wantCapAtLeast time.Duration
	}{
		{"zero base", 0, time.Minute, time.Second, time.Minute},
		{"negative base", -time.Second, time.Minute, time.Second, time.Minute},
		{"cap below base", 10 * time.Second, time.Second, 10 * time.Second, 10 * time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := New(tc.base, tc.capped)
			if p.Base != tc.wantBase {
				t.Errorf("Base = %s, want %s", p.Base, tc.wantBase)
			}
			if p.Cap < tc.wantCapAtLeast {
				t.Errorf("Cap = %s, want at least %s", p.Cap, tc.wantCapAtLeast)
			}
			if p.Cap < p.Base {
				t.Errorf("Cap %s is below Base %s, which would invert the curve", p.Cap, p.Base)
			}
		})
	}
}

func TestRetryAt(t *testing.T) {
	p := fixed(New(time.Second, time.Minute), 1)
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	if got, want := p.RetryAt(now, 3), now.Add(4*time.Second); !got.Equal(want) {
		t.Errorf("RetryAt = %s, want %s", got, want)
	}
}
