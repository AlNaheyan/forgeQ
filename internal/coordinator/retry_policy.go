package coordinator

import (
	"math"
	"math/rand"
	"time"
)

const (
	DefaultRetryBaseDelay      = 2 * time.Second
	DefaultRetryMaxDelay       = 60 * time.Second
	DefaultRetryJitterFraction = 0.20
)

// RetryPolicy calculates a capped exponential retry delay. Jitter is additive
// and is sampled from [0, exponentialDelay*jitterFraction], so retries never
// happen before the configured exponential delay.
type RetryPolicy struct {
	baseDelay      time.Duration
	maxDelay       time.Duration
	jitterFraction float64
	random         func() float64
}

// NewRetryPolicy creates a retry policy. Non-positive delays use the project
// defaults, maxDelay is raised to baseDelay when necessary, and jitterFraction
// is clamped to [0, 1]. Supplying random makes tests and simulations
// deterministic; nil uses the concurrency-safe package-level PRNG.
func NewRetryPolicy(
	baseDelay time.Duration,
	maxDelay time.Duration,
	jitterFraction float64,
	random func() float64,
) RetryPolicy {
	if baseDelay <= 0 {
		baseDelay = DefaultRetryBaseDelay
	}
	if maxDelay <= 0 {
		maxDelay = DefaultRetryMaxDelay
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}
	if math.IsNaN(jitterFraction) || jitterFraction < 0 {
		jitterFraction = 0
	}
	if jitterFraction > 1 {
		jitterFraction = 1
	}
	if random == nil {
		random = rand.Float64
	}

	return RetryPolicy{
		baseDelay:      baseDelay,
		maxDelay:       maxDelay,
		jitterFraction: jitterFraction,
		random:         random,
	}
}

// Delay returns baseDelay*2^(attempt-1), plus bounded additive jitter, capped
// at maxDelay. Attempt values below one are treated as the first attempt.
func (p RetryPolicy) Delay(attempt int) time.Duration {
	baseDelay := p.baseDelay
	if baseDelay <= 0 {
		baseDelay = DefaultRetryBaseDelay
	}
	maxDelay := p.maxDelay
	if maxDelay <= 0 {
		maxDelay = DefaultRetryMaxDelay
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}

	exponential := baseDelay
	for step := 1; step < attempt && exponential < maxDelay; step++ {
		if exponential > maxDelay/2 {
			exponential = maxDelay
			break
		}
		exponential *= 2
	}
	if exponential >= maxDelay {
		return maxDelay
	}

	jitterFraction := p.jitterFraction
	if math.IsNaN(jitterFraction) || jitterFraction <= 0 {
		return exponential
	}
	if jitterFraction > 1 {
		jitterFraction = 1
	}
	random := p.random
	if random == nil {
		random = rand.Float64
	}
	sample := random()
	if math.IsNaN(sample) || sample < 0 {
		sample = 0
	}
	if sample > 1 {
		sample = 1
	}

	// Floating point is used only to select a fraction of a duration. Clamp
	// against the remaining headroom before conversion to avoid overflow.
	jitter := time.Duration(float64(exponential) * jitterFraction * sample)
	if jitter >= maxDelay-exponential {
		return maxDelay
	}
	return exponential + jitter
}

// DueAt returns the durable scheduled_at value for a failure on attempt.
func (p RetryPolicy) DueAt(now time.Time, attempt int) time.Time {
	return now.Add(p.Delay(attempt))
}
