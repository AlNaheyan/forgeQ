package coordinator

import (
	"testing"
	"time"
)

func TestRetryPolicyUsesCappedExponentialBackoff(t *testing.T) {
	policy := NewRetryPolicy(2*time.Second, 10*time.Second, 0, func() float64 { return 1 })

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: -3, want: 2 * time.Second},
		{attempt: 0, want: 2 * time.Second},
		{attempt: 1, want: 2 * time.Second},
		{attempt: 2, want: 4 * time.Second},
		{attempt: 3, want: 8 * time.Second},
		{attempt: 4, want: 10 * time.Second},
		{attempt: 1_000_000, want: 10 * time.Second},
	}
	for _, test := range tests {
		if got := policy.Delay(test.attempt); got != test.want {
			t.Errorf("Delay(%d) = %s, want %s", test.attempt, got, test.want)
		}
	}
}

func TestRetryPolicyAddsDeterministicJitter(t *testing.T) {
	policy := NewRetryPolicy(2*time.Second, time.Minute, 0.25, func() float64 { return 0.5 })

	if got, want := policy.Delay(1), 2250*time.Millisecond; got != want {
		t.Fatalf("Delay(1) = %s, want %s", got, want)
	}
	if got, want := policy.Delay(2), 4500*time.Millisecond; got != want {
		t.Fatalf("Delay(2) = %s, want %s", got, want)
	}
}

func TestRetryPolicyCapsDelayAfterJitter(t *testing.T) {
	policy := NewRetryPolicy(8*time.Second, 10*time.Second, 1, func() float64 { return 1 })

	if got := policy.Delay(1); got != 10*time.Second {
		t.Fatalf("Delay(1) = %s, want 10s", got)
	}
}

func TestRetryPolicyClampsJitterInputs(t *testing.T) {
	negativeSample := NewRetryPolicy(time.Second, 10*time.Second, 0.5, func() float64 { return -10 })
	if got := negativeSample.Delay(1); got != time.Second {
		t.Fatalf("negative sample delay = %s, want 1s", got)
	}

	overSampleAndFraction := NewRetryPolicy(time.Second, 10*time.Second, 12, func() float64 { return 12 })
	if got := overSampleAndFraction.Delay(1); got != 2*time.Second {
		t.Fatalf("clamped sample delay = %s, want 2s", got)
	}
}

func TestRetryPolicyDefaultsAndDueAt(t *testing.T) {
	policy := NewRetryPolicy(0, 0, -1, func() float64 { return 1 })
	now := time.Date(2026, time.October, 14, 12, 0, 0, 0, time.UTC)

	if got, want := policy.Delay(1), DefaultRetryBaseDelay; got != want {
		t.Fatalf("default Delay(1) = %s, want %s", got, want)
	}
	if got, want := policy.DueAt(now, 2), now.Add(2*DefaultRetryBaseDelay); !got.Equal(want) {
		t.Fatalf("DueAt = %s, want %s", got, want)
	}
}
