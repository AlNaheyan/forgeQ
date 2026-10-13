package jobs

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestCanTransition(t *testing.T) {
	tests := []struct {
		name string
		from Status
		to   Status
		want bool
	}{
		{name: "queued to running", from: StatusQueued, to: StatusRunning, want: true},
		{name: "running to succeeded", from: StatusRunning, to: StatusSucceeded, want: true},
		{name: "retry to queued", from: StatusRetryWait, to: StatusQueued, want: true},
		{name: "succeeded is terminal", from: StatusSucceeded, to: StatusRunning, want: false},
		{name: "canceled is terminal", from: StatusCanceled, to: StatusQueued, want: false},
		{name: "cannot skip from queued to success", from: StatusQueued, to: StatusSucceeded, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CanTransition(tt.from, tt.to); got != tt.want {
				t.Fatalf("CanTransition(%q, %q) = %v, want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func TestNewValidatesInputAndAppliesInitialState(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("test", 3600))
	job, err := New(CreateInput{
		Type:        "sleep",
		Payload:     json.RawMessage(`{"duration_ms":100}`),
		MaxAttempts: 3,
	}, now)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if job.ID == "" || job.Status != StatusQueued || !job.CreatedAt.Equal(now.UTC()) {
		t.Fatalf("unexpected job: %+v", job)
	}

	_, err = New(CreateInput{Type: "unknown", Payload: json.RawMessage(`{}`), MaxAttempts: 3}, now)
	if !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("expected ErrInvalidJob, got %v", err)
	}
}
