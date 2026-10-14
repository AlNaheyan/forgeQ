package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type SleepHandler struct{}

func (SleepHandler) Execute(ctx context.Context, payload json.RawMessage) (any, error) {
	var request struct {
		DurationMS int64 `json:"duration_ms"`
	}
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, fmt.Errorf("decode sleep payload: %w", err)
	}
	if request.DurationMS < 0 || request.DurationMS > int64(time.Hour/time.Millisecond) {
		return nil, errors.New("duration_ms must be between 0 and 3600000")
	}
	duration := time.Duration(request.DurationMS) * time.Millisecond
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return map[string]any{"slept_ms": request.DurationMS}, nil
	}
}

type FailHandler struct{}

func (FailHandler) Execute(ctx context.Context, payload json.RawMessage) (any, error) {
	var request struct {
		FailuresBeforeSuccess int `json:"failures_before_success"`
	}
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, fmt.Errorf("decode fail payload: %w", err)
	}
	if request.FailuresBeforeSuccess < 0 {
		return nil, errors.New("failures_before_success must not be negative")
	}
	attempt := attemptFromContext(ctx)
	if attempt <= request.FailuresBeforeSuccess {
		return nil, fmt.Errorf("intentional failure on attempt %d of %d", attempt, request.FailuresBeforeSuccess)
	}
	return map[string]any{"succeeded_on_attempt": attempt}, nil
}

func BuiltInHandlers() map[string]Handler {
	return map[string]Handler{
		"sleep": SleepHandler{},
		"fail":  FailHandler{},
	}
}
