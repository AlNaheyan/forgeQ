package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestSleepHandlerCompletesAndCancels(t *testing.T) {
	handler := SleepHandler{}
	result, err := handler.Execute(context.Background(), json.RawMessage(`{"duration_ms":1}`))
	if err != nil || result == nil {
		t.Fatalf("sleep execution: result=%v err=%v", result, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = handler.Execute(ctx, json.RawMessage(`{"duration_ms":1000}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestFailHandlerUsesAttemptNumber(t *testing.T) {
	handler := FailHandler{}
	payload := json.RawMessage(`{"failures_before_success":2}`)
	if _, err := handler.Execute(withAttempt(context.Background(), 2), payload); err == nil {
		t.Fatal("attempt two should fail")
	}
	result, err := handler.Execute(withAttempt(context.Background(), 3), payload)
	if err != nil || result == nil {
		t.Fatalf("attempt three should succeed: result=%v err=%v", result, err)
	}
}

func TestExecuteSafelyConvertsPanicToError(t *testing.T) {
	_, err := executeSafely(context.Background(), panicHandler{}, json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("expected panic to become an error")
	}
}

type panicHandler struct{}

func (panicHandler) Execute(context.Context, json.RawMessage) (any, error) {
	panic("boom")
}

func TestSleepHandlerRejectsExcessiveDuration(t *testing.T) {
	_, err := (SleepHandler{}).Execute(context.Background(), json.RawMessage(`{"duration_ms":3600001}`))
	if err == nil {
		t.Fatal("expected duration validation error")
	}
	if deadline, ok := t.Deadline(); ok && time.Until(deadline) <= 0 {
		t.Fatal("test deadline unexpectedly elapsed")
	}
}
