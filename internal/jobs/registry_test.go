package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobtypes"
)

func TestSubmissionUsesSelectedRegistry(t *testing.T) {
	d := jobtypes.Descriptor{Name: "custom.v1", MaxPayloadBytes: 10, Validate: func(data json.RawMessage) error {
		if string(data) != `{}` {
			return errors.New("invalid payload")
		}
		return nil
	}}
	r, err := jobtypes.NewRegistry([]jobtypes.Descriptor{d})
	if err != nil {
		t.Fatal(err)
	}
	input := CreateInput{Type: "custom.v1", Payload: json.RawMessage(`{}`), MaxAttempts: 1}
	if _, err = NewWithRegistry(input, time.Now(), r); err != nil {
		t.Fatal(err)
	}
	if _, err = New(input, time.Now()); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("default registry accepted custom type: %v", err)
	}
	input.Payload = json.RawMessage(`{"x":1}`)
	if _, err = NewWithRegistry(input, time.Now(), r); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("custom validator not applied: %v", err)
	}
}

func TestApplicationTypesAcceptJobsWithoutRegisteredWorkers(t *testing.T) {
	r, err := jobtypes.ApplicationRegistry([]string{"invoice.send"})
	if err != nil {
		t.Fatal(err)
	}
	repo := &recordingRepository{}
	service := NewService(repo, &recordingEnqueuer{}, 3).WithJobTypes(r)
	result, err := service.Submit(context.Background(), CreateInput{Type: "invoice.send", Payload: json.RawMessage(`{"invoice_id":"123"}`)})
	if err != nil || !result.Created || repo.created.Type != "invoice.send" {
		t.Fatalf("custom submission=%+v err=%v", result, err)
	}
	if _, err := service.Submit(context.Background(), CreateInput{Type: "unknown", Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("unknown type accepted: %v", err)
	}
	if _, err := service.Submit(context.Background(), CreateInput{Type: "invoice.send", Payload: json.RawMessage(`[]`)}); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("non-object payload accepted: %v", err)
	}
	if _, err := jobtypes.ApplicationRegistry([]string{"sleep"}); err == nil {
		t.Fatal("built-in collision accepted")
	}
}
