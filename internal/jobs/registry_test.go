package jobs

import (
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
