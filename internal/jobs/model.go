package jobs

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"forgequeue/internal/jobtypes"
	"strings"
	"time"
)

type Status string

const (
	StatusQueued     Status = "queued"
	StatusLeased     Status = "leased"
	StatusRunning    Status = "running"
	StatusRetryWait  Status = "retry_wait"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
	StatusDeadLetter Status = "dead_letter"
	StatusCanceled   Status = "canceled"
)

type Job struct {
	TenantID        string          `json:"tenant_id"`
	ID              string          `json:"job_id"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	Status          Status          `json:"status"`
	Priority        int             `json:"priority"`
	AttemptCount    int             `json:"attempt_count"`
	MaxAttempts     int             `json:"max_attempts"`
	ScheduledAt     time.Time       `json:"scheduled_at"`
	LeaseOwner      string          `json:"lease_owner,omitempty"`
	LeaseGeneration int64           `json:"lease_generation"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at,omitempty"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	Result          json.RawMessage `json:"result,omitempty"`
	LastError       string          `json:"last_error,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type CreateInput struct {
	Type           string
	Payload        json.RawMessage
	Priority       int
	MaxAttempts    int
	IdempotencyKey string
}

type ListFilter struct {
	Status Status
	Type   string
	Limit  int
	Cursor string
}

func New(input CreateInput, now time.Time) (Job, error) {
	return NewWithRegistry(input, now, jobtypes.Fixtures())
}

// NewWithRegistry validates against an immutable registry chosen at startup.
func NewWithRegistry(input CreateInput, now time.Time, registry *jobtypes.Registry) (Job, error) {
	input.Type = strings.TrimSpace(input.Type)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if err := ValidateInputWithRegistry(input, registry); err != nil {
		return Job{}, err
	}

	id, err := newID()
	if err != nil {
		return Job{}, fmt.Errorf("generate job ID: %w", err)
	}
	now = now.UTC()
	return Job{
		ID:             id,
		Type:           input.Type,
		Payload:        append(json.RawMessage(nil), input.Payload...),
		Status:         StatusQueued,
		Priority:       input.Priority,
		MaxAttempts:    input.MaxAttempts,
		ScheduledAt:    now,
		IdempotencyKey: input.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

func ValidateInput(input CreateInput) error {
	return ValidateInputWithRegistry(input, jobtypes.Fixtures())
}

func ValidateInputWithRegistry(input CreateInput, registry *jobtypes.Registry) error {
	if _, ok := registry.Lookup(input.Type); !ok {
		return fmt.Errorf("%w: unsupported type %q", ErrInvalidJob, input.Type)
	}
	if len(input.Payload) == 0 || !json.Valid(input.Payload) {
		return fmt.Errorf("%w: payload must be valid JSON", ErrInvalidJob)
	}
	var payloadObject map[string]json.RawMessage
	if err := json.Unmarshal(input.Payload, &payloadObject); err != nil {
		return fmt.Errorf("%w: payload must be a JSON object", ErrInvalidJob)
	}
	if payloadObject == nil {
		return fmt.Errorf("%w: payload must be a JSON object", ErrInvalidJob)
	}
	if err := registry.Validate(input.Type, input.Payload); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidJob, err)
	}
	if input.MaxAttempts < 1 || input.MaxAttempts > 100 {
		return fmt.Errorf("%w: max_attempts must be between 1 and 100", ErrInvalidJob)
	}
	if input.Priority < -100 || input.Priority > 100 {
		return fmt.Errorf("%w: priority must be between -100 and 100", ErrInvalidJob)
	}
	if len(input.IdempotencyKey) > 255 {
		return fmt.Errorf("%w: idempotency_key must not exceed 255 characters", ErrInvalidJob)
	}
	return nil
}

func SupportedType(jobType string) bool {
	_, ok := jobtypes.Fixtures().Lookup(jobType)
	return ok
}

func CanTransition(from, to Status) bool {
	switch from {
	case StatusQueued:
		return to == StatusLeased || to == StatusRunning || to == StatusCanceled
	case StatusLeased:
		return to == StatusRunning || to == StatusQueued || to == StatusCanceled
	case StatusRunning:
		return to == StatusSucceeded || to == StatusFailed || to == StatusRetryWait || to == StatusCanceled
	case StatusRetryWait:
		return to == StatusQueued || to == StatusDeadLetter || to == StatusCanceled
	case StatusFailed:
		return to == StatusRetryWait || to == StatusDeadLetter
	case StatusDeadLetter:
		return to == StatusQueued
	default:
		return false
	}
}

func ValidStatus(status Status) bool {
	switch status {
	case StatusQueued, StatusLeased, StatusRunning, StatusRetryWait,
		StatusSucceeded, StatusFailed, StatusDeadLetter, StatusCanceled:
		return true
	default:
		return false
	}
}

func newID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32], nil
}
