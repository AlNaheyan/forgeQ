// Package eventstream defines ForgeQueue's durable audit event contract.
package eventstream

import (
	"context"
	"encoding/json"
	"time"
)

type Type string

const (
	JobCreated       Type = "JobCreated"
	JobLeased        Type = "JobLeased"
	JobStarted       Type = "JobStarted"
	JobSucceeded     Type = "JobSucceeded"
	JobFailed        Type = "JobFailed"
	JobRetried       Type = "JobRetried"
	JobDeadLettered  Type = "JobDeadLettered"
	WorkerRegistered Type = "WorkerRegistered"
	WorkerLost       Type = "WorkerLost"
)

// Event is an immutable envelope read from the PostgreSQL outbox. Attempts is
// internal delivery metadata and is intentionally omitted from Kafka records.
type Event struct {
	ID            int64           `json:"event_id"`
	Type          Type            `json:"event_type"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
	Attempts      int             `json:"-"`
}

type Recorder interface {
	RecordEvent(context.Context, Type, string, string, json.RawMessage) error
}

type Outbox interface {
	ClaimEvents(context.Context, string, int, time.Duration) ([]Event, error)
	MarkEventPublished(context.Context, string, int64) error
	RetryEvent(context.Context, string, int64, time.Duration, error) error
}

type Writer interface {
	WriteEvent(context.Context, Event) error
	Close() error
}
