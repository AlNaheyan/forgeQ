package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlNaheyan/forgeQ/internal/eventstream"
)

var ErrEventLeaseLost = errors.New("event outbox lease lost")

type EventOutboxRepository struct {
	db *sql.DB
}

func NewEventOutboxRepository(db *sql.DB) *EventOutboxRepository {
	return &EventOutboxRepository{db: db}
}

func (r *EventOutboxRepository) RecordEvent(ctx context.Context, eventType eventstream.Type, aggregateType, aggregateID string, payload json.RawMessage) error {
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		return errors.New("record event: payload must be valid JSON")
	}
	if _, err := r.db.ExecContext(ctx, "SELECT forgequeue_record_event($1, $2, $3, $4)", string(eventType), aggregateType, aggregateID, []byte(payload)); err != nil {
		return fmt.Errorf("record event %s: %w", eventType, err)
	}
	return nil
}

func (r *EventOutboxRepository) ClaimEvents(ctx context.Context, owner string, limit int, lease time.Duration) ([]eventstream.Event, error) {
	if strings.TrimSpace(owner) == "" || limit < 1 || lease <= 0 {
		return nil, errors.New("claim events requires owner, positive limit, and positive lease")
	}
	if limit > 1000 {
		limit = 1000
	}
	leaseMicros := lease.Microseconds()
	if leaseMicros < 1 {
		leaseMicros = 1
	}
	rows, err := r.db.QueryContext(ctx, `
		WITH candidates AS (
			SELECT e.id
			FROM event_outbox e
			WHERE e.published_at IS NULL
				AND e.next_attempt_at <= clock_timestamp()
				AND (e.lease_until IS NULL OR e.lease_until <= clock_timestamp())
				AND NOT EXISTS (
					SELECT 1 FROM event_outbox previous
					WHERE previous.aggregate_type = e.aggregate_type
						AND previous.aggregate_id = e.aggregate_id
						AND previous.id < e.id
						AND previous.published_at IS NULL
				)
			ORDER BY e.id
			FOR UPDATE OF e SKIP LOCKED
			LIMIT $2
		), claimed AS (
			UPDATE event_outbox e
			SET lease_owner = $1,
				lease_until = clock_timestamp() + ($3 * INTERVAL '1 microsecond'),
				attempts = attempts + 1
			FROM candidates c
			WHERE e.id = c.id
			RETURNING e.id, e.event_type, e.aggregate_type, e.aggregate_id, e.occurred_at, e.payload, e.attempts
		)
		SELECT id, event_type, aggregate_type, aggregate_id, occurred_at, payload, attempts
		FROM claimed
		ORDER BY id`, owner, limit, leaseMicros)
	if err != nil {
		return nil, fmt.Errorf("claim pending events: %w", err)
	}
	defer rows.Close()

	events := make([]eventstream.Event, 0, limit)
	for rows.Next() {
		var event eventstream.Event
		if err := rows.Scan(&event.ID, &event.Type, &event.AggregateType, &event.AggregateID, &event.OccurredAt, &event.Payload, &event.Attempts); err != nil {
			return nil, fmt.Errorf("scan claimed event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed events: %w", err)
	}
	return events, nil
}

func (r *EventOutboxRepository) MarkEventPublished(ctx context.Context, owner string, id int64) error {
	result, err := r.db.ExecContext(ctx, `
		UPDATE event_outbox
		SET published_at = clock_timestamp(), lease_owner = NULL, lease_until = NULL, last_error = NULL
		WHERE id = $1 AND lease_owner = $2 AND published_at IS NULL`, id, owner)
	if err != nil {
		return fmt.Errorf("mark event published: %w", err)
	}
	return requireOutboxRows(result)
}

func (r *EventOutboxRepository) RetryEvent(ctx context.Context, owner string, id int64, delay time.Duration, cause error) error {
	if delay < 0 {
		delay = 0
	}
	message := "event publish failed"
	if cause != nil {
		message = cause.Error()
	}
	message = truncateUTF8(message, 2000)
	result, err := r.db.ExecContext(ctx, `
		UPDATE event_outbox
		SET lease_owner = NULL,
			lease_until = NULL,
			next_attempt_at = clock_timestamp() + ($3 * INTERVAL '1 microsecond'),
			last_error = $4
		WHERE id = $1 AND lease_owner = $2 AND published_at IS NULL`, id, owner, delay.Microseconds(), message)
	if err != nil {
		return fmt.Errorf("schedule event retry: %w", err)
	}
	return requireOutboxRows(result)
}

func requireOutboxRows(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrEventLeaseLost
	}
	return nil
}

func truncateUTF8(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	value = value[:maxBytes]
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
