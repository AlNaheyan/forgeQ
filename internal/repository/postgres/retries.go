package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"forgequeue/internal/jobs"
)

// StartAttempt atomically starts a leased job and records the corresponding
// attempt. Any unfinished attempt left by a previously expired lease is closed
// as expired before the new attempt is inserted.
func (r *JobRepository) StartAttempt(ctx context.Context, id, workerID string) (jobs.Job, error) {
	attemptID, err := newAttemptID()
	if err != nil {
		return jobs.Job{}, fmt.Errorf("generate attempt ID: %w", err)
	}

	job, err := scanJob(r.db.QueryRowContext(ctx, `
		WITH started AS (
			UPDATE jobs
			SET status = $1,
				attempt_count = attempt_count + 1,
				updated_at = clock_timestamp()
			WHERE id = $2
				AND status = $3
				AND lease_owner = $4
				AND lease_expires_at > clock_timestamp()
			RETURNING *
		), expired_attempts AS (
			UPDATE job_attempts AS a
			SET status = 'expired',
				finished_at = clock_timestamp(),
				error = COALESCE(a.error, 'lease expired')
			FROM started AS s
			WHERE a.job_id = s.id
				AND a.finished_at IS NULL
			RETURNING a.id
		), inserted_attempt AS (
			INSERT INTO job_attempts (
				id, job_id, worker_id, attempt_number, started_at, status
			)
			SELECT $5, s.id, $4, s.attempt_count, s.updated_at, 'running'
			FROM started AS s
			RETURNING job_id
		)
		SELECT `+joinedJobColumns+`
		FROM started AS j
		JOIN inserted_attempt AS a ON a.job_id = j.id`,
		jobs.StatusRunning, id, jobs.StatusLeased, workerID, attemptID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("%w: lease is not active or is owned by another worker", jobs.ErrInvalidTransition)
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("start job attempt: %w", err)
	}
	return job, nil
}

// SucceedAttempt atomically closes the current attempt and records the
// successful job result. A stale worker cannot complete an expired lease.
func (r *JobRepository) SucceedAttempt(
	ctx context.Context,
	id string,
	workerID string,
	result []byte,
) (jobs.Job, error) {
	job, err := scanJob(r.db.QueryRowContext(ctx, `
		WITH succeeded AS (
			UPDATE jobs AS j
			SET status = $1,
				result = $2,
				last_error = NULL,
				lease_owner = NULL,
				lease_expires_at = NULL,
				updated_at = clock_timestamp()
			WHERE j.id = $3
				AND j.status = $4
				AND j.lease_owner = $5
				AND j.lease_expires_at > clock_timestamp()
				AND EXISTS (
					SELECT 1
					FROM job_attempts AS a
					WHERE a.job_id = j.id
						AND a.attempt_number = j.attempt_count
						AND a.worker_id = $5
						AND a.finished_at IS NULL
				)
			RETURNING j.*
		), closed_attempt AS (
			UPDATE job_attempts AS a
			SET status = 'succeeded',
				finished_at = s.updated_at,
				error = NULL
			FROM succeeded AS s
			WHERE a.job_id = s.id
				AND a.attempt_number = s.attempt_count
				AND a.worker_id = $5
				AND a.finished_at IS NULL
			RETURNING a.job_id
		)
		SELECT `+joinedJobColumns+`
		FROM succeeded AS j
		JOIN closed_attempt AS a ON a.job_id = j.id`,
		jobs.StatusSucceeded, nullableJSON(result), id, jobs.StatusRunning, workerID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("%w: running attempt is not active or is owned by another worker", jobs.ErrInvalidTransition)
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("succeed job attempt: %w", err)
	}
	return job, nil
}

// FailAttempt closes the current attempt and either schedules another attempt
// or dead-letters the job. Non-retryable failures and exhausted jobs are sent
// directly to dead_letter.
func (r *JobRepository) FailAttempt(
	ctx context.Context,
	id string,
	workerID string,
	lastError string,
	retryable bool,
	retryAt time.Time,
) (jobs.Job, error) {
	if retryable && retryAt.IsZero() {
		return jobs.Job{}, fmt.Errorf("%w: retry time is required", jobs.ErrInvalidTransition)
	}

	job, err := scanJob(r.db.QueryRowContext(ctx, `
		WITH failed AS (
			UPDATE jobs AS j
			SET status = CASE
					WHEN $1 AND j.attempt_count < j.max_attempts THEN $2
					ELSE $3
				END,
				scheduled_at = CASE
					WHEN $1 AND j.attempt_count < j.max_attempts THEN $4
					ELSE j.scheduled_at
				END,
				result = NULL,
				last_error = NULLIF($5, ''),
				lease_owner = NULL,
				lease_expires_at = NULL,
				updated_at = clock_timestamp()
			WHERE j.id = $6
				AND j.status = $7
				AND j.lease_owner = $8
				AND j.lease_expires_at > clock_timestamp()
				AND EXISTS (
					SELECT 1
					FROM job_attempts AS a
					WHERE a.job_id = j.id
						AND a.attempt_number = j.attempt_count
						AND a.worker_id = $8
						AND a.finished_at IS NULL
				)
			RETURNING j.*
		), closed_attempt AS (
			UPDATE job_attempts AS a
			SET status = 'failed',
				finished_at = f.updated_at,
				error = NULLIF($5, '')
			FROM failed AS f
			WHERE a.job_id = f.id
				AND a.attempt_number = f.attempt_count
				AND a.worker_id = $8
				AND a.finished_at IS NULL
			RETURNING a.job_id
		)
		SELECT `+joinedJobColumns+`
		FROM failed AS j
		JOIN closed_attempt AS a ON a.job_id = j.id`,
		retryable, jobs.StatusRetryWait, jobs.StatusDeadLetter, retryAt.UTC(),
		lastError, id, jobs.StatusRunning, workerID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("%w: running attempt is not active or is owned by another worker", jobs.ErrInvalidTransition)
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("fail job attempt: %w", err)
	}
	return job, nil
}

// RetryDeadLetter returns a dead-lettered job to the ready state without
// erasing attempt history. If its budget was exhausted, it grants exactly one
// additional attempt by raising max_attempts to attempt_count+1.
func (r *JobRepository) RetryDeadLetter(ctx context.Context, id string) (jobs.Job, error) {
	job, err := scanJob(r.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $1,
			max_attempts = GREATEST(max_attempts, attempt_count + 1),
			scheduled_at = clock_timestamp(),
			lease_owner = NULL,
			lease_expires_at = NULL,
			result = NULL,
			last_error = NULL,
			updated_at = clock_timestamp()
		WHERE id = $2 AND status = $3
		RETURNING `+jobColumns,
		jobs.StatusQueued, id, jobs.StatusDeadLetter,
	))
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("retry dead-lettered job: %w", err)
	}

	current, getErr := r.GetJob(ctx, id)
	if getErr != nil {
		return jobs.Job{}, getErr
	}
	return jobs.Job{}, fmt.Errorf("%w: cannot retry job in %s state", jobs.ErrInvalidTransition, current.Status)
}

// ListDueRetries returns retry_wait jobs whose backoff has elapsed, in stable
// due-time order. Promotion remains a separate compare-and-set operation so a
// concurrent cancel or promoter cannot be overwritten.
func (r *JobRepository) ListDueRetries(ctx context.Context, now time.Time, limit int) ([]jobs.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT `+jobColumns+`
		FROM jobs
		WHERE status = $1 AND scheduled_at <= $2
		ORDER BY scheduled_at ASC, priority DESC, created_at ASC, id ASC
		LIMIT $3`,
		jobs.StatusRetryWait, now.UTC(), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list due retries: %w", err)
	}
	defer rows.Close()

	due := make([]jobs.Job, 0, limit)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan due retry: %w", err)
		}
		due = append(due, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due retries: %w", err)
	}
	return due, nil
}

// PromoteRetry atomically returns a due retry to the ready state. The due-time
// predicate prevents callers from bypassing the persisted backoff.
func (r *JobRepository) PromoteRetry(ctx context.Context, id string) (jobs.Job, error) {
	job, err := scanJob(r.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $1,
			updated_at = clock_timestamp()
		WHERE id = $2
			AND status = $3
			AND scheduled_at <= clock_timestamp()
		RETURNING `+jobColumns,
		jobs.StatusQueued, id, jobs.StatusRetryWait,
	))
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("promote retry: %w", err)
	}

	current, getErr := r.GetJob(ctx, id)
	if getErr != nil {
		return jobs.Job{}, getErr
	}
	return jobs.Job{}, fmt.Errorf("%w: cannot promote job in %s state at %s", jobs.ErrInvalidTransition, current.Status, current.ScheduledAt.Format(time.RFC3339Nano))
}

// RecoverExpiredAttempts requeues expired leases and closes any unfinished
// attempt owned by the expired execution. Multiple scanners can safely divide
// work through SKIP LOCKED.
func (r *JobRepository) RecoverExpiredAttempts(ctx context.Context, limit int) ([]jobs.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}

	rows, err := r.db.QueryContext(ctx, `
		WITH expired AS (
			SELECT id, attempt_count
			FROM jobs
			WHERE status IN ($1, $2)
				AND lease_expires_at <= clock_timestamp()
			ORDER BY lease_expires_at ASC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		), closed_attempts AS (
			UPDATE job_attempts AS a
			SET status = 'expired',
				finished_at = clock_timestamp(),
				error = COALESCE(a.error, 'lease expired')
			FROM expired AS e
			WHERE a.job_id = e.id
				AND a.attempt_number = e.attempt_count
				AND a.finished_at IS NULL
			RETURNING a.job_id
		), requeued AS (
			UPDATE jobs AS j
			SET status = $4,
				lease_owner = NULL,
				lease_expires_at = NULL,
				updated_at = clock_timestamp()
			FROM expired AS e
			WHERE j.id = e.id
			RETURNING j.*
		)
		SELECT `+joinedJobColumns+`
		FROM requeued AS j`,
		jobs.StatusLeased, jobs.StatusRunning, limit, jobs.StatusQueued,
	)
	if err != nil {
		return nil, fmt.Errorf("recover expired attempts: %w", err)
	}
	defer rows.Close()

	requeued := make([]jobs.Job, 0, limit)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan recovered job: %w", err)
		}
		requeued = append(requeued, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recovered jobs: %w", err)
	}
	return requeued, nil
}

func newAttemptID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32], nil
}
