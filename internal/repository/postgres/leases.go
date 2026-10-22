package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/internal/tenant"
)

// LeaseJob atomically assigns an eligible queued job to workerID. The state
// predicate is part of the UPDATE so concurrent workers cannot both acquire
// the same job.
func (r *JobRepository) LeaseJob(
	ctx context.Context,
	id string,
	workerID string,
	duration time.Duration,
) (jobs.Job, error) {
	if workerID == "" || duration <= 0 {
		return jobs.Job{}, fmt.Errorf("%w: worker ID and positive lease duration are required", jobs.ErrClaimConflict)
	}
	leaseMicroseconds := duration.Microseconds()
	if leaseMicroseconds == 0 {
		leaseMicroseconds = 1
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return jobs.Job{}, err
	}
	defer tx.Rollback()
	var tenantID string
	if err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM jobs WHERE id=$1`, id).Scan(&tenantID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return jobs.Job{}, jobs.ErrClaimConflict
		}
		return jobs.Job{}, err
	}
	if err := lockTenant(ctx, tx, tenantID); err != nil {
		return jobs.Job{}, err
	}
	var limit, active int
	if err := tx.QueryRowContext(ctx, `SELECT max_concurrent FROM tenants WHERE id=$1`, tenantID).Scan(&limit); err != nil {
		return jobs.Job{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE tenant_id=$1 AND status IN ('leased','running') AND lease_expires_at>clock_timestamp()`, tenantID).Scan(&active); err != nil {
		return jobs.Job{}, err
	}
	if active >= limit {
		return jobs.Job{}, tenant.ErrConcurrency
	}
	job, err := scanJob(tx.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $1,
			lease_owner = $2,
			lease_generation = lease_generation + 1,
			lease_expires_at = clock_timestamp() + ($3 * INTERVAL '1 microsecond'),
			updated_at = clock_timestamp()
		WHERE id = $4
			AND status = $5
			AND scheduled_at <= clock_timestamp()
		RETURNING `+jobColumns,
		jobs.StatusLeased, workerID, leaseMicroseconds, id, jobs.StatusQueued,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, jobs.ErrClaimConflict
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("lease job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return jobs.Job{}, err
	}
	return job, nil
}

// StartLeasedJob starts one attempt while the caller still owns an unexpired
// lease. Expiry and ownership are checked by PostgreSQL in the same statement
// as the transition, closing the check-then-update race with the reaper.
func (r *JobRepository) StartLeasedJob(ctx context.Context, id, workerID string, generation int64) (jobs.Job, error) {
	job, err := scanJob(r.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $1,
			attempt_count = attempt_count + 1,
			updated_at = clock_timestamp()
		WHERE id = $2
			AND status = $3
			AND lease_owner = $4
			AND lease_generation = $5 AND $5 > 0
			AND lease_expires_at > clock_timestamp()
		RETURNING `+jobColumns,
		jobs.StatusRunning, id, jobs.StatusLeased, workerID, generation,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("%w: lease is not active or is owned by another worker", jobs.ErrInvalidTransition)
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("start leased job: %w", err)
	}
	return job, nil
}

// RenewLease replaces the expiry of an active leased or running job. It does
// not revive an already-expired lease; recovery owns the job after that point.
func (r *JobRepository) RenewLease(
	ctx context.Context,
	id string,
	workerID string,
	duration time.Duration,
	generation int64,
) (jobs.Job, error) {
	if workerID == "" || duration <= 0 {
		return jobs.Job{}, fmt.Errorf("%w: worker ID and positive lease duration are required", jobs.ErrInvalidTransition)
	}
	leaseMicroseconds := duration.Microseconds()
	if leaseMicroseconds == 0 {
		leaseMicroseconds = 1
	}

	job, err := scanJob(r.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET lease_expires_at = clock_timestamp() + ($1 * INTERVAL '1 microsecond'),
			updated_at = clock_timestamp()
		WHERE id = $2
			AND status IN ($3, $4)
			AND lease_owner = $5
			AND lease_generation = $6 AND $6 > 0
			AND lease_expires_at > clock_timestamp()
		RETURNING `+jobColumns,
		leaseMicroseconds, id, jobs.StatusLeased, jobs.StatusRunning, workerID, generation,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("%w: lease is not active or is owned by another worker", jobs.ErrInvalidTransition)
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("renew lease: %w", err)
	}
	return job, nil
}

// FinishLeasedJob records a terminal result only when workerID still owns the
// running, unexpired lease. Clearing lease metadata makes terminal ownership
// unambiguous to readers.
func (r *JobRepository) FinishLeasedJob(
	ctx context.Context,
	id string,
	workerID string,
	status jobs.Status,
	result []byte,
	lastError string,
	generation int64,
) (jobs.Job, error) {
	if status != jobs.StatusSucceeded && status != jobs.StatusFailed {
		return jobs.Job{}, fmt.Errorf("%w: running -> %s", jobs.ErrInvalidTransition, status)
	}

	job, err := scanJob(r.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $1,
			result = $2,
			last_error = NULLIF($3, ''),
			lease_owner = NULL,
			lease_expires_at = NULL,
			updated_at = clock_timestamp()
		WHERE id = $4
			AND status = $5
			AND lease_owner = $6
			AND lease_generation = $7 AND $7 > 0
			AND lease_expires_at > clock_timestamp()
		RETURNING `+jobColumns,
		status, nullableJSON(result), lastError, id, jobs.StatusRunning, workerID, generation,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("%w: running lease is not active or is owned by another worker", jobs.ErrInvalidTransition)
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("finish leased job: %w", err)
	}
	return job, nil
}

// RequeueExpiredLeases recovers abandoned work in bounded, non-blocking
// batches. Row locks serialize recovery against renewal/completion, while
// SKIP LOCKED lets multiple scanners safely divide the work.
func (r *JobRepository) RequeueExpiredLeases(ctx context.Context, limit int) ([]jobs.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}

	rows, err := r.db.QueryContext(ctx, `
		WITH expired AS (
			SELECT id
			FROM jobs
			WHERE status IN ($1, $2)
				AND lease_expires_at <= clock_timestamp()
			ORDER BY lease_expires_at ASC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		)
		UPDATE jobs AS j
		SET status = $4,
			lease_owner = NULL,
			lease_expires_at = NULL,
			updated_at = clock_timestamp()
		FROM expired
		WHERE j.id = expired.id
		RETURNING `+joinedJobColumns,
		jobs.StatusLeased, jobs.StatusRunning, limit, jobs.StatusQueued,
	)
	if err != nil {
		return nil, fmt.Errorf("requeue expired leases: %w", err)
	}
	defer rows.Close()

	requeued := make([]jobs.Job, 0, limit)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan requeued job: %w", err)
		}
		requeued = append(requeued, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate requeued jobs: %w", err)
	}
	return requeued, nil
}
