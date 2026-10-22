package postgres

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/internal/repository"
	"forgequeue/internal/tenant"
)

const jobColumns = `
	id, type, payload, status, priority, attempt_count, max_attempts,
	scheduled_at, lease_owner, lease_expires_at, idempotency_key,
	result, last_error, created_at, updated_at, lease_generation, tenant_id`

const joinedJobColumns = `
	j.id, j.type, j.payload, j.status, j.priority, j.attempt_count, j.max_attempts,
	j.scheduled_at, j.lease_owner, j.lease_expires_at, j.idempotency_key,
	j.result, j.last_error, j.created_at, j.updated_at, j.lease_generation, j.tenant_id`

type JobRepository struct {
	db *sql.DB
}

func NewJobRepository(db *sql.DB) *JobRepository {
	return &JobRepository{db: db}
}

func (r *JobRepository) CreateJob(ctx context.Context, job jobs.Job) (jobs.Job, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("begin create job: %w", err)
	}
	defer tx.Rollback()
	job.TenantID = tenant.ID(ctx)
	if err := lockTenant(ctx, tx, job.TenantID); err != nil {
		return jobs.Job{}, false, err
	}
	if job.IdempotencyKey != "" {
		existing, err := scanJob(tx.QueryRowContext(ctx, `SELECT `+joinedJobColumns+` FROM jobs j JOIN idempotency_keys i ON i.job_id=j.id WHERE i.tenant_id=$1 AND i.key=$2`, job.TenantID, job.IdempotencyKey))
		if err == nil {
			return existing, false, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return jobs.Job{}, false, err
		}
	}
	if err := admitTenant(ctx, tx, job.TenantID, len(job.Payload)); err != nil {
		return jobs.Job{}, false, err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO jobs (
			id, type, payload, status, priority, attempt_count, max_attempts,
			scheduled_at, idempotency_key, created_at, updated_at, tenant_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), $10, $11, $12)`,
		job.ID, job.Type, []byte(job.Payload), job.Status, job.Priority,
		job.AttemptCount, job.MaxAttempts, job.ScheduledAt, job.IdempotencyKey,
		job.CreatedAt, job.UpdatedAt, job.TenantID,
	); err != nil {
		return jobs.Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	if job.IdempotencyKey != "" {
		var insertedJobID string
		err := tx.QueryRowContext(ctx, `
			INSERT INTO idempotency_keys (key, job_id, tenant_id)
			VALUES ($1, $2, $3)
			ON CONFLICT (tenant_id, key) DO NOTHING
			RETURNING job_id`, job.IdempotencyKey, job.ID, job.TenantID).Scan(&insertedJobID)
		if errors.Is(err, sql.ErrNoRows) {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return jobs.Job{}, false, fmt.Errorf("rollback duplicate job: %w", rollbackErr)
			}
			existing, getErr := r.GetJobByIdempotencyKey(ctx, job.IdempotencyKey)
			return existing, false, getErr
		}
		if err != nil {
			return jobs.Job{}, false, fmt.Errorf("insert idempotency key: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return jobs.Job{}, false, fmt.Errorf("commit create job: %w", err)
	}
	return job, true, nil
}

func (r *JobRepository) GetJobByIdempotencyKey(ctx context.Context, key string) (jobs.Job, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT `+joinedJobColumns+`
		FROM jobs j
		JOIN idempotency_keys i ON i.job_id = j.id
		WHERE i.key = $1 AND i.tenant_id = $2`, key, tenant.ID(ctx))
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("get job by idempotency key: %w", err)
	}
	return job, nil
}

func (r *JobRepository) GetJob(ctx context.Context, id string) (jobs.Job, error) {
	job, err := scanJob(r.db.QueryRowContext(ctx,
		"SELECT "+jobColumns+" FROM jobs WHERE id = $1 AND ($2::uuid IS NULL OR tenant_id=$2)", id, tenant.Scope(ctx),
	))
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

func (r *JobRepository) ListJobs(ctx context.Context, filter repository.ListFilter) ([]jobs.Job, string, error) {
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Limit > 100 {
		filter.Limit = 100
	}

	clauses := make([]string, 0, 3)
	args := make([]any, 0, 5)
	addArg := func(value any) string {
		args = append(args, value)
		return "$" + strconv.Itoa(len(args))
	}

	if p, ok := tenant.FromContext(ctx); ok {
		clauses = append(clauses, "tenant_id = "+addArg(p.ID))
	}
	if filter.Status != "" {
		clauses = append(clauses, "status = "+addArg(filter.Status))
	}
	if filter.Type != "" {
		clauses = append(clauses, "type = "+addArg(filter.Type))
	}
	if filter.Cursor != "" {
		createdAt, id, err := decodeCursor(filter.Cursor)
		if err != nil {
			return nil, "", err
		}
		createdArg := addArg(createdAt)
		idArg := addArg(id)
		clauses = append(clauses, fmt.Sprintf("(created_at, id) < (%s, %s)", createdArg, idArg))
	}

	query := "SELECT " + jobColumns + " FROM jobs"
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at DESC, id DESC LIMIT " + addArg(filter.Limit+1)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	result := make([]jobs.Job, 0, filter.Limit)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, "", fmt.Errorf("scan listed job: %w", err)
		}
		result = append(result, job)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate jobs: %w", err)
	}

	nextCursor := ""
	if len(result) > filter.Limit {
		result = result[:filter.Limit]
		last := result[len(result)-1]
		nextCursor = encodeCursor(last.CreatedAt, last.ID)
	}
	return result, nextCursor, nil
}

func (r *JobRepository) UpdateStatus(ctx context.Context, id string, target jobs.Status) (jobs.Job, error) {
	current, err := r.GetJob(ctx, id)
	if err != nil {
		return jobs.Job{}, err
	}
	if !jobs.CanTransition(current.Status, target) {
		return jobs.Job{}, fmt.Errorf("%w: %s -> %s", jobs.ErrInvalidTransition, current.Status, target)
	}

	updated, err := scanJob(r.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $1, updated_at = NOW()
		WHERE id = $2 AND status = $3
		RETURNING `+jobColumns, target, id, current.Status))
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("%w: job changed concurrently", jobs.ErrInvalidTransition)
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("update job status: %w", err)
	}
	return updated, nil
}

func (r *JobRepository) CancelJob(ctx context.Context, id string) (jobs.Job, error) {
	job, err := scanJob(r.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $1, updated_at = NOW()
		WHERE id = $2 AND status IN ($3, $4) AND ($5::uuid IS NULL OR tenant_id=$5)
		RETURNING `+jobColumns,
		jobs.StatusCanceled, id, jobs.StatusQueued, jobs.StatusRetryWait, tenant.Scope(ctx),
	))
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("cancel job: %w", err)
	}

	current, getErr := r.GetJob(ctx, id)
	if getErr != nil {
		return jobs.Job{}, getErr
	}
	return jobs.Job{}, fmt.Errorf("%w: cannot cancel job in %s state", jobs.ErrInvalidTransition, current.Status)
}

func (r *JobRepository) ListQueuedJobs(ctx context.Context, limit int) ([]jobs.Job, error) {
	if limit <= 0 {
		limit = 1000
	}
	if limit > 5000 {
		limit = 5000
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT `+jobColumns+`
		FROM jobs
		WHERE status = $1 AND scheduled_at <= NOW()
		ORDER BY priority DESC, created_at ASC
		LIMIT $2`, jobs.StatusQueued, limit)
	if err != nil {
		return nil, fmt.Errorf("list queued jobs: %w", err)
	}
	defer rows.Close()

	result := make([]jobs.Job, 0, limit)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan queued job: %w", err)
		}
		result = append(result, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queued jobs: %w", err)
	}
	return result, nil
}

// ListQueuedJobsPage walks the complete durable ready set without repeatedly
// visiting only the first priority batch. ID ordering is stable across updates.
func (r *JobRepository) ListQueuedJobsPage(ctx context.Context, afterID string, limit int) ([]jobs.Job, error) {
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	if afterID == "" {
		afterID = "00000000-0000-0000-0000-000000000000"
	}
	rows, err := r.db.QueryContext(ctx, `SELECT `+jobColumns+`
		FROM jobs WHERE status = $1 AND scheduled_at <= clock_timestamp() AND id > $2::uuid
		ORDER BY id LIMIT $3`, jobs.StatusQueued, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("list queued page: %w", err)
	}
	defer rows.Close()
	var result []jobs.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, job)
	}
	return result, rows.Err()
}

// ClaimJob atomically moves a queued job to running and records its next
// attempt number. Leasing replaces this direct claim in Wave 5.
func (r *JobRepository) ClaimJob(ctx context.Context, id string) (jobs.Job, error) {
	job, err := scanJob(r.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $1,
			attempt_count = attempt_count + 1,
			updated_at = NOW()
		WHERE id = $2 AND status = $3 AND scheduled_at <= NOW()
		RETURNING `+jobColumns,
		jobs.StatusRunning, id, jobs.StatusQueued,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, jobs.ErrClaimConflict
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("claim job: %w", err)
	}
	return job, nil
}

func (r *JobRepository) FinishJob(
	ctx context.Context,
	id string,
	status jobs.Status,
	result []byte,
	lastError string,
) (jobs.Job, error) {
	if status != jobs.StatusSucceeded && status != jobs.StatusFailed {
		return jobs.Job{}, fmt.Errorf("%w: running -> %s", jobs.ErrInvalidTransition, status)
	}
	job, err := scanJob(r.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $1,
			result = $2,
			last_error = NULLIF($3, ''),
			updated_at = NOW()
		WHERE id = $4 AND status = $5
		RETURNING `+jobColumns,
		status, nullableJSON(result), lastError, id, jobs.StatusRunning,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("%w: job is no longer running", jobs.ErrInvalidTransition)
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("finish job: %w", err)
	}
	return job, nil
}

type scanner interface {
	Scan(...any) error
}

func scanJob(row scanner) (jobs.Job, error) {
	var job jobs.Job
	var leaseOwner sql.NullString
	var leaseExpiresAt sql.NullTime
	var idempotencyKey sql.NullString
	var result []byte
	var lastError sql.NullString

	err := row.Scan(
		&job.ID, &job.Type, &job.Payload, &job.Status, &job.Priority,
		&job.AttemptCount, &job.MaxAttempts, &job.ScheduledAt, &leaseOwner,
		&leaseExpiresAt, &idempotencyKey, &result, &lastError,
		&job.CreatedAt, &job.UpdatedAt, &job.LeaseGeneration, &job.TenantID,
	)
	if err != nil {
		return jobs.Job{}, err
	}
	job.LeaseOwner = leaseOwner.String
	if leaseExpiresAt.Valid {
		job.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	job.IdempotencyKey = idempotencyKey.String
	job.Result = result
	job.LastError = lastError.String
	job.ScheduledAt = job.ScheduledAt.UTC()
	job.CreatedAt = job.CreatedAt.UTC()
	job.UpdatedAt = job.UpdatedAt.UTC()
	if job.LeaseExpiresAt != nil {
		leaseUTC := job.LeaseExpiresAt.UTC()
		job.LeaseExpiresAt = &leaseUTC
	}
	return job, nil
}

func encodeCursor(createdAt time.Time, id string) string {
	raw := createdAt.UTC().Format(time.RFC3339Nano) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor: %w", err)
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 || parts[1] == "" {
		return time.Time{}, "", errors.New("invalid cursor")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor timestamp: %w", err)
	}
	return createdAt, parts[1], nil
}

func nullableJSON(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}
