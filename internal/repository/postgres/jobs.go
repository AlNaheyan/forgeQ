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
)

const jobColumns = `
	id, type, payload, status, priority, attempt_count, max_attempts,
	scheduled_at, lease_owner, lease_expires_at, idempotency_key,
	result, last_error, created_at, updated_at`

const joinedJobColumns = `
	j.id, j.type, j.payload, j.status, j.priority, j.attempt_count, j.max_attempts,
	j.scheduled_at, j.lease_owner, j.lease_expires_at, j.idempotency_key,
	j.result, j.last_error, j.created_at, j.updated_at`

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

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO jobs (
			id, type, payload, status, priority, attempt_count, max_attempts,
			scheduled_at, idempotency_key, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), $10, $11)`,
		job.ID, job.Type, []byte(job.Payload), job.Status, job.Priority,
		job.AttemptCount, job.MaxAttempts, job.ScheduledAt, job.IdempotencyKey,
		job.CreatedAt, job.UpdatedAt,
	); err != nil {
		return jobs.Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	if job.IdempotencyKey != "" {
		var insertedJobID string
		err := tx.QueryRowContext(ctx, `
			INSERT INTO idempotency_keys (key, job_id)
			VALUES ($1, $2)
			ON CONFLICT (key) DO NOTHING
			RETURNING job_id`, job.IdempotencyKey, job.ID).Scan(&insertedJobID)
		if errors.Is(err, sql.ErrNoRows) {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return jobs.Job{}, false, fmt.Errorf("rollback duplicate job: %w", rollbackErr)
			}
			return r.getByIdempotencyKey(ctx, job.IdempotencyKey)
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

func (r *JobRepository) getByIdempotencyKey(ctx context.Context, key string) (jobs.Job, bool, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT `+joinedJobColumns+`
		FROM jobs j
		JOIN idempotency_keys i ON i.job_id = j.id
		WHERE i.key = $1`, key)
	job, err := scanJob(row)
	if err != nil {
		return jobs.Job{}, false, fmt.Errorf("get job by idempotency key: %w", err)
	}
	return job, false, nil
}

func (r *JobRepository) GetJob(ctx context.Context, id string) (jobs.Job, error) {
	job, err := scanJob(r.db.QueryRowContext(ctx,
		"SELECT "+jobColumns+" FROM jobs WHERE id = $1", id,
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
		WHERE id = $2 AND status IN ($3, $4)
		RETURNING `+jobColumns,
		jobs.StatusCanceled, id, jobs.StatusQueued, jobs.StatusRetryWait,
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
		&job.CreatedAt, &job.UpdatedAt,
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
