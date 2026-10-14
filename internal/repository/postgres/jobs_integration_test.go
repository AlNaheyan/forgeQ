package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/internal/platform"
	postgresrepository "forgequeue/internal/repository/postgres"
	"forgequeue/migrations"
)

func TestJobRepositoryLifecycleAndIdempotency(t *testing.T) {
	databaseURL := os.Getenv("FORGEQUEUE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("FORGEQUEUE_TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	db, err := platform.OpenPostgres(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	defer db.Close()
	lockConnection, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("reserve test lock connection: %v", err)
	}
	defer lockConnection.Close()
	if _, err := lockConnection.ExecContext(ctx, "SELECT pg_advisory_lock(7346871)"); err != nil {
		t.Fatalf("lock integration database: %v", err)
	}
	defer lockConnection.ExecContext(context.Background(), "SELECT pg_advisory_unlock(7346871)")
	if err := migrations.Up(ctx, db); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := db.ExecContext(ctx, "TRUNCATE job_attempts, idempotency_keys, jobs CASCADE"); err != nil {
		t.Fatalf("clean tables: %v", err)
	}

	repository := postgresrepository.NewJobRepository(db)
	first, err := jobs.New(jobs.CreateInput{
		Type:           "sleep",
		Payload:        json.RawMessage(`{"duration_ms":25}`),
		Priority:       10,
		MaxAttempts:    3,
		IdempotencyKey: "integration-key",
	}, time.Now())
	if err != nil {
		t.Fatalf("new job: %v", err)
	}

	createdJob, created, err := repository.CreateJob(ctx, first)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if !created || createdJob.ID != first.ID {
		t.Fatalf("unexpected create result: created=%v job=%+v", created, createdJob)
	}

	duplicate, err := jobs.New(jobs.CreateInput{
		Type:           "fail",
		Payload:        json.RawMessage(`{"failures_before_success":1}`),
		MaxAttempts:    5,
		IdempotencyKey: "integration-key",
	}, time.Now())
	if err != nil {
		t.Fatalf("new duplicate job: %v", err)
	}
	duplicateResult, duplicateCreated, err := repository.CreateJob(ctx, duplicate)
	if err != nil {
		t.Fatalf("create duplicate job: %v", err)
	}
	if duplicateCreated || duplicateResult.ID != first.ID {
		t.Fatalf("idempotency did not return original job: created=%v job=%+v", duplicateCreated, duplicateResult)
	}

	got, err := repository.GetJob(ctx, first.ID)
	if err != nil || got.Type != "sleep" {
		t.Fatalf("get job: job=%+v err=%v", got, err)
	}
	queued, err := repository.ListQueuedJobs(ctx, 10)
	if err != nil || len(queued) != 1 || queued[0].ID != first.ID {
		t.Fatalf("list queued jobs: jobs=%+v err=%v", queued, err)
	}

	listed, cursor, err := repository.ListJobs(ctx, jobs.ListFilter{Status: jobs.StatusQueued, Limit: 1})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(listed) != 1 || cursor != "" {
		t.Fatalf("unexpected list: jobs=%+v cursor=%q", listed, cursor)
	}

	canceled, err := repository.CancelJob(ctx, first.ID)
	if err != nil || canceled.Status != jobs.StatusCanceled {
		t.Fatalf("cancel job: job=%+v err=%v", canceled, err)
	}
	if _, err := repository.CancelJob(ctx, first.ID); !errors.Is(err, jobs.ErrInvalidTransition) {
		t.Fatalf("expected invalid transition, got %v", err)
	}
}
