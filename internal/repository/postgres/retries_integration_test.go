package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/platform"
	postgresrepository "github.com/AlNaheyan/forgeQ/internal/repository/postgres"
	"github.com/AlNaheyan/forgeQ/migrations"
)

func TestJobRepositoryRetryLifecycle(t *testing.T) {
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
	createJob := func(t *testing.T, maxAttempts int) jobs.Job {
		t.Helper()
		job, err := jobs.New(jobs.CreateInput{
			Type:        "sleep",
			Payload:     json.RawMessage(`{"duration_ms":25}`),
			MaxAttempts: maxAttempts,
		}, time.Now().Add(-time.Second))
		if err != nil {
			t.Fatalf("new job: %v", err)
		}
		created, inserted, err := repository.CreateJob(ctx, job)
		if err != nil || !inserted {
			t.Fatalf("create job: inserted=%v err=%v", inserted, err)
		}
		return created
	}
	start := func(t *testing.T, jobID, workerID string) jobs.Job {
		t.Helper()
		leased, err := repository.LeaseJob(ctx, jobID, workerID, time.Minute)
		if err != nil {
			t.Fatalf("lease job: %v", err)
		}
		running, err := repository.StartAttempt(ctx, jobID, workerID, leased.LeaseGeneration)
		if err != nil {
			t.Fatalf("start attempt: %v", err)
		}
		return running
	}

	t.Run("start and success persist one attempt", func(t *testing.T) {
		job := createJob(t, 3)
		if _, err := repository.LeaseJob(ctx, job.ID, "owner", time.Minute); err != nil {
			t.Fatalf("lease job: %v", err)
		}
		if _, err := repository.StartAttempt(ctx, job.ID, "intruder", 1); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("wrong owner start error = %v, want ErrInvalidTransition", err)
		}
		running, err := repository.StartAttempt(ctx, job.ID, "owner", 1)
		if err != nil {
			t.Fatalf("start attempt: %v", err)
		}
		if running.Status != jobs.StatusRunning || running.AttemptCount != 1 {
			t.Fatalf("unexpected running job: %+v", running)
		}
		assertAttempt(t, db, job.ID, 1, "owner", "running", false, "")

		if _, err := repository.SucceedAttempt(ctx, job.ID, "intruder", []byte(`{"ok":true}`), 1); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("wrong owner success error = %v, want ErrInvalidTransition", err)
		}
		succeeded, err := repository.SucceedAttempt(ctx, job.ID, "owner", []byte(`{"ok":true}`), 1)
		if err != nil {
			t.Fatalf("succeed attempt: %v", err)
		}
		var result struct {
			OK bool `json:"ok"`
		}
		if err := json.Unmarshal(succeeded.Result, &result); err != nil {
			t.Fatalf("decode success result: %v", err)
		}
		if succeeded.Status != jobs.StatusSucceeded || succeeded.LeaseOwner != "" || !result.OK {
			t.Fatalf("unexpected succeeded job: %+v", succeeded)
		}
		assertAttempt(t, db, job.ID, 1, "owner", "succeeded", true, "")
	})

	t.Run("retryable failure waits and promotes when due", func(t *testing.T) {
		job := createJob(t, 3)
		start(t, job.ID, "worker-a")
		retryAt := time.Now().Add(time.Minute).UTC()

		if _, err := repository.FailAttempt(ctx, job.ID, "intruder", "temporary", true, retryAt, 1); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("wrong owner failure error = %v, want ErrInvalidTransition", err)
		}
		waiting, err := repository.FailAttempt(ctx, job.ID, "worker-a", "temporary", true, retryAt, 1)
		if err != nil {
			t.Fatalf("fail attempt: %v", err)
		}
		if waiting.Status != jobs.StatusRetryWait || waiting.LeaseOwner != "" || waiting.LastError != "temporary" {
			t.Fatalf("unexpected retry wait job: %+v", waiting)
		}
		if waiting.ScheduledAt.Sub(retryAt) > time.Millisecond || retryAt.Sub(waiting.ScheduledAt) > time.Millisecond {
			t.Fatalf("scheduled_at=%s want %s", waiting.ScheduledAt, retryAt)
		}
		assertAttempt(t, db, job.ID, 1, "worker-a", "failed", true, "temporary")

		due, err := repository.ListDueRetries(ctx, retryAt.Add(-time.Second), 10)
		if err != nil || containsRetryJob(due, job.ID) {
			t.Fatalf("job was due early: jobs=%+v err=%v", due, err)
		}
		due, err = repository.ListDueRetries(ctx, retryAt.Add(time.Second), 10)
		if err != nil || !containsRetryJob(due, job.ID) {
			t.Fatalf("due job missing: jobs=%+v err=%v", due, err)
		}
		if _, err := repository.PromoteRetry(ctx, job.ID); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("early promotion error = %v, want ErrInvalidTransition", err)
		}
		if _, err := db.ExecContext(ctx, "UPDATE jobs SET scheduled_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1", job.ID); err != nil {
			t.Fatalf("make retry due: %v", err)
		}
		promoted, err := repository.PromoteRetry(ctx, job.ID)
		if err != nil {
			t.Fatalf("promote retry: %v", err)
		}
		if promoted.Status != jobs.StatusQueued || promoted.AttemptCount != 1 {
			t.Fatalf("unexpected promoted job: %+v", promoted)
		}
	})

	t.Run("exhaustion dead letters and manual retry grants one attempt", func(t *testing.T) {
		job := createJob(t, 2)
		start(t, job.ID, "worker-a")
		waiting, err := repository.FailAttempt(ctx, job.ID, "worker-a", "first", true, time.Now().Add(-time.Second), 1)
		if err != nil || waiting.Status != jobs.StatusRetryWait {
			t.Fatalf("first failure: job=%+v err=%v", waiting, err)
		}
		if _, err := repository.PromoteRetry(ctx, job.ID); err != nil {
			t.Fatalf("promote first retry: %v", err)
		}
		start(t, job.ID, "worker-b")
		dead, err := repository.FailAttempt(ctx, job.ID, "worker-b", "second", true, time.Now().Add(time.Minute), 2)
		if err != nil {
			t.Fatalf("exhaust attempt: %v", err)
		}
		if dead.Status != jobs.StatusDeadLetter || dead.AttemptCount != 2 || dead.MaxAttempts != 2 {
			t.Fatalf("unexpected dead-letter job: %+v", dead)
		}

		retried, err := repository.RetryDeadLetter(ctx, job.ID)
		if err != nil {
			t.Fatalf("retry dead-letter: %v", err)
		}
		if retried.Status != jobs.StatusQueued || retried.AttemptCount != 2 || retried.MaxAttempts != 3 || retried.LastError != "" {
			t.Fatalf("manual retry did not preserve history and grant one attempt: %+v", retried)
		}
		start(t, job.ID, "worker-c")
		dead, err = repository.FailAttempt(ctx, job.ID, "worker-c", "permanent", false, time.Time{}, 3)
		if err != nil || dead.Status != jobs.StatusDeadLetter || dead.AttemptCount != 3 {
			t.Fatalf("non-retryable failure: job=%+v err=%v", dead, err)
		}
		assertAttempt(t, db, job.ID, 3, "worker-c", "failed", true, "permanent")
	})

	t.Run("expired execution cannot report and recovery closes attempt", func(t *testing.T) {
		job := createJob(t, 3)
		start(t, job.ID, "crashed-worker")
		if _, err := db.ExecContext(ctx, `
			UPDATE jobs
			SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
			WHERE id = $1`, job.ID); err != nil {
			t.Fatalf("expire lease: %v", err)
		}

		if _, err := repository.FailAttempt(ctx, job.ID, "crashed-worker", "late", true, time.Now(), 1); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("expired failure error = %v, want ErrInvalidTransition", err)
		}
		if _, err := repository.SucceedAttempt(ctx, job.ID, "crashed-worker", []byte(`{"late":true}`), 1); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("expired success error = %v, want ErrInvalidTransition", err)
		}
		recovered, err := repository.RecoverExpiredAttempts(ctx, 10)
		if err != nil {
			t.Fatalf("recover expired attempt: %v", err)
		}
		if len(recovered) != 1 || recovered[0].ID != job.ID || recovered[0].Status != jobs.StatusQueued {
			t.Fatalf("unexpected recovered jobs: %+v", recovered)
		}
		assertAttempt(t, db, job.ID, 1, "crashed-worker", "expired", true, "lease expired")
	})

	t.Run("missing and stale manual operations return domain errors", func(t *testing.T) {
		if _, err := repository.PromoteRetry(ctx, "00000000-0000-4000-8000-000000000001"); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("missing promote error = %v, want ErrNotFound", err)
		}
		if _, err := repository.RetryDeadLetter(ctx, "00000000-0000-4000-8000-000000000002"); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("missing manual retry error = %v, want ErrNotFound", err)
		}
		job := createJob(t, 1)
		if _, err := repository.RetryDeadLetter(ctx, job.ID); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("queued manual retry error = %v, want ErrInvalidTransition", err)
		}
	})
}

func assertAttempt(t *testing.T, db *sql.DB, jobID string, attemptNumber int, workerID, status string, finished bool, errorText string) {
	t.Helper()
	var gotWorkerID, gotStatus string
	var gotFinishedAt sql.NullTime
	var gotError sql.NullString
	err := db.QueryRow(`
		SELECT worker_id, status, finished_at, error
		FROM job_attempts
		WHERE job_id = $1 AND attempt_number = $2`, jobID, attemptNumber).
		Scan(&gotWorkerID, &gotStatus, &gotFinishedAt, &gotError)
	if err != nil {
		t.Fatalf("read attempt %d: %v", attemptNumber, err)
	}
	if gotWorkerID != workerID || gotStatus != status || gotFinishedAt.Valid != finished || gotError.String != errorText {
		t.Fatalf("attempt %d = worker:%q status:%q finished:%v error:%q; want worker:%q status:%q finished:%v error:%q",
			attemptNumber, gotWorkerID, gotStatus, gotFinishedAt.Valid, gotError.String,
			workerID, status, finished, errorText)
	}
}

func containsRetryJob(list []jobs.Job, id string) bool {
	for _, job := range list {
		if job.ID == id {
			return true
		}
	}
	return false
}
