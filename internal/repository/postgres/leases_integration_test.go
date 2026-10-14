package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/internal/platform"
	postgresrepository "forgequeue/internal/repository/postgres"
	"forgequeue/migrations"
)

func TestJobRepositoryLeaseLifecycle(t *testing.T) {
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
	createJob := func(t *testing.T) jobs.Job {
		t.Helper()
		job, err := jobs.New(jobs.CreateInput{
			Type:        "sleep",
			Payload:     json.RawMessage(`{"duration_ms":25}`),
			MaxAttempts: 3,
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

	t.Run("only one worker acquires the lease", func(t *testing.T) {
		job := createJob(t)
		start := make(chan struct{})
		results := make(chan error, 2)
		var wg sync.WaitGroup
		for _, workerID := range []string{"worker-a", "worker-b"} {
			workerID := workerID
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := repository.LeaseJob(ctx, job.ID, workerID, time.Minute)
				results <- err
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		succeeded := 0
		conflicted := 0
		for err := range results {
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, jobs.ErrClaimConflict):
				conflicted++
			default:
				t.Fatalf("unexpected lease error: %v", err)
			}
		}
		if succeeded != 1 || conflicted != 1 {
			t.Fatalf("lease outcomes: succeeded=%d conflicted=%d", succeeded, conflicted)
		}
	})

	t.Run("owner starts renews and finishes an active lease", func(t *testing.T) {
		job := createJob(t)
		leased, err := repository.LeaseJob(ctx, job.ID, "owner", 2*time.Second)
		if err != nil {
			t.Fatalf("lease job: %v", err)
		}
		if leased.Status != jobs.StatusLeased || leased.LeaseOwner != "owner" || leased.LeaseExpiresAt == nil {
			t.Fatalf("unexpected leased job: %+v", leased)
		}

		if _, err := repository.StartLeasedJob(ctx, job.ID, "intruder"); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("wrong owner start error = %v, want ErrInvalidTransition", err)
		}
		running, err := repository.StartLeasedJob(ctx, job.ID, "owner")
		if err != nil {
			t.Fatalf("start leased job: %v", err)
		}
		if running.Status != jobs.StatusRunning || running.AttemptCount != 1 {
			t.Fatalf("unexpected running job: %+v", running)
		}

		if _, err := repository.RenewLease(ctx, job.ID, "intruder", time.Minute); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("wrong owner renewal error = %v, want ErrInvalidTransition", err)
		}
		renewed, err := repository.RenewLease(ctx, job.ID, "owner", time.Minute)
		if err != nil {
			t.Fatalf("renew lease: %v", err)
		}
		if renewed.LeaseExpiresAt == nil || !renewed.LeaseExpiresAt.After(*running.LeaseExpiresAt) {
			t.Fatalf("renewal did not extend expiry: before=%v after=%v", running.LeaseExpiresAt, renewed.LeaseExpiresAt)
		}

		if _, err := repository.FinishLeasedJob(ctx, job.ID, "intruder", jobs.StatusSucceeded, []byte(`{"ok":true}`), ""); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("wrong owner finish error = %v, want ErrInvalidTransition", err)
		}
		finished, err := repository.FinishLeasedJob(ctx, job.ID, "owner", jobs.StatusSucceeded, []byte(`{"ok":true}`), "")
		if err != nil {
			t.Fatalf("finish leased job: %v", err)
		}
		if finished.Status != jobs.StatusSucceeded || finished.LeaseOwner != "" || finished.LeaseExpiresAt != nil {
			t.Fatalf("unexpected finished job: %+v", finished)
		}
		requeued, err := repository.RequeueExpiredLeases(ctx, 10)
		if err != nil || containsLeaseJob(requeued, job.ID) {
			t.Fatalf("completed job was requeued: jobs=%+v err=%v", requeued, err)
		}
	})

	t.Run("expiration wins over stale completion", func(t *testing.T) {
		job := createJob(t)
		if _, err := repository.LeaseJob(ctx, job.ID, "crashed-worker", time.Minute); err != nil {
			t.Fatalf("lease job: %v", err)
		}
		if _, err := repository.StartLeasedJob(ctx, job.ID, "crashed-worker"); err != nil {
			t.Fatalf("start leased job: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE jobs
			SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
			WHERE id = $1`, job.ID); err != nil {
			t.Fatalf("expire lease: %v", err)
		}

		if _, err := repository.FinishLeasedJob(ctx, job.ID, "crashed-worker", jobs.StatusSucceeded, []byte(`{"stale":true}`), ""); !errors.Is(err, jobs.ErrInvalidTransition) {
			t.Fatalf("expired completion error = %v, want ErrInvalidTransition", err)
		}
		requeued, err := repository.RequeueExpiredLeases(ctx, 10)
		if err != nil {
			t.Fatalf("requeue expired lease: %v", err)
		}
		if len(requeued) != 1 || requeued[0].ID != job.ID || requeued[0].Status != jobs.StatusQueued {
			t.Fatalf("unexpected requeued jobs: %+v", requeued)
		}
		if requeued[0].LeaseOwner != "" || requeued[0].LeaseExpiresAt != nil {
			t.Fatalf("requeued lease metadata was not cleared: %+v", requeued[0])
		}
	})

	t.Run("requeues expired leased and running jobs", func(t *testing.T) {
		leasedJob := createJob(t)
		runningJob := createJob(t)
		for _, job := range []jobs.Job{leasedJob, runningJob} {
			if _, err := repository.LeaseJob(ctx, job.ID, "dead-worker", time.Minute); err != nil {
				t.Fatalf("lease job %s: %v", job.ID, err)
			}
		}
		if _, err := repository.StartLeasedJob(ctx, runningJob.ID, "dead-worker"); err != nil {
			t.Fatalf("start leased job: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE jobs
			SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
			WHERE id IN ($1, $2)`, leasedJob.ID, runningJob.ID); err != nil {
			t.Fatalf("expire leases: %v", err)
		}

		firstBatch, err := repository.RequeueExpiredLeases(ctx, 1)
		if err != nil || len(firstBatch) != 1 {
			t.Fatalf("first requeue batch: jobs=%+v err=%v", firstBatch, err)
		}
		secondBatch, err := repository.RequeueExpiredLeases(ctx, 1)
		if err != nil || len(secondBatch) != 1 {
			t.Fatalf("second requeue batch: jobs=%+v err=%v", secondBatch, err)
		}
		if firstBatch[0].ID == secondBatch[0].ID {
			t.Fatalf("same job recovered twice: %s", firstBatch[0].ID)
		}
		for _, recovered := range append(firstBatch, secondBatch...) {
			if recovered.Status != jobs.StatusQueued || recovered.LeaseOwner != "" || recovered.LeaseExpiresAt != nil {
				t.Fatalf("unexpected recovered job: %+v", recovered)
			}
		}
	})
}

func containsLeaseJob(list []jobs.Job, id string) bool {
	for _, job := range list {
		if job.ID == id {
			return true
		}
	}
	return false
}
