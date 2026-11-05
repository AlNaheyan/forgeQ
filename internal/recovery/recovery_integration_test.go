package recovery_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/coordinator"
	"github.com/AlNaheyan/forgeQ/internal/idempotency"
	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/platform"
	"github.com/AlNaheyan/forgeQ/internal/queue"
	"github.com/AlNaheyan/forgeQ/internal/repository/postgres"
	"github.com/AlNaheyan/forgeQ/migrations"
	"github.com/redis/go-redis/v9"
)

type fixture struct {
	ctx    context.Context
	db     *sql.DB
	redis  *redis.Client
	repo   *postgres.JobRepository
	q      *queue.RedisQueue
	logger *slog.Logger
}

func setup(t *testing.T) *fixture {
	t.Helper()
	if os.Getenv("FORGEQUEUE_RECOVERY_ISOLATED") != "1" {
		t.Skip("run make test-recovery for disposable dependencies")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	db, err := platform.OpenPostgres(ctx, os.Getenv("FORGEQUEUE_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var name string
	if err := db.QueryRowContext(ctx, "SELECT current_database()").Scan(&name); err != nil || name != "forgequeue_recovery" {
		t.Fatalf("refusing recovery tests outside forgequeue_recovery: %q %v", name, err)
	}
	if err := migrations.Up(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.ConfigureEventStream(ctx, db, false); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "TRUNCATE jobs, execution_idempotency_keys, event_outbox CASCADE"); err != nil {
		t.Fatal(err)
	}
	r := redis.NewClient(&redis.Options{Addr: os.Getenv("FORGEQUEUE_TEST_REDIS_ADDR"), MaxRetries: -1, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = r.Close() })
	if err := r.Del(ctx, "forgequeue:ready", "forgequeue:delayed").Err(); err != nil {
		t.Fatal(err)
	}
	return &fixture{ctx: ctx, db: db, redis: r, repo: postgres.NewJobRepository(db), q: queue.NewRedisQueue(r, "forgequeue"), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func (f *fixture) create(t *testing.T) jobs.Job {
	t.Helper()
	j, err := jobs.New(jobs.CreateInput{Type: "sleep", Payload: json.RawMessage(`{"duration_ms":1}`), MaxAttempts: 3}, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	j, _, err = f.repo.CreateJob(f.ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func (f *fixture) lease(t *testing.T, id string) jobs.Job {
	t.Helper()
	j, err := f.repo.LeaseJob(f.ctx, id, "same-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func (f *fixture) expire(t *testing.T, id string) {
	t.Helper()
	if _, err := f.db.ExecContext(f.ctx, "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	recovered, err := f.repo.RecoverExpiredAttempts(f.ctx, 10)
	if err != nil || len(recovered) != 1 || recovered[0].ID != id {
		t.Fatalf("recover: %+v %v", recovered, err)
	}
}

func TestRecoverySameWorkerLeaseFencing(t *testing.T) {
	f := setup(t)
	job := f.create(t)
	first := f.lease(t, job.ID)
	f.expire(t, job.ID) // Crash after claim, before starting an attempt.
	second := f.lease(t, job.ID)
	if second.LeaseGeneration != first.LeaseGeneration+1 {
		t.Fatal("generation did not advance")
	}
	for _, old := range []int64{0, first.LeaseGeneration} {
		_, err := f.repo.StartAttempt(f.ctx, job.ID, "same-worker", old)
		requireFenced(t, err)
		_, err = f.repo.StartLeasedJob(f.ctx, job.ID, "same-worker", old)
		requireFenced(t, err)
		_, err = f.repo.RenewLease(f.ctx, job.ID, "same-worker", time.Minute, old)
		requireFenced(t, err)
	}
	if _, err := f.repo.StartAttempt(f.ctx, job.ID, "same-worker", second.LeaseGeneration); err != nil {
		t.Fatal(err)
	}
	f.expire(t, job.ID) // Crash while the handler is running.
	third := f.lease(t, job.ID)
	if _, err := f.repo.StartAttempt(f.ctx, job.ID, "same-worker", third.LeaseGeneration); err != nil {
		t.Fatal(err)
	}
	for _, old := range []int64{0, first.LeaseGeneration, second.LeaseGeneration} {
		_, err := f.repo.SucceedAttempt(f.ctx, job.ID, "same-worker", []byte(`{"stale":true}`), old)
		requireFenced(t, err)
		_, err = f.repo.FailAttempt(f.ctx, job.ID, "same-worker", "stale", true, time.Now(), old)
		requireFenced(t, err)
		_, err = f.repo.RenewLease(f.ctx, job.ID, "same-worker", time.Minute, old)
		requireFenced(t, err)
		_, err = f.repo.FinishLeasedJob(f.ctx, job.ID, "same-worker", jobs.StatusSucceeded, nil, "", old)
		requireFenced(t, err)
	}
	finished, err := f.repo.SucceedAttempt(f.ctx, job.ID, "same-worker", []byte(`{"current":true}`), third.LeaseGeneration)
	if err != nil || finished.Status != jobs.StatusSucceeded || finished.AttemptCount != 2 || finished.LeaseGeneration != 3 {
		t.Fatalf("current completion: %+v %v", finished, err)
	}
	t.Log("claim crash + execution crash: 3 lease generations, 2 attempts; all stale mutations rejected")
}

func requireFenced(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, jobs.ErrInvalidTransition) {
		t.Fatalf("expected fenced mutation, got %v", err)
	}
}

func TestRecoveryRedisLossAcrossPagesAndRetries(t *testing.T) {
	f := setup(t)
	// 1,001 rows are the smallest regression fixture exceeding the production page.
	if _, err := f.db.ExecContext(f.ctx, `INSERT INTO jobs(id,type,payload,status)
  SELECT md5('recovery-'||n)::uuid,'sleep','{}','queued' FROM generate_series(1,1001) n`); err != nil {
		t.Fatal(err)
	}
	r := queue.NewReconciler(f.repo, f.q, f.logger)
	if n, err := r.ReconcileOnce(f.ctx); err != nil || n != 1000 {
		t.Fatalf("page one: %d %v", n, err)
	}
	if n, err := r.ReconcileOnce(f.ctx); err != nil || n != 1 {
		t.Fatalf("page two: %d %v", n, err)
	}
	retry := f.create(t)
	leased := f.lease(t, retry.ID)
	if _, err := f.repo.StartAttempt(f.ctx, retry.ID, "same-worker", leased.LeaseGeneration); err != nil {
		t.Fatal(err)
	}
	waiting, err := f.repo.FailAttempt(f.ctx, retry.ID, "same-worker", "retry me", true, time.Now().Add(-time.Second), leased.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.q.ScheduleRetry(f.ctx, retry.ID, waiting.ScheduledAt); err != nil {
		t.Fatal(err)
	}
	// Delete only this isolated stack's ready and delayed projections.
	if err := f.redis.Del(f.ctx, "forgequeue:ready", "forgequeue:delayed").Err(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	promoter := coordinator.NewRetryPromoter(f.repo, f.q, f.logger, 10)
	if n, err := promoter.PromoteOnce(f.ctx); n != 1 || err != nil {
		t.Fatalf("lost delayed hint: %d %v", n, err)
	}
	for i := 0; i < 4; i++ {
		if _, err := r.ReconcileOnce(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	depth, err := f.q.Depth(f.ctx)
	if err != nil || depth != 1002 {
		t.Fatalf("restored depth=%d err=%v", depth, err)
	}
	t.Logf("restored 1,002 distinct ready hints including due retry after Redis data loss in %s", time.Since(start))
}

func TestRecoverySideEffectBeforeAcknowledgment(t *testing.T) {
	f := setup(t)
	if _, err := f.db.ExecContext(f.ctx, `CREATE TABLE IF NOT EXISTS recovery_effects(job_id uuid PRIMARY KEY, executions int NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, guarded := range []bool{false, true} {
		name := "unguarded"
		if guarded {
			name = "transactionally_idempotent"
		}
		t.Run(name, func(t *testing.T) {
			job := f.create(t)
			effect := func() {
				sqlText := `INSERT INTO recovery_effects VALUES($1,1) ON CONFLICT(job_id) DO UPDATE SET executions=recovery_effects.executions+1`
				var err error
				if guarded {
					_, err = idempotency.NewExecutor(f.db).ExecuteOnceTx(f.ctx, "effect:"+job.ID, func(ctx context.Context, tx *sql.Tx) error { _, e := tx.ExecContext(ctx, sqlText, job.ID); return e })
				} else {
					_, err = f.db.ExecContext(f.ctx, sqlText, job.ID)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			first := f.lease(t, job.ID)
			if _, err := f.repo.StartAttempt(f.ctx, job.ID, "same-worker", first.LeaseGeneration); err != nil {
				t.Fatal(err)
			}
			effect()
			f.expire(t, job.ID) // Deliberately lose completion after the effect commits.
			second := f.lease(t, job.ID)
			if _, err := f.repo.StartAttempt(f.ctx, job.ID, "same-worker", second.LeaseGeneration); err != nil {
				t.Fatal(err)
			}
			effect()
			if _, err := f.repo.SucceedAttempt(f.ctx, job.ID, "same-worker", nil, second.LeaseGeneration); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := f.db.QueryRowContext(f.ctx, "SELECT executions FROM recovery_effects WHERE job_id=$1", job.ID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 2
			if guarded {
				want = 1
			}
			if count != want {
				t.Fatalf("effects=%d want=%d", count, want)
			}
			t.Logf("2 handler attempts; %d committed side effects", count)
		})
	}
}

func TestRecoveryEventCaptureDoesNotBreakCancellation(t *testing.T) {
	f := setup(t)
	for _, enabled := range []bool{false, true} {
		if err := migrations.ConfigureEventStream(f.ctx, f.db, enabled); err != nil {
			t.Fatal(err)
		}
		job := f.create(t)
		canceled, err := f.repo.CancelJob(f.ctx, job.ID)
		if err != nil || canceled.Status != jobs.StatusCanceled {
			t.Fatalf("events=%t cancellation: %+v %v", enabled, canceled, err)
		}
	}
}
