package queue_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/platform"
	"github.com/AlNaheyan/forgeQ/internal/queue"
	postgresrepository "github.com/AlNaheyan/forgeQ/internal/repository/postgres"
	"github.com/AlNaheyan/forgeQ/migrations"

	"github.com/redis/go-redis/v9"
)

func TestReconcilerRestoresDurableJobMissingFromRedis(t *testing.T) {
	databaseURL := os.Getenv("FORGEQUEUE_TEST_DATABASE_URL")
	redisAddress := os.Getenv("FORGEQUEUE_TEST_REDIS_ADDR")
	if databaseURL == "" || redisAddress == "" {
		t.Skip("FORGEQUEUE_TEST_DATABASE_URL and FORGEQUEUE_TEST_REDIS_ADDR are required")
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
	job, err := jobs.New(jobs.CreateInput{
		Type:        "sleep",
		Payload:     json.RawMessage(`{"duration_ms":10}`),
		Priority:    7,
		MaxAttempts: 3,
	}, time.Now())
	if err != nil {
		t.Fatalf("new job: %v", err)
	}
	if _, _, err := repository.CreateJob(ctx, job); err != nil {
		t.Fatalf("persist job: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	defer redisClient.Close()
	prefix := "forgequeue:reconcile-test:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	defer redisClient.Del(ctx, prefix+":ready", prefix+":delayed")
	redisQueue := queue.NewRedisQueue(redisClient, prefix)
	reconciler := queue.NewReconciler(repository, redisQueue, slog.New(slog.NewTextHandler(io.Discard, nil)))

	restored, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	present, err := redisQueue.Contains(ctx, job.ID)
	if err != nil || restored != 1 || !present {
		t.Fatalf("durable job was not restored: restored=%d present=%v err=%v", restored, present, err)
	}
}
