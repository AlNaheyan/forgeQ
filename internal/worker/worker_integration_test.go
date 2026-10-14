package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/internal/platform"
	"forgequeue/internal/queue"
	postgresrepository "forgequeue/internal/repository/postgres"
	"forgequeue/internal/worker"
	"forgequeue/migrations"

	"github.com/redis/go-redis/v9"
)

func TestSubmitToWorkerSuccessPipeline(t *testing.T) {
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

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	defer redisClient.Close()
	prefix := "forgequeue:worker-test:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	defer redisClient.Del(ctx, prefix+":ready", prefix+":delayed")
	jobQueue := queue.NewRedisQueue(redisClient, prefix)
	repository := postgresrepository.NewJobRepository(db)
	service := jobs.NewService(repository, jobQueue, 3)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	process, err := worker.New(worker.Config{
		ID:           "worker-integration",
		Concurrency:  2,
		PollInterval: 5 * time.Millisecond,
		JobTypes:     []string{"sleep", "fail"},
	}, repository, jobQueue, worker.BuiltInHandlers(), logger)
	if err != nil {
		t.Fatalf("initialize worker: %v", err)
	}

	workerCtx, cancelWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- process.Run(workerCtx) }()

	submission, err := service.Submit(ctx, jobs.CreateInput{
		Type:        "sleep",
		Payload:     json.RawMessage(`{"duration_ms":20}`),
		MaxAttempts: 3,
	})
	if err != nil || submission.EnqueueError != nil {
		cancelWorker()
		t.Fatalf("submit job: result=%+v err=%v", submission, err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		persisted, err := repository.GetJob(ctx, submission.Job.ID)
		if err != nil {
			cancelWorker()
			t.Fatalf("get job: %v", err)
		}
		if persisted.Status == jobs.StatusSucceeded {
			if persisted.AttemptCount != 1 || len(persisted.Result) == 0 {
				cancelWorker()
				t.Fatalf("unexpected completed job: %+v", persisted)
			}
			break
		}
		if time.Now().After(deadline) {
			cancelWorker()
			t.Fatalf("job did not succeed before deadline; last status=%s", persisted.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancelWorker()
	if err := <-workerDone; err != nil {
		t.Fatalf("worker stopped with error: %v", err)
	}
}
