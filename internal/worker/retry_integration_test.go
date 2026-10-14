package worker_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"forgequeue/internal/coordinator"
	"forgequeue/internal/jobs"
	"forgequeue/internal/platform"
	"forgequeue/internal/queue"
	postgresrepository "forgequeue/internal/repository/postgres"
	"forgequeue/internal/worker"
	"forgequeue/migrations"
	"forgequeue/proto/workerpb"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestRetryToSuccessDeadLetterAndManualRetryPipeline(t *testing.T) {
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
	prefix := "forgequeue:retry-test:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	defer redisClient.Del(ctx, prefix+":ready", prefix+":delayed")
	jobQueue := queue.NewRedisQueue(redisClient, prefix)
	repository := postgresrepository.NewJobRepository(db)
	service := jobs.NewService(repository, jobQueue, 3)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry, err := coordinator.NewRegistry(coordinator.HealthThresholds{
		SuspectAfter: time.Second, UnhealthyAfter: 2 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("initialize registry: %v", err)
	}
	coordinatorService, err := coordinator.NewGRPCServer(coordinator.ServerConfig{
		HeartbeatInterval: 50 * time.Millisecond,
		LeaseDuration:     time.Second,
		RetryPolicy:       coordinator.NewRetryPolicy(20*time.Millisecond, 50*time.Millisecond, 0, nil),
		Logger:            logger,
	}, registry, repository, jobQueue)
	if err != nil {
		t.Fatalf("initialize coordinator: %v", err)
	}
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	workerpb.RegisterWorkerCoordinatorServer(grpcServer, coordinatorService)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()
	connection, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatalf("connect to coordinator: %v", err)
	}
	defer connection.Close()
	client := workerpb.NewWorkerCoordinatorClient(connection)
	broker, err := worker.NewGRPCBroker(client)
	if err != nil {
		t.Fatalf("initialize broker: %v", err)
	}
	process, err := worker.NewWithBroker(worker.Config{
		ID:                 "worker-retry-integration",
		Concurrency:        1,
		PollInterval:       3 * time.Millisecond,
		LeaseRenewInterval: 100 * time.Millisecond,
		JobTypes:           []string{"fail"},
	}, broker, worker.BuiltInHandlers(), logger)
	if err != nil {
		t.Fatalf("initialize worker: %v", err)
	}
	remote, err := worker.NewRemoteCoordinator(worker.RemoteConfig{
		WorkerID:          process.ID(),
		MaxConcurrency:    1,
		SupportedJobTypes: []string{"fail"},
	}, worker.NewGRPCCoordinatorRPC(client), process.ActiveJobs, logger)
	if err != nil {
		t.Fatalf("initialize remote coordinator: %v", err)
	}
	settings, err := remote.Register(ctx)
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 2)
	go func() { done <- remote.RunHeartbeat(runCtx, settings.HeartbeatInterval) }()
	go func() { done <- process.Run(runCtx) }()
	promoter := coordinator.NewRetryPromoter(repository, jobQueue, logger, 100)
	go promoter.Run(runCtx, 5*time.Millisecond)

	automatic, err := service.Submit(ctx, jobs.CreateInput{
		Type: "fail", Payload: json.RawMessage(`{"failures_before_success":2}`), MaxAttempts: 3,
	})
	if err != nil || automatic.EnqueueError != nil {
		cancel()
		t.Fatalf("submit automatic retry job: result=%+v err=%v", automatic, err)
	}
	automaticJob := waitForJobStatus(t, ctx, repository, automatic.Job.ID, jobs.StatusSucceeded, 3*time.Second)
	if automaticJob.AttemptCount != 3 {
		cancel()
		t.Fatalf("automatic retry attempts = %d, want 3", automaticJob.AttemptCount)
	}
	assertAttemptHistory(t, db, automatic.Job.ID, []string{"failed", "failed", "succeeded"})

	manual, err := service.Submit(ctx, jobs.CreateInput{
		Type: "fail", Payload: json.RawMessage(`{"failures_before_success":2}`), MaxAttempts: 2,
	})
	if err != nil || manual.EnqueueError != nil {
		cancel()
		t.Fatalf("submit dead-letter job: result=%+v err=%v", manual, err)
	}
	deadLetter := waitForJobStatus(t, ctx, repository, manual.Job.ID, jobs.StatusDeadLetter, 3*time.Second)
	if deadLetter.AttemptCount != 2 || deadLetter.LastError == "" {
		cancel()
		t.Fatalf("unexpected dead-letter job: %+v", deadLetter)
	}
	retried, err := service.Retry(ctx, manual.Job.ID)
	if err != nil || retried.EnqueueError != nil {
		cancel()
		t.Fatalf("manual retry: result=%+v err=%v", retried, err)
	}
	if retried.Job.Status != jobs.StatusQueued || retried.Job.MaxAttempts != 3 {
		cancel()
		t.Fatalf("unexpected manual retry transition: %+v", retried.Job)
	}
	manualJob := waitForJobStatus(t, ctx, repository, manual.Job.ID, jobs.StatusSucceeded, 3*time.Second)
	if manualJob.AttemptCount != 3 {
		cancel()
		t.Fatalf("manual retry attempts = %d, want 3", manualJob.AttemptCount)
	}
	assertAttemptHistory(t, db, manual.Job.ID, []string{"failed", "failed", "succeeded"})

	cancel()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("worker component stopped with error: %v", err)
		}
	}
}

func waitForJobStatus(
	t *testing.T,
	ctx context.Context,
	repository *postgresrepository.JobRepository,
	jobID string,
	want jobs.Status,
	timeout time.Duration,
) jobs.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		job, err := repository.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("get job %s: %v", jobID, err)
		}
		if job.Status == want {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s did not reach %s; last state=%+v", jobID, want, job)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func assertAttemptHistory(t *testing.T, db *sql.DB, jobID string, want []string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT status, finished_at IS NOT NULL
		FROM job_attempts
		WHERE job_id = $1
		ORDER BY attempt_number`, jobID)
	if err != nil {
		t.Fatalf("query attempts for %s: %v", jobID, err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var status string
		var finished bool
		if err := rows.Scan(&status, &finished); err != nil {
			t.Fatalf("scan attempt: %v", err)
		}
		if !finished {
			t.Fatalf("attempt for %s remained unfinished", jobID)
		}
		got = append(got, status)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate attempts: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("attempt statuses = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("attempt statuses = %v, want %v", got, want)
		}
	}
}
