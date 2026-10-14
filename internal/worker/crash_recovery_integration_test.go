package worker_test

import (
	"context"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestWorkerCrashRecoveryPipeline(t *testing.T) {
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
	prefix := "forgequeue:crash-test:" + strconv.FormatInt(time.Now().UnixNano(), 10)
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
	const leaseDuration = 100 * time.Millisecond
	coordinatorService, err := coordinator.NewGRPCServer(coordinator.ServerConfig{
		HeartbeatInterval: 100 * time.Millisecond,
		LeaseDuration:     leaseDuration,
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

	submission, err := service.Submit(ctx, jobs.CreateInput{
		Type:        "sleep",
		Payload:     json.RawMessage(`{"duration_ms":250}`),
		MaxAttempts: 3,
	})
	if err != nil || submission.EnqueueError != nil {
		t.Fatalf("submit job: result=%+v err=%v", submission, err)
	}
	jobID := submission.Job.ID

	if _, err := client.RegisterWorker(ctx, &workerpb.RegisterWorkerRequest{
		WorkerId: "worker-a", MaxConcurrency: 1, SupportedJobTypes: []string{"sleep"},
	}); err != nil {
		t.Fatalf("register worker A: %v", err)
	}
	claimed, err := client.ClaimJob(ctx, &workerpb.ClaimJobRequest{WorkerId: "worker-a"})
	if err != nil || claimed.GetJob().GetId() != jobID {
		t.Fatalf("worker A claim: response=%v err=%v", claimed, err)
	}
	if _, err := client.StartJob(ctx, &workerpb.StartJobRequest{WorkerId: "worker-a", JobId: jobID}); err != nil {
		t.Fatalf("worker A start: %v", err)
	}
	// Worker A now disappears without completing or renewing its lease.
	scanner := coordinator.NewRecoveryScanner(repository, jobQueue, logger, 10)
	recoveryDeadline := time.Now().Add(2 * time.Second)
	for {
		recovered, err := scanner.RecoverOnce(ctx)
		if err != nil {
			t.Fatalf("recover expired lease: %v", err)
		}
		if recovered == 1 {
			break
		}
		if time.Now().After(recoveryDeadline) {
			t.Fatal("worker A lease did not expire before deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := client.CompleteJob(ctx, &workerpb.CompleteJobRequest{
		WorkerId: "worker-a", JobId: jobID, ResultJson: []byte(`{"stale":true}`),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale worker completion error = %v, want FailedPrecondition", err)
	}

	if _, err := client.RegisterWorker(ctx, &workerpb.RegisterWorkerRequest{
		WorkerId: "worker-b", MaxConcurrency: 1, SupportedJobTypes: []string{"sleep"},
	}); err != nil {
		t.Fatalf("register worker B: %v", err)
	}
	broker, err := worker.NewGRPCBroker(client)
	if err != nil {
		t.Fatalf("initialize worker B broker: %v", err)
	}
	process, err := worker.NewWithBroker(worker.Config{
		ID:                 "worker-b",
		Concurrency:        1,
		PollInterval:       5 * time.Millisecond,
		LeaseRenewInterval: 25 * time.Millisecond,
		JobTypes:           []string{"sleep"},
	}, broker, worker.BuiltInHandlers(), logger)
	if err != nil {
		t.Fatalf("initialize worker B: %v", err)
	}

	scanCtx, cancelScanner := context.WithCancel(ctx)
	defer cancelScanner()
	go scanner.Run(scanCtx, 10*time.Millisecond)
	workerCtx, cancelWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- process.Run(workerCtx) }()

	completionDeadline := time.Now().Add(3 * time.Second)
	for {
		persisted, err := repository.GetJob(ctx, jobID)
		if err != nil {
			cancelWorker()
			t.Fatalf("get recovered job: %v", err)
		}
		if persisted.Status == jobs.StatusSucceeded {
			if persisted.AttemptCount != 2 || persisted.LeaseOwner != "" || persisted.LeaseExpiresAt != nil {
				cancelWorker()
				t.Fatalf("unexpected recovered completion: %+v", persisted)
			}
			break
		}
		if time.Now().After(completionDeadline) {
			cancelWorker()
			t.Fatalf("recovered job did not succeed; last state=%+v", persisted)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancelWorker()
	if err := <-workerDone; err != nil {
		t.Fatalf("worker B stopped with error: %v", err)
	}
}
