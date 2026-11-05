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

	"github.com/AlNaheyan/forgeQ/internal/coordinator"
	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/platform"
	"github.com/AlNaheyan/forgeQ/internal/queue"
	postgresrepository "github.com/AlNaheyan/forgeQ/internal/repository/postgres"
	"github.com/AlNaheyan/forgeQ/internal/worker"
	"github.com/AlNaheyan/forgeQ/migrations"
	"github.com/AlNaheyan/forgeQ/proto/workerpb"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
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
	registry, err := coordinator.NewRegistry(coordinator.HealthThresholds{
		SuspectAfter:   50 * time.Millisecond,
		UnhealthyAfter: 100 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("initialize registry: %v", err)
	}
	coordinatorService, err := coordinator.NewGRPCServer(coordinator.ServerConfig{
		HeartbeatInterval: 10 * time.Millisecond,
		LeaseDuration:     30 * time.Second,
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
	coordinatorClient := workerpb.NewWorkerCoordinatorClient(connection)
	broker, err := worker.NewGRPCBroker(coordinatorClient)
	if err != nil {
		t.Fatalf("initialize broker: %v", err)
	}
	process, err := worker.NewWithBroker(worker.Config{
		ID:           "worker-integration",
		Concurrency:  2,
		PollInterval: 5 * time.Millisecond,
		JobTypes:     []string{"sleep", "fail"},
	}, broker, worker.BuiltInHandlers(), logger)
	if err != nil {
		t.Fatalf("initialize worker: %v", err)
	}
	remote, err := worker.NewRemoteCoordinator(worker.RemoteConfig{
		WorkerID:          process.ID(),
		MaxConcurrency:    2,
		SupportedJobTypes: []string{"sleep", "fail"},
	}, worker.NewGRPCCoordinatorRPC(coordinatorClient), process.ActiveJobs, logger)
	if err != nil {
		t.Fatalf("initialize remote coordinator: %v", err)
	}
	settings, err := remote.Register(ctx)
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	workerCtx, cancelWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 2)
	go func() { workerDone <- remote.RunHeartbeat(workerCtx, settings.HeartbeatInterval) }()
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
		t.Fatalf("worker component stopped with error: %v", err)
	}
	if err := <-workerDone; err != nil {
		t.Fatalf("worker stopped with error: %v", err)
	}
	workers := registry.List()
	if len(workers) != 1 || workers[0].WorkerID != "worker-integration" {
		t.Fatalf("worker registry = %+v", workers)
	}
}
