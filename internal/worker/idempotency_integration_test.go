package worker_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/coordinator"
	"github.com/AlNaheyan/forgeQ/internal/idempotency"
	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/platform"
	"github.com/AlNaheyan/forgeQ/internal/queue"
	postgresrepository "github.com/AlNaheyan/forgeQ/internal/repository/postgres"
	"github.com/AlNaheyan/forgeQ/migrations"
	"github.com/AlNaheyan/forgeQ/proto/workerpb"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const duplicateExecutionSideEffectsTable = "forgequeue_test_execution_side_effects"

type idempotentSideEffectHandler struct {
	executor *idempotency.Executor
}

type idempotentSideEffectInput struct {
	EffectKey string `json:"effect_key"`
}

type idempotentSideEffectResult struct {
	Executed bool `json:"executed"`
}

func (h idempotentSideEffectHandler) Execute(ctx context.Context, payload json.RawMessage) (any, error) {
	var input idempotentSideEffectInput
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Errorf("decode side-effect payload: %w", err)
	}
	if input.EffectKey == "" {
		return nil, fmt.Errorf("effect_key is required")
	}

	executed, err := h.executor.ExecuteOnceTx(ctx, input.EffectKey, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			"INSERT INTO "+duplicateExecutionSideEffectsTable+" (effect_key) VALUES ($1)",
			input.EffectKey,
		)
		return err
	})
	if err != nil {
		return nil, err
	}
	return idempotentSideEffectResult{Executed: executed}, nil
}

func TestIdempotentHandlerExecutesSideEffectOnceAfterLostCompletion(t *testing.T) {
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
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS forgequeue_test_execution_side_effects (
			id BIGSERIAL PRIMARY KEY,
			effect_key TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
		)`); err != nil {
		t.Fatalf("create test side-effect table: %v", err)
	}
	defer db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+duplicateExecutionSideEffectsTable)
	if _, err := db.ExecContext(ctx,
		"TRUNCATE "+duplicateExecutionSideEffectsTable+", execution_idempotency_keys, job_attempts, idempotency_keys, jobs CASCADE",
	); err != nil {
		t.Fatalf("clean tables: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	defer redisClient.Close()
	prefix := "forgequeue:idempotency-test:" + strconv.FormatInt(time.Now().UnixNano(), 10)
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
	const leaseDuration = 150 * time.Millisecond
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

	payload := json.RawMessage(`{"effect_key":"send-receipt-order-42"}`)
	submission, err := service.Submit(ctx, jobs.CreateInput{
		Type: "sleep", Payload: payload, MaxAttempts: 3,
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
	if _, err := client.StartJob(ctx, &workerpb.StartJobRequest{WorkerId: "worker-a", JobId: jobID, LeaseGeneration: 1}); err != nil {
		t.Fatalf("worker A start: %v", err)
	}
	firstResult, err := (idempotentSideEffectHandler{executor: idempotency.NewExecutor(db)}).Execute(ctx, payload)
	if err != nil {
		t.Fatalf("worker A execute handler: %v", err)
	}
	if result := firstResult.(idempotentSideEffectResult); !result.Executed {
		t.Fatal("worker A did not perform the initial side effect")
	}
	// The side effect committed, but worker A disappears before its completion
	// acknowledgement reaches the coordinator. The durable job remains running.

	scanner := coordinator.NewRecoveryScanner(repository, jobQueue, logger, 10)
	recoveryDeadline := time.Now().Add(3 * time.Second)
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

	if _, err := client.RegisterWorker(ctx, &workerpb.RegisterWorkerRequest{
		WorkerId: "worker-b", MaxConcurrency: 1, SupportedJobTypes: []string{"sleep"},
	}); err != nil {
		t.Fatalf("register worker B: %v", err)
	}
	reclaimed, err := client.ClaimJob(ctx, &workerpb.ClaimJobRequest{WorkerId: "worker-b"})
	if err != nil || reclaimed.GetJob().GetId() != jobID {
		t.Fatalf("worker B claim: response=%v err=%v", reclaimed, err)
	}
	if _, err := client.StartJob(ctx, &workerpb.StartJobRequest{WorkerId: "worker-b", JobId: jobID, LeaseGeneration: 2}); err != nil {
		t.Fatalf("worker B start: %v", err)
	}
	secondResult, err := (idempotentSideEffectHandler{executor: idempotency.NewExecutor(db)}).Execute(ctx, payload)
	if err != nil {
		t.Fatalf("worker B execute handler: %v", err)
	}
	if result := secondResult.(idempotentSideEffectResult); result.Executed {
		t.Fatal("worker B repeated an already committed side effect")
	}
	if _, err := client.CompleteJob(ctx, &workerpb.CompleteJobRequest{
		WorkerId: "worker-b", JobId: jobID, ResultJson: []byte(`{"deduplicated":true}`), LeaseGeneration: 2,
	}); err != nil {
		t.Fatalf("worker B complete: %v", err)
	}

	var sideEffectCount int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM "+duplicateExecutionSideEffectsTable+" WHERE effect_key = $1",
		"send-receipt-order-42",
	).Scan(&sideEffectCount); err != nil {
		t.Fatalf("count side effects: %v", err)
	}
	if sideEffectCount != 1 {
		t.Fatalf("side-effect executions = %d, want 1", sideEffectCount)
	}

	persisted, err := repository.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get completed job: %v", err)
	}
	if persisted.Status != jobs.StatusSucceeded || persisted.AttemptCount != 2 {
		t.Fatalf("completed job = %+v, want succeeded after two attempts", persisted)
	}
	var attempts, expired, succeeded int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*),
			count(*) FILTER (WHERE status = 'expired'),
			count(*) FILTER (WHERE status = 'succeeded')
		FROM job_attempts
		WHERE job_id = $1`, jobID,
	).Scan(&attempts, &expired, &succeeded); err != nil {
		t.Fatalf("read attempt history: %v", err)
	}
	if attempts != 2 || expired != 1 || succeeded != 1 {
		t.Fatalf("attempt history: total=%d expired=%d succeeded=%d", attempts, expired, succeeded)
	}
}
