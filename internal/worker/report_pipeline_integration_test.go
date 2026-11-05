package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/coordinator"
	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/jobtypes"
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

type integrationReportSink struct {
	mu      sync.Mutex
	target  worker.ReportSink
	loseAck bool
}

func (s *integrationReportSink) PutIfAbsent(ctx context.Context, tenant, key string, report worker.ReportFixture) (worker.ReportFixture, error) {
	stored, err := s.target.PutIfAbsent(ctx, tenant, key, report)
	if err != nil {
		return worker.ReportFixture{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loseAck {
		s.loseAck = false
		return worker.ReportFixture{}, errors.New("simulated lost acknowledgment")
	}
	return stored, nil
}

func TestReportFixtureRetriesAcrossGRPCWithoutRepeatingEffect(t *testing.T) {
	databaseURL, redisAddress := os.Getenv("FORGEQUEUE_TEST_DATABASE_URL"), os.Getenv("FORGEQUEUE_TEST_REDIS_ADDR")
	if databaseURL == "" || redisAddress == "" || os.Getenv("FORGEQUEUE_RECOVERY_ISOLATED") != "1" {
		t.Skip("disposable PostgreSQL and Redis required")
	}
	ctx := context.Background()
	db, err := platform.OpenPostgres(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	lock, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if _, err = lock.ExecContext(ctx, "SELECT pg_advisory_lock(7346871)"); err != nil {
		t.Fatal(err)
	}
	defer lock.ExecContext(context.Background(), "SELECT pg_advisory_unlock(7346871)")
	if err = migrations.Up(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, "TRUNCATE job_attempts, idempotency_keys, jobs CASCADE"); err != nil {
		t.Fatal(err)
	}
	schema, err := os.ReadFile("../../examples/report-fixture/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, string(schema)); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS forgeq_report_fixture CASCADE")
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	defer redisClient.Close()
	prefix := "forgequeue:report-fixture:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	defer redisClient.Del(ctx, prefix+":ready", prefix+":delayed")
	q := queue.NewRedisQueue(redisClient, prefix)
	repo := postgresrepository.NewJobRepository(db)
	descriptors := append(jobtypes.Fixtures().Descriptors(), jobtypes.ReportFixtureDescriptor())
	types, err := jobtypes.NewRegistry(descriptors)
	if err != nil {
		t.Fatal(err)
	}
	service := jobs.NewService(repo, q, 3).WithJobTypes(types)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry, err := coordinator.NewRegistry(coordinator.HealthThresholds{SuspectAfter: time.Second, UnhealthyAfter: 2 * time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server, err := coordinator.NewGRPCServer(coordinator.ServerConfig{HeartbeatInterval: 50 * time.Millisecond, LeaseDuration: time.Second, RetryPolicy: coordinator.NewRetryPolicy(20*time.Millisecond, 50*time.Millisecond, 0, nil), Logger: logger}, registry, repo, q)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	workerpb.RegisterWorkerCoordinatorServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := workerpb.NewWorkerCoordinatorClient(conn)
	broker, err := worker.NewGRPCBroker(client)
	if err != nil {
		t.Fatal(err)
	}
	sink := &integrationReportSink{target: worker.PostgresReportSink{DB: db}, loseAck: true}
	handler := worker.ReportExportHandler{Sink: sink}
	process, err := worker.NewWithBroker(worker.Config{ID: "report-worker", Concurrency: 1, PollInterval: 3 * time.Millisecond, LeaseRenewInterval: 100 * time.Millisecond, JobTypes: []string{"report_export.v1"}, Registry: types}, broker, map[string]worker.Handler{"report_export.v1": handler}, logger)
	if err != nil {
		t.Fatal(err)
	}
	remote, err := worker.NewRemoteCoordinator(worker.RemoteConfig{WorkerID: process.ID(), MaxConcurrency: 1, SupportedJobTypes: []string{"report_export.v1"}}, worker.NewGRPCCoordinatorRPC(client), process.ActiveJobs, logger)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := remote.Register(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 2)
	go func() { done <- remote.RunHeartbeat(runCtx, settings.HeartbeatInterval) }()
	go func() { done <- process.Run(runCtx) }()
	defer func() { cancel(); <-done; <-done }()
	go coordinator.NewRetryPromoter(repo, q, logger, 100).Run(runCtx, 5*time.Millisecond)
	submission, err := service.Submit(ctx, jobs.CreateInput{Type: "report_export.v1", Payload: json.RawMessage(`{"report_id":"fixture","values":[4,7,-2]}`), MaxAttempts: 3})
	if err != nil || submission.EnqueueError != nil {
		t.Fatalf("submit: %+v %v", submission, err)
	}
	finished := waitForJobStatus(t, ctx, repo, submission.Job.ID, jobs.StatusSucceeded, 5*time.Second)
	if finished.AttemptCount != 2 {
		t.Fatalf("attempts = %d", finished.AttemptCount)
	}
	var result worker.ReportFixture
	if err := json.Unmarshal(finished.Result, &result); err != nil || result.Sum != 9 || result.Count != 3 {
		t.Fatalf("result=%s error=%v", finished.Result, err)
	}
	var writes int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM forgeq_report_fixture.reports").Scan(&writes); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatalf("effect executed %d times", writes)
	}
	// Corrupt a stored payload after validation to exercise the worker's
	// defensive validation and non-retryable failure through the gRPC broker.
	invalid, err := jobs.NewWithRegistry(jobs.CreateInput{Type: "report_export.v1", Payload: json.RawMessage(`{"report_id":"fixture","values":[1]}`), MaxAttempts: 3}, time.Now(), types)
	if err != nil {
		t.Fatal(err)
	}
	invalid.Payload = json.RawMessage(`{"report_id":"fixture","values":[1],"url":"http://internal"}`)
	stored, _, err := repo.CreateJob(ctx, invalid)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, stored.ID, stored.Priority, stored.ScheduledAt); err != nil {
		t.Fatal(err)
	}
	dead := waitForJobStatus(t, ctx, repo, stored.ID, jobs.StatusDeadLetter, 5*time.Second)
	if dead.AttemptCount != 1 || dead.LastError != "invalid_payload" {
		t.Fatalf("invalid payload was retried or misclassified: %+v", dead)
	}
}
