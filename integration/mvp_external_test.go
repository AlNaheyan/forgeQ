package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/client"
	"github.com/AlNaheyan/forgeQ/internal/api"
	"github.com/AlNaheyan/forgeQ/internal/coordinator"
	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/jobtypes"
	"github.com/AlNaheyan/forgeQ/internal/platform"
	"github.com/AlNaheyan/forgeQ/internal/queue"
	postgresrepository "github.com/AlNaheyan/forgeQ/internal/repository/postgres"
	"github.com/AlNaheyan/forgeQ/internal/tenant"
	"github.com/AlNaheyan/forgeQ/migrations"
	"github.com/AlNaheyan/forgeQ/proto/workerpb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExternalInvoiceAppRecoversLostCompletion(t *testing.T) {
	dsn, redisAddr := os.Getenv("FORGEQUEUE_TEST_DATABASE_URL"), os.Getenv("FORGEQUEUE_TEST_REDIS_ADDR")
	if dsn == "" || redisAddr == "" || os.Getenv("FORGEQUEUE_RECOVERY_ISOLATED") != "1" {
		t.Skip("isolated PostgreSQL and Redis are required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := platform.OpenPostgres(ctx, dsn)
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
	schema, err := os.ReadFile("../examples/invoice-app/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, string(schema)); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS invoice_app CASCADE")
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()
	prefix := "forgeq:mvp-invoice:" + time.Now().Format("150405.000000000")
	defer redisClient.Del(ctx, prefix+":ready", prefix+":delayed")
	q := queue.NewRedisQueue(redisClient, prefix)
	repo := postgresrepository.NewJobRepository(db)
	types, err := jobtypes.ApplicationRegistry([]string{"invoice.create"})
	if err != nil {
		t.Fatal(err)
	}
	service := jobs.NewService(repo, q, 3).WithJobTypes(types)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := tenant.NewStore(db)
	credential, err := keys.Issue(ctx, tenant.LegacyID, false)
	if err != nil {
		t.Fatal(err)
	}
	apiServer := httptest.NewServer(api.NewServer(service, nil, logger).WithAuthenticator(keys).Routes())
	defer apiServer.Close()
	producer, err := client.New(client.Options{BaseURL: apiServer.URL, APIKey: credential.Token, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	job, err := producer.Submit(ctx, client.SubmitRequest{Type: "invoice.create", Payload: map[string]any{"invoice_id": "inv-mvp", "amount_cents": 4500}, IdempotencyKey: "mvp-invoice-key"})
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != "queued" {
		t.Fatalf("job submitted before worker: %+v", job)
	}
	registry, err := coordinator.NewRegistry(coordinator.HealthThresholds{SuspectAfter: time.Second, UnhealthyAfter: 2 * time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	coord, err := coordinator.NewGRPCServer(coordinator.ServerConfig{HeartbeatInterval: 50 * time.Millisecond, LeaseDuration: 500 * time.Millisecond, RetryPolicy: coordinator.NewRetryPolicy(20*time.Millisecond, 50*time.Millisecond, 0, nil), Logger: logger}, registry, repo, q)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("t", 32)
	t.Setenv("APP_ENV", "test")
	t.Setenv("CONTROL_PLANE_TOKEN", token)
	auth, err := tenant.LoadRPCAuth()
	if err != nil {
		t.Fatal(err)
	}
	var lost atomic.Bool
	completionAttempt := make(chan struct{}, 1)
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		return auth.Intercept(ctx, req, info, func(ctx context.Context, req any) (any, error) {
			if strings.HasSuffix(info.FullMethod, "/CompleteJob") && lost.CompareAndSwap(false, true) {
				completionAttempt <- struct{}{}
				return nil, status.Error(codes.Unavailable, "simulated lost completion")
			}
			return next(ctx, req)
		})
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	workerpb.RegisterWorkerCoordinatorServer(server, coord)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve(listener)
	defer server.Stop()
	go coordinator.NewRecoveryScanner(repo, q, logger, 100).Run(ctx, 25*time.Millisecond)
	go coordinator.NewRetryPromoter(repo, q, logger, 100).Run(ctx, 25*time.Millisecond)
	exampleDir, err := filepath.Abs("../examples/invoice-app")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "invoice-app")
	build := exec.CommandContext(ctx, "go", "build", "-p", "1", "-o", binary, ".")
	build.Dir = exampleDir
	build.Env = append(os.Environ(), "GOWORK=off", "GOMAXPROCS=2")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build external example: %v\n%s", err, output)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	startWorker := func(id string) *exec.Cmd {
		process := exec.CommandContext(workerCtx, binary, "worker")
		process.Env = append(os.Environ(), "INVOICE_DATABASE_URL="+dsn, "FORGEQ_COORDINATOR_ADDR="+listener.Addr().String(), "FORGEQ_DEV_TOKEN="+token, "FORGEQ_ALLOW_INSECURE=true", "FORGEQ_WORKER_ID="+id)
		process.Stdout = io.Discard
		process.Stderr = io.Discard
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		return process
	}
	process := startWorker("mvp-invoice-worker-1")
	defer func() { stopWorker(); _ = process.Wait() }()
	select {
	case <-completionAttempt:
		if err := process.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		_ = process.Wait()
		process = startWorker("mvp-invoice-worker-2")
	case <-time.After(10 * time.Second):
		t.Fatal("first worker did not execute the invoice")
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, err = producer.Get(ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == "succeeded" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if job.Status != "succeeded" || job.AttemptCount < 2 || !lost.Load() {
		t.Fatalf("job did not recover lost completion: status=%s attempts=%d", job.Status, job.AttemptCount)
	}
	var result struct {
		InvoiceID   string `json:"invoice_id"`
		AmountCents int64  `json:"amount_cents"`
	}
	if err = json.Unmarshal(job.Result, &result); err != nil || result.InvoiceID != "inv-mvp" || result.AmountCents != 4500 {
		t.Fatalf("invoice result=%s err=%v", job.Result, err)
	}
	var count int
	if err = db.QueryRowContext(ctx, "SELECT count(*) FROM invoice_app.invoices WHERE invoice_id=$1", "inv-mvp").Scan(&count); err != nil || count != 1 {
		t.Fatalf("invoice rows=%d err=%v", count, err)
	}
	replay, err := producer.Submit(ctx, client.SubmitRequest{Type: "invoice.create", Payload: map[string]any{"invoice_id": "inv-mvp", "amount_cents": 4500}, IdempotencyKey: "mvp-invoice-key"})
	if err != nil || replay.ID != job.ID {
		t.Fatalf("idempotent submit replay=%+v err=%v", replay, err)
	}
	t.Run("dashboard", func(t *testing.T) {
		chrome := os.Getenv("CHROME_PATH")
		if chrome == "" {
			candidate := "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
			if _, err := os.Stat(candidate); err == nil {
				chrome = candidate
			} else {
				chrome, _ = exec.LookPath("google-chrome")
			}
		}
		_, nodeErr := exec.LookPath("node")
		if chrome == "" || nodeErr != nil {
			if os.Getenv("FORGEQ_REQUIRE_BROWSER") == "1" {
				t.Fatal("Chrome and Node are required for the dashboard check")
			}
			t.Skip("Chrome or Node unavailable")
		}
		script, err := filepath.Abs("../scripts/test-dashboard-mvp.mjs")
		if err != nil {
			t.Fatal(err)
		}
		browser := exec.CommandContext(ctx, "node", script)
		browser.Env = append(os.Environ(), "FORGEQ_MVP_URL="+apiServer.URL, "FORGEQ_MVP_KEY="+credential.Token, "CHROME_PATH="+chrome)
		if output, err := browser.CombinedOutput(); err != nil {
			t.Fatalf("dashboard check: %v\n%s", err, output)
		} else {
			t.Log(strings.TrimSpace(string(output)))
		}
	})
}
