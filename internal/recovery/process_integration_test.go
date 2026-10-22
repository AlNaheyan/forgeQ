package recovery_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/proto/workerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// This test kills only processes it starts and stops only label-verified test
// containers. Run through scripts/test-recovery.sh, never a development stack.
func TestRecoveryProcessAndDependencyOutages(t *testing.T) {
	f := setup(t)
	apiPort, grpcPort, metricsPort := freePort(t), freePort(t), freePort(t)
	env := serviceEnv(map[string]string{
		"APP_ENV": "test", "DATABASE_URL": os.Getenv("FORGEQUEUE_TEST_DATABASE_URL"),
		"REDIS_ADDR": os.Getenv("FORGEQUEUE_TEST_REDIS_ADDR"), "COORDINATOR_ADDR": "127.0.0.1:" + grpcPort,
		"HTTP_PORT": apiPort, "GRPC_PORT": grpcPort, "METRICS_PORT": metricsPort,
		"EVENTS_ENABLED": "false", "DATABASE_TIMEOUT": "300ms", "REDIS_TIMEOUT": "300ms",
		"HEARTBEAT_INTERVAL": "100ms", "WORKER_TIMEOUT": "1s", "LEASE_DURATION": "5s", "LEASE_RENEW_INTERVAL": "500ms",
		"LEASE_SCAN_INTERVAL": "100ms", "QUEUE_RECONCILE_INTERVAL": "100ms", "RETRY_PROMOTION_INTERVAL": "100ms",
		"SHUTDOWN_TIMEOUT": "1s", "WORKER_CONCURRENCY": "1", "WORKER_ID": "process-worker", "WORKER_JOB_TYPES": "sleep",
		"POSTGRES_MAX_OPEN_CONNS": "5", "POSTGRES_MAX_IDLE_CONNS": "2", "REDIS_POOL_SIZE": "2",
	})
	base := "http://127.0.0.1:" + apiPort
	httpClient := &http.Client{Timeout: 2 * time.Second}
	getStatus := func(path string) int {
		res, err := httpClient.Get(base + path)
		if err != nil {
			return 0
		}
		defer res.Body.Close()
		_, _ = io.Copy(io.Discard, res.Body)
		return res.StatusCode
	}
	post := func(key string) (int, jobs.Job) {
		req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, base+"/v1/jobs", bytes.NewBufferString(`{"type":"sleep","payload":{"duration_ms":2000}}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		res, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var job jobs.Job
		if res.StatusCode < 300 {
			if err := json.NewDecoder(res.Body).Decode(&job); err != nil {
				t.Fatal(err)
			}
		}
		return res.StatusCode, job
	}
	api := startService(t, "api", env)
	eventually(t, "API startup readiness", func() bool { return getStatus("/readyz") == 200 })
	coord := startService(t, "coordinator", env)
	conn, err := grpc.NewClient("127.0.0.1:"+grpcPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	health := healthpb.NewHealthClient(conn)
	coordinatorReady := func() bool {
		ctx, cancel := context.WithTimeout(f.ctx, 300*time.Millisecond)
		defer cancel()
		res, err := health.Check(ctx, &healthpb.HealthCheckRequest{})
		return err == nil && res.Status == healthpb.HealthCheckResponse_SERVING
	}
	eventually(t, "coordinator startup readiness", coordinatorReady)
	client := workerpb.NewWorkerCoordinatorClient(conn)
	register := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
		defer cancel()
		if _, err := client.RegisterWorker(ctx, &workerpb.RegisterWorkerRequest{WorkerId: "manual-worker", MaxConcurrency: 1, SupportedJobTypes: []string{"sleep"}}); err != nil {
			t.Fatal(err)
		}
	}
	register()
	code, job := post("process-restart")
	if code != 201 {
		t.Fatalf("submit status=%d", code)
	}
	claim, err := client.ClaimJob(f.ctx, &workerpb.ClaimJobRequest{WorkerId: "manual-worker"})
	if err != nil || claim.GetJob().GetId() != job.ID {
		t.Fatalf("claim=%v err=%v", claim, err)
	}
	generation := claim.GetJob().GetLeaseGeneration()
	if _, err := client.StartJob(f.ctx, &workerpb.StartJobRequest{WorkerId: "manual-worker", JobId: job.ID, LeaseGeneration: generation}); err != nil {
		t.Fatal(err)
	}

	// Lose all in-memory coordinator state while a durable lease is active.
	coord.stop()
	started := time.Now()
	coord = startService(t, "coordinator", env)
	eventually(t, "coordinator restart readiness", coordinatorReady)
	if _, err := client.RenewLease(f.ctx, &workerpb.RenewLeaseRequest{WorkerId: "manual-worker", JobId: job.ID, LeaseGeneration: generation}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CompleteJob(f.ctx, &workerpb.CompleteJobRequest{WorkerId: "manual-worker", JobId: job.ID, LeaseGeneration: generation, ResultJson: []byte(`{"ok":true}`)}); err != nil {
		t.Fatal(err)
	}
	t.Logf("coordinator SIGKILL/restart: existing lease renewed/completed in %s", time.Since(started))

	api.stop()
	started = time.Now()
	api = startService(t, "api", env)
	eventually(t, "API restart readiness", func() bool { return getStatus("/readyz") == 200 })
	if getStatus("/v1/jobs/"+job.ID) != 200 {
		t.Fatal("API restart lost durable job")
	}
	code, replayed := post("process-restart")
	if code != 200 || replayed.ID != job.ID {
		t.Fatalf("idempotent replay after API restart: %d %+v", code, replayed)
	}
	t.Logf("API SIGKILL/restart: persisted job visible and submission deduplicated in %s", time.Since(started))

	code, queued := post("redis-loss")
	if code != 201 {
		t.Fatal(code)
	}
	containerAction(t, "redis", "stop")
	if getStatus("/readyz") != 503 || getStatus("/healthz") != 200 {
		t.Fatal("Redis outage readiness/liveness incorrect")
	}
	code, _ = post("redis-outage-rejected")
	if code < 500 {
		t.Fatalf("admission should fail closed during Redis outage, got %d", code)
	}
	started = time.Now()
	containerAction(t, "redis", "start")
	eventually(t, "Redis reconnect", func() bool { return getStatus("/readyz") == 200 })
	eventually(t, "Redis hint repair", func() bool { present, err := f.q.Contains(f.ctx, queued.ID); return err == nil && present })
	t.Logf("Redis stop/start with no persistence: queued hint restored in %s", time.Since(started))
	if _, err := f.repo.CancelJob(f.ctx, queued.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.q.Remove(f.ctx, queued.ID); err != nil {
		t.Fatal(err)
	}

	containerAction(t, "postgres", "stop")
	if getStatus("/readyz") != 503 || getStatus("/healthz") != 200 {
		t.Fatal("PostgreSQL outage readiness/liveness incorrect")
	}
	if code := getStatus("/v1/jobs/" + job.ID); code < 500 {
		t.Fatalf("database outage read status=%d", code)
	}
	code, _ = post("database-outage-rejected")
	if code < 500 {
		t.Fatalf("database outage submission status=%d", code)
	}
	// Startup against an unavailable database must fail before listening.
	ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
	unavailable := exec.CommandContext(ctx, filepath.Join(os.Getenv("FORGEQUEUE_RECOVERY_BIN_DIR"), "api"))
	unavailable.Env = env
	output, startErr := unavailable.CombinedOutput()
	cancel()
	if startErr == nil || !strings.Contains(string(output), "ping PostgreSQL") {
		t.Fatalf("startup should fail dependency check: %v %s", startErr, output)
	}
	started = time.Now()
	containerAction(t, "postgres", "start")
	eventually(t, "PostgreSQL reconnect", func() bool { return getStatus("/readyz") == 200 })
	// The fixture has its own pool; API readiness cannot drain its old sockets.
	// Retry this read as the driver discards connections killed by PostgreSQL.
	eventually(t, "fixture pool reconnect and durable result", func() bool {
		persisted, err := f.repo.GetJob(f.ctx, job.ID)
		return err == nil && persisted.Status == jobs.StatusSucceeded
	})
	code, retried := post("database-outage-rejected")
	if code != 201 {
		t.Fatalf("retry rejected submission: %d", code)
	}
	if _, err := f.repo.CancelJob(f.ctx, retried.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.q.Remove(f.ctx, retried.ID); err != nil {
		t.Fatal(err)
	}
	t.Logf("PostgreSQL stop/start: pool reconnect, committed result retained, rejected submission retry succeeds in %s", time.Since(started))

	// A real worker is killed during execution and restarted with the SAME ID.
	code, crashedJob := post("worker-crash")
	if code != 201 {
		t.Fatal(code)
	}
	workerEnv := serviceEnvFrom(env, map[string]string{"METRICS_PORT": freePort(t)})
	worker := startService(t, "worker", workerEnv)
	var firstGeneration int64
	eventually(t, "first worker attempt", func() bool {
		j, err := f.repo.GetJob(f.ctx, crashedJob.ID)
		firstGeneration = j.LeaseGeneration
		return err == nil && j.Status == jobs.StatusRunning
	})
	worker.stop()
	started = time.Now()
	worker = startService(t, "worker", workerEnv)
	eventually(t, "recovered worker attempt", func() bool {
		j, err := f.repo.GetJob(f.ctx, crashedJob.ID)
		return err == nil && j.Status == jobs.StatusRunning && j.LeaseGeneration > firstGeneration
	})
	if _, err := client.CompleteJob(f.ctx, &workerpb.CompleteJobRequest{WorkerId: "process-worker", JobId: crashedJob.ID, LeaseGeneration: firstGeneration}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale RPC completion accepted: %v", err)
	}
	if _, err := client.FailJob(f.ctx, &workerpb.FailJobRequest{WorkerId: "process-worker", JobId: crashedJob.ID, LeaseGeneration: firstGeneration, Error: "late", Retryable: true}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale RPC failure accepted: %v", err)
	}
	if _, err := client.RenewLease(f.ctx, &workerpb.RenewLeaseRequest{WorkerId: "process-worker", JobId: crashedJob.ID, LeaseGeneration: firstGeneration}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale RPC renewal accepted: %v", err)
	}
	eventually(t, "worker crash completion", func() bool {
		j, err := f.repo.GetJob(f.ctx, crashedJob.ID)
		return err == nil && j.Status == jobs.StatusSucceeded && j.AttemptCount == 2
	})
	t.Logf("worker SIGKILL/restart with reused ID: 2 attempts, stale RPCs fenced, completed in %s (5s lease, 2s handler)", time.Since(started))
}

func eventually(t *testing.T, label string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", label)
}

func freePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
}

type serviceProcess struct {
	cmd  *exec.Cmd
	done chan error
	once sync.Once
}

func (p *serviceProcess) stop() {
	p.once.Do(func() { _ = p.cmd.Process.Kill(); <-p.done })
}

func startService(t *testing.T, name string, env []string) *serviceProcess {
	t.Helper()
	binary := filepath.Join(os.Getenv("FORGEQUEUE_RECOVERY_BIN_DIR"), name)
	logFile, err := os.CreateTemp(os.Getenv("FORGEQUEUE_RECOVERY_EVIDENCE_DIR"), name+"-*.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary)
	cmd.Env = env
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	p := &serviceProcess{cmd: cmd, done: make(chan error, 1)}
	go func() { err := cmd.Wait(); _ = logFile.Close(); p.done <- err }()
	t.Cleanup(p.stop)
	return p
}

func serviceEnv(overrides map[string]string) []string { return serviceEnvFrom(os.Environ(), overrides) }
func serviceEnvFrom(base []string, overrides map[string]string) []string {
	values := map[string]string{}
	for _, entry := range base {
		k, v, ok := strings.Cut(entry, "=")
		if ok {
			values[k] = v
		}
	}
	for k, v := range overrides {
		values[k] = v
	}
	result := make([]string, 0, len(values))
	for k, v := range values {
		result = append(result, k+"="+v)
	}
	sort.Strings(result)
	return result
}

func containerAction(t *testing.T, service, action string) {
	t.Helper()
	project := os.Getenv("FORGEQUEUE_RECOVERY_PROJECT")
	id := os.Getenv("FORGEQUEUE_RECOVERY_" + strings.ToUpper(service) + "_CONTAINER")
	if !strings.HasPrefix(project, "forgequeue-recovery-") || id == "" || (action != "start" && action != "stop") {
		t.Fatal("invalid isolated recovery target")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "inspect", "--format", `{{ index .Config.Labels "com.docker.compose.project" }}:{{ index .Config.Labels "com.docker.compose.service" }}`, id).CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != project+":"+service {
		t.Fatalf("refusing container outside test project: %s %v", output, err)
	}
	args := []string{action}
	if action == "stop" {
		args = append(args, "--time=1")
	}
	args = append(args, id)
	if output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("%s %s: %v %s", action, service, err, output)
	}
}
