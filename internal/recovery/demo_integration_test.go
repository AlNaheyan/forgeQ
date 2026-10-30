package recovery_test

import (
	"forgequeue/internal/tenant"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestLiveDashboardDemo(t *testing.T) {
	if os.Getenv("FORGEQUEUE_CAPTURE_DEMO") != "1" {
		t.Skip("opt-in browser demo; set FORGEQUEUE_CAPTURE_DEMO=1")
	}
	f := setup(t)
	id, err := tenant.NewStore(f.db).Create(f.ctx, "demo", tenant.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	key, err := tenant.NewStore(f.db).Issue(f.ctx, id, true)
	if err != nil {
		t.Fatal(err)
	}
	apiPort, grpcPort := freePort(t), freePort(t)
	env := serviceEnv(map[string]string{
		"DATABASE_URL": os.Getenv("FORGEQUEUE_TEST_DATABASE_URL"), "REDIS_ADDR": os.Getenv("FORGEQUEUE_TEST_REDIS_ADDR"),
		"CONTROL_PLANE_TOKEN": strings.Repeat("demo-only-", 8), "COORDINATOR_ADDR": "127.0.0.1:" + grpcPort,
		"HTTP_PORT": apiPort, "GRPC_PORT": grpcPort, "METRICS_PORT": freePort(t), "EVENTS_ENABLED": "false", "RETENTION_ENABLED": "false",
		"WORKER_CONCURRENCY": "2", "WORKER_ID": "demo-worker", "WORKER_JOB_TYPES": "sleep,fail",
		"POSTGRES_MAX_OPEN_CONNS": "5", "POSTGRES_MAX_IDLE_CONNS": "2", "REDIS_POOL_SIZE": "2",
	})
	startService(t, "api", env)
	base := "http://127.0.0.1:" + apiPort
	eventually(t, "demo API readiness", func() bool {
		r, err := http.Get(base + "/readyz")
		if err != nil {
			return false
		}
		defer r.Body.Close()
		return r.StatusCode == 200
	})
	startService(t, "coordinator", env)
	startService(t, "worker", env)
	script, err := filepath.Abs("../../scripts/capture-demo.mjs")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(f.ctx, "node", script)
	cmd.Env = serviceEnv(map[string]string{"FORGEQUEUE_DEMO_URL": base, "FORGEQUEUE_DEMO_KEY": key.Token, "FORGEQUEUE_DEMO_OUTPUT": os.Getenv("FORGEQUEUE_RECOVERY_EVIDENCE_DIR")})
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("browser demo failed: %v %s", err, out)
	}
	t.Log(strings.TrimSpace(string(out)))
}
