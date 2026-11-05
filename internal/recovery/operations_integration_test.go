package recovery_test

import (
	"bytes"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/operations"
	"github.com/AlNaheyan/forgeQ/internal/platform"
	"github.com/AlNaheyan/forgeQ/internal/repository/postgres"
	"github.com/AlNaheyan/forgeQ/internal/telemetry"
	"github.com/AlNaheyan/forgeQ/internal/tenant"
	"github.com/AlNaheyan/forgeQ/migrations"
)

func TestRetentionBoundsAndPreservesWork(t *testing.T) {
	f := setup(t)
	metrics := telemetry.NewMetrics()
	m := operations.Maintenance{DB: f.db, Metrics: metrics, Config: operations.Config{Batch: 2, JobAge: 24 * time.Hour, EventAge: 24 * time.Hour}}
	var removable []string
	for i := 0; i < 5; i++ {
		j := f.create(t)
		removable = append(removable, j.ID)
		if _, err := f.db.ExecContext(f.ctx, `UPDATE jobs SET status='succeeded',updated_at=now()-interval '40 days' WHERE id=$1`, j.ID); err != nil {
			t.Fatal(err)
		}
	}
	active := f.create(t)
	keyed, err := jobs.New(jobs.CreateInput{Type: "sleep", Payload: json.RawMessage(`{}`), MaxAttempts: 3, IdempotencyKey: "preserve-marker"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	keyed, _, err = f.repo.CreateJob(f.ctx, keyed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(f.ctx, `UPDATE jobs SET status='succeeded',updated_at=now()-interval '40 days' WHERE id=$1`, keyed.ID); err != nil {
		t.Fatal(err)
	}
	dead := f.create(t)
	if _, err := f.db.ExecContext(f.ctx, `UPDATE jobs SET status='dead_letter',updated_at=now()-interval '40 days' WHERE id=$1`, dead.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(f.ctx, `INSERT INTO execution_idempotency_keys(key,completed_at) VALUES('keep-forever',now()-interval '40 days'); INSERT INTO event_outbox(event_type,aggregate_type,aggregate_id,payload,published_at) SELECT 'JobCreated','job','fixture','{}',now()-interval '40 days' FROM generate_series(1,5); INSERT INTO event_outbox(event_type,aggregate_type,aggregate_id,payload) VALUES('JobCreated','job','pending','{}')`); err != nil {
		t.Fatal(err)
	}
	count := func(table string) int {
		t.Helper()
		var n int
		if err := f.db.QueryRowContext(f.ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if err := m.Tick(f.ctx); err != nil {
		t.Fatal(err)
	}
	if count("jobs") != 8 {
		t.Fatal("disabled retention deleted jobs")
	}
	m.Config.Enabled = true
	if err := m.Tick(f.ctx); err != nil {
		t.Fatal(err)
	}
	if count("jobs") != 6 || count("event_outbox") != 4 {
		t.Fatal("cleanup exceeded or missed batch size")
	}
	for i := 0; i < 3; i++ {
		if err := m.Tick(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{active.ID, keyed.ID, dead.ID} {
		if _, err := f.repo.GetJob(f.ctx, id); err != nil {
			t.Fatalf("protected job removed: %v", err)
		}
	}
	if count("jobs") != 3 || count("event_outbox") != 1 || count("execution_idempotency_keys") != 1 || count("idempotency_keys") != 1 {
		t.Fatal("retention removed protected records")
	}
}

// Uses only the disposable container provided by test-recovery.sh. Dumps an
// old-schema representative database, restores into a fresh DB, then upgrades.
func TestBackupRestoreAndUpgrade(t *testing.T) {
	f := setup(t)
	container := os.Getenv("FORGEQUEUE_RECOVERY_POSTGRES_CONTAINER")
	project := os.Getenv("FORGEQUEUE_RECOVERY_PROJECT")
	label, err := exec.CommandContext(f.ctx, "docker", "inspect", "--format", `{{index .Config.Labels "com.docker.compose.project"}}`, container).Output()
	if err != nil || strings.TrimSpace(string(label)) != project || !strings.HasPrefix(project, "forgequeue-recovery-") {
		t.Fatal("refusing drill outside isolated container")
	}
	// Unique databases are destroyed with the disposable container, never production.
	source := "upgrade_source"
	dest := "upgrade_restored"
	for _, name := range []string{source, dest} {
		if _, err := f.db.ExecContext(f.ctx, "CREATE DATABASE "+name); err != nil {
			t.Fatal(err)
		}
	}
	u, err := url.Parse(os.Getenv("FORGEQUEUE_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + source
	old, err := platform.OpenPostgres(f.ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err := migrations.UpTo(f.ctx, old, "008_tenants.sql"); err != nil {
		t.Fatal(err)
	}
	store := tenant.NewStore(old)
	id, err := store.Create(f.ctx, "restore-drill", tenant.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	key, err := store.Issue(f.ctx, id, false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := tenant.WithPrincipal(f.ctx, tenant.Principal{ID: id})
	repo := postgres.NewJobRepository(old)
	j, err := jobs.New(jobs.CreateInput{Type: "sleep", Payload: json.RawMessage(`{"duration_ms":1}`), MaxAttempts: 3, IdempotencyKey: "backup-key"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	j, _, err = repo.CreateJob(ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.ExecContext(f.ctx, `UPDATE jobs SET status='succeeded',result='{"restored":true}' WHERE id=$1`, j.ID); err != nil {
		t.Fatal(err)
	}
	dump, err := exec.CommandContext(f.ctx, "docker", "exec", container, "pg_dump", "-U", "forgequeue", "-d", source, "--format=custom", "--no-owner", "--no-acl").Output()
	if err != nil {
		t.Fatal("pg_dump failed", err)
	}
	restore := exec.CommandContext(f.ctx, "docker", "exec", "-i", container, "pg_restore", "-U", "forgequeue", "-d", dest, "--exit-on-error", "--single-transaction", "--no-owner", "--no-acl")
	restore.Stdin = bytes.NewReader(dump)
	if out, err := restore.CombinedOutput(); err != nil {
		t.Fatalf("pg_restore: %v %s", err, out)
	}
	u.Path = "/" + dest
	restored, err := platform.OpenPostgres(f.ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := migrations.Up(f.ctx, restored); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Up(f.ctx, restored); err != nil {
		t.Fatalf("repeat upgrade: %v", err)
	}
	if err := migrations.UpTo(f.ctx, restored, "008_tenants.sql"); err == nil {
		t.Fatal("older migration target accepted newer schema")
	}
	p, err := tenant.NewStore(restored).Authenticate(f.ctx, key.Token)
	if err != nil || p.ID != id {
		t.Fatal("credential verifier not restored")
	}
	got, err := postgres.NewJobRepository(restored).GetJobByIdempotencyKey(ctx, "backup-key")
	if err != nil || got.ID != j.ID || got.Status != jobs.StatusSucceeded || !strings.Contains(string(got.Result), "true") {
		t.Fatalf("restored job mismatch: %v", err)
	}
	t.Logf("custom-format backup restored and upgraded 008 -> 009; tenant, credential, job result and submission marker verified (%d archive bytes)", len(dump))
}
