package backup

import (
	"context"
	"database/sql"
	"fmt"
	"forgequeue/internal/idempotency"
	"forgequeue/internal/jobs"
	"forgequeue/internal/repository/postgres"
	"forgequeue/internal/tenant"
	"forgequeue/migrations"
	"net/url"
	"os"
	"testing"
	"time"
)

func TestIsolatedPostgresRestore(t *testing.T) {
	dsn := os.Getenv("FORGEQUEUE_BACKUP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("explicit isolated backup database absent")
	}
	u, e := url.Parse(dsn)
	if e != nil || u.Hostname() != "127.0.0.1" || u.Path != "/forgequeue_backup_test" {
		t.Fatal("unsafe integration target")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, e := sql.Open("pgx", dsn)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { admin.Close() })
	suffix := fmt.Sprint(time.Now().UnixNano())
	source := "forgeq_restore_source_" + suffix
	dest := "forgeq_restore_dest_" + suffix
	open := func(name string) *sql.DB {
		t.Helper()
		if _, e := admin.ExecContext(ctx, "CREATE DATABASE "+name); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })
		u.Path = "/" + name
		db, e := sql.Open("pgx", u.String())
		if e != nil {
			t.Fatal(e)
		}
		return db
	}
	src := open(source)
	defer src.Close()
	sourceDSN := u.String()
	dst := open(dest)
	defer dst.Close()
	destDSN := u.String()
	if e = migrations.Up(ctx, src); e != nil {
		t.Fatal(e)
	}
	store := tenant.NewStore(src)
	tid, e := store.Create(ctx, "backup-synthetic", tenant.DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	key, e := store.Issue(ctx, tid, false)
	if e != nil {
		t.Fatal(e)
	}
	scope := tenant.WithPrincipal(ctx, tenant.Principal{ID: tid})
	repo := postgres.NewJobRepository(src)
	newJob := func(key string) jobs.Job {
		t.Helper()
		j, e := jobs.New(jobs.CreateInput{Type: "sleep", Payload: []byte(`{"duration_ms":1}`), MaxAttempts: 3, IdempotencyKey: key}, time.Now().Add(-time.Minute))
		if e != nil {
			t.Fatal(e)
		}
		j, _, e = repo.CreateJob(scope, j)
		if e != nil {
			t.Fatal(e)
		}
		return j
	}
	done := newJob("completed")
	queued := newJob("queued")
	active := newJob("active")
	leased, e := repo.LeaseJob(ctx, active.ID, "fixture", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = repo.StartAttempt(ctx, leased.ID, "fixture", leased.LeaseGeneration); e != nil {
		t.Fatal(e)
	}
	if _, e = src.ExecContext(ctx, `UPDATE jobs SET status='succeeded', result='{"answer":42}' WHERE id=$1`, done.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = idempotency.NewExecutor(src).ExecuteOnce(scope, "effect", func(context.Context) error { return nil }); e != nil {
		t.Fatal(e)
	}
	r, id := fixture(t)
	m, e := r.Backup(ctx, DumpPostgres(sourceDSN))
	if e != nil {
		t.Fatal(e)
	}
	src.Close()
	if _, e = admin.ExecContext(ctx, "DROP DATABASE "+source+" WITH (FORCE)"); e != nil {
		t.Fatal(e)
	}
	if e = RestorePostgres(ctx, r.Store, m, destDSN, t.TempDir(), id); e != nil {
		t.Fatal(e)
	}
	if e = RestorePostgres(ctx, r.Store, m, destDSN, t.TempDir(), id); e == nil {
		t.Fatal("nonempty restore allowed")
	}
	if _, e = tenant.NewStore(dst).Authenticate(ctx, key.Token); e != nil {
		t.Fatal("credential missing", e)
	}
	restored := postgres.NewJobRepository(dst)
	got, e := restored.GetJobByIdempotencyKey(scope, "completed")
	if e != nil || got.ID != done.ID || string(got.Result) != `{"answer": 42}` {
		t.Fatal("result/submission mismatch", e)
	}
	var attempts int
	if e = dst.QueryRowContext(ctx, "SELECT count(*) FROM job_attempts WHERE job_id=$1", active.ID).Scan(&attempts); e != nil || attempts != 1 {
		t.Fatal("attempt missing", e)
	}
	executed, e := idempotency.NewExecutor(dst).ExecuteOnce(scope, "effect", func(context.Context) error { t.Fatal("duplicate effect"); return nil })
	if e != nil || executed {
		t.Fatal(e)
	}
	j, e := restored.LeaseJob(ctx, queued.ID, "restored-worker", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = restored.StartAttempt(ctx, j.ID, "restored-worker", j.LeaseGeneration); e != nil {
		t.Fatal(e)
	}
	if _, e = restored.SucceedAttempt(ctx, j.ID, "restored-worker", []byte(`{"executed":true}`), j.LeaseGeneration); e != nil {
		t.Fatal(e)
	}
	t.Log("encrypted restore passed after source database deletion; credential/result/attempt/submission/execution marker and runnable queue verified")
}
