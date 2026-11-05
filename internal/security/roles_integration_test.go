package security_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
	pg "github.com/AlNaheyan/forgeQ/internal/repository/postgres"
	"github.com/AlNaheyan/forgeQ/internal/security"
	"github.com/AlNaheyan/forgeQ/internal/tenant"
	"github.com/AlNaheyan/forgeQ/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// The caller supplies a fresh disposable database/cluster; no default DSN is used.
func TestRuntimeRolesIntegration(t *testing.T) {
	dsn := os.Getenv("SECURITY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated SECURITY_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal("open fixture")
	}
	defer admin.Close()
	var tables int
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname='public'").Scan(&tables); err != nil || tables != 0 {
		t.Fatal("requires empty dedicated fixture database")
	}
	if err := migrations.Up(ctx, admin); err != nil {
		t.Fatal(err)
	}
	if err := migrations.ConfigureEventStream(ctx, admin, true); err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile("../../deployments/security/bootstrap-roles.sql")
	if err != nil {
		t.Fatal(err)
	}
	lines := []string{}
	for _, line := range strings.Split(string(script), "\n") {
		if !strings.HasPrefix(line, "\\") {
			lines = append(lines, line)
		}
	}
	if _, err := admin.ExecContext(ctx, strings.Join(lines, "\n")); err != nil {
		t.Fatal(err)
	}
	adminStore := tenant.NewStore(admin)
	tenantID, err := adminStore.Create(ctx, "synthetic", tenant.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	key, err := adminStore.Issue(ctx, tenantID, false)
	if err != nil {
		t.Fatal(err)
	}
	scoped := map[string]*sql.DB{}
	for _, role := range []string{"api", "coordinator"} {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		defer db.Close()
		if _, err := db.ExecContext(ctx, "SET ROLE forgeq_"+role); err != nil {
			t.Fatal(err)
		}
		scoped[role] = db
		if err := security.CheckRuntimeRole(ctx, db, role); err != nil {
			t.Fatalf("%s role: %v", role, err)
		}
		if err := migrations.Verify(ctx, db, true); err != nil {
			t.Fatal(err)
		}
		if err := migrations.Verify(ctx, db, false); err == nil {
			t.Fatal("event config mismatch accepted")
		}
		for _, statement := range []string{"CREATE TABLE denied(id int)", "CREATE TEMP TABLE denied_temp(id int)", "ALTER TABLE jobs ADD COLUMN denied int", "UPDATE api_keys SET operator=true", "UPDATE tenants SET max_pending=9999", "UPDATE event_stream_config SET enabled=false", "DELETE FROM schema_migrations"} {
			if _, err := db.ExecContext(ctx, statement); err == nil {
				t.Fatalf("%s accepted privileged operation %s", role, statement)
			}
		}
	}
	if _, err := admin.ExecContext(ctx, "GRANT UPDATE(operator) ON api_keys TO forgeq_api"); err != nil {
		t.Fatal(err)
	}
	if err := security.CheckRuntimeRole(ctx, scoped["api"], "api"); err == nil {
		t.Fatal("column-level credential administration passed startup")
	}
	if _, err := admin.ExecContext(ctx, "REVOKE UPDATE(operator) ON api_keys FROM forgeq_api"); err != nil {
		t.Fatal(err)
	}

	if err := security.CheckRuntimeRole(ctx, admin, "api"); err == nil {
		t.Fatal("admin runtime accepted")
	}
	apiStore := tenant.NewStore(scoped["api"])
	principal, err := apiStore.Authenticate(ctx, key.Token)
	if err != nil {
		t.Fatal(err)
	}
	tenantCtx := tenant.WithPrincipal(ctx, principal)
	now := time.Now()
	job := jobs.Job{ID: tenant.NewID(), Type: "sleep", Payload: json.RawMessage(`{"duration_ms":1}`), Status: jobs.StatusQueued, MaxAttempts: 3, CreatedAt: now, UpdatedAt: now, ScheduledAt: now, IdempotencyKey: "role-fixture"}
	created, _, err := pg.NewJobRepository(scoped["api"]).CreateJob(tenantCtx, job)
	if err != nil {
		t.Fatal(err)
	}
	verifyPipeline(t, scoped["coordinator"], created.ID)

	if err := adminStore.Revoke(ctx, key.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := apiStore.Authenticate(ctx, key.Token); err != tenant.ErrUnauthorized {
		t.Fatal("revoked key still accepted", err)
	}
}
