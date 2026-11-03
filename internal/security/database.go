package security

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
)

// ValidateDatabaseURL requires explicit authenticated, verified TLS connections.
func ValidateDatabaseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" || u.User == nil || u.User.Username() == "" {
		return errors.New("DATABASE_URL must specify an authenticated PostgreSQL URL")
	}
	if password, ok := u.User.Password(); !ok || password == "" {
		return errors.New("DATABASE_URL requires scoped credentials")
	}
	q := u.Query()
	if q.Get("sslmode") != "verify-full" || len(q["sslmode"]) != 1 || q.Get("sslrootcert") == "" || len(q["sslrootcert"]) != 1 {
		return errors.New("DATABASE_URL requires sslmode=verify-full and sslrootcert")
	}
	f, err := os.Open(q.Get("sslrootcert"))
	if err != nil {
		return errors.New("DATABASE_URL root certificate is unreadable")
	}
	defer f.Close()
	return nil
}

// CheckRuntimeRole rejects common privilege escalation and credential-management
// rights before listeners open. Run with the actual runtime login, not an admin.
func CheckRuntimeRole(ctx context.Context, db *sql.DB, service string) error {
	if service != "api" && service != "coordinator" {
		return errors.New("unsupported runtime database role")
	}
	var unsafe bool
	err := db.QueryRowContext(ctx, `SELECT
 NOT pg_has_role(current_user, $1, 'MEMBER')
 OR EXISTS(SELECT 1 FROM pg_roles WHERE rolname <> current_user AND rolname <> $1 AND pg_has_role(current_user,oid,'MEMBER'))
 OR
 EXISTS(SELECT 1 FROM pg_roles WHERE (rolname=current_user OR pg_has_role(current_user,oid,'MEMBER')) AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls))
 OR has_database_privilege(current_user,current_database(),'CREATE')
 OR EXISTS(SELECT 1 FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema' AND has_schema_privilege(current_user,oid,'CREATE'))
 OR EXISTS(SELECT 1 FROM pg_class WHERE relnamespace='public'::regnamespace AND relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user))
 OR has_table_privilege(current_user,'api_keys','INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
 OR has_table_privilege(current_user,'tenants','INSERT,DELETE,TRUNCATE,REFERENCES,TRIGGER')
 OR has_column_privilege(current_user,'tenants','max_pending','UPDATE')
 OR has_column_privilege(current_user,'tenants','max_concurrent','UPDATE')
 OR has_column_privilege(current_user,'tenants','per_minute','UPDATE')
 OR has_column_privilege(current_user,'tenants','max_payload_bytes','UPDATE')
 OR has_table_privilege(current_user,'event_stream_config','INSERT,UPDATE,DELETE,TRUNCATE')
 OR has_table_privilege(current_user,'schema_migrations','INSERT,UPDATE,DELETE,TRUNCATE')`, "forgeq_"+service).Scan(&unsafe)
	if err != nil {
		return errors.New("runtime database privilege verification failed")
	}
	if unsafe {
		return errors.New("runtime database role has administrative privileges")
	}
	return nil
}
