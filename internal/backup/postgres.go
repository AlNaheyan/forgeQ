package backup

import (
	"context"
	"database/sql"
	"errors"
	"filippo.io/age"
	_ "github.com/jackc/pgx/v5/stdlib"
	"io"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

func SecretFile(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", errors.New("secret file unavailable")
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > 65536 {
		return "", errors.New("secret file must be private regular bounded file")
	}
	b, e := io.ReadAll(io.LimitReader(f, 65537))
	if e != nil || len(b) > 65536 {
		return "", errors.New("secret file read failed")
	}
	v := strings.TrimSuffix(string(b), "\n")
	if v == "" {
		return "", errors.New("secret file empty")
	}
	return v, nil
}
func DumpPostgres(dsn string) func(context.Context, io.Writer) error {
	return func(ctx context.Context, w io.Writer) error {
		c := exec.CommandContext(ctx, "pg_dump", "--format=custom", "--no-owner", "--no-acl")
		var e error
		c.Env, e = postgresEnv(dsn)
		if e != nil {
			return e
		}
		c.Stdout = w
		if c.Run() != nil {
			return errors.New("pg_dump failed")
		}
		return nil
	}
}

// RestorePostgres accepts only an explicitly named, empty loopback fixture DB.
// There is deliberately no destructive overwrite option.
func RestorePostgres(ctx context.Context, s Storage, m Manifest, dsn, temp string, ids ...age.Identity) error {
	u, e := url.Parse(dsn)
	if e != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") || !regexp.MustCompile(`^forgeq_restore_[a-z0-9_]{1,48}$`).MatchString(strings.TrimPrefix(u.Path, "/")) {
		return errors.New("restore requires isolated loopback forgeq_restore_ database")
	}
	for key := range u.Query() {
		if key == "host" || key == "hostaddr" || key == "service" || key == "dbname" {
			return errors.New("restore connection override refused")
		}
	}
	f, e := Decrypt(ctx, s, m, temp, ids...)
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	db, e := sql.Open("pgx", dsn)
	if e != nil {
		return errors.New("restore destination unavailable")
	}
	defer db.Close()
	var count int
	e = db.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' UNION ALL SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') UNION ALL SELECT oid FROM pg_namespace WHERE nspname NOT IN ('pg_catalog','information_schema','public') AND nspname NOT LIKE 'pg_toast%' AND nspname NOT LIKE 'pg_temp%') objects`).Scan(&count)
	if e != nil {
		return errors.New("restore destination inspection failed")
	}
	if count != 0 {
		return errors.New("restore destination nonempty")
	}
	c := exec.CommandContext(ctx, "pg_restore", "--dbname", strings.TrimPrefix(u.Path, "/"), "--exit-on-error", "--single-transaction", "--no-owner", "--no-acl")
	c.Env, e = postgresEnv(dsn)
	if e != nil {
		return e
	}
	c.Stdin = f
	if c.Run() != nil {
		return errors.New("pg_restore failed")
	}
	return recordRestore(ctx, s, m)
}

// libpq does not expand a URI in PGDATABASE. Split validated URLs into
// environment settings so passwords never appear in process arguments.
func postgresEnv(dsn string) ([]string, error) {
	u, e := url.Parse(dsn)
	if e != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" || u.Path == "" {
		return nil, errors.New("invalid PostgreSQL URL")
	}
	var env []string
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "PG") {
			env = append(env, v)
		}
	}
	env = append(env, "PGHOST="+u.Hostname(), "PGDATABASE="+strings.TrimPrefix(u.Path, "/"), "PGCONNECT_TIMEOUT=15")
	if u.Port() != "" {
		env = append(env, "PGPORT="+u.Port())
	}
	if u.User != nil {
		env = append(env, "PGUSER="+u.User.Username())
		if pw, ok := u.User.Password(); ok {
			env = append(env, "PGPASSWORD="+pw)
		}
	}
	allowed := map[string]string{"sslmode": "PGSSLMODE", "sslrootcert": "PGSSLROOTCERT", "sslcert": "PGSSLCERT", "sslkey": "PGSSLKEY", "connect_timeout": "PGCONNECT_TIMEOUT", "application_name": "PGAPPNAME"}
	for k, v := range u.Query() {
		name, ok := allowed[k]
		if !ok || len(v) != 1 {
			return nil, errors.New("unsupported PostgreSQL connection option")
		}
		env = append(env, name+"="+v[0])
	}
	if os.Getenv("APP_ENV") == "production" && (u.Query().Get("sslmode") != "verify-full" || u.Query().Get("sslrootcert") == "") {
		return nil, errors.New("production backup requires verified PostgreSQL TLS")
	}
	return env, nil
}
