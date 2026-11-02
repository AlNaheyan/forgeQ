package backup

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRestoreScope(t *testing.T) {
	for _, dsn := range []string{"postgres://host/forgeq_restore_a", "postgres://localhost/production", "postgres://localhost/forgeq_restore_a?host=remote", "garbage"} {
		if RestorePostgres(context.Background(), nil, Manifest{}, dsn, t.TempDir()) == nil {
			t.Fatal("unsafe destination")
		}
	}
}
func TestSecretPermissions(t *testing.T) {
	p := filepath.Join(t.TempDir(), "secret")
	os.WriteFile(p, []byte("synthetic\n"), 0644)
	if _, e := SecretFile(p); e == nil {
		t.Fatal("public secret")
	}
	os.Chmod(p, 0600)
	v, e := SecretFile(p)
	if e != nil || v != "synthetic" {
		t.Fatal(e)
	}
}

func TestPostgresEnvironment(t *testing.T) {
	t.Setenv("PGSERVICE", "unexpected")
	env, err := postgresEnv("postgres://backup:synthetic@127.0.0.1:5433/fixture?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(env, "\n")
	if strings.Contains(got, "PGSERVICE=") || !strings.Contains(got, "PGPASSWORD=synthetic") || !strings.Contains(got, "PGDATABASE=fixture") {
		t.Fatal("incorrect credential environment")
	}
	if _, err = postgresEnv("postgres://localhost/fixture?host=remote"); err == nil {
		t.Fatal("override accepted")
	}
	t.Setenv("APP_ENV", "production")
	if _, err = postgresEnv("postgres://localhost/fixture?sslmode=disable"); err == nil {
		t.Fatal("insecure production database")
	}
}
