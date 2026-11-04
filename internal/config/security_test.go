package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSecurityStartupBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, service string
		env           map[string]string
		want          string
	}{
		{"worker needs mTLS not database", "worker", map[string]string{"APP_ENV": "production"}, "GRPC_AUTH_MODE"},
		{"worker certificate required", "worker", map[string]string{"APP_ENV": "production", "GRPC_AUTH_MODE": "mtls", "COORDINATOR_ADDR": "localhost:9090"}, "service TLS"},
		{"token cannot ignore partial TLS", "worker", map[string]string{"SERVICE_TLS_KEY_FILE": "missing"}, "conflict"},
		{"API production cannot use plaintext", "api", map[string]string{"APP_ENV": "production", "DATABASE_URL": "postgres://scoped:synthetic@localhost/db", "REDIS_ADDR": "localhost:6379"}, "HTTP TLS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(tc.service, mapLookup(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %s got %v", tc.want, err)
			}
		})
	}
}
func TestDatabaseSecretSourceConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dsn")
	os.WriteFile(path, []byte("postgres://synthetic:secret@localhost/db\n"), 0600)
	_, err := load("api", mapLookup(map[string]string{"DATABASE_URL": "postgres://other:secret@localhost/db", "DATABASE_URL_FILE": path}))
	if err == nil || !strings.Contains(err.Error(), "conflicting") || strings.Contains(err.Error(), "secret@") {
		t.Fatal("secret conflict handling", err)
	}
	cfg, err := load("api", mapLookup(map[string]string{"DATABASE_URL_FILE": path}))
	if err != nil || strings.HasSuffix(cfg.DatabaseURL, "\n") {
		t.Fatal("file DSN failed", err)
	}
}
