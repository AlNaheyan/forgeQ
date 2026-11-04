package config

import (
	"strings"
	"testing"
)

func TestDependencySecurityBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
		env  map[string]string
		want string
	}{
		{"production Redis TLS", Config{Env: Production, Service: "api"}, map[string]string{"REDIS_PASSWORD": "synthetic"}, "verified TLS"},
		{"Redis secret conflict", Config{Env: Development, Service: "api"}, map[string]string{"REDIS_PASSWORD": "synthetic", "REDIS_PASSWORD_FILE": "missing"}, "conflicting"},
		{"partial Redis TLS", Config{Env: Development, Service: "api"}, map[string]string{"REDIS_TLS_CA_FILE": "missing"}, "conflicting"},
		{"short metrics token", Config{Env: Development, Service: "coordinator"}, map[string]string{"METRICS_AUTH_TOKEN": "too-short"}, "32 bytes"},
		{"partial metrics TLS", Config{Env: Development, Service: "coordinator"}, map[string]string{"METRICS_TLS_CERT_FILE": "missing"}, "metrics TLS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := loadSecureDependencies(&tc.cfg, mapLookup(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %s got %v", tc.want, err)
			}
		})
	}
	if err := loadSecureDependencies(&Config{Env: Production, Service: "worker"}, mapLookup(nil)); err != nil {
		t.Fatal("worker required control-plane dependency credentials", err)
	}
}
