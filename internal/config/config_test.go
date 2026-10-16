package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadUsesDevelopmentDefaults(t *testing.T) {
	cfg, err := load("api", mapLookup(nil))
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}

	if cfg.Service != "api" || cfg.Env != Development {
		t.Fatalf("unexpected identity: service=%q env=%q", cfg.Service, cfg.Env)
	}
	if cfg.HTTPPort != 8080 || cfg.GRPCPort != 9090 {
		t.Fatalf("unexpected ports: HTTP=%d gRPC=%d", cfg.HTTPPort, cfg.GRPCPort)
	}
	if cfg.MetricsPort != 9101 {
		t.Fatalf("unexpected metrics port: %d", cfg.MetricsPort)
	}
	if cfg.HeartbeatInterval != 5*time.Second || cfg.WorkerConcurrency != 4 || cfg.LeaseScanInterval != time.Second || cfg.RetryBaseDelay != 2*time.Second || cfg.RetryMaxDelay != time.Minute {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.MaxQueueDepth != 100_000 {
		t.Fatalf("unexpected max queue depth: %d", cfg.MaxQueueDepth)
	}
	if len(cfg.WorkerJobTypes) != 2 || cfg.WorkerJobTypes[0] != "sleep" || cfg.WorkerPollInterval != 250*time.Millisecond {
		t.Fatalf("unexpected worker runtime defaults: types=%v poll=%s", cfg.WorkerJobTypes, cfg.WorkerPollInterval)
	}
	if cfg.DatabaseURL == "" || cfg.RedisAddr == "" {
		t.Fatal("development dependency defaults must not be empty")
	}
	if cfg.CoordinatorAddr != "localhost:9090" {
		t.Fatalf("unexpected coordinator address: %q", cfg.CoordinatorAddr)
	}
	if cfg.SchedulerStrategy != SchedulerLeastLoaded {
		t.Fatalf("unexpected scheduler strategy: %q", cfg.SchedulerStrategy)
	}
}

func TestLoadReadsOverrides(t *testing.T) {
	cfg, err := load("worker", mapLookup(map[string]string{
		"HTTP_PORT":           "8181",
		"METRICS_PORT":        "9191",
		"MAX_QUEUE_DEPTH":     "42",
		"WORKER_CONCURRENCY":  "12",
		"WORKER_ID":           "worker-test",
		"WORKER_JOB_TYPES":    "sleep, fail, sleep",
		"LEASE_DURATION":      "45s",
		"LEASE_SCAN_INTERVAL": "2s",
		"RETRY_BASE_DELAY":    "3s",
		"RETRY_MAX_DELAY":     "20s",
		"SCHEDULER_STRATEGY":  "weighted_capacity",
		"LOG_LEVEL":           "debug",
	}))
	if err != nil {
		t.Fatalf("load overrides: %v", err)
	}

	if cfg.HTTPPort != 8181 || cfg.MetricsPort != 9191 || cfg.MaxQueueDepth != 42 || cfg.WorkerConcurrency != 12 || cfg.WorkerID != "worker-test" || len(cfg.WorkerJobTypes) != 2 || cfg.LeaseDuration != 45*time.Second || cfg.LeaseScanInterval != 2*time.Second || cfg.RetryBaseDelay != 3*time.Second || cfg.RetryMaxDelay != 20*time.Second || cfg.SchedulerStrategy != SchedulerWeightedCapacity || cfg.LogLevel != "debug" {
		t.Fatalf("overrides were not applied: %+v", cfg)
	}
}

func TestLoadRequiresProductionDependencies(t *testing.T) {
	_, err := load("api", mapLookup(map[string]string{"APP_ENV": Production}))
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Fatalf("expected missing DATABASE_URL error, got %v", err)
	}
}

func TestLoadRejectsInvalidSettings(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{name: "empty value", env: map[string]string{"REDIS_ADDR": ""}, want: "REDIS_ADDR cannot be empty"},
		{name: "bad integer", env: map[string]string{"MAX_QUEUE_DEPTH": "many"}, want: "MAX_QUEUE_DEPTH must be an integer"},
		{name: "bad duration", env: map[string]string{"LEASE_DURATION": "later"}, want: "LEASE_DURATION must be a duration"},
		{name: "heartbeat relationship", env: map[string]string{"WORKER_TIMEOUT": "5s"}, want: "WORKER_TIMEOUT must be greater"},
		{name: "lease relationship", env: map[string]string{"LEASE_RENEW_INTERVAL": "30s"}, want: "LEASE_RENEW_INTERVAL must be less"},
		{name: "retry relationship", env: map[string]string{"RETRY_BASE_DELAY": "61s"}, want: "RETRY_BASE_DELAY must not exceed"},
		{name: "scheduler strategy", env: map[string]string{"SCHEDULER_STRATEGY": "random"}, want: "SCHEDULER_STRATEGY must be one of"},
		{name: "empty job type", env: map[string]string{"WORKER_JOB_TYPES": "sleep,,fail"}, want: "WORKER_JOB_TYPES must contain non-empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load("api", mapLookup(tt.env))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
		})
	}
}

func TestLoadRequiresServiceName(t *testing.T) {
	_, err := load("  ", mapLookup(nil))
	if err == nil || err.Error() != "service name is required" {
		t.Fatalf("expected required service error, got %v", err)
	}
}

func mapLookup(values map[string]string) lookupFunc {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
