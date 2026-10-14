// Package config loads and validates ForgeQueue configuration from the
// environment.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	Development = "development"
	Production  = "production"
)

// Config contains the shared settings used by ForgeQueue processes.
type Config struct {
	Service string
	Env     string

	HTTPPort int
	GRPCPort int

	DatabaseURL string
	RedisAddr   string

	HeartbeatInterval      time.Duration
	WorkerTimeout          time.Duration
	LeaseDuration          time.Duration
	LeaseRenewInterval     time.Duration
	QueueReconcileInterval time.Duration

	DefaultMaxAttempts int
	MaxQueueDepth      int
	WorkerConcurrency  int

	LogLevel string
}

type lookupFunc func(string) (string, bool)

// Load reads configuration from the process environment. Local development
// has safe defaults; production requires explicit dependency addresses.
func Load(service string) (Config, error) {
	return load(service, os.LookupEnv)
}

func load(service string, lookup lookupFunc) (Config, error) {
	service = strings.TrimSpace(service)
	if service == "" {
		return Config{}, errors.New("service name is required")
	}

	cfg := Config{Service: service}
	var err error

	if cfg.Env, err = stringValue(lookup, "APP_ENV", Development); err != nil {
		return Config{}, err
	}
	if cfg.Env != Development && cfg.Env != Production && cfg.Env != "test" {
		return Config{}, fmt.Errorf("APP_ENV must be one of development, test, or production, got %q", cfg.Env)
	}

	if cfg.HTTPPort, err = intValue(lookup, "HTTP_PORT", 8080, 1); err != nil {
		return Config{}, err
	}
	if cfg.GRPCPort, err = intValue(lookup, "GRPC_PORT", 9090, 1); err != nil {
		return Config{}, err
	}

	databaseDefault := "postgres://forgequeue:forgequeue@localhost:55432/forgequeue?sslmode=disable"
	redisDefault := "localhost:6379"
	if cfg.Env == Production {
		databaseDefault = ""
		redisDefault = ""
	}
	if cfg.DatabaseURL, err = stringValue(lookup, "DATABASE_URL", databaseDefault); err != nil {
		return Config{}, err
	}
	if cfg.RedisAddr, err = stringValue(lookup, "REDIS_ADDR", redisDefault); err != nil {
		return Config{}, err
	}
	if cfg.Env == Production && cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required when APP_ENV=production")
	}
	if cfg.Env == Production && cfg.RedisAddr == "" {
		return Config{}, errors.New("REDIS_ADDR is required when APP_ENV=production")
	}

	if cfg.HeartbeatInterval, err = durationValue(lookup, "HEARTBEAT_INTERVAL", 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.WorkerTimeout, err = durationValue(lookup, "WORKER_TIMEOUT", 15*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.LeaseDuration, err = durationValue(lookup, "LEASE_DURATION", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.LeaseRenewInterval, err = durationValue(lookup, "LEASE_RENEW_INTERVAL", 10*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.QueueReconcileInterval, err = durationValue(lookup, "QUEUE_RECONCILE_INTERVAL", 10*time.Second); err != nil {
		return Config{}, err
	}

	if cfg.DefaultMaxAttempts, err = intValue(lookup, "DEFAULT_MAX_ATTEMPTS", 3, 1); err != nil {
		return Config{}, err
	}
	if cfg.MaxQueueDepth, err = intValue(lookup, "MAX_QUEUE_DEPTH", 100_000, 1); err != nil {
		return Config{}, err
	}
	if cfg.WorkerConcurrency, err = intValue(lookup, "WORKER_CONCURRENCY", 4, 1); err != nil {
		return Config{}, err
	}
	if cfg.LogLevel, err = stringValue(lookup, "LOG_LEVEL", "info"); err != nil {
		return Config{}, err
	}
	if !validLogLevel(cfg.LogLevel) {
		return Config{}, fmt.Errorf("LOG_LEVEL must be one of debug, info, warn, or error, got %q", cfg.LogLevel)
	}

	if cfg.WorkerTimeout <= cfg.HeartbeatInterval {
		return Config{}, errors.New("WORKER_TIMEOUT must be greater than HEARTBEAT_INTERVAL")
	}
	if cfg.LeaseRenewInterval >= cfg.LeaseDuration {
		return Config{}, errors.New("LEASE_RENEW_INTERVAL must be less than LEASE_DURATION")
	}

	return cfg, nil
}

func stringValue(lookup lookupFunc, key, fallback string) (string, error) {
	value, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s cannot be empty", key)
	}
	return value, nil
}

func intValue(lookup lookupFunc, key string, fallback, minimum int) (int, error) {
	raw, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if value < minimum {
		return 0, fmt.Errorf("%s must be at least %d", key, minimum)
	}
	return value, nil
}

func durationValue(lookup lookupFunc, key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration such as 5s or 1m: %w", key, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", key)
	}
	return value, nil
}

func validLogLevel(level string) bool {
	switch level {
	case "debug", "info", "warn", "error":
		return true
	default:
		return false
	}
}
