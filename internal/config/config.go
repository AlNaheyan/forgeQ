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

	SchedulerRoundRobin       = "round_robin"
	SchedulerLeastLoaded      = "least_loaded"
	SchedulerWeightedCapacity = "weighted_capacity"
)

// Config contains the shared settings used by ForgeQueue processes.
type Config struct {
	Service string
	Env     string

	HTTPPort int
	GRPCPort int

	DatabaseURL     string
	RedisAddr       string
	CoordinatorAddr string

	HeartbeatInterval      time.Duration
	WorkerTimeout          time.Duration
	LeaseDuration          time.Duration
	LeaseRenewInterval     time.Duration
	LeaseScanInterval      time.Duration
	RetryBaseDelay         time.Duration
	RetryMaxDelay          time.Duration
	RetryPromotionInterval time.Duration
	QueueReconcileInterval time.Duration

	DefaultMaxAttempts int
	MaxQueueDepth      int
	WorkerConcurrency  int
	WorkerID           string
	WorkerJobTypes     []string
	WorkerPollInterval time.Duration
	SchedulerStrategy  string

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
	if cfg.CoordinatorAddr, err = stringValue(lookup, "COORDINATOR_ADDR", "localhost:9090"); err != nil {
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
	if cfg.LeaseScanInterval, err = durationValue(lookup, "LEASE_SCAN_INTERVAL", time.Second); err != nil {
		return Config{}, err
	}
	if cfg.RetryBaseDelay, err = durationValue(lookup, "RETRY_BASE_DELAY", 2*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.RetryMaxDelay, err = durationValue(lookup, "RETRY_MAX_DELAY", 60*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.RetryPromotionInterval, err = durationValue(lookup, "RETRY_PROMOTION_INTERVAL", 250*time.Millisecond); err != nil {
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
	cfg.WorkerID, _ = optionalStringValue(lookup, "WORKER_ID")
	workerJobTypes, err := stringValue(lookup, "WORKER_JOB_TYPES", "sleep,fail")
	if err != nil {
		return Config{}, err
	}
	cfg.WorkerJobTypes, err = csvValue("WORKER_JOB_TYPES", workerJobTypes)
	if err != nil {
		return Config{}, err
	}
	if cfg.WorkerPollInterval, err = durationValue(lookup, "WORKER_POLL_INTERVAL", 250*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.SchedulerStrategy, err = stringValue(lookup, "SCHEDULER_STRATEGY", SchedulerLeastLoaded); err != nil {
		return Config{}, err
	}
	if !validSchedulerStrategy(cfg.SchedulerStrategy) {
		return Config{}, fmt.Errorf("SCHEDULER_STRATEGY must be one of round_robin, least_loaded, or weighted_capacity, got %q", cfg.SchedulerStrategy)
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
	if cfg.RetryBaseDelay > cfg.RetryMaxDelay {
		return Config{}, errors.New("RETRY_BASE_DELAY must not exceed RETRY_MAX_DELAY")
	}

	return cfg, nil
}

func optionalStringValue(lookup lookupFunc, key string) (string, bool) {
	value, ok := lookup(key)
	return strings.TrimSpace(value), ok
}

func csvValue(key, raw string) ([]string, error) {
	seen := make(map[string]struct{})
	values := make([]string, 0)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil, fmt.Errorf("%s must contain non-empty comma-separated values", key)
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		values = append(values, item)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("%s must contain at least one value", key)
	}
	return values, nil
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

func validSchedulerStrategy(strategy string) bool {
	switch strategy {
	case SchedulerRoundRobin, SchedulerLeastLoaded, SchedulerWeightedCapacity:
		return true
	default:
		return false
	}
}
