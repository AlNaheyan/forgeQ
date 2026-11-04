// Package config loads and validates ForgeQueue configuration from the
// environment.
package config

import (
	"crypto/tls"
	"errors"
	"fmt"
	"forgequeue/internal/security"
	"net"
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

	RedisUsername    string
	RedisPassword    string
	RedisTLS         *tls.Config
	MetricsBindHost  string
	MetricsTLS       *tls.Config
	MetricsAuthToken string
	GRPCBindHost     string
	GRPCAuthMode     string
	ServiceTLS       *tls.Config
	HTTPBindHost     string
	HTTPSTLS         *tls.Config
	HTTPPort         int
	GRPCPort         int
	MetricsPort      int

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

	HTTPReadHeaderTimeout time.Duration
	HTTPReadTimeout       time.Duration
	HTTPWriteTimeout      time.Duration
	HTTPIdleTimeout       time.Duration
	HTTPRequestTimeout    time.Duration
	ShutdownTimeout       time.Duration
	DatabaseTimeout       time.Duration
	RedisTimeout          time.Duration
	GRPCTimeout           time.Duration
	JobTimeout            time.Duration
	PostgresConnMaxIdle   time.Duration

	PostgresMaxOpenConns int
	PostgresMaxIdleConns int
	RedisPoolSize        int

	EventsEnabled        bool
	KafkaBrokers         []string
	KafkaTopic           string
	EventPublishInterval time.Duration
	EventOutboxLease     time.Duration
	EventBatchSize       int

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
	cfg.HTTPBindHost, _ = lookup("HTTP_BIND_HOST")
	if cfg.HTTPBindHost != "" && net.ParseIP(cfg.HTTPBindHost) == nil && cfg.HTTPBindHost != "localhost" {
		return Config{}, errors.New("HTTP_BIND_HOST must be an IP address or localhost")
	}
	httpMode, ok := lookup("HTTP_TLS_MODE")
	if !ok {
		httpMode = "disabled"
	}
	httpCert, _ := lookup("HTTP_TLS_CERT_FILE")
	httpKey, _ := lookup("HTTP_TLS_KEY_FILE")
	if cfg.GRPCPort, err = intValue(lookup, "GRPC_PORT", 9090, 1); err != nil {
		return Config{}, err
	}
	metricsPortDefault := 9101
	if service == "worker" {
		metricsPortDefault = 9102
	}
	if cfg.MetricsPort, err = intValue(lookup, "METRICS_PORT", metricsPortDefault, 1); err != nil {
		return Config{}, err
	}

	databaseDefault := "postgres://forgequeue:forgequeue@localhost:55432/forgequeue?sslmode=disable"
	redisDefault := "localhost:6379"
	if cfg.Env == Production {
		databaseDefault = ""
		redisDefault = ""
	}
	directDSN, _ := lookup("DATABASE_URL")
	dsnFile, _ := lookup("DATABASE_URL_FILE")
	cfg.DatabaseURL, err = security.Secret("DATABASE_URL", directDSN, dsnFile)
	if err != nil {
		return Config{}, err
	}
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = databaseDefault
	}
	if cfg.RedisAddr, err = stringValue(lookup, "REDIS_ADDR", redisDefault); err != nil {
		return Config{}, err
	}
	if cfg.CoordinatorAddr, err = stringValue(lookup, "COORDINATOR_ADDR", "localhost:9090"); err != nil {
		return Config{}, err
	}
	if cfg.Env == Production && service != "worker" && cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required when APP_ENV=production")
	}
	if cfg.Env == Production && service != "worker" && cfg.RedisAddr == "" {
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
	if cfg.HTTPReadHeaderTimeout, err = durationValue(lookup, "HTTP_READ_HEADER_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.HTTPReadTimeout, err = durationValue(lookup, "HTTP_READ_TIMEOUT", 15*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.HTTPWriteTimeout, err = durationValue(lookup, "HTTP_WRITE_TIMEOUT", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.HTTPIdleTimeout, err = durationValue(lookup, "HTTP_IDLE_TIMEOUT", 60*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.HTTPRequestTimeout, err = durationValue(lookup, "HTTP_REQUEST_TIMEOUT", 15*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = durationValue(lookup, "SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.DatabaseTimeout, err = durationValue(lookup, "DATABASE_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.RedisTimeout, err = durationValue(lookup, "REDIS_TIMEOUT", 3*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.GRPCTimeout, err = durationValue(lookup, "GRPC_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.JobTimeout, err = durationValue(lookup, "JOB_TIMEOUT", 5*time.Minute); err != nil {
		return Config{}, err
	}
	if cfg.PostgresConnMaxIdle, err = durationValue(lookup, "POSTGRES_CONN_MAX_IDLE", 5*time.Minute); err != nil {
		return Config{}, err
	}
	if cfg.PostgresMaxOpenConns, err = intValue(lookup, "POSTGRES_MAX_OPEN_CONNS", 25, 1); err != nil {
		return Config{}, err
	}
	if cfg.PostgresMaxIdleConns, err = intValue(lookup, "POSTGRES_MAX_IDLE_CONNS", 5, 0); err != nil {
		return Config{}, err
	}
	if cfg.RedisPoolSize, err = intValue(lookup, "REDIS_POOL_SIZE", 10, 1); err != nil {
		return Config{}, err
	}
	if cfg.EventsEnabled, err = boolValue(lookup, "EVENTS_ENABLED", false); err != nil {
		return Config{}, err
	}
	kafkaBrokersDefault := "localhost:9092"
	if cfg.Env == Production {
		kafkaBrokersDefault = ""
	}
	kafkaBrokersRaw, err := stringValue(lookup, "KAFKA_BROKERS", kafkaBrokersDefault)
	if err != nil {
		return Config{}, err
	}
	if kafkaBrokersRaw != "" {
		cfg.KafkaBrokers, err = csvValue("KAFKA_BROKERS", kafkaBrokersRaw)
		if err != nil {
			return Config{}, err
		}
	}
	if cfg.KafkaTopic, err = stringValue(lookup, "KAFKA_TOPIC", "forgequeue.events"); err != nil {
		return Config{}, err
	}
	if cfg.EventPublishInterval, err = durationValue(lookup, "EVENT_PUBLISH_INTERVAL", 500*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.EventOutboxLease, err = durationValue(lookup, "EVENT_OUTBOX_LEASE", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.EventBatchSize, err = intValue(lookup, "EVENT_BATCH_SIZE", 100, 1); err != nil {
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
	if cfg.PostgresMaxIdleConns > cfg.PostgresMaxOpenConns {
		return Config{}, errors.New("POSTGRES_MAX_IDLE_CONNS must not exceed POSTGRES_MAX_OPEN_CONNS")
	}
	if cfg.EventsEnabled && len(cfg.KafkaBrokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS is required when EVENTS_ENABLED=true")
	}

	// Only API exposes HTTP ingress.
	if service == "api" {
		cfg.HTTPSTLS, err = security.HTTPConfig(httpMode, httpCert, httpKey, cfg.Env == Production)
		if err != nil {
			return Config{}, err
		}
	}

	cfg.GRPCBindHost, _ = lookup("GRPC_BIND_HOST")
	if cfg.GRPCBindHost != "" && net.ParseIP(cfg.GRPCBindHost) == nil && cfg.GRPCBindHost != "localhost" {
		return Config{}, errors.New("GRPC_BIND_HOST must be an IP address or localhost")
	}
	cfg.GRPCAuthMode, _ = lookup("GRPC_AUTH_MODE")
	if cfg.GRPCAuthMode == "" {
		cfg.GRPCAuthMode = "token"
	}
	if cfg.GRPCAuthMode != "token" && cfg.GRPCAuthMode != "mtls" {
		return Config{}, errors.New("GRPC_AUTH_MODE must be token or mtls")
	}
	if cfg.Env == Production && cfg.GRPCAuthMode != "mtls" {
		return Config{}, errors.New("production requires GRPC_AUTH_MODE=mtls")
	}
	caFile, _ := lookup("SERVICE_TLS_CA_FILE")
	certFile, _ := lookup("SERVICE_TLS_CERT_FILE")
	keyFile, _ := lookup("SERVICE_TLS_KEY_FILE")
	serverName, _ := lookup("COORDINATOR_TLS_SERVER_NAME")
	if cfg.GRPCAuthMode == "token" && (caFile != "" || certFile != "" || keyFile != "" || serverName != "") {
		return Config{}, errors.New("service TLS files conflict with token mode")
	}
	if cfg.GRPCAuthMode == "mtls" {
		if service != "coordinator" && cfg.Env == Production {
			explicit, _ := lookup("COORDINATOR_ADDR")
			if explicit == "" {
				return Config{}, errors.New("COORDINATOR_ADDR is required in production")
			}
		}
		cfg.ServiceTLS, err = security.ServiceTLS(caFile, certFile, keyFile, serverName, service == "coordinator")
		if err != nil {
			return Config{}, err
		}
		if service == "api" || service == "worker" {
			identity, e := security.CertificateIdentity(cfg.ServiceTLS.Certificates[0].Leaf)
			if e != nil || identity.Role != service {
				return Config{}, errors.New("service certificate identity does not match process role")
			}
			if service == "worker" && (cfg.WorkerID == "" || cfg.WorkerID != identity.ID) {
				return Config{}, errors.New("WORKER_ID must match certificate identity")
			}
		}
	}

	if cfg.Env == Production && service != "worker" {
		if err := security.ValidateDatabaseURL(cfg.DatabaseURL); err != nil {
			return Config{}, err
		}
	}

	if err := loadSecureDependencies(&cfg, lookup); err != nil {
		return Config{}, err
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

func boolValue(lookup lookupFunc, key string, fallback bool) (bool, error) {
	raw, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", key, err)
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
