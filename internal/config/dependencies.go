package config

import (
	"errors"
	"github.com/AlNaheyan/forgeQ/internal/security"
	"net"
)

func loadSecureDependencies(cfg *Config, lookup lookupFunc) error {
	if cfg.Service == "worker" {
		return nil
	}
	var err error
	cfg.RedisUsername, _ = lookup("REDIS_USERNAME")
	direct, _ := lookup("REDIS_PASSWORD")
	path, _ := lookup("REDIS_PASSWORD_FILE")
	cfg.RedisPassword, err = security.Secret("REDIS_PASSWORD", direct, path)
	if err != nil {
		return err
	}
	enabled, err := boolValue(lookup, "REDIS_TLS_ENABLED", false)
	if err != nil {
		return err
	}
	ca, _ := lookup("REDIS_TLS_CA_FILE")
	name, _ := lookup("REDIS_TLS_SERVER_NAME")
	cfg.RedisTLS, err = security.RedisTLS(enabled, ca, name, cfg.Env == Production)
	if err != nil {
		return err
	}
	if cfg.Env == Production && cfg.RedisPassword == "" {
		return errors.New("production Redis requires REDIS_PASSWORD or REDIS_PASSWORD_FILE")
	}
	if cfg.Service != "coordinator" {
		return nil
	}
	cfg.MetricsBindHost, _ = lookup("METRICS_BIND_HOST")
	if cfg.MetricsBindHost != "" && net.ParseIP(cfg.MetricsBindHost) == nil && cfg.MetricsBindHost != "localhost" {
		return errors.New("METRICS_BIND_HOST must be an IP address or localhost")
	}
	cert, _ := lookup("METRICS_TLS_CERT_FILE")
	key, _ := lookup("METRICS_TLS_KEY_FILE")
	mode := "disabled"
	if cert != "" || key != "" {
		mode = "tls"
	}
	cfg.MetricsTLS, err = security.HTTPConfig(mode, cert, key, cfg.Env == Production)
	if err != nil {
		return errors.New("metrics TLS configuration is invalid")
	}
	direct, _ = lookup("METRICS_AUTH_TOKEN")
	path, _ = lookup("METRICS_AUTH_TOKEN_FILE")
	cfg.MetricsAuthToken, err = security.Secret("METRICS_AUTH_TOKEN", direct, path)
	if err != nil {
		return err
	}
	if (cfg.Env == Production || cfg.MetricsAuthToken != "") && len(cfg.MetricsAuthToken) < 32 {
		return errors.New("METRICS_AUTH_TOKEN must contain at least 32 bytes")
	}
	return nil
}
