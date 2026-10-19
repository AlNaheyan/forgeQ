package platform

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPostgresSettingsRejectInvalidPool(t *testing.T) {
	_, err := OpenPostgresWithSettings(context.Background(), "", PostgresSettings{
		MaxOpenConns: 2, MaxIdleConns: 3, ConnMaxIdleTime: time.Minute, PingTimeout: time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "invalid PostgreSQL pool sizes") {
		t.Fatalf("OpenPostgresWithSettings error = %v", err)
	}
}

func TestRedisSettingsRejectInvalidTimeout(t *testing.T) {
	_, err := OpenRedisWithSettings(context.Background(), "", RedisSettings{
		PoolSize: 2, DialTimeout: time.Second, IOTimeout: 0, PingTimeout: time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "Redis pool size and timeouts") {
		t.Fatalf("OpenRedisWithSettings error = %v", err)
	}
}
