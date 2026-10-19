package platform

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func OpenRedis(ctx context.Context, address string) (*redis.Client, error) {
	return OpenRedisWithSettings(ctx, address, RedisSettings{
		PoolSize:    10,
		DialTimeout: 5 * time.Second,
		IOTimeout:   3 * time.Second,
		PingTimeout: 5 * time.Second,
	})
}

type RedisSettings struct {
	PoolSize    int
	DialTimeout time.Duration
	IOTimeout   time.Duration
	PingTimeout time.Duration
}

func OpenRedisWithSettings(ctx context.Context, address string, settings RedisSettings) (*redis.Client, error) {
	if settings.PoolSize < 1 || settings.DialTimeout <= 0 || settings.IOTimeout <= 0 || settings.PingTimeout <= 0 {
		return nil, fmt.Errorf("Redis pool size and timeouts must be positive")
	}
	client := redis.NewClient(&redis.Options{
		Addr:            address,
		PoolSize:        settings.PoolSize,
		DialTimeout:     settings.DialTimeout,
		ReadTimeout:     settings.IOTimeout,
		WriteTimeout:    settings.IOTimeout,
		MaxRetries:      3,
		MinRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff: 1 * time.Second,
	})
	pingCtx, cancel := context.WithTimeout(ctx, settings.PingTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping Redis: %w", err)
	}
	return client, nil
}
