package platform

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func OpenPostgres(ctx context.Context, databaseURL string) (*sql.DB, error) {
	return OpenPostgresWithSettings(ctx, databaseURL, PostgresSettings{
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxIdleTime: 5 * time.Minute,
		PingTimeout:     5 * time.Second,
	})
}

type PostgresSettings struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	PingTimeout     time.Duration
}

func OpenPostgresWithSettings(ctx context.Context, databaseURL string, settings PostgresSettings) (*sql.DB, error) {
	if settings.MaxOpenConns < 1 || settings.MaxIdleConns < 0 || settings.MaxIdleConns > settings.MaxOpenConns {
		return nil, fmt.Errorf("invalid PostgreSQL pool sizes: max open=%d max idle=%d", settings.MaxOpenConns, settings.MaxIdleConns)
	}
	if settings.ConnMaxIdleTime <= 0 || settings.PingTimeout <= 0 {
		return nil, fmt.Errorf("PostgreSQL idle and ping timeouts must be positive")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL: configuration rejected")
	}
	db.SetMaxOpenConns(settings.MaxOpenConns)
	db.SetMaxIdleConns(settings.MaxIdleConns)
	db.SetConnMaxIdleTime(settings.ConnMaxIdleTime)

	pingCtx, cancel := context.WithTimeout(ctx, settings.PingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping PostgreSQL: connection failed")
	}
	return db, nil
}
