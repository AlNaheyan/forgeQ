// Package migrations embeds and applies ForgeQueue's PostgreSQL schema.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed *.sql
var files embed.FS

// Up applies every unapplied migration in filename order.
func Up(ctx context.Context, db *sql.DB) error {
	return UpTo(ctx, db, "")
}

// UpTo applies an embedded prefix for isolated upgrade drills. It never undoes
// migrations and refuses databases newer than the requested target.
func UpTo(ctx context.Context, db *sql.DB, target string) error {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	known := map[string]bool{}
	found := target == ""
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".sql") {
			if entry.Name() == target {
				found = true
			}
			if target == "" || entry.Name() <= target {
				known[entry.Name()] = true
			}
		}
	}
	if !found {
		return fmt.Errorf("unknown migration target %q", target)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(7346872)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			rows.Close()
			return err
		}
		if !known[version] {
			rows.Close()
			return fmt.Errorf("database migration %s is newer than or unknown to this target", version)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || !known[entry.Name()] {
			continue
		}
		if err := apply(ctx, tx, entry.Name()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ConfigureEventStream toggles transactional job-event capture. It remains
// disabled by default so deployments without Kafka do not accumulate outbox
// rows they cannot publish.
func ConfigureEventStream(ctx context.Context, db *sql.DB, enabled bool) error {
	if _, err := db.ExecContext(ctx, "UPDATE event_stream_config SET enabled = $1 WHERE singleton = TRUE", enabled); err != nil {
		return fmt.Errorf("configure event stream: %w", err)
	}
	return nil
}

func apply(ctx context.Context, tx *sql.Tx, name string) error {
	var applied bool
	if err := tx.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", name,
	).Scan(&applied); err != nil {
		return fmt.Errorf("check migration %s: %w", name, err)
	}
	if applied {
		return nil
	}

	contents, err := files.ReadFile(name)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", name, err)
	}

	if _, err := tx.ExecContext(ctx, string(contents)); err != nil {
		return fmt.Errorf("execute migration %s: %w", name, err)
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO schema_migrations (version) VALUES ($1)", name,
	); err != nil {
		return fmt.Errorf("record migration %s: %w", name, err)
	}
	return nil
}

// Verify is the production startup path: no DDL or global configuration writes.
func Verify(ctx context.Context, db *sql.DB, eventsEnabled bool) error {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return err
	}
	expected := map[string]bool{}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".sql") {
			expected[entry.Name()] = true
		}
	}
	rows, err := db.QueryContext(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return fmt.Errorf("verify schema: migration metadata unavailable")
	}
	defer rows.Close()
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("verify schema: invalid migration metadata")
		}
		if !expected[version] {
			return fmt.Errorf("verify schema: unknown migration")
		}
		delete(expected, version)
	}
	if rows.Err() != nil || len(expected) != 0 {
		return fmt.Errorf("verify schema: migrations are incomplete")
	}
	var enabled bool
	if err := db.QueryRowContext(ctx, "SELECT enabled FROM event_stream_config WHERE singleton=TRUE").Scan(&enabled); err != nil {
		return fmt.Errorf("verify event stream configuration: unavailable")
	}
	if enabled != eventsEnabled {
		return fmt.Errorf("event stream configuration differs; run privileged bootstrap")
	}
	return nil
}
