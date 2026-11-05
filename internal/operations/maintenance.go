// Package operations implements bounded, opt-in retention and durable health gauges.
package operations

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/telemetry"
)

type Config struct {
	Enabled          bool
	JobAge, EventAge time.Duration
	Batch            int
}

func LoadConfig() (Config, error) {
	c := Config{JobAge: 30 * 24 * time.Hour, EventAge: 7 * 24 * time.Hour, Batch: 100}
	if raw := os.Getenv("RETENTION_ENABLED"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return c, fmt.Errorf("invalid RETENTION_ENABLED")
		}
		c.Enabled = v
	}
	for _, v := range []struct {
		name   string
		target *time.Duration
	}{{"RETENTION_JOB_AGE", &c.JobAge}, {"RETENTION_EVENT_AGE", &c.EventAge}} {
		if raw := os.Getenv(v.name); raw != "" {
			d, err := time.ParseDuration(raw)
			if err != nil || d < 24*time.Hour {
				return c, fmt.Errorf("%s must be at least 24h", v.name)
			}
			*v.target = d
		}
	}
	if raw := os.Getenv("RETENTION_BATCH"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 1000 {
			return c, fmt.Errorf("RETENTION_BATCH must be 1..1000")
		}
		c.Batch = n
	}
	return c, nil
}

type Maintenance struct {
	DB      *sql.DB
	Config  Config
	Metrics *telemetry.Metrics
}

// Run executes serial ticks with a query deadline; shutdown cancels in-flight work.
func (m *Maintenance) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		tick, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = m.Tick(tick)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (m *Maintenance) Tick(ctx context.Context) error {
	var ready, age, lag float64
	err := m.DB.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM jobs WHERE status='queued' AND scheduled_at<=clock_timestamp()),
 COALESCE((SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(scheduled_at)) FROM jobs WHERE status='queued' AND scheduled_at<=clock_timestamp()),0),
 COALESCE((SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(occurred_at)) FROM event_outbox WHERE published_at IS NULL),0)`).Scan(&ready, &age, &lag)
	if err != nil {
		m.Metrics.DurableHealth.Set(0)
		return err
	}
	m.Metrics.DurableHealth.Set(1)
	m.Metrics.DurableQueueDepth.Set(ready)
	m.Metrics.OldestReadyAge.Set(age)
	m.Metrics.OutboxLag.Set(lag)
	if !m.Config.Enabled {
		return nil
	}
	if m.Config.Batch < 1 || m.Config.Batch > 1000 || m.Config.JobAge < 24*time.Hour || m.Config.EventAge < 24*time.Hour {
		return fmt.Errorf("invalid retention configuration")
	}
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// The lock skips overlapping replicas; transactions keep metrics tied to commits.
	var locked bool
	if err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(7346873)`).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return nil
	}
	counts := make(map[string]int64)
	for _, q := range []struct {
		kind, sql string
		age       time.Duration
	}{
		{"jobs", `WITH doomed AS (SELECT j.id FROM jobs j WHERE j.status IN ('succeeded','canceled') AND j.updated_at < clock_timestamp()-($1*interval '1 second') AND NOT EXISTS (SELECT 1 FROM idempotency_keys i WHERE i.job_id=j.id) ORDER BY j.updated_at,j.id LIMIT $2 FOR UPDATE OF j SKIP LOCKED) DELETE FROM jobs j USING doomed d WHERE j.id=d.id`, m.Config.JobAge},
		{"events", `WITH doomed AS (SELECT id FROM event_outbox WHERE published_at < clock_timestamp()-($1*interval '1 second') AND lease_owner IS NULL ORDER BY published_at,id LIMIT $2 FOR UPDATE SKIP LOCKED) DELETE FROM event_outbox e USING doomed d WHERE e.id=d.id`, m.Config.EventAge},
	} {
		result, e := tx.ExecContext(ctx, q.sql, q.age.Seconds(), m.Config.Batch)
		if e != nil {
			m.Metrics.RetentionErrors.Inc()
			return e
		}
		counts[q.kind], err = result.RowsAffected()
		if err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		m.Metrics.RetentionErrors.Inc()
		return err
	}
	for kind, n := range counts {
		m.Metrics.RetentionDeleted.WithLabelValues(kind).Add(float64(n))
	}
	m.Metrics.RetentionLastSuccess.SetToCurrentTime()
	return nil
}
