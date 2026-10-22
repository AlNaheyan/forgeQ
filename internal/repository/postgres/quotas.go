package postgres

import (
	"context"
	"database/sql"

	"forgequeue/internal/tenant"
)

// All admissions and lease acquisitions serialize on this row across replicas.
// Separate statements after acquiring the lock see the previous holder's commit.
func lockTenant(ctx context.Context, tx *sql.Tx, id string) error {
	var locked string
	return tx.QueryRowContext(ctx, `SELECT id FROM tenants WHERE id=$1 FOR NO KEY UPDATE`, id).Scan(&locked)
}

func admitTenant(ctx context.Context, tx *sql.Tx, id string, payloadBytes int) error {
	var maxPending, perMinute, maxPayload, count int
	if err := tx.QueryRowContext(ctx, `SELECT max_pending,per_minute,max_payload_bytes,
 CASE WHEN window_start=date_trunc('minute',clock_timestamp()) THEN window_count ELSE 0 END
 FROM tenants WHERE id=$1`, id).Scan(&maxPending, &perMinute, &maxPayload, &count); err != nil {
		return err
	}
	if payloadBytes > maxPayload {
		return tenant.ErrPayload
	}
	if count >= perMinute {
		return tenant.ErrQuota
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE tenant_id=$1 AND status IN ('queued','retry_wait','leased','running')`, id).Scan(&pending); err != nil {
		return err
	}
	if pending >= maxPending {
		return tenant.ErrQuota
	}
	_, err := tx.ExecContext(ctx, `UPDATE tenants SET window_start=date_trunc('minute',clock_timestamp()),window_count=$2 WHERE id=$1`, id, count+1)
	return err
}
