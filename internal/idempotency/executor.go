// Package idempotency provides durable execution guards for job handlers.
package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/AlNaheyan/forgeQ/internal/tenant"
	"strings"
	"unicode/utf8"
)

const maxKeyBytes = 1024

var (
	// ErrInvalidKey indicates that an execution key cannot be stored safely.
	ErrInvalidKey = errors.New("invalid idempotency key")
	// ErrNilCallback indicates that ExecuteOnce was called without work to run.
	ErrNilCallback = errors.New("idempotency callback is nil")
)

// Executor coordinates idempotent handler execution through PostgreSQL.
//
// An Executor is safe for concurrent use and coordinates with every other
// Executor connected to the same database. Completion records do not expire;
// callers should therefore choose keys whose lifetime matches the operation.
type Executor struct {
	db *sql.DB
}

// NewExecutor creates an execution idempotency helper backed by db.
func NewExecutor(db *sql.DB) *Executor {
	return &Executor{db: db}
}

// ExecuteOnce runs fn unless key has already completed successfully.
//
// The returned executed value is true only when this call ran fn and committed
// its completion marker. Concurrent calls with the same key wait for the first
// call: after its success they return (false, nil), and after its error one of
// them may run fn. A callback error rolls back the marker and is returned, so a
// later call can retry. A callback panic is not recovered, but the transaction
// is still rolled back.
//
// fn runs while a database transaction is open. This prevents overlapping
// execution, but it cannot make an external side effect exactly-once. A crash
// after an external effect and before transaction commit allows fn to run
// again. Use ExecuteOnceTx and perform database effects through its transaction
// when the effect and completion marker must commit atomically.
func (e *Executor) ExecuteOnce(
	ctx context.Context,
	key string,
	fn func(context.Context) error,
) (bool, error) {
	if fn == nil {
		return false, ErrNilCallback
	}
	return e.executeOnce(ctx, key, func(ctx context.Context, _ *sql.Tx) error {
		return fn(ctx)
	})
}

// ExecuteOnceTx is ExecuteOnce with access to the transaction that owns the
// completion marker. Database effects performed through tx are committed or
// rolled back atomically with that marker. The callback must not commit or roll
// back tx itself.
//
// Effects outside tx retain the same crash window documented on ExecuteOnce.
// A commit error is an ambiguous outcome: PostgreSQL may have committed even
// though the client did not receive confirmation. Retrying the same key is the
// safe way to discover the durable outcome, but non-transactional effects must
// themselves be idempotent.
func (e *Executor) ExecuteOnceTx(
	ctx context.Context,
	key string,
	fn func(context.Context, *sql.Tx) error,
) (bool, error) {
	if fn == nil {
		return false, ErrNilCallback
	}
	return e.executeOnce(ctx, key, fn)
}

func (e *Executor) executeOnce(
	ctx context.Context,
	key string,
	fn func(context.Context, *sql.Tx) error,
) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	if e == nil || e.db == nil {
		return false, errors.New("idempotency executor has no database")
	}

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin idempotent execution: %w", err)
	}
	defer tx.Rollback()

	var insertedKey string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO execution_idempotency_keys (key,tenant_id)
		VALUES ($1,$2)
		ON CONFLICT (tenant_id,key) DO NOTHING
		RETURNING key`, key, tenant.ID(ctx)).Scan(&insertedKey)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim idempotency key: %w", err)
	}

	if err := fn(ctx, tx); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE execution_idempotency_keys
		SET completed_at = NOW()
		WHERE key = $1 AND tenant_id=$2`, key, tenant.ID(ctx)); err != nil {
		return false, fmt.Errorf("complete idempotency key: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit idempotent execution: %w", err)
	}
	return true, nil
}

func validateKey(key string) error {
	switch {
	case strings.TrimSpace(key) == "":
		return fmt.Errorf("%w: key must not be empty", ErrInvalidKey)
	case len(key) > maxKeyBytes:
		return fmt.Errorf("%w: key must not exceed %d bytes", ErrInvalidKey, maxKeyBytes)
	case !utf8.ValidString(key):
		return fmt.Errorf("%w: key must be valid UTF-8", ErrInvalidKey)
	case strings.IndexByte(key, 0) >= 0:
		return fmt.Errorf("%w: key must not contain NUL", ErrInvalidKey)
	default:
		return nil
	}
}
