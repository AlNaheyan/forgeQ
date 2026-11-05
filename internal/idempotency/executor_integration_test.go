package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/platform"
	"github.com/AlNaheyan/forgeQ/migrations"
)

func TestExecutorPostgresSemantics(t *testing.T) {
	databaseURL := os.Getenv("FORGEQUEUE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("FORGEQUEUE_TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	db, err := platform.OpenPostgres(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	defer db.Close()
	lockConnection, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("reserve test lock connection: %v", err)
	}
	defer lockConnection.Close()
	if _, err := lockConnection.ExecContext(ctx, "SELECT pg_advisory_lock(7346871)"); err != nil {
		t.Fatalf("lock integration database: %v", err)
	}
	defer lockConnection.ExecContext(context.Background(), "SELECT pg_advisory_unlock(7346871)")
	if err := migrations.Up(ctx, db); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := db.ExecContext(ctx, "TRUNCATE execution_idempotency_keys"); err != nil {
		t.Fatalf("clean execution idempotency keys: %v", err)
	}

	executor := NewExecutor(db)

	t.Run("successful callback only runs once", func(t *testing.T) {
		var calls atomic.Int32
		callback := func(context.Context) error {
			calls.Add(1)
			return nil
		}

		executed, err := executor.ExecuteOnce(ctx, "sequential-success", callback)
		if err != nil || !executed {
			t.Fatalf("first execution: executed=%v err=%v", executed, err)
		}
		executed, err = executor.ExecuteOnce(ctx, "sequential-success", callback)
		if err != nil || executed {
			t.Fatalf("duplicate execution: executed=%v err=%v", executed, err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("callback calls = %d, want 1", got)
		}
	})

	t.Run("callback failure permits retry", func(t *testing.T) {
		callbackErr := errors.New("transient side effect failure")
		executed, err := executor.ExecuteOnce(ctx, "retry-after-error", func(context.Context) error {
			return callbackErr
		})
		if executed || !errors.Is(err, callbackErr) {
			t.Fatalf("failed execution: executed=%v err=%v", executed, err)
		}

		executed, err = executor.ExecuteOnce(ctx, "retry-after-error", func(context.Context) error {
			return nil
		})
		if err != nil || !executed {
			t.Fatalf("retry execution: executed=%v err=%v", executed, err)
		}
	})

	t.Run("callback panic permits retry", func(t *testing.T) {
		panicked := false
		func() {
			defer func() {
				panicked = recover() != nil
			}()
			_, _ = executor.ExecuteOnce(ctx, "retry-after-panic", func(context.Context) error {
				panic("handler panic")
			})
		}()
		if !panicked {
			t.Fatal("callback panic was not propagated")
		}

		executed, err := executor.ExecuteOnce(ctx, "retry-after-panic", func(context.Context) error {
			return nil
		})
		if err != nil || !executed {
			t.Fatalf("retry after panic: executed=%v err=%v", executed, err)
		}
	})

	t.Run("concurrent callers serialize", func(t *testing.T) {
		const callers = 12
		start := make(chan struct{})
		results := make(chan bool, callers)
		errorsCh := make(chan error, callers)
		var calls atomic.Int32
		var wg sync.WaitGroup

		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				executed, err := executor.ExecuteOnce(ctx, "concurrent-success", func(context.Context) error {
					calls.Add(1)
					time.Sleep(25 * time.Millisecond)
					return nil
				})
				results <- executed
				errorsCh <- err
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		close(errorsCh)

		for err := range errorsCh {
			if err != nil {
				t.Fatalf("concurrent execution: %v", err)
			}
		}
		var executedCount int
		for executed := range results {
			if executed {
				executedCount++
			}
		}
		if executedCount != 1 || calls.Load() != 1 {
			t.Fatalf("executions=%d callback calls=%d, want 1 each", executedCount, calls.Load())
		}
	})

	t.Run("transactional callback rolls back with marker", func(t *testing.T) {
		callbackErr := errors.New("abort transaction")
		executed, err := executor.ExecuteOnceTx(ctx, "transactional-retry", func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO execution_idempotency_keys (key, completed_at)
				VALUES ('transactional-side-effect', NOW())`); err != nil {
				return err
			}
			return callbackErr
		})
		if executed || !errors.Is(err, callbackErr) {
			t.Fatalf("failed transaction: executed=%v err=%v", executed, err)
		}

		var count int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM execution_idempotency_keys
			WHERE key IN ('transactional-retry', 'transactional-side-effect')`).Scan(&count); err != nil {
			t.Fatalf("count rolled-back rows: %v", err)
		}
		if count != 0 {
			t.Fatalf("found %d rows after callback rollback", count)
		}

		executed, err = executor.ExecuteOnceTx(ctx, "transactional-retry", func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `
				INSERT INTO execution_idempotency_keys (key, completed_at)
				VALUES ('transactional-side-effect', NOW())`)
			return err
		})
		if err != nil || !executed {
			t.Fatalf("successful transaction: executed=%v err=%v", executed, err)
		}
	})

	var completedAt time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT completed_at
		FROM execution_idempotency_keys
		WHERE key = $1`, "sequential-success").Scan(&completedAt); err != nil {
		t.Fatalf("read durable completion marker: %v", err)
	}
	if completedAt.IsZero() {
		t.Fatal("completion marker has zero completed_at")
	}

	var incomplete int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM execution_idempotency_keys
		WHERE completed_at IS NULL`).Scan(&incomplete); err != nil {
		t.Fatalf("count incomplete markers: %v", err)
	}
	if incomplete != 0 {
		t.Fatalf("found %d incomplete committed markers", incomplete)
	}
}
