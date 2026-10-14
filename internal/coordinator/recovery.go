package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"forgequeue/internal/jobs"
)

const defaultRecoveryBatchSize = 100

type expiredLeaseRepository interface {
	RecoverExpiredAttempts(context.Context, int) ([]jobs.Job, error)
}

type recoveryQueue interface {
	Enqueue(context.Context, string, int, time.Time) error
}

// RecoveryScanner restores jobs whose durable leases have expired to the
// ready queue. PostgreSQL remains authoritative: if a Redis enqueue fails,
// the job is still queued durably and can be restored by a later scan or the
// general queue reconciler.
type RecoveryScanner struct {
	repository expiredLeaseRepository
	queue      recoveryQueue
	logger     *slog.Logger
	batchSize  int
}

func NewRecoveryScanner(
	repository expiredLeaseRepository,
	queue recoveryQueue,
	logger *slog.Logger,
	batchSize int,
) *RecoveryScanner {
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = defaultRecoveryBatchSize
	}
	return &RecoveryScanner{
		repository: repository,
		queue:      queue,
		logger:     logger,
		batchSize:  batchSize,
	}
}

// RecoverOnce atomically requeues one batch of expired leases in the durable
// store, then makes every returned job visible in the ready queue.
func (s *RecoveryScanner) RecoverOnce(ctx context.Context) (int, error) {
	expired, err := s.repository.RecoverExpiredAttempts(ctx, s.batchSize)
	if err != nil {
		return 0, fmt.Errorf("requeue expired leases: %w", err)
	}

	recovered := 0
	var enqueueErrors []error
	for _, job := range expired {
		if err := s.queue.Enqueue(ctx, job.ID, job.Priority, job.CreatedAt); err != nil {
			enqueueErrors = append(enqueueErrors, fmt.Errorf("enqueue recovered job %s: %w", job.ID, err))
			continue
		}
		recovered++
	}
	return recovered, errors.Join(enqueueErrors...)
}

// Run performs an immediate recovery pass and then scans until ctx is
// canceled. Callers must provide a positive interval.
func (s *RecoveryScanner) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		s.logger.Error("lease recovery scanner disabled", "error", "interval must be greater than zero")
		return
	}

	s.recoverAndLog(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.recoverAndLog(ctx)
		}
	}
}

func (s *RecoveryScanner) recoverAndLog(ctx context.Context) {
	recovered, err := s.RecoverOnce(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Warn("expired lease recovery incomplete", "recovered_jobs", recovered, "error", err)
		return
	}
	if recovered > 0 {
		s.logger.Info("expired leases recovered", "recovered_jobs", recovered)
	}
}
