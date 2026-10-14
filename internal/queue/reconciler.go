package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"forgequeue/internal/jobs"
)

type queuedJobRepository interface {
	ListQueuedJobs(context.Context, int) ([]jobs.Job, error)
}

type Reconciler struct {
	repository queuedJobRepository
	queue      Queue
	logger     *slog.Logger
	batchSize  int
}

func NewReconciler(repository queuedJobRepository, queue Queue, logger *slog.Logger) *Reconciler {
	return &Reconciler{
		repository: repository,
		queue:      queue,
		logger:     logger,
		batchSize:  1000,
	}
}

func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	r.reconcileAndLog(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcileAndLog(ctx)
		}
	}
}

func (r *Reconciler) ReconcileOnce(ctx context.Context) (int, error) {
	queuedJobs, err := r.repository.ListQueuedJobs(ctx, r.batchSize)
	if err != nil {
		return 0, fmt.Errorf("list durable queued jobs: %w", err)
	}

	restored := 0
	var reconcileErrors []error
	for _, job := range queuedJobs {
		present, err := r.queue.Contains(ctx, job.ID)
		if err != nil {
			reconcileErrors = append(reconcileErrors, err)
			continue
		}
		if present {
			continue
		}
		if err := r.queue.Enqueue(ctx, job.ID, job.Priority, job.CreatedAt); err != nil {
			reconcileErrors = append(reconcileErrors, err)
			continue
		}
		restored++
	}
	return restored, errors.Join(reconcileErrors...)
}

func (r *Reconciler) reconcileAndLog(ctx context.Context) {
	restored, err := r.ReconcileOnce(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		r.logger.Warn("queue reconciliation incomplete", "restored_jobs", restored, "error", err)
		return
	}
	if restored > 0 {
		r.logger.Info("queue reconciliation restored jobs", "restored_jobs", restored)
	}
}
