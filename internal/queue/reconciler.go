package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
)

type queuedJobRepository interface {
	ListQueuedJobsPage(context.Context, string, int) ([]jobs.Job, error)
}

type Reconciler struct {
	repository queuedJobRepository
	queue      Queue
	logger     *slog.Logger
	batchSize  int
	mu         sync.Mutex
	afterID    string
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
	r.mu.Lock()
	defer r.mu.Unlock()
	queuedJobs, err := r.repository.ListQueuedJobsPage(ctx, r.afterID, r.batchSize)
	if err != nil {
		return 0, fmt.Errorf("list durable queued jobs: %w", err)
	}
	// A bounded page per tick guarantees progress beyond already-present jobs.
	// New jobs behind the cursor and failed enqueues are retried on the next sweep.
	if len(queuedJobs) < r.batchSize {
		r.afterID = ""
	} else {
		r.afterID = queuedJobs[len(queuedJobs)-1].ID
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
