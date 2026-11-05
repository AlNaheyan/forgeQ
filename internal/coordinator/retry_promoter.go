package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
)

const defaultRetryPromotionBatchSize = 100

type retryPromotionRepository interface {
	// ListDueRetries makes PostgreSQL the durable source of truth. The delayed
	// Redis set is an acceleration hint and may lose or contain stale entries.
	ListDueRetries(context.Context, time.Time, int) ([]jobs.Job, error)
	// PromoteRetry must atomically transition retry_wait to queued.
	PromoteRetry(context.Context, string) (jobs.Job, error)
}

type retryPromotionQueue interface {
	PopDueRetries(context.Context, time.Time, int64) ([]string, error)
	Enqueue(context.Context, string, int, time.Time) error
}

// RetryPromoter moves due retry_wait jobs back to the ready queue. Every pass
// combines PostgreSQL due rows with Redis due IDs: PostgreSQL repairs missing
// Redis hints, while Redis keeps the normal path inexpensive. PromoteRetry's
// atomic state predicate makes concurrent promoter instances safe.
type RetryPromoter struct {
	repository retryPromotionRepository
	queue      retryPromotionQueue
	logger     *slog.Logger
	batchSize  int
	now        func() time.Time
}

func NewRetryPromoter(
	repository retryPromotionRepository,
	queue retryPromotionQueue,
	logger *slog.Logger,
	batchSize int,
) *RetryPromoter {
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = defaultRetryPromotionBatchSize
	}
	return &RetryPromoter{
		repository: repository,
		queue:      queue,
		logger:     logger,
		batchSize:  batchSize,
		now:        time.Now,
	}
}

// PromoteOnce performs one bounded pass. The returned count is the number of
// jobs made visible in the ready queue. Individual failures do not prevent
// independent jobs from being promoted.
func (p *RetryPromoter) PromoteOnce(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	now := p.now().UTC()

	durable, durableErr := p.repository.ListDueRetries(ctx, now, p.batchSize)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	delayedIDs, delayedErr := p.queue.PopDueRetries(ctx, now, int64(p.batchSize))
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	var promotionErrors []error
	if durableErr != nil {
		promotionErrors = append(promotionErrors, fmt.Errorf("list durable due retries: %w", durableErr))
	}
	if delayedErr != nil {
		promotionErrors = append(promotionErrors, fmt.Errorf("pop delayed retries: %w", delayedErr))
	}

	// Durable candidates come first so a full Redis batch cannot starve repair
	// of a missing delayed-set entry. Preserve source order for predictability.
	ids := make([]string, 0, len(durable)+len(delayedIDs))
	seen := make(map[string]struct{}, len(durable)+len(delayedIDs))
	add := func(id string) {
		if id == "" {
			return
		}
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if durableErr == nil {
		for _, job := range durable {
			add(job.ID)
		}
	}
	if delayedErr == nil {
		for _, id := range delayedIDs {
			add(id)
		}
	}

	ready := 0
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			promotionErrors = append(promotionErrors, err)
			break
		}
		job, err := p.repository.PromoteRetry(ctx, id)
		if err != nil {
			// Unknown Redis members and jobs won by another promoter are stale
			// hints, not operational failures.
			if errors.Is(err, jobs.ErrNotFound) ||
				errors.Is(err, jobs.ErrInvalidTransition) ||
				errors.Is(err, jobs.ErrClaimConflict) {
				continue
			}
			promotionErrors = append(promotionErrors, fmt.Errorf("promote retry %s: %w", id, err))
			continue
		}
		if job.ID == "" || job.ID != id || job.Status != jobs.StatusQueued {
			promotionErrors = append(promotionErrors, fmt.Errorf(
				"promote retry %s: repository returned invalid queued job id=%q status=%q",
				id, job.ID, job.Status,
			))
			continue
		}
		if err := p.queue.Enqueue(ctx, job.ID, job.Priority, job.CreatedAt); err != nil {
			promotionErrors = append(promotionErrors, fmt.Errorf("enqueue promoted retry %s: %w", id, err))
			continue
		}
		ready++
	}

	return ready, errors.Join(promotionErrors...)
}

// Run performs an immediate promotion pass and then scans until ctx is
// canceled. Callers must provide a positive interval.
func (p *RetryPromoter) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		p.logger.Error("retry promoter disabled", "error", "interval must be greater than zero")
		return
	}

	p.promoteAndLog(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.promoteAndLog(ctx)
		}
	}
}

func (p *RetryPromoter) promoteAndLog(ctx context.Context) {
	ready, err := p.PromoteOnce(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		p.logger.Warn("retry promotion incomplete", "ready_jobs", ready, "error", err)
		return
	}
	if ready > 0 {
		p.logger.Info("due retries promoted", "ready_jobs", ready)
	}
}
