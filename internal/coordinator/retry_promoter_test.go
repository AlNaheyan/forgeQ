package coordinator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
)

type retryPromotionRepositoryStub struct {
	mu          sync.Mutex
	due         []jobs.Job
	listErr     error
	promoteErrs map[string]error
	promoted    []string
	lastNow     time.Time
	lastLimit   int
	called      chan struct{}
}

func (r *retryPromotionRepositoryStub) ListDueRetries(_ context.Context, now time.Time, limit int) ([]jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastNow = now
	r.lastLimit = limit
	if r.called != nil {
		select {
		case r.called <- struct{}{}:
		default:
		}
	}
	return append([]jobs.Job(nil), r.due...), r.listErr
}

func (r *retryPromotionRepositoryStub) PromoteRetry(_ context.Context, id string) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.promoted = append(r.promoted, id)
	if err := r.promoteErrs[id]; err != nil {
		return jobs.Job{}, err
	}
	return jobs.Job{
		ID:        id,
		Status:    jobs.StatusQueued,
		Priority:  len(id),
		CreatedAt: time.Unix(int64(len(id)), 0).UTC(),
	}, nil
}

type retryPromotionQueueStub struct {
	mu         sync.Mutex
	due        []string
	popErr     error
	enqueueErr map[string]error
	enqueued   []string
	lastNow    time.Time
	lastLimit  int64
}

func (q *retryPromotionQueueStub) PopDueRetries(_ context.Context, now time.Time, limit int64) ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastNow = now
	q.lastLimit = limit
	return append([]string(nil), q.due...), q.popErr
}

func (q *retryPromotionQueueStub) Enqueue(_ context.Context, id string, _ int, _ time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.enqueued = append(q.enqueued, id)
	return q.enqueueErr[id]
}

func retryPromoterTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRetryPromoterMergesDurableAndRedisCandidates(t *testing.T) {
	now := time.Date(2026, time.October, 14, 12, 0, 0, 0, time.UTC)
	repository := &retryPromotionRepositoryStub{due: []jobs.Job{{ID: "db-only"}, {ID: "both"}}}
	readyQueue := &retryPromotionQueueStub{due: []string{"both", "redis-only"}}
	promoter := NewRetryPromoter(repository, readyQueue, retryPromoterTestLogger(), 17)
	promoter.now = func() time.Time { return now }

	ready, err := promoter.PromoteOnce(context.Background())
	if err != nil {
		t.Fatalf("PromoteOnce: %v", err)
	}
	if ready != 3 {
		t.Fatalf("ready = %d, want 3", ready)
	}
	if got, want := repository.promoted, []string{"db-only", "both", "redis-only"}; !equalStrings(got, want) {
		t.Fatalf("promoted = %v, want %v", got, want)
	}
	if got, want := readyQueue.enqueued, repository.promoted; !equalStrings(got, want) {
		t.Fatalf("enqueued = %v, want %v", got, want)
	}
	if !repository.lastNow.Equal(now) || !readyQueue.lastNow.Equal(now) {
		t.Fatalf("scan times = %s and %s, want %s", repository.lastNow, readyQueue.lastNow, now)
	}
	if repository.lastLimit != 17 || readyQueue.lastLimit != 17 {
		t.Fatalf("limits = %d and %d, want 17", repository.lastLimit, readyQueue.lastLimit)
	}
}

func TestRetryPromoterRepairsMissingRedisHintFromDurableState(t *testing.T) {
	repository := &retryPromotionRepositoryStub{due: []jobs.Job{{ID: "lost-from-redis"}}}
	readyQueue := &retryPromotionQueueStub{}

	ready, err := NewRetryPromoter(repository, readyQueue, nil, 0).PromoteOnce(context.Background())
	if err != nil || ready != 1 {
		t.Fatalf("PromoteOnce = (%d, %v), want (1, nil)", ready, err)
	}
	if !equalStrings(readyQueue.enqueued, []string{"lost-from-redis"}) {
		t.Fatalf("enqueued = %v", readyQueue.enqueued)
	}
	if repository.lastLimit != defaultRetryPromotionBatchSize {
		t.Fatalf("default limit = %d, want %d", repository.lastLimit, defaultRetryPromotionBatchSize)
	}
}

func TestRetryPromoterIgnoresStaleRedisIDsAndContinues(t *testing.T) {
	repository := &retryPromotionRepositoryStub{promoteErrs: map[string]error{
		"missing": jobs.ErrNotFound,
		"raced":   errors.Join(errors.New("changed"), jobs.ErrInvalidTransition),
	}}
	readyQueue := &retryPromotionQueueStub{due: []string{"missing", "valid", "raced"}}

	ready, err := NewRetryPromoter(repository, readyQueue, nil, 10).PromoteOnce(context.Background())
	if err != nil {
		t.Fatalf("PromoteOnce: %v", err)
	}
	if ready != 1 || !equalStrings(readyQueue.enqueued, []string{"valid"}) {
		t.Fatalf("ready/enqueued = %d/%v, want 1/[valid]", ready, readyQueue.enqueued)
	}
}

func TestRetryPromoterUsesEitherSourceWhenTheOtherFails(t *testing.T) {
	t.Run("redis fails", func(t *testing.T) {
		popErr := errors.New("redis unavailable")
		repository := &retryPromotionRepositoryStub{due: []jobs.Job{{ID: "durable"}}}
		readyQueue := &retryPromotionQueueStub{popErr: popErr}

		ready, err := NewRetryPromoter(repository, readyQueue, nil, 10).PromoteOnce(context.Background())
		if ready != 1 || !errors.Is(err, popErr) {
			t.Fatalf("PromoteOnce = (%d, %v), want (1, wrapped pop error)", ready, err)
		}
	})

	t.Run("database list fails", func(t *testing.T) {
		listErr := errors.New("database unavailable")
		repository := &retryPromotionRepositoryStub{listErr: listErr}
		readyQueue := &retryPromotionQueueStub{due: []string{"redis"}}

		ready, err := NewRetryPromoter(repository, readyQueue, nil, 10).PromoteOnce(context.Background())
		if ready != 1 || !errors.Is(err, listErr) {
			t.Fatalf("PromoteOnce = (%d, %v), want (1, wrapped list error)", ready, err)
		}
	})
}

func TestRetryPromoterContinuesAfterIndependentFailures(t *testing.T) {
	promoteErr := errors.New("update failed")
	enqueueErr := errors.New("ready queue failed")
	repository := &retryPromotionRepositoryStub{
		due:         []jobs.Job{{ID: "promote-fails"}, {ID: "enqueue-fails"}, {ID: "works"}},
		promoteErrs: map[string]error{"promote-fails": promoteErr},
	}
	readyQueue := &retryPromotionQueueStub{enqueueErr: map[string]error{"enqueue-fails": enqueueErr}}

	ready, err := NewRetryPromoter(repository, readyQueue, nil, 10).PromoteOnce(context.Background())
	if ready != 1 {
		t.Fatalf("ready = %d, want 1", ready)
	}
	if !errors.Is(err, promoteErr) || !errors.Is(err, enqueueErr) {
		t.Fatalf("error = %v, want both operation errors", err)
	}
	if !equalStrings(readyQueue.enqueued, []string{"enqueue-fails", "works"}) {
		t.Fatalf("enqueue attempts = %v", readyQueue.enqueued)
	}
}

func TestRetryPromoterRejectsInvalidRepositoryResult(t *testing.T) {
	repository := &invalidRetryPromotionRepository{}
	readyQueue := &retryPromotionQueueStub{due: []string{"job"}}

	ready, err := NewRetryPromoter(repository, readyQueue, nil, 10).PromoteOnce(context.Background())
	if ready != 0 || err == nil {
		t.Fatalf("PromoteOnce = (%d, %v), want validation error", ready, err)
	}
	if len(readyQueue.enqueued) != 0 {
		t.Fatalf("invalid result was enqueued: %v", readyQueue.enqueued)
	}
}

type invalidRetryPromotionRepository struct{}

func (*invalidRetryPromotionRepository) ListDueRetries(context.Context, time.Time, int) ([]jobs.Job, error) {
	return nil, nil
}

func (*invalidRetryPromotionRepository) PromoteRetry(_ context.Context, id string) (jobs.Job, error) {
	return jobs.Job{ID: id, Status: jobs.StatusRetryWait}, nil
}

func TestRetryPromoterHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	repository := &retryPromotionRepositoryStub{due: []jobs.Job{{ID: "job"}}}
	readyQueue := &retryPromotionQueueStub{due: []string{"job"}}

	ready, err := NewRetryPromoter(repository, readyQueue, nil, 10).PromoteOnce(ctx)
	if ready != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("PromoteOnce = (%d, %v), want context cancellation", ready, err)
	}
	if repository.lastLimit != 0 || readyQueue.lastLimit != 0 {
		t.Fatal("canceled pass called its dependencies")
	}
}

func TestRetryPromoterRunScansImmediatelyAndStops(t *testing.T) {
	called := make(chan struct{}, 4)
	repository := &retryPromotionRepositoryStub{called: called}
	promoter := NewRetryPromoter(repository, &retryPromotionQueueStub{}, retryPromoterTestLogger(), 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		promoter.Run(ctx, 5*time.Millisecond)
		close(done)
	}()

	for range 2 {
		select {
		case <-called:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for promotion pass")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("promoter did not stop after cancellation")
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
