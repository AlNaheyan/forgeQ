package queue

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"forgequeue/internal/jobs"
)

type reconcileRepository struct {
	jobs []jobs.Job
}

func (r reconcileRepository) ListQueuedJobs(context.Context, int) ([]jobs.Job, error) {
	return r.jobs, nil
}

type memoryQueue struct {
	items map[string]bool
}

func (q *memoryQueue) Enqueue(_ context.Context, id string, _ int, _ time.Time) error {
	q.items[id] = true
	return nil
}

func (*memoryQueue) Dequeue(context.Context) (string, error) { return "", ErrEmpty }
func (q *memoryQueue) Remove(_ context.Context, id string) error {
	delete(q.items, id)
	return nil
}
func (q *memoryQueue) Depth(context.Context) (int64, error) { return int64(len(q.items)), nil }
func (q *memoryQueue) Contains(_ context.Context, id string) (bool, error) {
	return q.items[id], nil
}
func (*memoryQueue) ScheduleRetry(context.Context, string, time.Time) error { return nil }
func (*memoryQueue) PopDueRetries(context.Context, time.Time, int64) ([]string, error) {
	return nil, nil
}

func TestReconcileOnceRestoresOnlyMissingJobs(t *testing.T) {
	queue := &memoryQueue{items: map[string]bool{"present": true}}
	reconciler := NewReconciler(reconcileRepository{jobs: []jobs.Job{
		{ID: "present", CreatedAt: time.Now()},
		{ID: "missing", Priority: 5, CreatedAt: time.Now()},
	}}, queue, slog.New(slog.NewTextHandler(io.Discard, nil)))

	restored, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if restored != 1 || !queue.items["missing"] {
		t.Fatalf("unexpected reconciliation: restored=%d items=%v", restored, queue.items)
	}
}
