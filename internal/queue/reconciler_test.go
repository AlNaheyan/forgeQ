package queue

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
)

type reconcileRepository struct {
	jobs []jobs.Job
}

func (r reconcileRepository) ListQueuedJobsPage(_ context.Context, afterID string, limit int) ([]jobs.Job, error) {
	var result []jobs.Job
	for _, job := range r.jobs {
		if job.ID > afterID {
			result = append(result, job)
		}
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func TestReconcilerProgressesPastFullPageAndWraps(t *testing.T) {
	q := &memoryQueue{items: map[string]bool{"a": true, "b": true}}
	r := NewReconciler(reconcileRepository{jobs: []jobs.Job{{ID: "a"}, {ID: "b"}, {ID: "c"}}}, q, slog.Default())
	r.batchSize = 2
	if n, err := r.ReconcileOnce(context.Background()); n != 0 || err != nil {
		t.Fatalf("first page: %d %v", n, err)
	}
	if n, err := r.ReconcileOnce(context.Background()); n != 1 || err != nil || !q.items["c"] {
		t.Fatalf("second page: %d %v", n, err)
	}
	delete(q.items, "a")
	if n, err := r.ReconcileOnce(context.Background()); n != 1 || err != nil || !q.items["a"] {
		t.Fatalf("wrapped page: %d %v", n, err)
	}
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
