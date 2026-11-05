package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/queue"
)

type finiteQueue struct {
	mu  sync.Mutex
	ids []string
}

func (q *finiteQueue) Dequeue(context.Context) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.ids) == 0 {
		return "", queue.ErrEmpty
	}
	id := q.ids[0]
	q.ids = q.ids[1:]
	return id, nil
}

func (*finiteQueue) Enqueue(context.Context, string, int, time.Time) error { return nil }

type memoryRepository struct {
	mu        sync.Mutex
	jobs      map[string]jobs.Job
	completed chan string
}

func (r *memoryRepository) GetJob(_ context.Context, id string) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return job, nil
}

func (r *memoryRepository) ClaimJob(_ context.Context, id string) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok || job.Status != jobs.StatusQueued {
		return jobs.Job{}, jobs.ErrClaimConflict
	}
	job.Status = jobs.StatusRunning
	job.AttemptCount++
	r.jobs[id] = job
	return job, nil
}

func (r *memoryRepository) FinishJob(_ context.Context, id string, status jobs.Status, result []byte, lastError string) (jobs.Job, error) {
	r.mu.Lock()
	job := r.jobs[id]
	job.Status = status
	job.Result = result
	job.LastError = lastError
	r.jobs[id] = job
	r.mu.Unlock()
	r.completed <- id
	return job, nil
}

type concurrencyHandler struct {
	active   atomic.Int32
	maximum  atomic.Int32
	duration time.Duration
}

type shutdownHandler struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (h shutdownHandler) Execute(ctx context.Context, _ json.RawMessage) (any, error) {
	h.started <- struct{}{}
	select {
	case <-h.release:
		return map[string]bool{"drained": true}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (h *concurrencyHandler) Execute(context.Context, json.RawMessage) (any, error) {
	active := h.active.Add(1)
	defer h.active.Add(-1)
	for {
		maximum := h.maximum.Load()
		if active <= maximum || h.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	time.Sleep(h.duration)
	return map[string]bool{"ok": true}, nil
}

func TestWorkerRespectsConcurrencyLimit(t *testing.T) {
	const jobCount = 12
	ids := make([]string, 0, jobCount)
	storedJobs := make(map[string]jobs.Job, jobCount)
	for index := 0; index < jobCount; index++ {
		id := string(rune('a' + index))
		ids = append(ids, id)
		storedJobs[id] = jobs.Job{
			ID:        id,
			Type:      "test",
			Payload:   json.RawMessage(`{}`),
			Status:    jobs.StatusQueued,
			CreatedAt: time.Now(),
		}
	}
	repository := &memoryRepository{jobs: storedJobs, completed: make(chan string, jobCount)}
	handler := &concurrencyHandler{duration: 15 * time.Millisecond}
	worker, err := New(Config{
		ID:           "worker-test",
		Concurrency:  3,
		PollInterval: time.Millisecond,
		JobTypes:     []string{"test"},
	}, repository, &finiteQueue{ids: ids}, map[string]Handler{"test": handler}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	for index := 0; index < jobCount; index++ {
		select {
		case <-repository.completed:
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not finish all jobs")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if maximum := handler.maximum.Load(); maximum != 3 {
		t.Fatalf("maximum concurrency = %d, want 3", maximum)
	}
}

func TestWorkerDrainsActiveJobOnShutdown(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	repository := &memoryRepository{
		jobs: map[string]jobs.Job{"job-1": {
			ID: "job-1", Type: "test", Payload: json.RawMessage(`{}`), Status: jobs.StatusQueued, CreatedAt: time.Now(),
		}},
		completed: make(chan string, 1),
	}
	process, err := New(Config{
		ID: "worker-drain", Concurrency: 1, PollInterval: time.Millisecond, JobTimeout: time.Second,
		JobTypes: []string{"test"},
	}, repository, &finiteQueue{ids: []string{"job-1"}}, map[string]Handler{
		"test": shutdownHandler{started: started, release: release},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("worker stopped before draining active job: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after active job drained")
	}
	repository.mu.Lock()
	status := repository.jobs["job-1"].Status
	repository.mu.Unlock()
	if status != jobs.StatusSucceeded {
		t.Fatalf("job status = %s, want succeeded", status)
	}
}
