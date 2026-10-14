package coordinator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"forgequeue/internal/jobs"
)

type recoveryRepositoryStub struct {
	mu        sync.Mutex
	jobs      []jobs.Job
	err       error
	calls     int
	lastLimit int
	called    chan struct{}
}

func (r *recoveryRepositoryStub) RecoverExpiredAttempts(_ context.Context, limit int) ([]jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.lastLimit = limit
	if r.called != nil {
		select {
		case r.called <- struct{}{}:
		default:
		}
	}
	return append([]jobs.Job(nil), r.jobs...), r.err
}

type recoveryQueueStub struct {
	mu       sync.Mutex
	jobs     []jobs.Job
	failures map[string]error
}

func (q *recoveryQueueStub) Enqueue(_ context.Context, id string, priority int, createdAt time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, jobs.Job{ID: id, Priority: priority, CreatedAt: createdAt})
	return q.failures[id]
}

func recoveryTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRecoveryScannerRecoverOnceEnqueuesEveryExpiredJob(t *testing.T) {
	now := time.Now().UTC()
	repository := &recoveryRepositoryStub{jobs: []jobs.Job{
		{ID: "first", Priority: 8, CreatedAt: now},
		{ID: "second", Priority: -2, CreatedAt: now.Add(time.Second)},
	}}
	readyQueue := &recoveryQueueStub{}
	scanner := NewRecoveryScanner(repository, readyQueue, recoveryTestLogger(), 37)

	recovered, err := scanner.RecoverOnce(context.Background())
	if err != nil {
		t.Fatalf("RecoverOnce: %v", err)
	}
	if recovered != 2 {
		t.Fatalf("recovered = %d, want 2", recovered)
	}
	if repository.lastLimit != 37 {
		t.Fatalf("repository limit = %d, want 37", repository.lastLimit)
	}
	if len(readyQueue.jobs) != 2 {
		t.Fatalf("enqueue calls = %d, want 2", len(readyQueue.jobs))
	}
	if readyQueue.jobs[0].ID != "first" || readyQueue.jobs[0].Priority != 8 || !readyQueue.jobs[0].CreatedAt.Equal(now) {
		t.Fatalf("first enqueued job = %+v", readyQueue.jobs[0])
	}
}

func TestRecoveryScannerRecoverOnceReturnsRepositoryError(t *testing.T) {
	wantErr := errors.New("database unavailable")
	repository := &recoveryRepositoryStub{err: wantErr}
	readyQueue := &recoveryQueueStub{}

	recovered, err := NewRecoveryScanner(repository, readyQueue, recoveryTestLogger(), 10).RecoverOnce(context.Background())
	if recovered != 0 {
		t.Fatalf("recovered = %d, want 0", recovered)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want wrapped %v", err, wantErr)
	}
	if len(readyQueue.jobs) != 0 {
		t.Fatalf("enqueue calls = %d, want 0", len(readyQueue.jobs))
	}
}

func TestRecoveryScannerRecoverOnceContinuesAfterEnqueueError(t *testing.T) {
	firstErr := errors.New("redis unavailable")
	repository := &recoveryRepositoryStub{jobs: []jobs.Job{
		{ID: "first"},
		{ID: "second"},
		{ID: "third"},
	}}
	readyQueue := &recoveryQueueStub{failures: map[string]error{"first": firstErr}}

	recovered, err := NewRecoveryScanner(repository, readyQueue, recoveryTestLogger(), 10).RecoverOnce(context.Background())
	if recovered != 2 {
		t.Fatalf("recovered = %d, want 2", recovered)
	}
	if !errors.Is(err, firstErr) {
		t.Fatalf("error = %v, want wrapped %v", err, firstErr)
	}
	if len(readyQueue.jobs) != 3 {
		t.Fatalf("enqueue calls = %d, want all 3 jobs attempted", len(readyQueue.jobs))
	}
}

func TestRecoveryScannerRunScansImmediatelyAndStopsOnCancellation(t *testing.T) {
	called := make(chan struct{}, 4)
	repository := &recoveryRepositoryStub{called: called}
	scanner := NewRecoveryScanner(repository, &recoveryQueueStub{}, recoveryTestLogger(), 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		scanner.Run(ctx, 10*time.Millisecond)
		close(done)
	}()

	for range 2 {
		select {
		case <-called:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for recovery pass")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scanner did not stop after cancellation")
	}
}
