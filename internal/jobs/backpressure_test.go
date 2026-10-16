package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type admissionRepository struct {
	mu       sync.Mutex
	created  int
	existing map[string]Job
	started  chan struct{}
	release  <-chan struct{}
}

func (r *admissionRepository) CreateJob(ctx context.Context, job Job) (Job, bool, error) {
	r.mu.Lock()
	r.created++
	r.mu.Unlock()
	if r.started != nil {
		r.started <- struct{}{}
	}
	if r.release != nil {
		select {
		case <-r.release:
		case <-ctx.Done():
			return Job{}, false, ctx.Err()
		}
	}
	return job, true, nil
}

func (r *admissionRepository) GetJobByIdempotencyKey(_ context.Context, key string) (Job, error) {
	if job, ok := r.existing[key]; ok {
		return job, nil
	}
	return Job{}, ErrNotFound
}

func (*admissionRepository) GetJob(context.Context, string) (Job, error) { return Job{}, ErrNotFound }
func (*admissionRepository) ListJobs(context.Context, ListFilter) ([]Job, string, error) {
	return nil, "", nil
}
func (*admissionRepository) UpdateStatus(context.Context, string, Status) (Job, error) {
	return Job{}, ErrNotFound
}
func (*admissionRepository) CancelJob(context.Context, string) (Job, error) {
	return Job{}, ErrNotFound
}
func (*admissionRepository) RetryDeadLetter(context.Context, string) (Job, error) {
	return Job{}, ErrNotFound
}
func (*admissionRepository) ListQueuedJobs(context.Context, int) ([]Job, error) { return nil, nil }

func (r *admissionRepository) createdCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.created
}

type admissionQueue struct {
	depth    int64
	depthErr error
	enqueued atomic.Int64
}

func (q *admissionQueue) Depth(context.Context) (int64, error) { return q.depth, q.depthErr }
func (q *admissionQueue) Enqueue(context.Context, string, int, time.Time) error {
	q.enqueued.Add(1)
	return nil
}

type rejectionRecorder struct {
	count    atomic.Int64
	recorded chan struct{}
}

func (r *rejectionRecorder) RecordBackpressureRejection() {
	r.count.Add(1)
	if r.recorded != nil {
		r.recorded <- struct{}{}
	}
}

func validSubmission(key string) CreateInput {
	return CreateInput{
		Type:           "sleep",
		Payload:        json.RawMessage(`{"duration_ms":10}`),
		IdempotencyKey: key,
	}
}

func TestSubmitRejectsAtCapacityBeforePersistence(t *testing.T) {
	repository := &admissionRepository{}
	queue := &admissionQueue{depth: 2}
	rejections := &rejectionRecorder{}
	service := NewService(repository, queue, 3).WithBackpressure(queue, 2, rejections)

	_, err := service.Submit(context.Background(), validSubmission(""))
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Submit error = %v, want ErrQueueFull", err)
	}
	var fullError QueueFullError
	if !errors.As(err, &fullError) || fullError.Depth != 2 || fullError.Limit != 2 {
		t.Fatalf("Submit error = %#v, want queue depth diagnostics", err)
	}
	if repository.createdCount() != 0 || queue.enqueued.Load() != 0 {
		t.Fatalf("rejected job was persisted or enqueued: created=%d enqueued=%d", repository.createdCount(), queue.enqueued.Load())
	}
	if rejections.count.Load() != 1 {
		t.Fatalf("rejection count = %d, want 1", rejections.count.Load())
	}
}

func TestSubmitIdempotentReplayBypassesCapacityLimit(t *testing.T) {
	existing := Job{
		ID:             "existing-job",
		Type:           "sleep",
		Payload:        json.RawMessage(`{"duration_ms":10}`),
		Status:         StatusQueued,
		IdempotencyKey: "same-request",
		CreatedAt:      time.Now(),
	}
	repository := &admissionRepository{existing: map[string]Job{"same-request": existing}}
	queue := &admissionQueue{depth: 10}
	rejections := &rejectionRecorder{}
	service := NewService(repository, queue, 3).WithBackpressure(queue, 10, rejections)

	result, err := service.Submit(context.Background(), validSubmission("same-request"))
	if err != nil {
		t.Fatalf("Submit idempotent replay: %v", err)
	}
	if result.Created || result.Job.ID != existing.ID {
		t.Fatalf("replay result = %+v, want existing job", result)
	}
	if repository.createdCount() != 0 {
		t.Fatalf("replay persisted %d new jobs", repository.createdCount())
	}
	if queue.enqueued.Load() != 1 {
		t.Fatalf("replay enqueue count = %d, want 1 idempotent repair enqueue", queue.enqueued.Load())
	}
	if rejections.count.Load() != 0 {
		t.Fatalf("replay incremented rejection count to %d", rejections.count.Load())
	}
}

func TestConcurrentBurstDoesNotOverAdmit(t *testing.T) {
	const (
		limit = 3
		burst = 24
	)
	release := make(chan struct{})
	repository := &admissionRepository{
		started: make(chan struct{}, limit),
		release: release,
	}
	// A deliberately stale depth reader proves in-flight reservations, rather
	// than lucky queue timing, enforce the local burst limit.
	queue := &admissionQueue{}
	rejections := &rejectionRecorder{recorded: make(chan struct{}, burst)}
	service := NewService(repository, queue, 3).WithBackpressure(queue, limit, rejections)

	var accepted atomic.Int64
	var rejected atomic.Int64
	errorsFound := make(chan error, burst)
	var workers sync.WaitGroup
	workers.Add(burst)
	for range burst {
		go func() {
			defer workers.Done()
			_, err := service.Submit(context.Background(), validSubmission(""))
			switch {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, ErrQueueFull):
				rejected.Add(1)
			default:
				errorsFound <- err
			}
		}()
	}

	for range limit {
		select {
		case <-repository.started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for admitted submissions")
		}
	}
	for range burst - limit {
		select {
		case <-rejections.recorded:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for rejected submissions")
		}
	}
	close(release)
	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("unexpected Submit error: %v", err)
	}

	if got := accepted.Load(); got != limit {
		t.Fatalf("accepted = %d, want %d", got, limit)
	}
	if got := rejected.Load(); got != burst-limit {
		t.Fatalf("rejected = %d, want %d", got, burst-limit)
	}
	if repository.createdCount() != limit || queue.enqueued.Load() != limit {
		t.Fatalf("burst crossed limit: persisted=%d enqueued=%d limit=%d", repository.createdCount(), queue.enqueued.Load(), limit)
	}
	if rejections.count.Load() != burst-limit {
		t.Fatalf("rejection metric = %d, want %d", rejections.count.Load(), burst-limit)
	}
}

func TestSubmitFailsClosedWhenQueueDepthIsUnavailable(t *testing.T) {
	repository := &admissionRepository{}
	queue := &admissionQueue{depthErr: errors.New("Redis unavailable")}
	service := NewService(repository, queue, 3).WithBackpressure(queue, 10, nil)

	_, err := service.Submit(context.Background(), validSubmission(""))
	if err == nil || errors.Is(err, ErrQueueFull) {
		t.Fatalf("Submit error = %v, want dependency error", err)
	}
	if repository.createdCount() != 0 || queue.enqueued.Load() != 0 {
		t.Fatal("job was persisted or enqueued without a capacity reading")
	}
}
