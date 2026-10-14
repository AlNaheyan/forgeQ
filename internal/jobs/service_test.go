package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type recordingRepository struct {
	created  Job
	retried  Job
	retryErr error
}

func (r *recordingRepository) CreateJob(_ context.Context, job Job) (Job, bool, error) {
	r.created = job
	return job, true, nil
}

func (*recordingRepository) GetJob(context.Context, string) (Job, error) {
	return Job{}, ErrNotFound
}

func (*recordingRepository) ListJobs(context.Context, ListFilter) ([]Job, string, error) {
	return nil, "", nil
}

func (*recordingRepository) UpdateStatus(context.Context, string, Status) (Job, error) {
	return Job{}, ErrNotFound
}

func (*recordingRepository) CancelJob(context.Context, string) (Job, error) {
	return Job{}, ErrNotFound
}

func (r *recordingRepository) RetryDeadLetter(_ context.Context, id string) (Job, error) {
	if r.retryErr != nil {
		return Job{}, r.retryErr
	}
	if r.retried.ID == "" {
		r.retried = Job{ID: id, Status: StatusQueued, ScheduledAt: time.Now()}
	}
	return r.retried, nil
}

func (*recordingRepository) ListQueuedJobs(context.Context, int) ([]Job, error) {
	return nil, nil
}

type recordingEnqueuer struct {
	jobID     string
	priority  int
	createdAt time.Time
	err       error
}

func (q *recordingEnqueuer) Enqueue(_ context.Context, jobID string, priority int, createdAt time.Time) error {
	q.jobID = jobID
	q.priority = priority
	q.createdAt = createdAt
	return q.err
}

func TestServiceKeepsPersistedJobWhenEnqueueFails(t *testing.T) {
	repository := &recordingRepository{}
	queueError := errors.New("Redis unavailable")
	service := NewService(repository, &recordingEnqueuer{err: queueError}, 3)

	result, err := service.Submit(context.Background(), CreateInput{
		Type:    "sleep",
		Payload: json.RawMessage(`{"duration_ms":10}`),
	})
	if err != nil {
		t.Fatalf("Submit returned a persistence error: %v", err)
	}
	if !result.Created || !errors.Is(result.EnqueueError, queueError) || result.Job.ID == "" {
		t.Fatalf("unexpected durable-first result: %+v", result)
	}
}

func TestServiceSubmitAppliesDefaultAttempts(t *testing.T) {
	repository := &recordingRepository{}
	queue := &recordingEnqueuer{}
	service := NewService(repository, queue, 7)

	result, err := service.Submit(context.Background(), CreateInput{
		Type:     "sleep",
		Payload:  json.RawMessage(`{"duration_ms":10}`),
		Priority: 4,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !result.Created || repository.created.MaxAttempts != 7 {
		t.Fatalf("default attempts not applied: result=%+v job=%+v", result, repository.created)
	}
	if queue.jobID != result.Job.ID || queue.priority != 4 {
		t.Fatalf("job was not enqueued: queue=%+v result=%+v", queue, result)
	}
}

func TestServiceRetryKeepsDurableTransitionWhenEnqueueFails(t *testing.T) {
	createdAt := time.Date(2026, time.October, 14, 12, 0, 0, 0, time.UTC)
	repository := &recordingRepository{retried: Job{
		ID:        "retry-job",
		Status:    StatusQueued,
		Priority:  9,
		CreatedAt: createdAt,
	}}
	queueError := errors.New("Redis unavailable")
	queue := &recordingEnqueuer{err: queueError}
	service := NewService(repository, queue, 3)

	result, err := service.Retry(context.Background(), "retry-job")
	if err != nil {
		t.Fatalf("Retry returned a persistence error: %v", err)
	}
	if result.Job.Status != StatusQueued || !errors.Is(result.EnqueueError, queueError) {
		t.Fatalf("unexpected durable-first result: %+v", result)
	}
	if queue.jobID != "retry-job" || queue.priority != 9 || !queue.createdAt.Equal(createdAt) {
		t.Fatalf("retried job was not enqueued: %+v", queue)
	}
}

func TestServiceRetryDoesNotEnqueueWhenTransitionFails(t *testing.T) {
	repository := &recordingRepository{retryErr: ErrInvalidTransition}
	queue := &recordingEnqueuer{}
	service := NewService(repository, queue, 3)

	_, err := service.Retry(context.Background(), "running-job")
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Retry error = %v, want ErrInvalidTransition", err)
	}
	if queue.jobID != "" {
		t.Fatalf("job was enqueued after failed transition: %+v", queue)
	}
}
