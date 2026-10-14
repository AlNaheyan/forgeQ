package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type recordingRepository struct {
	created Job
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

func (*recordingRepository) ListQueuedJobs(context.Context, int) ([]Job, error) {
	return nil, nil
}

type recordingEnqueuer struct {
	jobID    string
	priority int
	err      error
}

func (q *recordingEnqueuer) Enqueue(_ context.Context, jobID string, priority int, _ time.Time) error {
	q.jobID = jobID
	q.priority = priority
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
