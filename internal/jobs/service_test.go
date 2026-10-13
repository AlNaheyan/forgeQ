package jobs

import (
	"context"
	"encoding/json"
	"testing"
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

func TestServiceSubmitAppliesDefaultAttempts(t *testing.T) {
	repository := &recordingRepository{}
	service := NewService(repository, 7)

	_, created, err := service.Submit(context.Background(), CreateInput{
		Type:    "sleep",
		Payload: json.RawMessage(`{"duration_ms":10}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !created || repository.created.MaxAttempts != 7 {
		t.Fatalf("default attempts not applied: created=%v job=%+v", created, repository.created)
	}
}
