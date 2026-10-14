package jobs

import (
	"context"
	"time"
)

type Repository interface {
	CreateJob(context.Context, Job) (Job, bool, error)
	GetJob(context.Context, string) (Job, error)
	ListJobs(context.Context, ListFilter) ([]Job, string, error)
	UpdateStatus(context.Context, string, Status) (Job, error)
	CancelJob(context.Context, string) (Job, error)
	RetryDeadLetter(context.Context, string) (Job, error)
	ListQueuedJobs(context.Context, int) ([]Job, error)
}

type Enqueuer interface {
	Enqueue(context.Context, string, int, time.Time) error
}

type Submission struct {
	Job          Job
	Created      bool
	EnqueueError error
}

type RetryResult struct {
	Job          Job
	EnqueueError error
}

type Service struct {
	repository         Repository
	queue              Enqueuer
	defaultMaxAttempts int
	now                func() time.Time
}

func NewService(repository Repository, queue Enqueuer, defaultMaxAttempts int) *Service {
	return &Service{
		repository:         repository,
		queue:              queue,
		defaultMaxAttempts: defaultMaxAttempts,
		now:                time.Now,
	}
}

func (s *Service) Submit(ctx context.Context, input CreateInput) (Submission, error) {
	if input.MaxAttempts == 0 {
		input.MaxAttempts = s.defaultMaxAttempts
	}
	job, err := New(input, s.now())
	if err != nil {
		return Submission{}, err
	}
	persisted, created, err := s.repository.CreateJob(ctx, job)
	if err != nil {
		return Submission{}, err
	}
	result := Submission{Job: persisted, Created: created}
	if s.queue != nil && persisted.Status == StatusQueued {
		result.EnqueueError = s.queue.Enqueue(ctx, persisted.ID, persisted.Priority, persisted.CreatedAt)
	}
	return result, nil
}

func (s *Service) Get(ctx context.Context, id string) (Job, error) {
	return s.repository.GetJob(ctx, id)
}

func (s *Service) List(ctx context.Context, filter ListFilter) ([]Job, string, error) {
	return s.repository.ListJobs(ctx, filter)
}

func (s *Service) Cancel(ctx context.Context, id string) (Job, error) {
	return s.repository.CancelJob(ctx, id)
}

// Retry moves a dead-lettered job back to the durable queued state before
// making it visible to workers. A queue error is returned separately because
// the durable transition has already succeeded and reconciliation can repair
// the queue entry.
func (s *Service) Retry(ctx context.Context, id string) (RetryResult, error) {
	job, err := s.repository.RetryDeadLetter(ctx, id)
	if err != nil {
		return RetryResult{}, err
	}

	result := RetryResult{Job: job}
	if s.queue != nil {
		result.EnqueueError = s.queue.Enqueue(ctx, job.ID, job.Priority, job.CreatedAt)
	}
	return result, nil
}
