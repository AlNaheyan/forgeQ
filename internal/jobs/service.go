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
}

type Service struct {
	repository         Repository
	defaultMaxAttempts int
	now                func() time.Time
}

func NewService(repository Repository, defaultMaxAttempts int) *Service {
	return &Service{
		repository:         repository,
		defaultMaxAttempts: defaultMaxAttempts,
		now:                time.Now,
	}
}

func (s *Service) Submit(ctx context.Context, input CreateInput) (Job, bool, error) {
	if input.MaxAttempts == 0 {
		input.MaxAttempts = s.defaultMaxAttempts
	}
	job, err := New(input, s.now())
	if err != nil {
		return Job{}, false, err
	}
	return s.repository.CreateJob(ctx, job)
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
