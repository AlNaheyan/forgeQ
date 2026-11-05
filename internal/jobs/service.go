package jobs

import (
	"context"
	"errors"
	"fmt"
	"github.com/AlNaheyan/forgeQ/internal/jobtypes"
	"sync"
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

// QueueDepthReader supplies the ready-queue depth used for admission control.
type QueueDepthReader interface {
	Depth(context.Context) (int64, error)
}

// IdempotencyRepository is implemented by repositories that can resolve an
// existing submission without creating a new durable job.
type IdempotencyRepository interface {
	GetJobByIdempotencyKey(context.Context, string) (Job, error)
}

// BackpressureObserver is notified once for each capacity rejection. Metrics
// implementations can satisfy this interface without jobs depending on them.
type BackpressureObserver interface {
	RecordBackpressureRejection()
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
	registry           *jobtypes.Registry

	admissionMu          sync.Mutex
	pendingAdmissions    int64
	queueDepth           QueueDepthReader
	maxQueueDepth        int64
	backpressureObserver BackpressureObserver
}

func NewService(repository Repository, queue Enqueuer, defaultMaxAttempts int) *Service {
	return &Service{
		repository:         repository,
		queue:              queue,
		defaultMaxAttempts: defaultMaxAttempts,
		now:                time.Now,
		registry:           jobtypes.Fixtures(),
	}
}

// WithJobTypes selects the immutable submission schema at startup.
func (s *Service) WithJobTypes(registry *jobtypes.Registry) *Service {
	s.registry = registry
	return s
}

// WithBackpressure enables ready-queue admission control. Submit accounts for
// in-flight admissions in this service instance so concurrent bursts cannot
// all pass the same stale depth observation. This is a soft per-process bound;
// multiple API replicas do not share the in-flight reservation count.
func (s *Service) WithBackpressure(depth QueueDepthReader, maxDepth int, observer BackpressureObserver) *Service {
	s.queueDepth = depth
	s.maxQueueDepth = int64(maxDepth)
	s.backpressureObserver = observer
	return s
}

func (s *Service) Submit(ctx context.Context, input CreateInput) (Submission, error) {
	if input.MaxAttempts == 0 {
		input.MaxAttempts = s.defaultMaxAttempts
	}
	job, err := NewWithRegistry(input, s.now(), s.registry)
	if err != nil {
		return Submission{}, err
	}

	if s.queueDepth != nil && s.maxQueueDepth > 0 {
		// Idempotent replays do not add work and must remain available even when
		// new submissions are being shed.
		if job.IdempotencyKey != "" {
			if lookup, ok := s.repository.(IdempotencyRepository); ok {
				existing, lookupErr := lookup.GetJobByIdempotencyKey(ctx, job.IdempotencyKey)
				switch {
				case lookupErr == nil:
					return s.enqueueSubmission(ctx, existing, false), nil
				case !errors.Is(lookupErr, ErrNotFound):
					return Submission{}, lookupErr
				}
			}
		}

		// Serialize the external depth observation with the local reservation.
		// Persistence and enqueue proceed concurrently after the reservation.
		s.admissionMu.Lock()
		depth, depthErr := s.queueDepth.Depth(ctx)
		if depthErr != nil {
			s.admissionMu.Unlock()
			return Submission{}, fmt.Errorf("check queue capacity: %w", depthErr)
		}
		effectiveDepth := depth + s.pendingAdmissions
		if effectiveDepth >= s.maxQueueDepth {
			s.admissionMu.Unlock()
			if s.backpressureObserver != nil {
				s.backpressureObserver.RecordBackpressureRejection()
			}
			return Submission{}, QueueFullError{Depth: effectiveDepth, Limit: s.maxQueueDepth}
		}
		s.pendingAdmissions++
		s.admissionMu.Unlock()
		defer func() {
			s.admissionMu.Lock()
			s.pendingAdmissions--
			s.admissionMu.Unlock()
		}()
	}

	persisted, created, err := s.repository.CreateJob(ctx, job)
	if err != nil {
		return Submission{}, err
	}
	return s.enqueueSubmission(ctx, persisted, created), nil
}

func (s *Service) enqueueSubmission(ctx context.Context, persisted Job, created bool) Submission {
	result := Submission{Job: persisted, Created: created}
	if s.queue != nil && persisted.Status == StatusQueued {
		result.EnqueueError = s.queue.Enqueue(ctx, persisted.ID, persisted.Priority, persisted.CreatedAt)
	}
	return result
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
