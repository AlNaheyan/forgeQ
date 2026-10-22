package worker

import (
	"context"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/internal/queue"
)

type Broker interface {
	ClaimJob(context.Context, string, []string) (jobs.Job, error)
	RenewLease(context.Context, string, string, int64) (time.Time, error)
	CompleteJob(context.Context, string, string, []byte, int64) error
	FailJob(context.Context, string, string, string, bool, int64) error
}

type LocalBroker struct {
	repository jobRepository
	queue      workQueue
}

func NewLocalBroker(repository jobRepository, workQueue workQueue) *LocalBroker {
	return &LocalBroker{repository: repository, queue: workQueue}
}

func (b *LocalBroker) ClaimJob(ctx context.Context, _ string, supportedTypes []string) (jobs.Job, error) {
	jobID, err := b.queue.Dequeue(ctx)
	if err != nil {
		return jobs.Job{}, err
	}
	candidate, err := b.repository.GetJob(ctx, jobID)
	if err != nil {
		return jobs.Job{}, err
	}
	if !containsType(supportedTypes, candidate.Type) {
		if err := b.queue.Enqueue(ctx, candidate.ID, candidate.Priority, candidate.CreatedAt); err != nil {
			return jobs.Job{}, err
		}
		return jobs.Job{}, queue.ErrEmpty
	}
	return b.repository.ClaimJob(ctx, jobID)
}

func (b *LocalBroker) CompleteJob(ctx context.Context, _ string, jobID string, result []byte, _ int64) error {
	_, err := b.repository.FinishJob(ctx, jobID, jobs.StatusSucceeded, result, "")
	return err
}

// RenewLease is a no-op for the in-process broker retained by unit tests and
// local callers. Durable ownership is enforced by the gRPC coordinator path.
func (b *LocalBroker) RenewLease(context.Context, string, string, int64) (time.Time, error) {
	return time.Now().Add(24 * time.Hour), nil
}

func (b *LocalBroker) FailJob(ctx context.Context, _ string, jobID, failure string, _ bool, _ int64) error {
	_, err := b.repository.FinishJob(ctx, jobID, jobs.StatusFailed, nil, failure)
	return err
}

func containsType(types []string, wanted string) bool {
	for _, jobType := range types {
		if jobType == wanted {
			return true
		}
	}
	return false
}

var _ Broker = (*LocalBroker)(nil)
