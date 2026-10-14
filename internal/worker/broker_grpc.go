package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/internal/queue"
	"forgequeue/proto/workerpb"
)

type GRPCBroker struct {
	client workerpb.WorkerCoordinatorClient
}

func NewGRPCBroker(client workerpb.WorkerCoordinatorClient) (*GRPCBroker, error) {
	if client == nil {
		return nil, errors.New("worker coordinator client is required")
	}
	return &GRPCBroker{client: client}, nil
}

func (b *GRPCBroker) ClaimJob(ctx context.Context, workerID string, supportedTypes []string) (jobs.Job, error) {
	response, err := b.client.ClaimJob(ctx, &workerpb.ClaimJobRequest{
		WorkerId:          workerID,
		SupportedJobTypes: append([]string(nil), supportedTypes...),
	})
	if err != nil {
		return jobs.Job{}, err
	}
	if response.GetJob() == nil {
		return jobs.Job{}, queue.ErrEmpty
	}
	started, err := b.client.StartJob(ctx, &workerpb.StartJobRequest{
		WorkerId: workerID,
		JobId:    response.GetJob().GetId(),
	})
	if err != nil {
		return jobs.Job{}, err
	}
	return jobFromProto(started.GetJob())
}

func (b *GRPCBroker) CompleteJob(ctx context.Context, workerID, jobID string, result []byte) error {
	_, err := b.client.CompleteJob(ctx, &workerpb.CompleteJobRequest{
		WorkerId:   workerID,
		JobId:      jobID,
		ResultJson: append([]byte(nil), result...),
	})
	return err
}

func (b *GRPCBroker) RenewLease(ctx context.Context, workerID, jobID string) (time.Time, error) {
	response, err := b.client.RenewLease(ctx, &workerpb.RenewLeaseRequest{
		WorkerId: workerID,
		JobId:    jobID,
	})
	if err != nil {
		return time.Time{}, err
	}
	return requiredProtoTime(response.GetLeaseExpiresAt(), "lease_expires_at")
}

func (b *GRPCBroker) FailJob(ctx context.Context, workerID, jobID, failure string) error {
	_, err := b.client.FailJob(ctx, &workerpb.FailJobRequest{
		WorkerId:  workerID,
		JobId:     jobID,
		Error:     failure,
		Retryable: true,
	})
	return err
}

func jobFromProto(value *workerpb.Job) (jobs.Job, error) {
	if value == nil || value.GetId() == "" {
		return jobs.Job{}, errors.New("coordinator returned an invalid job")
	}
	if !json.Valid(value.GetPayloadJson()) {
		return jobs.Job{}, errors.New("coordinator returned invalid job payload JSON")
	}
	scheduledAt, err := requiredProtoTime(value.GetScheduledAt(), "scheduled_at")
	if err != nil {
		return jobs.Job{}, err
	}
	createdAt, err := requiredProtoTime(value.GetCreatedAt(), "created_at")
	if err != nil {
		return jobs.Job{}, err
	}
	updatedAt, err := requiredProtoTime(value.GetUpdatedAt(), "updated_at")
	if err != nil {
		return jobs.Job{}, err
	}
	job := jobs.Job{
		ID:             value.GetId(),
		Type:           value.GetType(),
		Payload:        append(json.RawMessage(nil), value.GetPayloadJson()...),
		Status:         statusFromProto(value.GetStatus()),
		Priority:       int(value.GetPriority()),
		AttemptCount:   int(value.GetAttemptCount()),
		MaxAttempts:    int(value.GetMaxAttempts()),
		ScheduledAt:    scheduledAt,
		LeaseOwner:     value.GetLeaseOwner(),
		IdempotencyKey: value.GetIdempotencyKey(),
		Result:         append(json.RawMessage(nil), value.GetResultJson()...),
		LastError:      value.GetLastError(),
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
	}
	if value.GetLeaseExpiresAt() != nil {
		leaseExpiry, err := requiredProtoTime(value.GetLeaseExpiresAt(), "lease_expires_at")
		if err != nil {
			return jobs.Job{}, err
		}
		job.LeaseExpiresAt = &leaseExpiry
	}
	return job, nil
}

func requiredProtoTime(value interface {
	CheckValid() error
	AsTime() time.Time
}, field string) (time.Time, error) {
	if value == nil {
		return time.Time{}, fmt.Errorf("coordinator job omitted %s", field)
	}
	if err := value.CheckValid(); err != nil {
		return time.Time{}, fmt.Errorf("coordinator job has invalid %s: %w", field, err)
	}
	return value.AsTime(), nil
}

func statusFromProto(value workerpb.JobStatus) jobs.Status {
	switch value {
	case workerpb.JobStatus_JOB_STATUS_QUEUED:
		return jobs.StatusQueued
	case workerpb.JobStatus_JOB_STATUS_LEASED:
		return jobs.StatusLeased
	case workerpb.JobStatus_JOB_STATUS_RUNNING:
		return jobs.StatusRunning
	case workerpb.JobStatus_JOB_STATUS_RETRY_WAIT:
		return jobs.StatusRetryWait
	case workerpb.JobStatus_JOB_STATUS_SUCCEEDED:
		return jobs.StatusSucceeded
	case workerpb.JobStatus_JOB_STATUS_FAILED:
		return jobs.StatusFailed
	case workerpb.JobStatus_JOB_STATUS_DEAD_LETTER:
		return jobs.StatusDeadLetter
	case workerpb.JobStatus_JOB_STATUS_CANCELED:
		return jobs.StatusCanceled
	default:
		return ""
	}
}

var _ Broker = (*GRPCBroker)(nil)
