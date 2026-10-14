package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/internal/queue"
	"forgequeue/proto/workerpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type jobRepository interface {
	GetJob(context.Context, string) (jobs.Job, error)
	LeaseJob(context.Context, string, string, time.Duration) (jobs.Job, error)
	StartAttempt(context.Context, string, string) (jobs.Job, error)
	RenewLease(context.Context, string, string, time.Duration) (jobs.Job, error)
	SucceedAttempt(context.Context, string, string, []byte) (jobs.Job, error)
	FailAttempt(context.Context, string, string, string, bool, time.Time) (jobs.Job, error)
}

type jobQueue interface {
	Dequeue(context.Context) (string, error)
	Enqueue(context.Context, string, int, time.Time) error
	ScheduleRetry(context.Context, string, time.Time) error
	Depth(context.Context) (int64, error)
}

type ServerConfig struct {
	HeartbeatInterval time.Duration
	LeaseDuration     time.Duration
	RetryPolicy       RetryPolicy
	Scheduler         Scheduler
	Logger            *slog.Logger
}

// GRPCServer implements the worker control plane. PostgreSQL leases are the
// authority for job ownership, so coordinator restarts do not lose assignments.
type GRPCServer struct {
	workerpb.UnimplementedWorkerCoordinatorServer

	registry          *Registry
	repository        jobRepository
	queue             jobQueue
	heartbeatInterval time.Duration
	leaseDuration     time.Duration
	retryPolicy       RetryPolicy
	scheduler         Scheduler
	logger            *slog.Logger
	offersMu          sync.Mutex
	offers            map[string]string
}

func NewGRPCServer(config ServerConfig, registry *Registry, repository jobRepository, queue jobQueue) (*GRPCServer, error) {
	if registry == nil || repository == nil || queue == nil {
		return nil, errors.New("coordinator registry, repository, and queue are required")
	}
	if config.HeartbeatInterval <= 0 || config.LeaseDuration <= 0 {
		return nil, errors.New("heartbeat interval and lease duration must be greater than zero")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.Scheduler == nil {
		config.Scheduler = NewLeastLoaded()
	}
	return &GRPCServer{
		registry:          registry,
		repository:        repository,
		queue:             queue,
		heartbeatInterval: config.HeartbeatInterval,
		leaseDuration:     config.LeaseDuration,
		retryPolicy:       config.RetryPolicy,
		scheduler:         config.Scheduler,
		logger:            config.Logger,
		offers:            make(map[string]string),
	}, nil
}

func (s *GRPCServer) RegisterWorker(_ context.Context, request *workerpb.RegisterWorkerRequest) (*workerpb.RegisterWorkerResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "registration request is required")
	}
	snapshot, err := s.registry.Register(Registration{
		WorkerID:          request.GetWorkerId(),
		MaxConcurrency:    int(request.GetMaxConcurrency()),
		SupportedJobTypes: request.GetSupportedJobTypes(),
	})
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &workerpb.RegisterWorkerResponse{
		HeartbeatInterval: durationpb.New(s.heartbeatInterval),
		LeaseDuration:     durationpb.New(s.leaseDuration),
		RegisteredAt:      timestamppb.New(snapshot.RegisteredAt),
	}, nil
}

func (s *GRPCServer) Heartbeat(_ context.Context, request *workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error) {
	if request == nil || request.GetTimestamp() == nil {
		return nil, status.Error(codes.InvalidArgument, "heartbeat timestamp is required")
	}
	if err := request.GetTimestamp().CheckValid(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "heartbeat timestamp is invalid")
	}
	snapshot, err := s.registry.RecordHeartbeat(Heartbeat{
		WorkerID:       request.GetWorkerId(),
		ActiveJobs:     int(request.GetActiveJobs()),
		MaxConcurrency: int(request.GetMaxConcurrency()),
		Timestamp:      request.GetTimestamp().AsTime(),
	})
	if err != nil {
		if errors.Is(err, ErrWorkerNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &workerpb.HeartbeatResponse{
		Accepted:   true,
		Health:     healthToProto(snapshot.Health),
		ServerTime: timestamppb.Now(),
	}, nil
}

func (s *GRPCServer) ClaimJob(ctx context.Context, request *workerpb.ClaimJobRequest) (*workerpb.ClaimJobResponse, error) {
	if request == nil || request.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	worker, ok := s.registry.Get(request.GetWorkerId())
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "worker must register before claiming jobs")
	}
	if worker.Health == HealthUnhealthy {
		return nil, status.Error(codes.FailedPrecondition, "unhealthy worker cannot claim jobs")
	}
	if worker.AvailableCapacity <= 0 {
		return &workerpb.ClaimJobResponse{LeaseDuration: durationpb.New(s.leaseDuration)}, nil
	}

	depth, err := s.queue.Depth(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, fmt.Sprintf("read queue depth: %v", err))
	}
	if depth > 1000 {
		depth = 1000
	}
	for attempt := int64(0); attempt < depth; attempt++ {
		jobID, err := s.queue.Dequeue(ctx)
		if errors.Is(err, queue.ErrEmpty) {
			break
		}
		if err != nil {
			return nil, status.Error(codes.Unavailable, fmt.Sprintf("dequeue job: %v", err))
		}
		candidate, err := s.repository.GetJob(ctx, jobID)
		if errors.Is(err, jobs.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, status.Error(codes.Unavailable, fmt.Sprintf("load queued job: %v", err))
		}
		selectedWorker, err := s.selectWorker(candidate)
		if errors.Is(err, ErrNoEligibleWorker) {
			if err := s.queue.Enqueue(ctx, candidate.ID, candidate.Priority, candidate.CreatedAt); err != nil {
				return nil, status.Error(codes.Unavailable, fmt.Sprintf("return unscheduled job to queue: %v", err))
			}
			continue
		}
		if err != nil {
			return nil, status.Error(codes.Unavailable, fmt.Sprintf("schedule job: %v", err))
		}
		if selectedWorker != worker.WorkerID {
			if err := s.queue.Enqueue(ctx, candidate.ID, candidate.Priority, candidate.CreatedAt); err != nil {
				return nil, status.Error(codes.Unavailable, fmt.Sprintf("return job offered to another worker: %v", err))
			}
			continue
		}

		if _, err := s.registry.TryReserve(worker.WorkerID); err != nil {
			s.deleteOffer(candidate.ID)
			if enqueueErr := s.queue.Enqueue(ctx, candidate.ID, candidate.Priority, candidate.CreatedAt); enqueueErr != nil {
				return nil, status.Error(codes.Unavailable, fmt.Sprintf("return unreserved job to queue: %v", enqueueErr))
			}
			continue
		}

		claimed, err := s.repository.LeaseJob(ctx, candidate.ID, worker.WorkerID, s.leaseDuration)
		if errors.Is(err, jobs.ErrClaimConflict) {
			_, _ = s.registry.RollbackReservation(worker.WorkerID)
			s.deleteOffer(candidate.ID)
			continue
		}
		if err != nil {
			_, _ = s.registry.RollbackReservation(worker.WorkerID)
			s.deleteOffer(candidate.ID)
			return nil, status.Error(codes.Unavailable, fmt.Sprintf("claim job: %v", err))
		}
		s.deleteOffer(candidate.ID)
		if _, err := s.registry.ConfirmAssignment(worker.WorkerID); err != nil {
			s.logger.Warn("durable lease assigned but assignment metric was not recorded",
				"job_id", claimed.ID,
				"worker_id", worker.WorkerID,
				"error", err,
			)
		}
		return &workerpb.ClaimJobResponse{
			Job:           jobToProto(claimed),
			LeaseDuration: durationpb.New(s.leaseDuration),
		}, nil
	}

	return &workerpb.ClaimJobResponse{LeaseDuration: durationpb.New(s.leaseDuration)}, nil
}

func (s *GRPCServer) StartJob(ctx context.Context, request *workerpb.StartJobRequest) (*workerpb.StartJobResponse, error) {
	if request == nil || request.GetWorkerId() == "" || request.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id and job_id are required")
	}
	job, err := s.repository.StartAttempt(ctx, request.GetJobId(), request.GetWorkerId())
	if err != nil {
		return nil, repositoryStatusError(err)
	}
	return &workerpb.StartJobResponse{Job: jobToProto(job)}, nil
}

func (s *GRPCServer) CompleteJob(ctx context.Context, request *workerpb.CompleteJobRequest) (*workerpb.CompleteJobResponse, error) {
	if request == nil || request.GetWorkerId() == "" || request.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id and job_id are required")
	}
	if len(request.GetResultJson()) > 0 && !json.Valid(request.GetResultJson()) {
		return nil, status.Error(codes.InvalidArgument, "result_json must contain valid JSON")
	}
	job, err := s.repository.SucceedAttempt(ctx, request.GetJobId(), request.GetWorkerId(), request.GetResultJson())
	if err != nil {
		return nil, repositoryStatusError(err)
	}
	_, _ = s.registry.Release(request.GetWorkerId())
	return &workerpb.CompleteJobResponse{Job: jobToProto(job)}, nil
}

func (s *GRPCServer) FailJob(ctx context.Context, request *workerpb.FailJobRequest) (*workerpb.FailJobResponse, error) {
	if request == nil || request.GetWorkerId() == "" || request.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id and job_id are required")
	}
	current, err := s.repository.GetJob(ctx, request.GetJobId())
	if err != nil {
		return nil, repositoryStatusError(err)
	}
	var retryAt time.Time
	if request.GetRetryable() {
		retryAt = s.retryPolicy.DueAt(time.Now(), current.AttemptCount)
	}
	job, err := s.repository.FailAttempt(ctx, request.GetJobId(), request.GetWorkerId(), request.GetError(), request.GetRetryable(), retryAt)
	if err != nil {
		return nil, repositoryStatusError(err)
	}
	_, _ = s.registry.Release(request.GetWorkerId())
	if job.Status == jobs.StatusRetryWait {
		if err := s.queue.ScheduleRetry(ctx, job.ID, job.ScheduledAt); err != nil {
			s.logger.Warn("retry persisted but delayed enqueue failed; promoter will recover from PostgreSQL",
				"job_id", job.ID,
				"retry_at", job.ScheduledAt,
				"error", err,
			)
		}
	}
	return &workerpb.FailJobResponse{Job: jobToProto(job)}, nil
}

func (s *GRPCServer) selectWorker(job jobs.Job) (string, error) {
	s.offersMu.Lock()
	defer s.offersMu.Unlock()

	if workerID, ok := s.offers[job.ID]; ok {
		if snapshot, exists := s.registry.Get(workerID); exists && len(eligibleWorkers(job, []WorkerSnapshot{snapshot})) == 1 {
			return workerID, nil
		}
		delete(s.offers, job.ID)
	}

	selected, err := s.scheduler.SelectWorker(job, s.registry.List())
	if err != nil {
		return "", err
	}
	s.offers[job.ID] = selected.WorkerID
	return selected.WorkerID, nil
}

func (s *GRPCServer) deleteOffer(jobID string) {
	s.offersMu.Lock()
	delete(s.offers, jobID)
	s.offersMu.Unlock()
}

func (s *GRPCServer) RenewLease(ctx context.Context, request *workerpb.RenewLeaseRequest) (*workerpb.RenewLeaseResponse, error) {
	if request == nil || request.GetWorkerId() == "" || request.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id and job_id are required")
	}
	job, err := s.repository.RenewLease(ctx, request.GetJobId(), request.GetWorkerId(), s.leaseDuration)
	if err != nil {
		return nil, repositoryStatusError(err)
	}
	if job.LeaseExpiresAt == nil {
		return nil, status.Error(codes.Internal, "repository returned a lease without an expiration")
	}
	return &workerpb.RenewLeaseResponse{LeaseExpiresAt: timestamppb.New(*job.LeaseExpiresAt)}, nil
}

func (s *GRPCServer) ListWorkers(_ context.Context, request *workerpb.ListWorkersRequest) (*workerpb.ListWorkersResponse, error) {
	filter := workerpb.WorkerHealth_WORKER_HEALTH_UNSPECIFIED
	if request != nil {
		filter = request.GetHealth()
	}
	response := &workerpb.ListWorkersResponse{}
	for _, snapshot := range s.registry.List() {
		worker := workerToProto(snapshot)
		if filter == workerpb.WorkerHealth_WORKER_HEALTH_UNSPECIFIED || worker.GetHealth() == filter {
			response.Workers = append(response.Workers, worker)
		}
	}
	return response, nil
}

func repositoryStatusError(err error) error {
	switch {
	case errors.Is(err, jobs.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, jobs.ErrInvalidTransition), errors.Is(err, jobs.ErrClaimConflict):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}

func workerToProto(snapshot WorkerSnapshot) *workerpb.Worker {
	return &workerpb.Worker{
		WorkerId:          snapshot.WorkerID,
		Health:            healthToProto(snapshot.Health),
		ActiveJobs:        int32(snapshot.ActiveJobs),
		MaxConcurrency:    int32(snapshot.MaxConcurrency),
		SupportedJobTypes: append([]string(nil), snapshot.SupportedJobTypes...),
		RegisteredAt:      timestamppb.New(snapshot.RegisteredAt),
		LastHeartbeatAt:   timestamppb.New(snapshot.LastHeartbeatAt),
		JobsAssignedTotal: snapshot.JobsAssignedTotal,
	}
}

func healthToProto(health Health) workerpb.WorkerHealth {
	switch health {
	case HealthHealthy:
		return workerpb.WorkerHealth_WORKER_HEALTH_HEALTHY
	case HealthSuspect:
		return workerpb.WorkerHealth_WORKER_HEALTH_SUSPECT
	case HealthUnhealthy:
		return workerpb.WorkerHealth_WORKER_HEALTH_UNHEALTHY
	default:
		return workerpb.WorkerHealth_WORKER_HEALTH_UNSPECIFIED
	}
}

func jobToProto(job jobs.Job) *workerpb.Job {
	result := &workerpb.Job{
		Id:             job.ID,
		Type:           job.Type,
		PayloadJson:    append([]byte(nil), job.Payload...),
		Status:         statusToProto(job.Status),
		AttemptCount:   int32(job.AttemptCount),
		MaxAttempts:    int32(job.MaxAttempts),
		Priority:       int32(job.Priority),
		ScheduledAt:    timestamppb.New(job.ScheduledAt),
		LeaseOwner:     job.LeaseOwner,
		IdempotencyKey: job.IdempotencyKey,
		ResultJson:     append([]byte(nil), job.Result...),
		LastError:      job.LastError,
		CreatedAt:      timestamppb.New(job.CreatedAt),
		UpdatedAt:      timestamppb.New(job.UpdatedAt),
	}
	if job.LeaseExpiresAt != nil {
		result.LeaseExpiresAt = timestamppb.New(*job.LeaseExpiresAt)
	}
	return result
}

func statusToProto(value jobs.Status) workerpb.JobStatus {
	switch value {
	case jobs.StatusQueued:
		return workerpb.JobStatus_JOB_STATUS_QUEUED
	case jobs.StatusLeased:
		return workerpb.JobStatus_JOB_STATUS_LEASED
	case jobs.StatusRunning:
		return workerpb.JobStatus_JOB_STATUS_RUNNING
	case jobs.StatusRetryWait:
		return workerpb.JobStatus_JOB_STATUS_RETRY_WAIT
	case jobs.StatusSucceeded:
		return workerpb.JobStatus_JOB_STATUS_SUCCEEDED
	case jobs.StatusFailed:
		return workerpb.JobStatus_JOB_STATUS_FAILED
	case jobs.StatusDeadLetter:
		return workerpb.JobStatus_JOB_STATUS_DEAD_LETTER
	case jobs.StatusCanceled:
		return workerpb.JobStatus_JOB_STATUS_CANCELED
	default:
		return workerpb.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}
