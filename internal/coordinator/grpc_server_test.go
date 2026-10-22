package coordinator

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"forgequeue/internal/eventstream"
	"forgequeue/internal/jobs"
	"forgequeue/internal/queue"
	"forgequeue/proto/workerpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type serverTestRepository struct {
	mu   sync.Mutex
	jobs map[string]jobs.Job
}

func (r *serverTestRepository) GetJob(_ context.Context, id string) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return job, nil
}

func (r *serverTestRepository) LeaseJob(_ context.Context, id, workerID string, duration time.Duration) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok || job.Status != jobs.StatusQueued {
		return jobs.Job{}, jobs.ErrClaimConflict
	}
	expiresAt := time.Now().Add(duration)
	job.Status = jobs.StatusLeased
	job.LeaseOwner = workerID
	job.LeaseGeneration++
	job.LeaseExpiresAt = &expiresAt
	r.jobs[id] = job
	return job, nil
}

func (r *serverTestRepository) StartAttempt(_ context.Context, id, workerID string, generation int64) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok || job.Status != jobs.StatusLeased || job.LeaseOwner != workerID || generation <= 0 || job.LeaseGeneration != generation || job.LeaseExpiresAt == nil || !job.LeaseExpiresAt.After(time.Now()) {
		return jobs.Job{}, jobs.ErrClaimConflict
	}
	job.Status = jobs.StatusRunning
	job.AttemptCount++
	r.jobs[id] = job
	return job, nil
}

func (r *serverTestRepository) RenewLease(_ context.Context, id, workerID string, duration time.Duration, generation int64) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok || job.LeaseOwner != workerID || generation <= 0 || job.LeaseGeneration != generation || job.LeaseExpiresAt == nil || !job.LeaseExpiresAt.After(time.Now()) {
		return jobs.Job{}, jobs.ErrClaimConflict
	}
	expiresAt := time.Now().Add(duration)
	job.LeaseExpiresAt = &expiresAt
	r.jobs[id] = job
	return job, nil
}

func (r *serverTestRepository) SucceedAttempt(_ context.Context, id, workerID string, result []byte, generation int64) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok {
		return jobs.Job{}, jobs.ErrNotFound
	}
	if job.Status != jobs.StatusRunning || job.LeaseOwner != workerID || generation <= 0 || job.LeaseGeneration != generation || job.LeaseExpiresAt == nil || !job.LeaseExpiresAt.After(time.Now()) {
		return jobs.Job{}, jobs.ErrInvalidTransition
	}
	job.Status = jobs.StatusSucceeded
	job.LeaseOwner = ""
	job.LeaseExpiresAt = nil
	job.Result = append(json.RawMessage(nil), result...)
	job.LastError = ""
	r.jobs[id] = job
	return job, nil
}

func (r *serverTestRepository) FailAttempt(_ context.Context, id, workerID, lastError string, retryable bool, retryAt time.Time, generation int64) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok {
		return jobs.Job{}, jobs.ErrNotFound
	}
	if job.Status != jobs.StatusRunning || job.LeaseOwner != workerID || generation <= 0 || job.LeaseGeneration != generation || job.LeaseExpiresAt == nil || !job.LeaseExpiresAt.After(time.Now()) {
		return jobs.Job{}, jobs.ErrInvalidTransition
	}
	if retryable && job.AttemptCount < job.MaxAttempts {
		job.Status = jobs.StatusRetryWait
		job.ScheduledAt = retryAt
	} else {
		job.Status = jobs.StatusDeadLetter
	}
	job.LeaseOwner = ""
	job.LeaseExpiresAt = nil
	job.LastError = lastError
	r.jobs[id] = job
	return job, nil
}

type serverTestQueue struct {
	mu      sync.Mutex
	ids     []string
	delayed map[string]time.Time
}

type serverTestEventRecorder struct {
	events []eventstream.Type
}

func (r *serverTestEventRecorder) RecordEvent(_ context.Context, eventType eventstream.Type, _, _ string, _ json.RawMessage) error {
	r.events = append(r.events, eventType)
	return nil
}

func (q *serverTestQueue) Dequeue(context.Context) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.ids) == 0 {
		return "", queue.ErrEmpty
	}
	id := q.ids[0]
	q.ids = q.ids[1:]
	return id, nil
}

func (q *serverTestQueue) Enqueue(_ context.Context, id string, _ int, _ time.Time) error {
	q.mu.Lock()
	q.ids = append(q.ids, id)
	q.mu.Unlock()
	return nil
}

func (q *serverTestQueue) ScheduleRetry(_ context.Context, id string, dueAt time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.delayed == nil {
		q.delayed = make(map[string]time.Time)
	}
	q.delayed[id] = dueAt
	return nil
}

func (q *serverTestQueue) Depth(context.Context) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int64(len(q.ids)), nil
}

func TestGRPCServerWorkerAndJobLifecycle(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	registry, err := NewRegistry(HealthThresholds{SuspectAfter: 10 * time.Second, UnhealthyAfter: 15 * time.Second}, ClockFunc(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	repository := &serverTestRepository{jobs: map[string]jobs.Job{
		"job-1": {
			ID: "job-1", Type: "sleep", Payload: json.RawMessage(`{"duration_ms":1}`),
			Status: jobs.StatusQueued, MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
	}}
	server, err := NewGRPCServer(ServerConfig{HeartbeatInterval: 5 * time.Second, LeaseDuration: 30 * time.Second}, registry, repository, &serverTestQueue{ids: []string{"job-1"}})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	if _, err := server.ClaimJob(context.Background(), &workerpb.ClaimJobRequest{WorkerId: "worker-1"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unregistered claim error = %v", err)
	}
	registration, err := server.RegisterWorker(context.Background(), &workerpb.RegisterWorkerRequest{
		WorkerId: "worker-1", MaxConcurrency: 4, SupportedJobTypes: []string{"sleep"},
	})
	if err != nil || registration.GetHeartbeatInterval().AsDuration() != 5*time.Second {
		t.Fatalf("register: response=%v err=%v", registration, err)
	}
	if _, err := server.Heartbeat(context.Background(), &workerpb.HeartbeatRequest{
		WorkerId: "worker-1", MaxConcurrency: 4, ActiveJobs: 0, Timestamp: timestamppb.New(now),
	}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	claimed, err := server.ClaimJob(context.Background(), &workerpb.ClaimJobRequest{WorkerId: "worker-1"})
	if err != nil || claimed.GetJob().GetStatus() != workerpb.JobStatus_JOB_STATUS_LEASED || claimed.GetJob().GetLeaseOwner() != "worker-1" {
		t.Fatalf("claim: response=%v err=%v", claimed, err)
	}
	started, err := server.StartJob(context.Background(), &workerpb.StartJobRequest{WorkerId: "worker-1", JobId: "job-1", LeaseGeneration: 1})
	if err != nil || started.GetJob().GetStatus() != workerpb.JobStatus_JOB_STATUS_RUNNING || started.GetJob().GetAttemptCount() != 1 {
		t.Fatalf("start: %v", err)
	}
	renewed, err := server.RenewLease(context.Background(), &workerpb.RenewLeaseRequest{WorkerId: "worker-1", JobId: "job-1", LeaseGeneration: 1})
	if err != nil || renewed.GetLeaseExpiresAt() == nil {
		t.Fatalf("renew: response=%v err=%v", renewed, err)
	}
	completed, err := server.CompleteJob(context.Background(), &workerpb.CompleteJobRequest{
		WorkerId: "worker-1", JobId: "job-1", ResultJson: []byte(`{"slept_ms":1}`), LeaseGeneration: 1,
	})
	if err != nil || completed.GetJob().GetStatus() != workerpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Fatalf("complete: response=%v err=%v", completed, err)
	}
	listed, err := server.ListWorkers(context.Background(), &workerpb.ListWorkersRequest{})
	if err != nil || len(listed.GetWorkers()) != 1 || listed.GetWorkers()[0].GetHealth() != workerpb.WorkerHealth_WORKER_HEALTH_HEALTHY {
		t.Fatalf("list workers: response=%v err=%v", listed, err)
	}
}

func TestGRPCServerSchedulesRetryableFailure(t *testing.T) {
	now := time.Now().UTC()
	registry, err := NewRegistry(HealthThresholds{SuspectAfter: time.Minute, UnhealthyAfter: 2 * time.Minute}, nil)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	repository := &serverTestRepository{jobs: map[string]jobs.Job{
		"job-retry": {
			ID: "job-retry", Type: "fail", Payload: json.RawMessage(`{"failures_before_success":1}`),
			Status: jobs.StatusQueued, MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
	}}
	jobQueue := &serverTestQueue{ids: []string{"job-retry"}}
	server, err := NewGRPCServer(ServerConfig{
		HeartbeatInterval: time.Second,
		LeaseDuration:     time.Minute,
		RetryPolicy:       NewRetryPolicy(2*time.Second, time.Minute, 0, nil),
	}, registry, repository, jobQueue)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if _, err := server.RegisterWorker(context.Background(), &workerpb.RegisterWorkerRequest{
		WorkerId: "worker-1", MaxConcurrency: 1, SupportedJobTypes: []string{"fail"},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := server.ClaimJob(context.Background(), &workerpb.ClaimJobRequest{WorkerId: "worker-1"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := server.StartJob(context.Background(), &workerpb.StartJobRequest{WorkerId: "worker-1", JobId: "job-retry", LeaseGeneration: 1}); err != nil {
		t.Fatalf("start: %v", err)
	}
	failed, err := server.FailJob(context.Background(), &workerpb.FailJobRequest{
		WorkerId: "worker-1", JobId: "job-retry", Error: "intentional failure", Retryable: true, LeaseGeneration: 1,
	})
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	if failed.GetJob().GetStatus() != workerpb.JobStatus_JOB_STATUS_RETRY_WAIT {
		t.Fatalf("failed status = %s", failed.GetJob().GetStatus())
	}
	jobQueue.mu.Lock()
	dueAt, scheduled := jobQueue.delayed["job-retry"]
	jobQueue.mu.Unlock()
	if !scheduled || dueAt.Before(now.Add(2*time.Second)) {
		t.Fatalf("retry schedule = %v, scheduled=%t", dueAt, scheduled)
	}
}

func TestGRPCServerRecordsWorkerRegistrationAndLossOnce(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)}
	registry := newTestRegistry(t, clock)
	recorder := &serverTestEventRecorder{}
	server, err := NewGRPCServer(ServerConfig{
		HeartbeatInterval: 5 * time.Second,
		LeaseDuration:     30 * time.Second,
		EventRecorder:     recorder,
	}, registry, &serverTestRepository{jobs: map[string]jobs.Job{}}, &serverTestQueue{})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if _, err := server.RegisterWorker(context.Background(), &workerpb.RegisterWorkerRequest{
		WorkerId: "worker-event-test", MaxConcurrency: 1, SupportedJobTypes: []string{"sleep"},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	clock.Advance(16 * time.Second)
	server.RecordWorkerLosses(context.Background())
	server.RecordWorkerLosses(context.Background())
	if len(recorder.events) != 2 || recorder.events[0] != eventstream.WorkerRegistered || recorder.events[1] != eventstream.WorkerLost {
		t.Fatalf("recorded events = %v, want registration and one loss", recorder.events)
	}
}
