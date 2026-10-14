package coordinator

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

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

func (r *serverTestRepository) ClaimJob(_ context.Context, id string) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok || job.Status != jobs.StatusQueued {
		return jobs.Job{}, jobs.ErrClaimConflict
	}
	job.Status = jobs.StatusRunning
	job.AttemptCount++
	r.jobs[id] = job
	return job, nil
}

func (r *serverTestRepository) FinishJob(_ context.Context, id string, target jobs.Status, result []byte, lastError string) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok {
		return jobs.Job{}, jobs.ErrNotFound
	}
	if job.Status != jobs.StatusRunning {
		return jobs.Job{}, jobs.ErrInvalidTransition
	}
	job.Status = target
	job.Result = append(json.RawMessage(nil), result...)
	job.LastError = lastError
	r.jobs[id] = job
	return job, nil
}

type serverTestQueue struct {
	mu  sync.Mutex
	ids []string
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
	if err != nil || claimed.GetJob().GetStatus() != workerpb.JobStatus_JOB_STATUS_RUNNING {
		t.Fatalf("claim: response=%v err=%v", claimed, err)
	}
	if _, err := server.StartJob(context.Background(), &workerpb.StartJobRequest{WorkerId: "worker-1", JobId: "job-1"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	completed, err := server.CompleteJob(context.Background(), &workerpb.CompleteJobRequest{
		WorkerId: "worker-1", JobId: "job-1", ResultJson: []byte(`{"slept_ms":1}`),
	})
	if err != nil || completed.GetJob().GetStatus() != workerpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Fatalf("complete: response=%v err=%v", completed, err)
	}
	listed, err := server.ListWorkers(context.Background(), &workerpb.ListWorkersRequest{})
	if err != nil || len(listed.GetWorkers()) != 1 || listed.GetWorkers()[0].GetHealth() != workerpb.WorkerHealth_WORKER_HEALTH_HEALTHY {
		t.Fatalf("list workers: response=%v err=%v", listed, err)
	}
}
