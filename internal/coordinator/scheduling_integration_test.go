package coordinator

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/proto/workerpb"
)

func TestClaimJobRoutesByCapabilityAndKeepsStickyOffer(t *testing.T) {
	now := time.Now().UTC()
	repository := &serverTestRepository{jobs: map[string]jobs.Job{
		"sleep-job": schedulingTestJob("sleep-job", "sleep", now),
	}}
	jobQueue := &serverTestQueue{ids: []string{"sleep-job"}}
	server, registry := newSchedulingTestServer(t, NewLeastLoaded(), repository, jobQueue)
	registerSchedulingTestWorker(t, server, "fail-worker", 10, "fail")
	registerSchedulingTestWorker(t, server, "sleep-worker", 1, "sleep")

	wrongWorker, err := server.ClaimJob(context.Background(), &workerpb.ClaimJobRequest{WorkerId: "fail-worker"})
	if err != nil {
		t.Fatalf("ineligible worker claim: %v", err)
	}
	if wrongWorker.GetJob() != nil {
		t.Fatalf("ineligible worker received job: %+v", wrongWorker.GetJob())
	}
	if got := schedulingTestQueueIDs(jobQueue); len(got) != 1 || got[0] != "sleep-job" {
		t.Fatalf("queue after wrong worker claim = %v, want [sleep-job]", got)
	}

	selectedWorker, err := server.ClaimJob(context.Background(), &workerpb.ClaimJobRequest{WorkerId: "sleep-worker"})
	if err != nil {
		t.Fatalf("selected worker claim: %v", err)
	}
	if selectedWorker.GetJob().GetId() != "sleep-job" || selectedWorker.GetJob().GetLeaseOwner() != "sleep-worker" {
		t.Fatalf("selected worker response = %+v", selectedWorker.GetJob())
	}

	failSnapshot, _ := registry.Get("fail-worker")
	sleepSnapshot, _ := registry.Get("sleep-worker")
	if failSnapshot.JobsAssignedTotal != 0 || sleepSnapshot.JobsAssignedTotal != 1 {
		t.Fatalf("assignment totals: fail=%d sleep=%d", failSnapshot.JobsAssignedTotal, sleepSnapshot.JobsAssignedTotal)
	}
}

func TestConcurrentClaimsDoNotOversubscribeWorker(t *testing.T) {
	now := time.Now().UTC()
	repository := &serverTestRepository{jobs: map[string]jobs.Job{
		"job-a": schedulingTestJob("job-a", "sleep", now),
		"job-b": schedulingTestJob("job-b", "sleep", now),
	}}
	jobQueue := &serverTestQueue{ids: []string{"job-a", "job-b"}}
	server, registry := newSchedulingTestServer(t, NewLeastLoaded(), repository, jobQueue)
	registerSchedulingTestWorker(t, server, "worker-1", 1, "sleep")

	start := make(chan struct{})
	responses := make(chan *workerpb.ClaimJobResponse, 2)
	errors := make(chan error, 2)
	var claims sync.WaitGroup
	for range 2 {
		claims.Add(1)
		go func() {
			defer claims.Done()
			<-start
			response, err := server.ClaimJob(context.Background(), &workerpb.ClaimJobRequest{WorkerId: "worker-1"})
			responses <- response
			errors <- err
		}()
	}
	close(start)
	claims.Wait()
	close(responses)
	close(errors)

	for err := range errors {
		if err != nil {
			t.Fatalf("concurrent claim: %v", err)
		}
	}
	claimed := 0
	for response := range responses {
		if response.GetJob() != nil {
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("successful claims = %d, want 1", claimed)
	}

	snapshot, _ := registry.Get("worker-1")
	if snapshot.ActiveJobs != 1 || snapshot.AvailableCapacity != 0 || snapshot.JobsAssignedTotal != 1 {
		t.Fatalf("worker capacity after claims = %+v", snapshot)
	}
	queued, leased := 0, 0
	repository.mu.Lock()
	for _, job := range repository.jobs {
		switch job.Status {
		case jobs.StatusQueued:
			queued++
		case jobs.StatusLeased:
			leased++
		}
	}
	repository.mu.Unlock()
	if queued != 1 || leased != 1 {
		t.Fatalf("job states: queued=%d leased=%d, want one of each", queued, leased)
	}
	if got := schedulingTestQueueIDs(jobQueue); len(got) != 1 {
		t.Fatalf("queue after concurrent claims = %v, want one job", got)
	}
}

func TestRejectedFailureDoesNotReleaseWorkerCapacity(t *testing.T) {
	now := time.Now().UTC()
	repository := &serverTestRepository{jobs: map[string]jobs.Job{
		"job-a": schedulingTestJob("job-a", "sleep", now),
		"job-b": schedulingTestJob("job-b", "sleep", now),
	}}
	jobQueue := &serverTestQueue{ids: []string{"job-a", "job-b"}}
	server, registry := newSchedulingTestServer(t, NewLeastLoaded(), repository, jobQueue)
	registerSchedulingTestWorker(t, server, "worker-1", 1, "sleep")

	assertSchedulingClaim(t, server, "worker-1", "job-a")
	if _, err := server.FailJob(context.Background(), &workerpb.FailJobRequest{
		WorkerId: "worker-1", JobId: "job-a", Error: "not started",
	}); err == nil {
		t.Fatal("FailJob on leased job succeeded, want transition error")
	}
	assertEmptySchedulingClaim(t, server, "worker-1")

	snapshot, _ := registry.Get("worker-1")
	if snapshot.ActiveJobs != 1 || snapshot.AvailableCapacity != 0 {
		t.Fatalf("failed terminal transition released capacity: %+v", snapshot)
	}
	if got := schedulingTestQueueIDs(jobQueue); len(got) != 1 || got[0] != "job-b" {
		t.Fatalf("queue after rejected failure = %v, want [job-b]", got)
	}
}

func TestRoundRobinOffersDistributeClaimsAcrossWorkers(t *testing.T) {
	now := time.Now().UTC()
	repository := &serverTestRepository{jobs: map[string]jobs.Job{
		"job-1": schedulingTestJob("job-1", "sleep", now),
		"job-2": schedulingTestJob("job-2", "sleep", now),
		"job-3": schedulingTestJob("job-3", "sleep", now),
	}}
	jobQueue := &serverTestQueue{ids: []string{"job-1"}}
	server, registry := newSchedulingTestServer(t, NewRoundRobin(), repository, jobQueue)
	registerSchedulingTestWorker(t, server, "worker-a", 2, "sleep")
	registerSchedulingTestWorker(t, server, "worker-b", 2, "sleep")

	assertEmptySchedulingClaim(t, server, "worker-b")
	assertSchedulingClaim(t, server, "worker-a", "job-1")

	if err := jobQueue.Enqueue(context.Background(), "job-2", 0, now); err != nil {
		t.Fatalf("enqueue job-2: %v", err)
	}
	assertEmptySchedulingClaim(t, server, "worker-a")
	assertSchedulingClaim(t, server, "worker-b", "job-2")

	if err := jobQueue.Enqueue(context.Background(), "job-3", 0, now); err != nil {
		t.Fatalf("enqueue job-3: %v", err)
	}
	assertSchedulingClaim(t, server, "worker-a", "job-3")

	workerA, _ := registry.Get("worker-a")
	workerB, _ := registry.Get("worker-b")
	if workerA.JobsAssignedTotal != 2 || workerB.JobsAssignedTotal != 1 {
		t.Fatalf("round-robin assignment totals: worker-a=%d worker-b=%d", workerA.JobsAssignedTotal, workerB.JobsAssignedTotal)
	}
}

func newSchedulingTestServer(t *testing.T, scheduler Scheduler, repository *serverTestRepository, jobQueue *serverTestQueue) (*GRPCServer, *Registry) {
	t.Helper()
	registry, err := NewRegistry(HealthThresholds{SuspectAfter: time.Minute, UnhealthyAfter: 2 * time.Minute}, nil)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	server, err := NewGRPCServer(ServerConfig{
		HeartbeatInterval: time.Second,
		LeaseDuration:     time.Minute,
		Scheduler:         scheduler,
	}, registry, repository, jobQueue)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return server, registry
}

func registerSchedulingTestWorker(t *testing.T, server *GRPCServer, workerID string, capacity int32, jobTypes ...string) {
	t.Helper()
	_, err := server.RegisterWorker(context.Background(), &workerpb.RegisterWorkerRequest{
		WorkerId:          workerID,
		MaxConcurrency:    capacity,
		SupportedJobTypes: jobTypes,
	})
	if err != nil {
		t.Fatalf("register %s: %v", workerID, err)
	}
}

func schedulingTestJob(id, jobType string, now time.Time) jobs.Job {
	return jobs.Job{
		ID: id, Type: jobType, Payload: json.RawMessage(`{}`), Status: jobs.StatusQueued,
		MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
	}
}

func schedulingTestQueueIDs(jobQueue *serverTestQueue) []string {
	jobQueue.mu.Lock()
	defer jobQueue.mu.Unlock()
	return append([]string(nil), jobQueue.ids...)
}

func assertEmptySchedulingClaim(t *testing.T, server *GRPCServer, workerID string) {
	t.Helper()
	response, err := server.ClaimJob(context.Background(), &workerpb.ClaimJobRequest{WorkerId: workerID})
	if err != nil {
		t.Fatalf("claim by %s: %v", workerID, err)
	}
	if response.GetJob() != nil {
		t.Fatalf("worker %s unexpectedly claimed %+v", workerID, response.GetJob())
	}
}

func assertSchedulingClaim(t *testing.T, server *GRPCServer, workerID, jobID string) {
	t.Helper()
	response, err := server.ClaimJob(context.Background(), &workerpb.ClaimJobRequest{WorkerId: workerID})
	if err != nil {
		t.Fatalf("claim %s by %s: %v", jobID, workerID, err)
	}
	if response.GetJob().GetId() != jobID || response.GetJob().GetLeaseOwner() != workerID {
		t.Fatalf("claim %s by %s returned %+v", jobID, workerID, response.GetJob())
	}
}
