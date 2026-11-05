package coordinator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/telemetry"
	"github.com/AlNaheyan/forgeQ/proto/workerpb"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestGRPCServerRecordsLifecycleMetrics(t *testing.T) {
	now := time.Now().UTC()
	registry, err := NewRegistry(HealthThresholds{SuspectAfter: time.Minute, UnhealthyAfter: 2 * time.Minute}, nil)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	repository := &serverTestRepository{jobs: map[string]jobs.Job{
		"observed-job": {
			ID: "observed-job", Type: "sleep", Payload: json.RawMessage(`{"duration_ms":1}`),
			Status: jobs.StatusQueued, MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
	}}
	metrics := telemetry.NewMetrics()
	server, err := NewGRPCServer(ServerConfig{
		HeartbeatInterval: time.Second,
		LeaseDuration:     time.Minute,
		Metrics:           metrics,
	}, registry, repository, &serverTestQueue{ids: []string{"observed-job"}})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	ctx := context.Background()
	if _, err := server.RegisterWorker(ctx, &workerpb.RegisterWorkerRequest{
		WorkerId: "worker-observed", MaxConcurrency: 1, SupportedJobTypes: []string{"sleep"},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := server.ClaimJob(ctx, &workerpb.ClaimJobRequest{WorkerId: "worker-observed"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := server.StartJob(ctx, &workerpb.StartJobRequest{WorkerId: "worker-observed", JobId: "observed-job", LeaseGeneration: 1}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := server.CompleteJob(ctx, &workerpb.CompleteJobRequest{WorkerId: "worker-observed", JobId: "observed-job", ResultJson: []byte(`{}`), LeaseGeneration: 1}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	assertMetricValue(t, metrics.WorkersRegistered, 1)
	assertMetricValue(t, metrics.WorkersHealthy, 1)
	assertMetricValue(t, metrics.ActiveJobs, 0)
	assertMetricValue(t, metrics.JobsStarted, 1)
	assertMetricValue(t, metrics.JobsCompleted, 1)
	assertMetricValue(t, metrics.JobsAssigned.WithLabelValues("worker-observed"), 1)
}

func TestGRPCServerRecordsRetryAndDeadLetterMetrics(t *testing.T) {
	now := time.Now().UTC()
	registry, err := NewRegistry(HealthThresholds{SuspectAfter: time.Minute, UnhealthyAfter: 2 * time.Minute}, nil)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	repository := &serverTestRepository{jobs: map[string]jobs.Job{
		"retry-job": {
			ID: "retry-job", Type: "fail", Payload: json.RawMessage(`{}`), Status: jobs.StatusQueued,
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		"dead-job": {
			ID: "dead-job", Type: "fail", Payload: json.RawMessage(`{}`), Status: jobs.StatusQueued,
			MaxAttempts: 1, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
	}}
	metrics := telemetry.NewMetrics()
	server, err := NewGRPCServer(ServerConfig{
		HeartbeatInterval: time.Second,
		LeaseDuration:     time.Minute,
		RetryPolicy:       NewRetryPolicy(time.Second, time.Minute, 0, nil),
		Metrics:           metrics,
	}, registry, repository, &serverTestQueue{ids: []string{"retry-job", "dead-job"}})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	ctx := context.Background()
	if _, err := server.RegisterWorker(ctx, &workerpb.RegisterWorkerRequest{
		WorkerId: "worker-observed", MaxConcurrency: 1, SupportedJobTypes: []string{"fail"},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	for _, jobID := range []string{"retry-job", "dead-job"} {
		claimed, err := server.ClaimJob(ctx, &workerpb.ClaimJobRequest{WorkerId: "worker-observed"})
		if err != nil || claimed.GetJob().GetId() != jobID {
			t.Fatalf("claim %s: response=%v err=%v", jobID, claimed, err)
		}
		if _, err := server.StartJob(ctx, &workerpb.StartJobRequest{WorkerId: "worker-observed", JobId: jobID, LeaseGeneration: 1}); err != nil {
			t.Fatalf("start %s: %v", jobID, err)
		}
		if _, err := server.FailJob(ctx, &workerpb.FailJobRequest{
			WorkerId: "worker-observed", JobId: jobID, Error: "expected", Retryable: true, LeaseGeneration: 1,
		}); err != nil {
			t.Fatalf("fail %s: %v", jobID, err)
		}
	}

	assertMetricValue(t, metrics.JobsFailed, 2)
	assertMetricValue(t, metrics.JobsRetried, 1)
	assertMetricValue(t, metrics.JobsDeadLetter, 1)
}

func assertMetricValue(t *testing.T, collector prometheus.Collector, want float64) {
	t.Helper()
	if got := testutil.ToFloat64(collector); got != want {
		t.Fatalf("metric value = %v, want %v", got, want)
	}
}
