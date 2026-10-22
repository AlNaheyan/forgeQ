package coordinator

import (
	"context"
	"forgequeue/internal/jobs"
	"forgequeue/internal/tenant"
	"forgequeue/proto/workerpb"
	"testing"
	"time"
)

type tenantQuotaRepository struct{ *serverTestRepository }

func (r tenantQuotaRepository) LeaseJob(ctx context.Context, id, worker string, d time.Duration) (jobs.Job, error) {
	if id == "saturated" {
		return jobs.Job{}, tenant.ErrConcurrency
	}
	return r.serverTestRepository.LeaseJob(ctx, id, worker, d)
}
func TestClaimDefersSaturatedTenant(t *testing.T) {
	registry, err := NewRegistry(HealthThresholds{SuspectAfter: time.Second, UnhealthyAfter: 2 * time.Second}, ClockFunc(time.Now))
	if err != nil {
		t.Fatal(err)
	}
	repo := tenantQuotaRepository{&serverTestRepository{jobs: map[string]jobs.Job{
		"saturated": {ID: "saturated", Type: "sleep", Status: jobs.StatusQueued},
		"available": {ID: "available", TenantID: "tenant-b", Type: "sleep", Status: jobs.StatusQueued},
	}}}
	q := &serverTestQueue{ids: []string{"saturated", "available"}}
	server, err := NewGRPCServer(ServerConfig{HeartbeatInterval: time.Second, LeaseDuration: time.Minute}, registry, repo, q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.RegisterWorker(context.Background(), &workerpb.RegisterWorkerRequest{WorkerId: "worker", MaxConcurrency: 1, SupportedJobTypes: []string{"sleep"}}); err != nil {
		t.Fatal(err)
	}
	result, err := server.ClaimJob(context.Background(), &workerpb.ClaimJobRequest{WorkerId: "worker"})
	if err != nil || result.GetJob().GetId() != "available" || result.GetJob().GetTenantId() != "tenant-b" {
		t.Fatalf("claim: %v %v", result, err)
	}
	if len(q.ids) != 1 || q.ids[0] != "saturated" {
		t.Fatalf("lost saturated tenant hint: %v", q.ids)
	}
}
