package api

import (
	"context"
	"net/http"
	"time"

	"github.com/AlNaheyan/forgeQ/proto/workerpb"
)

type WorkerInfo struct {
	WorkerID          string    `json:"worker_id"`
	Health            string    `json:"health"`
	ActiveJobs        int       `json:"active_jobs"`
	MaxConcurrency    int       `json:"max_concurrency"`
	AvailableCapacity int       `json:"available_capacity"`
	SupportedJobTypes []string  `json:"supported_job_types"`
	RegisteredAt      time.Time `json:"registered_at"`
	LastHeartbeatAt   time.Time `json:"last_heartbeat_at"`
	JobsAssignedTotal uint64    `json:"jobs_assigned_total"`
}

type WorkerProvider interface {
	ListWorkers(context.Context) ([]WorkerInfo, error)
}

type GRPCWorkerProvider struct {
	client  workerpb.WorkerCoordinatorClient
	timeout time.Duration
}

func NewGRPCWorkerProvider(client workerpb.WorkerCoordinatorClient) *GRPCWorkerProvider {
	return &GRPCWorkerProvider{client: client}
}

func (p *GRPCWorkerProvider) WithTimeout(timeout time.Duration) *GRPCWorkerProvider {
	p.timeout = timeout
	return p
}

func (p *GRPCWorkerProvider) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}
	response, err := p.client.ListWorkers(ctx, &workerpb.ListWorkersRequest{})
	if err != nil {
		return nil, err
	}
	workers := make([]WorkerInfo, 0, len(response.GetWorkers()))
	for _, worker := range response.GetWorkers() {
		info := WorkerInfo{
			WorkerID:          worker.GetWorkerId(),
			Health:            workerHealthString(worker.GetHealth()),
			ActiveJobs:        int(worker.GetActiveJobs()),
			MaxConcurrency:    int(worker.GetMaxConcurrency()),
			AvailableCapacity: int(worker.GetMaxConcurrency() - worker.GetActiveJobs()),
			SupportedJobTypes: append([]string(nil), worker.GetSupportedJobTypes()...),
			JobsAssignedTotal: worker.GetJobsAssignedTotal(),
		}
		if worker.GetRegisteredAt() != nil {
			info.RegisteredAt = worker.GetRegisteredAt().AsTime()
		}
		if worker.GetLastHeartbeatAt() != nil {
			info.LastHeartbeatAt = worker.GetLastHeartbeatAt().AsTime()
		}
		workers = append(workers, info)
	}
	return workers, nil
}

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request) {
	if s.workers == nil {
		writeError(w, http.StatusServiceUnavailable, "workers_unavailable", "worker registry is unavailable")
		return
	}
	workers, err := s.workers.ListWorkers(r.Context())
	if err != nil {
		s.logger.Warn("worker registry request failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "workers_unavailable", "worker registry is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": workers})
}

func workerHealthString(health workerpb.WorkerHealth) string {
	switch health {
	case workerpb.WorkerHealth_WORKER_HEALTH_HEALTHY:
		return "healthy"
	case workerpb.WorkerHealth_WORKER_HEALTH_SUSPECT:
		return "suspect"
	case workerpb.WorkerHealth_WORKER_HEALTH_UNHEALTHY:
		return "unhealthy"
	default:
		return "unknown"
	}
}
