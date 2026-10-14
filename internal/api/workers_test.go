package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type staticWorkerProvider struct {
	workers []WorkerInfo
	err     error
}

func (p staticWorkerProvider) ListWorkers(context.Context) ([]WorkerInfo, error) {
	return p.workers, p.err
}

func TestListWorkersEndpoint(t *testing.T) {
	server := NewServer(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).
		WithWorkerProvider(staticWorkerProvider{workers: []WorkerInfo{{
			WorkerID:          "worker-1",
			Health:            "healthy",
			ActiveJobs:        1,
			MaxConcurrency:    4,
			AvailableCapacity: 3,
			JobsAssignedTotal: 7,
			SupportedJobTypes: []string{"sleep"},
		}}})
	request := httptest.NewRequest(http.MethodGet, "/v1/workers", nil)
	response := httptest.NewRecorder()
	server.Routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	if body := response.Body.String(); !strings.Contains(body, `"worker_id":"worker-1"`) || !strings.Contains(body, `"available_capacity":3`) || !strings.Contains(body, `"jobs_assigned_total":7`) {
		t.Fatalf("unexpected response: %s", body)
	}
}
