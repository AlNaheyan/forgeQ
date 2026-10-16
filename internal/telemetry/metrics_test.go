package telemetry

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsExposeCoreCollectors(t *testing.T) {
	metrics := NewMetrics()
	metrics.JobsSubmitted.Inc()
	metrics.RecordBackpressureRejection()
	metrics.JobsAssigned.WithLabelValues("worker-1").Inc()

	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", response.Code)
	}
	for _, name := range []string{
		"forgequeue_jobs_submitted_total 1",
		"forgequeue_backpressure_rejections_total 1",
		`forgequeue_jobs_assigned_total{worker_id="worker-1"} 1`,
	} {
		if !strings.Contains(response.Body.String(), name) {
			t.Errorf("metrics output missing %q", name)
		}
	}
}

func TestHTTPMiddlewarePropagatesRequestIDAndRecordsDuration(t *testing.T) {
	metrics := NewMetrics()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /example/{id}", func(w http.ResponseWriter, r *http.Request) {
		if RequestID(r.Context()) != "client-id" {
			t.Errorf("context request ID = %q", RequestID(r.Context()))
		}
		w.WriteHeader(http.StatusAccepted)
	})
	handler := HTTPMiddleware(mux, logger, metrics)
	request := httptest.NewRequest(http.MethodGet, "/example/123", nil)
	request.Header.Set(RequestIDHeader, "client-id")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if got := response.Header().Get(RequestIDHeader); got != "client-id" {
		t.Fatalf("response request ID = %q", got)
	}
	if got := testutil.CollectAndCount(metrics.APIRequestDuration, "forgequeue_api_request_duration_seconds"); got != 1 {
		t.Fatalf("API histogram count = %d", got)
	}
}
