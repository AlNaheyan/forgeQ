package api

import (
	"context"
	"forgequeue/internal/jobs"
	"forgequeue/internal/telemetry"
	"forgequeue/internal/tenant"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type testAuthenticator struct{ operator bool }

func (a testAuthenticator) Authenticate(_ context.Context, key string) (tenant.Principal, error) {
	if key != "test-key" {
		return tenant.Principal{}, tenant.ErrUnauthorized
	}
	return tenant.Principal{ID: tenant.LegacyID, Operator: a.operator, MaxPayloadBytes: 1048576}, nil
}

// Existing handler tests explicitly use a trusted fixture credential. Production
// NewServer has no authentication bypass; security tests below use it directly.
type authenticatedTestServer struct{ *Server }

func newTestServer(service *jobs.Service, ready func(*http.Request) error, logger *slog.Logger) *authenticatedTestServer {
	return &authenticatedTestServer{NewServer(service, ready, logger).WithAuthenticator(testAuthenticator{true})}
}
func (s *authenticatedTestServer) WithWorkerProvider(p WorkerProvider) *authenticatedTestServer {
	s.Server.WithWorkerProvider(p)
	return s
}
func (s *authenticatedTestServer) WithRequestTimeout(d time.Duration) *authenticatedTestServer {
	s.Server.WithRequestTimeout(d)
	return s
}
func (s *authenticatedTestServer) Routes() http.Handler {
	next := s.Server.Routes()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("Authorization", "Bearer test-key")
		next.ServeHTTP(w, r)
	})
}

func TestProtectedRoutesRequireCredentials(t *testing.T) {
	for _, path := range []string{"/v1/jobs", "/v1/jobs/id", "/v1/jobs/id/cancel", "/v1/jobs/id/retry", "/v1/workers", "/metrics"} {
		method := "GET"
		if path == "/v1/jobs/id/cancel" || path == "/v1/jobs/id/retry" {
			method = "POST"
		}
		for _, key := range []string{"", "Bearer invalid"} {
			s := NewServer(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).WithMetrics(telemetry.NewMetrics()).WithAuthenticator(testAuthenticator{})
			req := httptest.NewRequest(method, path, nil)
			if key != "" {
				req.Header.Set("Authorization", key)
			}
			w := httptest.NewRecorder()
			s.Routes().ServeHTTP(w, req)
			if w.Code != 401 {
				t.Fatalf("%s: %d", path, w.Code)
			}
		}
	}
	s := NewServer(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).WithAuthenticator(testAuthenticator{})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v1/workers", nil)
	r.Header.Set("Authorization", "Bearer test-key")
	s.Routes().ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("tenant fleet view=%d", w.Code)
	}
	w = httptest.NewRecorder()
	NewServer(nil, nil, slog.Default()).Routes().ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("unconfigured server must fail closed")
	}
}
