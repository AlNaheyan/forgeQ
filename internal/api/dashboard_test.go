package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
)

func TestDashboardRoutes(t *testing.T) {
	server := newTestServer(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes()

	redirectRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	redirectResponse := httptest.NewRecorder()
	server.ServeHTTP(redirectResponse, redirectRequest)
	if redirectResponse.Code != http.StatusTemporaryRedirect || redirectResponse.Header().Get("Location") != "/dashboard" {
		t.Fatalf("root response = %d location=%q", redirectResponse.Code, redirectResponse.Header().Get("Location"))
	}

	dashboardRequest := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	dashboardResponse := httptest.NewRecorder()
	server.ServeHTTP(dashboardResponse, dashboardRequest)
	if dashboardResponse.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d", dashboardResponse.Code)
	}
	if contentType := dashboardResponse.Header().Get("Content-Type"); contentType != "text/html; charset=utf-8" {
		t.Fatalf("dashboard content type = %q", contentType)
	}
	body := dashboardResponse.Body.String()
	for _, marker := range []string{"ForgeQueue", "/v1/jobs", "/v1/workers", "Launch job"} {
		if !strings.Contains(body, marker) {
			t.Fatalf("dashboard body does not contain %q", marker)
		}
	}
}

func TestAPICancelsTimedOutRequestContext(t *testing.T) {
	repository := &burstRepository{started: make(chan struct{}, 1), release: make(chan struct{})}
	service := jobs.NewService(repository, &burstQueue{}, 3)
	server := newTestServer(service, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).
		WithRequestTimeout(10 * time.Millisecond).
		Routes()
	response := httptest.NewRecorder()
	server.ServeHTTP(response, newCreateRequest())
	if response.Code != http.StatusGatewayTimeout {
		t.Fatalf("timed-out request status = %d, want 504: %s", response.Code, response.Body.String())
	}
}

func TestCreateJobRejectsOversizedRequestBody(t *testing.T) {
	server := newTestServer(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes()
	request := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(`{"type":"sleep","payload":"`+strings.Repeat("x", maxRequestBody)+`"}`))
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized request status = %d, want 413: %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"code":"payload_too_large"`) {
		t.Fatalf("oversized request error = %s", response.Body.String())
	}
}
