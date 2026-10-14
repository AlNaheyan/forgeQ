package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardRoutes(t *testing.T) {
	server := NewServer(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes()

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
