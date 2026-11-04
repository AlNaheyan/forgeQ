package security

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsCredentialBoundary(t *testing.T) {
	token := strings.Repeat("m", 32)
	handler := MetricsHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }), token)
	for _, tc := range []struct {
		path, auth string
		code       int
	}{{"/metrics", "", 401}, {"/metrics", "Bearer fq_tenant", 401}, {"/metrics", "Bearer " + token, 204}, {"/healthz", "", 200}} {
		req := httptest.NewRequest("GET", tc.path, nil)
		req.Header.Set("Authorization", tc.auth)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != tc.code {
			t.Fatalf("%s: %d", tc.path, rec.Code)
		}
	}
}
