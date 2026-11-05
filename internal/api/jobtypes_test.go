package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/jobtypes"
)

func TestJobTypesListsConfiguredApplicationTypes(t *testing.T) {
	registry, err := jobtypes.ApplicationRegistry([]string{"invoice.send"})
	if err != nil {
		t.Fatal(err)
	}
	service := jobs.NewService(nil, nil, 3).WithJobTypes(registry)
	server := newTestServer(service, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes()
	w := httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/job-types", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"name":"invoice.send"`) || !strings.Contains(w.Body.String(), `"max_payload_bytes":1048576`) {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}
