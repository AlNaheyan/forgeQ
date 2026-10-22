package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"forgequeue/internal/jobs"
	"forgequeue/internal/tenant"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type identityHandler struct{ t *testing.T }

func (h identityHandler) Execute(ctx context.Context, _ json.RawMessage) (any, error) {
	if tenant.ID(ctx) != "tenant-test" {
		h.t.Error("handler lost authenticated tenant identity")
	}
	return nil, errors.New("secret-handler-payload")
}
func TestWorkerTenantContextAndLogRedaction(t *testing.T) {
	var logs bytes.Buffer
	job := jobs.Job{ID: "job", TenantID: "tenant-test", Type: "sleep", Status: jobs.StatusRunning, AttemptCount: 1}
	repo := &memoryRepository{jobs: map[string]jobs.Job{"job": job}, completed: make(chan string, 1)}
	w := &Worker{config: Config{ID: "worker", JobTimeout: time.Second}, broker: NewLocalBroker(repo, &finiteQueue{}), logger: slog.New(slog.NewTextHandler(&logs, nil))}
	w.execute(context.Background(), job, identityHandler{t})
	if strings.Contains(logs.String(), "secret-handler-payload") {
		t.Fatal("shared worker log contains handler error")
	}
	if repo.jobs["job"].LastError != "secret-handler-payload" {
		t.Fatal("tenant-owned durable error missing")
	}
}
