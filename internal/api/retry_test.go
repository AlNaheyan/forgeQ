package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"forgequeue/internal/jobs"
)

type retryRepository struct {
	job jobs.Job
	err error
}

func (*retryRepository) CreateJob(context.Context, jobs.Job) (jobs.Job, bool, error) {
	return jobs.Job{}, false, jobs.ErrNotFound
}

func (*retryRepository) GetJob(context.Context, string) (jobs.Job, error) {
	return jobs.Job{}, jobs.ErrNotFound
}

func (*retryRepository) ListJobs(context.Context, jobs.ListFilter) ([]jobs.Job, string, error) {
	return nil, "", nil
}

func (*retryRepository) UpdateStatus(context.Context, string, jobs.Status) (jobs.Job, error) {
	return jobs.Job{}, jobs.ErrNotFound
}

func (*retryRepository) CancelJob(context.Context, string) (jobs.Job, error) {
	return jobs.Job{}, jobs.ErrNotFound
}

func (r *retryRepository) RetryDeadLetter(context.Context, string) (jobs.Job, error) {
	return r.job, r.err
}

func (*retryRepository) ListQueuedJobs(context.Context, int) ([]jobs.Job, error) {
	return nil, nil
}

type retryQueue struct {
	err error
}

func (q *retryQueue) Enqueue(context.Context, string, int, time.Time) error {
	return q.err
}

func TestRetryJobReturnsPersistedQueuedJob(t *testing.T) {
	repository := &retryRepository{job: jobs.Job{ID: "dead-job", Status: jobs.StatusQueued}}
	service := jobs.NewService(repository, &retryQueue{err: errors.New("Redis unavailable")}, 3)
	server := newTestServer(service, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes()
	request := httptest.NewRequest(http.MethodPost, "/v1/jobs/dead-job/retry", nil)
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("retry status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
}

func TestRetryJobRejectsNonDeadLetterState(t *testing.T) {
	repository := &retryRepository{err: jobs.ErrInvalidTransition}
	service := jobs.NewService(repository, &retryQueue{}, 3)
	server := newTestServer(service, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes()
	request := httptest.NewRequest(http.MethodPost, "/v1/jobs/running-job/retry", nil)
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("retry status = %d, want %d: %s", response.Code, http.StatusConflict, response.Body.String())
	}
}
