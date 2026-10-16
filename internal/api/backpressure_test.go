package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"forgequeue/internal/jobs"
)

type burstRepository struct {
	started chan struct{}
	release <-chan struct{}
	created atomic.Int64
}

func (r *burstRepository) CreateJob(ctx context.Context, job jobs.Job) (jobs.Job, bool, error) {
	r.created.Add(1)
	r.started <- struct{}{}
	select {
	case <-r.release:
		return job, true, nil
	case <-ctx.Done():
		return jobs.Job{}, false, ctx.Err()
	}
}

func (*burstRepository) GetJob(context.Context, string) (jobs.Job, error) {
	return jobs.Job{}, jobs.ErrNotFound
}
func (*burstRepository) ListJobs(context.Context, jobs.ListFilter) ([]jobs.Job, string, error) {
	return nil, "", nil
}
func (*burstRepository) UpdateStatus(context.Context, string, jobs.Status) (jobs.Job, error) {
	return jobs.Job{}, jobs.ErrNotFound
}
func (*burstRepository) CancelJob(context.Context, string) (jobs.Job, error) {
	return jobs.Job{}, jobs.ErrNotFound
}
func (*burstRepository) RetryDeadLetter(context.Context, string) (jobs.Job, error) {
	return jobs.Job{}, jobs.ErrNotFound
}
func (*burstRepository) ListQueuedJobs(context.Context, int) ([]jobs.Job, error) {
	return nil, nil
}

type burstQueue struct{ enqueued atomic.Int64 }

func (*burstQueue) Depth(context.Context) (int64, error) { return 0, nil }
func (q *burstQueue) Enqueue(context.Context, string, int, time.Time) error {
	q.enqueued.Add(1)
	return nil
}

func TestBurstBackpressureReturns429AndKeepsHealthResponsive(t *testing.T) {
	const burst = 32
	release := make(chan struct{})
	repository := &burstRepository{
		started: make(chan struct{}, 1),
		release: release,
	}
	queue := &burstQueue{}
	service := jobs.NewService(repository, queue, 3).WithBackpressure(queue, 1, nil)
	handler := NewServer(service, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes()

	acceptedResponse := httptest.NewRecorder()
	acceptedDone := make(chan struct{})
	go func() {
		defer close(acceptedDone)
		handler.ServeHTTP(acceptedResponse, newCreateRequest())
	}()
	select {
	case <-repository.started:
	case <-time.After(time.Second):
		t.Fatal("first submission did not reach persistence")
	}

	responses := make(chan *httptest.ResponseRecorder, burst-1)
	var overload sync.WaitGroup
	overload.Add(burst - 1)
	for range burst - 1 {
		go func() {
			defer overload.Done()
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, newCreateRequest())
			responses <- response
		}()
	}
	overload.Wait()
	close(responses)

	for response := range responses {
		if response.Code != http.StatusTooManyRequests {
			t.Fatalf("overload response = %d, want 429: %s", response.Code, response.Body.String())
		}
		if retryAfter := response.Header().Get("Retry-After"); retryAfter != "5" {
			t.Fatalf("Retry-After = %q, want 5", retryAfter)
		}
		if !strings.Contains(response.Body.String(), `"code":"queue_full"`) {
			t.Fatalf("overload response body = %s", response.Body.String())
		}
	}

	// The accepted write is deliberately stalled, but unrelated routes must
	// remain responsive while the API sheds the rest of the burst.
	healthDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		healthDone <- response
	}()
	select {
	case response := <-healthDone:
		if response.Code != http.StatusOK {
			t.Fatalf("health response = %d, want 200", response.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("health endpoint became unresponsive during submission burst")
	}

	close(release)
	select {
	case <-acceptedDone:
	case <-time.After(time.Second):
		t.Fatal("accepted submission did not finish")
	}
	if acceptedResponse.Code != http.StatusCreated {
		t.Fatalf("accepted response = %d, want 201: %s", acceptedResponse.Code, acceptedResponse.Body.String())
	}
	if repository.created.Load() != 1 || queue.enqueued.Load() != 1 {
		t.Fatalf("burst crossed limit: persisted=%d enqueued=%d", repository.created.Load(), queue.enqueued.Load())
	}
}

func newCreateRequest() *http.Request {
	return httptest.NewRequest(
		http.MethodPost,
		"/v1/jobs",
		strings.NewReader(`{"type":"sleep","payload":{"duration_ms":10}}`),
	)
}
