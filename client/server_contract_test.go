package client_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/client"
	"github.com/AlNaheyan/forgeQ/internal/api"
	"github.com/AlNaheyan/forgeQ/internal/jobs"
	"github.com/AlNaheyan/forgeQ/internal/jobtypes"
	"github.com/AlNaheyan/forgeQ/internal/tenant"
)

type contractAuth struct{}

func (contractAuth) Authenticate(_ context.Context, key string) (tenant.Principal, error) {
	if key != "tenant-a" && key != "tenant-b" {
		return tenant.Principal{}, tenant.ErrUnauthorized
	}
	return tenant.Principal{ID: key, MaxPayloadBytes: 1 << 20}, nil
}

type contractRepo struct {
	mu    sync.Mutex
	items map[string]jobs.Job
}

func (r *contractRepo) CreateJob(ctx context.Context, j jobs.Job) (jobs.Job, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j.TenantID = tenant.ID(ctx)
	r.items[j.ID] = j
	return j, true, nil
}
func (r *contractRepo) GetJob(ctx context.Context, id string) (jobs.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.items[id]
	if !ok || j.TenantID != tenant.ID(ctx) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return j, nil
}
func (r *contractRepo) ListJobs(ctx context.Context, _ jobs.ListFilter) ([]jobs.Job, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []jobs.Job
	for _, j := range r.items {
		if j.TenantID == tenant.ID(ctx) {
			out = append(out, j)
		}
	}
	return out, "", nil
}
func (*contractRepo) UpdateStatus(context.Context, string, jobs.Status) (jobs.Job, error) {
	return jobs.Job{}, jobs.ErrNotFound
}
func (*contractRepo) CancelJob(context.Context, string) (jobs.Job, error) {
	return jobs.Job{}, jobs.ErrNotFound
}
func (*contractRepo) RetryDeadLetter(context.Context, string) (jobs.Job, error) {
	return jobs.Job{}, jobs.ErrNotFound
}
func (*contractRepo) ListQueuedJobs(context.Context, int) ([]jobs.Job, error) { return nil, nil }

type contractQueue struct{}

func (contractQueue) Enqueue(context.Context, string, int, time.Time) error { return nil }

func TestClientAgainstAPIAndTenantIsolation(t *testing.T) {
	registry, err := jobtypes.ApplicationRegistry([]string{"invoice.send"})
	if err != nil {
		t.Fatal(err)
	}
	service := jobs.NewService(&contractRepo{items: make(map[string]jobs.Job)}, contractQueue{}, 3).WithJobTypes(registry)
	server := httptest.NewServer(api.NewServer(service, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).WithAuthenticator(contractAuth{}).Routes())
	defer server.Close()
	newClient := func(key string) *client.Client {
		c, err := client.New(client.Options{BaseURL: server.URL, APIKey: key, AllowInsecureHTTP: true})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a, b := newClient("tenant-a"), newClient("tenant-b")
	created, err := a.Submit(context.Background(), client.SubmitRequest{Type: "invoice.send", Payload: map[string]string{"invoice_id": "inv-1"}, IdempotencyKey: "key-1"})
	if err != nil || created.ID == "" {
		t.Fatalf("submit=%+v err=%v", created, err)
	}
	got, err := a.Get(context.Background(), created.ID)
	if err != nil || got.ID != created.ID || got.Type != "invoice.send" {
		t.Fatalf("get=%+v err=%v", got, err)
	}
	_, err = b.Get(context.Background(), created.ID)
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 404 {
		t.Fatalf("cross-tenant read=%v", err)
	}
	page, err := a.List(context.Background(), client.ListOptions{Type: "invoice.send"})
	if err != nil || len(page.Jobs) != 1 {
		t.Fatalf("list=%+v err=%v", page, err)
	}
	types, err := a.JobTypes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, kind := range types {
		if kind.Name == "invoice.send" {
			found = true
		}
	}
	if !found {
		t.Fatalf("custom type missing: %+v", types)
	}
	_, err = newClient("invalid").Get(context.Background(), created.ID)
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 401 {
		t.Fatalf("unauthorized read=%v", err)
	}
}
