package recovery_test

import (
	"context"
	"encoding/json"
	"errors"
	"forgequeue/internal/api"
	"forgequeue/internal/idempotency"
	"forgequeue/internal/jobs"
	"forgequeue/internal/tenant"
	"forgequeue/migrations"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTenantSecurityAndAtomicQuotas(t *testing.T) {
	f := setup(t)
	store := tenant.NewStore(f.db)
	newTenant := func(l tenant.Limits) (context.Context, tenant.Credential) {
		t.Helper()
		id, err := store.Create(f.ctx, "security-test", l)
		if err != nil {
			t.Fatal(err)
		}
		key, err := store.Issue(f.ctx, id, false)
		if err != nil {
			t.Fatal(err)
		}
		return tenant.WithPrincipal(f.ctx, tenant.Principal{ID: id}), key
	}
	a, keyA := newTenant(tenant.DefaultLimits())
	b, keyB := newTenant(tenant.DefaultLimits())
	server := api.NewServer(jobs.NewService(f.repo, f.q, 3), nil, f.logger).WithAuthenticator(store).Routes()
	request := func(method, path, key, body string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if key != "" {
			r.Header.Set("Authorization", "Bearer "+key)
		}
		w := httptest.NewRecorder()
		server.ServeHTTP(w, r)
		return w
	}
	submit := func(ctx context.Context, key string) (jobs.Job, error) {
		j, err := jobs.New(jobs.CreateInput{Type: "sleep", Payload: json.RawMessage(`{"duration_ms":1}`), MaxAttempts: 3, IdempotencyKey: key}, time.Now())
		if err != nil {
			return jobs.Job{}, err
		}
		j, _, err = f.repo.CreateJob(ctx, j)
		return j, err
	}
	ja, err := submit(a, "shared-key")
	if err != nil {
		t.Fatal(err)
	}
	jb, err := submit(b, "shared-key")
	if err != nil {
		t.Fatal(err)
	}
	if ja.ID == jb.ID {
		t.Fatal("cross-tenant submission deduplication")
	}
	for _, action := range []struct{ method, path string }{{"GET", "/v1/jobs/" + ja.ID}, {"POST", "/v1/jobs/" + ja.ID + "/cancel"}, {"POST", "/v1/jobs/" + ja.ID + "/retry"}} {
		if w := request(action.method, action.path, keyB.Token, ""); w.Code != 404 {
			t.Fatalf("cross-tenant %s: %d %s", action.path, w.Code, w.Body)
		}
		if w := request(action.method, action.path, "", ""); w.Code != 401 {
			t.Fatalf("unauthenticated %s: %d", action.path, w.Code)
		}
	}
	if w := request("GET", "/v1/jobs", keyB.Token, ""); w.Code != 200 || strings.Contains(w.Body.String(), ja.ID) || !strings.Contains(w.Body.String(), jb.ID) {
		t.Fatalf("tenant listing: %d %s", w.Code, w.Body)
	}
	if w := request("GET", "/v1/workers", keyA.Token, ""); w.Code != 403 {
		t.Fatalf("fleet permission: %d", w.Code)
	}
	if _, err := store.Authenticate(f.ctx, keyA.Token); err != nil {
		t.Fatal(err)
	}
	var verifier []byte
	if err := f.db.QueryRowContext(f.ctx, "SELECT verifier FROM api_keys WHERE id=$1", keyA.ID).Scan(&verifier); err != nil || len(verifier) != 32 || strings.Contains(string(verifier), keyA.Token) {
		t.Fatal("credential not stored as verifier")
	}
	rotated, err := store.Rotate(f.ctx, keyA.ID)
	if err != nil {
		t.Fatal(err)
	}
	if w := request("GET", "/v1/jobs", keyA.Token, ""); w.Code != 401 {
		t.Fatal("rotated key accepted")
	}
	if w := request("GET", "/v1/jobs", rotated.Token, ""); w.Code != 200 {
		t.Fatal("replacement key rejected")
	}
	if err := store.Revoke(f.ctx, rotated.ID); err != nil {
		t.Fatal(err)
	}
	if w := request("GET", "/v1/jobs", rotated.Token, ""); w.Code != 401 {
		t.Fatal("revoked key accepted")
	}
	executor := idempotency.NewExecutor(f.db)
	for _, ctx := range []context.Context{a, b} {
		ran, err := executor.ExecuteOnce(ctx, "same-execution-key", func(context.Context) error { return nil })
		if err != nil || !ran {
			t.Fatalf("tenant execution marker: %v %v", ran, err)
		}
		ran, err = executor.ExecuteOnce(ctx, "same-execution-key", func(context.Context) error { t.Error("duplicate callback"); return nil })
		if err != nil || ran {
			t.Fatal("duplicate executed")
		}
	}

	for _, kind := range []string{"pending", "rate"} {
		t.Run(kind, func(t *testing.T) {
			limits := tenant.Limits{MaxPending: 100, MaxConcurrent: 2, PerMinute: 100, MaxPayloadBytes: 64}
			if kind == "pending" {
				limits.MaxPending = 3
			} else {
				limits.PerMinute = 3
			}
			ctx, key := newTenant(limits)
			var admitted atomic.Int32
			var wg sync.WaitGroup
			for i := 0; i < 12; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := submit(ctx, "")
					if err == nil {
						admitted.Add(1)
					} else if !errors.Is(err, tenant.ErrQuota) {
						t.Errorf("submit: %v", err)
					}
				}()
			}
			wg.Wait()
			if admitted.Load() != 3 {
				t.Fatalf("admitted %d, want 3", admitted.Load())
			}
			if w := request("POST", "/v1/jobs", key.Token, `{"type":"sleep","payload":{"duration_ms":1}}`); w.Code != 429 {
				t.Fatalf("quota HTTP=%d %s", w.Code, w.Body)
			}
		})
	}
	limited, key := newTenant(tenant.Limits{MaxPending: 1, MaxConcurrent: 1, PerMinute: 1, MaxPayloadBytes: 64})
	one, err := submit(limited, "replay")
	if err != nil {
		t.Fatal(err)
	}
	again, err := submit(limited, "replay")
	if err != nil || again.ID != one.ID {
		t.Fatalf("replay at quota: %v", err)
	}
	if w := request("POST", "/v1/jobs", key.Token, `{"type":"sleep","payload":{"padding":"`+strings.Repeat("x", 70)+`"}}`); w.Code != 413 {
		t.Fatalf("payload HTTP=%d %s", w.Code, w.Body)
	}
	if _, err := f.db.ExecContext(f.ctx, `UPDATE jobs SET status='dead_letter' WHERE id=$1`, one.ID); err != nil {
		t.Fatal(err)
	}
	if w := request("POST", "/v1/jobs/"+one.ID+"/retry", key.Token, ""); w.Code != 429 {
		t.Fatalf("retry bypassed rate quota: %d", w.Code)
	}

	concurrent, _ := newTenant(tenant.Limits{MaxPending: 20, MaxConcurrent: 2, PerMinute: 20, MaxPayloadBytes: 64})
	var ids []string
	for i := 0; i < 8; i++ {
		j, err := submit(concurrent, "")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	var leased atomic.Int32
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, err := f.repo.LeaseJob(f.ctx, id, "tenant-worker", time.Minute)
			if err == nil {
				leased.Add(1)
			} else if !errors.Is(err, tenant.ErrConcurrency) {
				t.Errorf("lease: %v", err)
			}
		}(id)
	}
	wg.Wait()
	if leased.Load() != 2 {
		t.Fatalf("active leases=%d, want 2", leased.Load())
	}
	// Events may contain status metadata, never handler error/payload content.
	if err := migrations.ConfigureEventStream(f.ctx, f.db, true); err != nil {
		t.Fatal(err)
	}
	defer migrations.ConfigureEventStream(f.ctx, f.db, false)
	if _, err := f.db.ExecContext(f.ctx, `UPDATE jobs SET status='dead_letter',last_error='sensitive-marker' WHERE id=$1`, jb.ID); err != nil {
		t.Fatal(err)
	}
	var leaked int
	if err := f.db.QueryRowContext(f.ctx, `SELECT count(*) FROM event_outbox WHERE payload::text LIKE '%sensitive-marker%'`).Scan(&leaked); err != nil || leaked != 0 {
		t.Fatalf("event leaked handler data: %d %v", leaked, err)
	}
}
