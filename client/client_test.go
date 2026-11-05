package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	c, err := New(Options{BaseURL: server.URL, APIKey: "secret-key", AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	c.retryBase = time.Millisecond
	return c
}

func TestSubmitRetriesOnlyWithStableIdempotencyKey(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer secret-key" || r.Header.Get("Idempotency-Key") != "stable-key" {
			t.Error("credentials or idempotency key changed")
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload["idempotency_key"] != "stable-key" {
			t.Errorf("payload=%v err=%v", payload, err)
		}
		if n == 1 {
			w.WriteHeader(503)
			fmt.Fprint(w, `{"error":{"code":"not_ready"}}`)
			return
		}
		w.WriteHeader(201)
		fmt.Fprint(w, `{"job_id":"job-1","type":"invoice.send","status":"queued"}`)
	}))
	defer server.Close()
	c := testClient(t, server)
	job, err := c.Submit(context.Background(), SubmitRequest{Type: "invoice.send", Payload: map[string]string{"invoice_id": "i1"}, IdempotencyKey: "stable-key"})
	if err != nil || job.ID != "job-1" || calls.Load() != 2 {
		t.Fatalf("job=%+v calls=%d err=%v", job, calls.Load(), err)
	}
}

func TestUnsafeMutationsAndUnauthorizedAreNotRetried(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
		fmt.Fprint(w, `{"error":{"code":"not_ready","message":"secret-key"}}`)
	}))
	defer server.Close()
	c := testClient(t, server)
	_, err := c.Submit(context.Background(), SubmitRequest{Type: "x", Payload: map[string]int{"a": 1}})
	if err == nil || calls.Load() != 1 || strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
	_, err = c.Cancel(context.Background(), "job-1")
	if err == nil || calls.Load() != 2 {
		t.Fatalf("cancel retried: %d %v", calls.Load(), err)
	}
	unauthorized := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(401)
		fmt.Fprint(w, `{"error":{"code":"unauthorized"}}`)
	}))
	defer unauthorized.Close()
	_, err = testClient(t, unauthorized).Get(context.Background(), "job-1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "unauthorized" || calls.Load() != 3 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
}

func TestRedirectNeverForwardsCredentials(t *testing.T) {
	var leaked atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true) }))
	defer other.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL, http.StatusFound) }))
	defer server.Close()
	_, err := testClient(t, server).Get(context.Background(), "job-1")
	if err == nil || leaked.Load() {
		t.Fatalf("redirect was followed: %v", err)
	}
}

func TestResponseLimitAndRetryAfterDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/jobs/large" {
			fmt.Fprint(w, strings.Repeat("x", 100))
			return
		}
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(429)
		fmt.Fprint(w, `{"error":{"code":"queue_full"}}`)
	}))
	defer server.Close()
	c := testClient(t, server)
	c.maxResponse = 32
	if _, err := c.Get(context.Background(), "large"); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("limit error=%v", err)
	}
	c.maxResponse = defaultMaxResponse
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := c.Get(ctx, "job-1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retry-after exceeded deadline: %v", err)
	}
}

func TestLongRetryAfterReturnsErrorWithoutIndefiniteWait(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"error":{"code":"not_ready"}}`)
	}))
	defer server.Close()
	start := time.Now()
	_, err := testClient(t, server).Get(context.Background(), "job-1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable || calls.Load() != 1 || time.Since(start) > time.Second {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
}

func TestListEncodesFiltersAndNoImplicitPagination(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("type") != "invoice.send" || r.URL.Query().Get("cursor") != "next+page" {
			t.Error("query lost filters")
		}
		fmt.Fprint(w, `{"jobs":[],"next_cursor":"opaque"}`)
	}))
	defer server.Close()
	page, err := testClient(t, server).List(context.Background(), ListOptions{Type: "invoice.send", Cursor: "next+page", Limit: 10})
	if err != nil || page.NextCursor != "opaque" {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}
