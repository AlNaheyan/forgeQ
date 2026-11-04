package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"forgequeue/internal/jobs"
)

type recordingBroker struct {
	failure   string
	retryable bool
	completed bool
}

func (*recordingBroker) ClaimJob(context.Context, string, []string) (jobs.Job, error) {
	return jobs.Job{}, errors.New("unused")
}
func (*recordingBroker) RenewLease(context.Context, string, string, int64) (time.Time, error) {
	return time.Now().Add(time.Minute), nil
}
func (b *recordingBroker) CompleteJob(context.Context, string, string, []byte, int64) error {
	b.completed = true
	return nil
}
func (b *recordingBroker) FailJob(_ context.Context, _, _, message string, retry bool, _ int64) error {
	b.failure, b.retryable = message, retry
	return nil
}

type handlerFunc func(context.Context, json.RawMessage) (any, error)

func (f handlerFunc) Execute(ctx context.Context, data json.RawMessage) (any, error) {
	return f(ctx, data)
}

func TestExecutionClassifiesAndRedactsFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		handler  Handler
		expected string
		retry    bool
	}{
		{"permanent", handlerFunc(func(context.Context, json.RawMessage) (any, error) {
			return nil, &HandlerError{Code: FailurePermanent, Cause: errors.New("private payload")}
		}), "permanent", false},
		{"transient", handlerFunc(func(context.Context, json.RawMessage) (any, error) { return nil, errors.New("secret downstream error") }), "unclassified", true},
		{"panic", panicHandler{}, "panic", false},
		{"result limit", handlerFunc(func(context.Context, json.RawMessage) (any, error) { return strings.Repeat("x", 100), nil }), "result_invalid", false},
		{"deadline", handlerFunc(func(ctx context.Context, _ json.RawMessage) (any, error) { <-ctx.Done(); return nil, ctx.Err() }), "timeout", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &recordingBroker{}
			w, err := NewWithBroker(Config{ID: "w", Concurrency: 1, PollInterval: time.Millisecond, JobTimeout: 10 * time.Millisecond, MaxResultBytes: 20, JobTypes: []string{"custom"}}, b, map[string]Handler{"custom": tc.handler}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			w.execute(context.Background(), jobs.Job{ID: "job", Type: "custom", Payload: json.RawMessage(`{}`)}, tc.handler)
			if b.failure != tc.expected || b.retryable != tc.retry || b.completed {
				t.Fatalf("failure=%q retry=%v completed=%v", b.failure, b.retryable, b.completed)
			}
		})
	}
}
