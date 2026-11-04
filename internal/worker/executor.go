package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
)

type Handler interface {
	Execute(context.Context, json.RawMessage) (any, error)
}

type attemptContextKey struct{}
type metadataContextKey struct{}

// ExecutionMetadata identifies the durable job and the current fenced attempt.
// It is passed by value so handlers cannot mutate the worker's context.
type ExecutionMetadata struct {
	TenantID, JobID, JobType, SubmissionKey string
	Attempt                                 int
	LeaseGeneration                         int64
}

// MetadataFromContext returns the durable identity of the executing job.
func MetadataFromContext(ctx context.Context) (ExecutionMetadata, bool) {
	m, ok := ctx.Value(metadataContextKey{}).(ExecutionMetadata)
	return m, ok
}

// OperationKey is stable across attempts and leases for one named effect.
func (m ExecutionMetadata) OperationKey(effect string) (string, error) {
	if m.TenantID == "" || m.JobID == "" || effect == "" {
		return "", errors.New("tenant, job and effect are required")
	}
	return "tenant/" + url.PathEscape(m.TenantID) + "/job/" + url.PathEscape(m.JobID) + "/effect/" + url.PathEscape(effect), nil
}

func withMetadata(ctx context.Context, m ExecutionMetadata) context.Context {
	return context.WithValue(ctx, metadataContextKey{}, m)
}

func withAttempt(ctx context.Context, attempt int) context.Context {
	return context.WithValue(ctx, attemptContextKey{}, attempt)
}

func attemptFromContext(ctx context.Context) int {
	attempt, _ := ctx.Value(attemptContextKey{}).(int)
	return attempt
}
