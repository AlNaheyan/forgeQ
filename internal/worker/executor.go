package worker

import (
	"context"
	"encoding/json"
)

type Handler interface {
	Execute(context.Context, json.RawMessage) (any, error)
}

type attemptContextKey struct{}

func withAttempt(ctx context.Context, attempt int) context.Context {
	return context.WithValue(ctx, attemptContextKey{}, attempt)
}

func attemptFromContext(ctx context.Context) int {
	attempt, _ := ctx.Value(attemptContextKey{}).(int)
	return attempt
}
