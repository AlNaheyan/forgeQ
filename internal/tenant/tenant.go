// Package tenant carries authenticated identity across HTTP, storage, and handlers.
package tenant

import (
	"context"
	"errors"
)

const LegacyID = "00000000-0000-4000-8000-000000000001"

var ErrUnauthorized = errors.New("invalid or revoked credential")
var ErrQuota = errors.New("tenant admission quota exceeded")
var ErrPayload = errors.New("tenant payload limit exceeded")
var ErrConcurrency = errors.New("tenant concurrency limit reached")

type Principal struct {
	ID              string
	Operator        bool
	MaxPayloadBytes int
}
type contextKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, p)
}
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(contextKey{}).(Principal)
	return p, ok && p.ID != ""
}
func ID(ctx context.Context) string {
	if p, ok := FromContext(ctx); ok {
		return p.ID
	}
	return LegacyID
}

// Scope is nil only for trusted internal repository operations (coordinator,
// recovery, migration tooling). Public HTTP always supplies a principal.
func Scope(ctx context.Context) any {
	if p, ok := FromContext(ctx); ok {
		return p.ID
	}
	return nil
}
