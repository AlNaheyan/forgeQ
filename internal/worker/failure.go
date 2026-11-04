package worker

import (
	"context"
	"errors"
)

type FailureCode string

const (
	FailureInvalidPayload        FailureCode = "invalid_payload"
	FailureUnsupportedType       FailureCode = "unsupported_type"
	FailurePermanent             FailureCode = "permanent"
	FailureResultInvalid         FailureCode = "result_invalid"
	FailurePanic                 FailureCode = "panic"
	FailureDownstreamUnavailable FailureCode = "downstream_unavailable"
	FailureRateLimited           FailureCode = "rate_limited"
	FailureTimeout               FailureCode = "timeout"
	FailureUnclassified          FailureCode = "unclassified"
)

// HandlerError carries an explicit classification; Cause is never persisted.
type HandlerError struct {
	Code  FailureCode
	Cause error
}

func (e *HandlerError) Error() string { return string(e.Code) }
func (e *HandlerError) Unwrap() error { return e.Cause }

func classifyFailure(err error) (FailureCode, bool) {
	if errors.Is(err, context.DeadlineExceeded) {
		return FailureTimeout, true
	}
	var classified *HandlerError
	if errors.As(err, &classified) {
		switch classified.Code {
		case FailureInvalidPayload, FailureUnsupportedType, FailurePermanent, FailureResultInvalid, FailurePanic:
			return classified.Code, false
		case FailureDownstreamUnavailable, FailureRateLimited, FailureTimeout:
			return classified.Code, true
		}
	}
	return FailureUnclassified, true
}
