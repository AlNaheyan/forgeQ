package jobs

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound          = errors.New("job not found")
	ErrInvalidTransition = errors.New("invalid job state transition")
	ErrInvalidJob        = errors.New("invalid job")
	ErrClaimConflict     = errors.New("job cannot be claimed")
	ErrQueueFull         = errors.New("job queue is full")
)

// QueueFullError reports the ready-queue capacity observed when a submission
// was rejected. It unwraps to ErrQueueFull so callers do not need to depend on
// the diagnostic fields.
type QueueFullError struct {
	Depth int64
	Limit int64
}

func (e QueueFullError) Error() string {
	return fmt.Sprintf("%s: depth %d reached limit %d", ErrQueueFull, e.Depth, e.Limit)
}

func (e QueueFullError) Unwrap() error { return ErrQueueFull }
