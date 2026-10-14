package queue

import (
	"context"
	"errors"
	"time"
)

var ErrEmpty = errors.New("queue is empty")

type Queue interface {
	Enqueue(context.Context, string, int, time.Time) error
	Dequeue(context.Context) (string, error)
	Remove(context.Context, string) error
	Depth(context.Context) (int64, error)
	Contains(context.Context, string) (bool, error)
	ScheduleRetry(context.Context, string, time.Time) error
	PopDueRetries(context.Context, time.Time, int64) ([]string, error)
}
