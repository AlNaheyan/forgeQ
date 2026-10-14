package queue

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestReadyScoreOrdersPriorityThenAge(t *testing.T) {
	now := time.Now()
	highPriority := readyScore(10, now.Add(time.Hour))
	lowPriority := readyScore(1, now)
	if highPriority >= lowPriority {
		t.Fatalf("higher priority score %f must sort before lower priority score %f", highPriority, lowPriority)
	}
	if readyScore(1, now) >= readyScore(1, now.Add(time.Second)) {
		t.Fatal("older job must sort before newer job at the same priority")
	}
}

func TestRedisQueueLifecycle(t *testing.T) {
	address := os.Getenv("FORGEQUEUE_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("FORGEQUEUE_TEST_REDIS_ADDR is not set")
	}

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address})
	defer client.Close()
	prefix := "forgequeue:test:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	queue := NewRedisQueue(client, prefix)
	defer client.Del(ctx, prefix+":ready", prefix+":delayed")

	now := time.Now()
	if err := queue.Enqueue(ctx, "low", 1, now); err != nil {
		t.Fatalf("enqueue low: %v", err)
	}
	if err := queue.Enqueue(ctx, "high", 10, now.Add(time.Minute)); err != nil {
		t.Fatalf("enqueue high: %v", err)
	}
	if err := queue.Enqueue(ctx, "high", 10, now.Add(time.Minute)); err != nil {
		t.Fatalf("idempotent enqueue: %v", err)
	}
	depth, err := queue.Depth(ctx)
	if err != nil || depth != 2 {
		t.Fatalf("depth = %d, err=%v", depth, err)
	}

	jobID, err := queue.Dequeue(ctx)
	if err != nil || jobID != "high" {
		t.Fatalf("first dequeue = %q, err=%v", jobID, err)
	}
	present, err := queue.Contains(ctx, "low")
	if err != nil || !present {
		t.Fatalf("contains low = %v, err=%v", present, err)
	}
	if err := queue.Remove(ctx, "low"); err != nil {
		t.Fatalf("remove low: %v", err)
	}
	if _, err := queue.Dequeue(ctx); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected empty queue, got %v", err)
	}

	if err := queue.ScheduleRetry(ctx, "future", now.Add(time.Minute)); err != nil {
		t.Fatalf("schedule future retry: %v", err)
	}
	if err := queue.ScheduleRetry(ctx, "due", now.Add(-time.Second)); err != nil {
		t.Fatalf("schedule due retry: %v", err)
	}
	due, err := queue.PopDueRetries(ctx, now, 10)
	if err != nil || len(due) != 1 || due[0] != "due" {
		t.Fatalf("due retries = %v, err=%v", due, err)
	}
}
