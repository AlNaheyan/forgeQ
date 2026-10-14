package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const priorityWindowMillis = int64(10_000_000_000_000)

type RedisQueue struct {
	client     redis.UniversalClient
	readyKey   string
	delayedKey string
}

func NewRedisQueue(client redis.UniversalClient, prefix string) *RedisQueue {
	return &RedisQueue{
		client:     client,
		readyKey:   prefix + ":ready",
		delayedKey: prefix + ":delayed",
	}
}

func (q *RedisQueue) Enqueue(ctx context.Context, jobID string, priority int, createdAt time.Time) error {
	if jobID == "" {
		return errors.New("enqueue job: job ID is required")
	}
	if err := q.client.ZAdd(ctx, q.readyKey, redis.Z{
		Score:  readyScore(priority, createdAt),
		Member: jobID,
	}).Err(); err != nil {
		return fmt.Errorf("enqueue job %s: %w", jobID, err)
	}
	return nil
}

func (q *RedisQueue) Dequeue(ctx context.Context) (string, error) {
	items, err := q.client.ZPopMin(ctx, q.readyKey, 1).Result()
	if err != nil {
		return "", fmt.Errorf("dequeue job: %w", err)
	}
	if len(items) == 0 {
		return "", ErrEmpty
	}
	jobID, ok := items[0].Member.(string)
	if !ok || jobID == "" {
		return "", errors.New("dequeue job: Redis returned an invalid job ID")
	}
	return jobID, nil
}

func (q *RedisQueue) Remove(ctx context.Context, jobID string) error {
	if err := q.client.ZRem(ctx, q.readyKey, jobID).Err(); err != nil {
		return fmt.Errorf("remove ready job %s: %w", jobID, err)
	}
	if err := q.client.ZRem(ctx, q.delayedKey, jobID).Err(); err != nil {
		return fmt.Errorf("remove delayed job %s: %w", jobID, err)
	}
	return nil
}

func (q *RedisQueue) Depth(ctx context.Context) (int64, error) {
	depth, err := q.client.ZCard(ctx, q.readyKey).Result()
	if err != nil {
		return 0, fmt.Errorf("read queue depth: %w", err)
	}
	return depth, nil
}

func (q *RedisQueue) Contains(ctx context.Context, jobID string) (bool, error) {
	_, err := q.client.ZScore(ctx, q.readyKey, jobID).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check queued job %s: %w", jobID, err)
	}
	return true, nil
}

func (q *RedisQueue) ScheduleRetry(ctx context.Context, jobID string, dueAt time.Time) error {
	if jobID == "" {
		return errors.New("schedule retry: job ID is required")
	}
	if err := q.client.ZAdd(ctx, q.delayedKey, redis.Z{
		Score:  float64(dueAt.UnixMilli()),
		Member: jobID,
	}).Err(); err != nil {
		return fmt.Errorf("schedule retry for job %s: %w", jobID, err)
	}
	return nil
}

var popDueRetries = redis.NewScript(`
local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #jobs > 0 then
  redis.call('ZREM', KEYS[1], unpack(jobs))
end
return jobs
`)

func (q *RedisQueue) PopDueRetries(ctx context.Context, now time.Time, limit int64) ([]string, error) {
	if limit <= 0 {
		return nil, errors.New("pop due retries: limit must be greater than zero")
	}
	result, err := popDueRetries.Run(ctx, q.client, []string{q.delayedKey}, now.UnixMilli(), limit).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("pop due retries: %w", err)
	}
	return result, nil
}

func readyScore(priority int, createdAt time.Time) float64 {
	return float64(createdAt.UnixMilli() - int64(priority)*priorityWindowMillis)
}
