package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"forgequeue/internal/api"
	"forgequeue/internal/jobs"
	"forgequeue/internal/platform"
	jobqueue "forgequeue/internal/queue"
	postgresrepository "forgequeue/internal/repository/postgres"
	"forgequeue/migrations"

	"github.com/redis/go-redis/v9"
)

func TestJobHTTPFlow(t *testing.T) {
	databaseURL := os.Getenv("FORGEQUEUE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("FORGEQUEUE_TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	db, err := platform.OpenPostgres(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	defer db.Close()
	lockConnection, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("reserve test lock connection: %v", err)
	}
	defer lockConnection.Close()
	if _, err := lockConnection.ExecContext(ctx, "SELECT pg_advisory_lock(7346871)"); err != nil {
		t.Fatalf("lock integration database: %v", err)
	}
	defer lockConnection.ExecContext(context.Background(), "SELECT pg_advisory_unlock(7346871)")
	if err := migrations.Up(ctx, db); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := db.ExecContext(ctx, "TRUNCATE job_attempts, idempotency_keys, jobs CASCADE"); err != nil {
		t.Fatalf("clean tables: %v", err)
	}

	repository := postgresrepository.NewJobRepository(db)
	var enqueuer jobs.Enqueuer
	var redisQueue *jobqueue.RedisQueue
	if redisAddress := os.Getenv("FORGEQUEUE_TEST_REDIS_ADDR"); redisAddress != "" {
		redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
		defer redisClient.Close()
		prefix := "forgequeue:http-test:" + strconv.FormatInt(time.Now().UnixNano(), 10)
		defer redisClient.Del(ctx, prefix+":ready", prefix+":delayed")
		redisQueue = jobqueue.NewRedisQueue(redisClient, prefix)
		enqueuer = redisQueue
	}
	service := jobs.NewService(repository, enqueuer, 3)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := api.NewServer(service, nil, logger).Routes()
	server := httptest.NewServer(handler)
	defer server.Close()

	jobID := submitJob(t, server.URL, http.StatusCreated)
	if redisQueue != nil {
		present, err := redisQueue.Contains(ctx, jobID)
		if err != nil || !present {
			t.Fatalf("submitted job missing from Redis queue: present=%v err=%v", present, err)
		}
	}
	duplicateID := submitJob(t, server.URL, http.StatusOK)
	if jobID != duplicateID {
		t.Fatalf("duplicate request returned %q, want %q", duplicateID, jobID)
	}

	response, err := http.Get(server.URL + "/v1/jobs/" + jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d", response.StatusCode)
	}

	response, err = http.Get(server.URL + "/v1/jobs?status=queued&type=sleep&limit=10")
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d", response.StatusCode)
	}

	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/jobs/"+jobID+"/cancel", nil)
	if err != nil {
		t.Fatalf("create cancel request: %v", err)
	}
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("cancel job: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d", response.StatusCode)
	}
}

func submitJob(t *testing.T, serverURL string, wantStatus int) string {
	t.Helper()
	body := bytes.NewBufferString(`{"type":"sleep","payload":{"duration_ms":10},"idempotency_key":"http-flow"}`)
	response, err := http.Post(serverURL+"/v1/jobs", "application/json", body)
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		contents, _ := io.ReadAll(response.Body)
		t.Fatalf("submit status = %d, want %d: %s", response.StatusCode, wantStatus, contents)
	}
	var job struct {
		ID string `json:"job_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatalf("decode submitted job: %v", err)
	}
	return job.ID
}
