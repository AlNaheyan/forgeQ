package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/internal/queue"
)

type jobRepository interface {
	GetJob(context.Context, string) (jobs.Job, error)
	ClaimJob(context.Context, string) (jobs.Job, error)
	FinishJob(context.Context, string, jobs.Status, []byte, string) (jobs.Job, error)
}

type workQueue interface {
	Dequeue(context.Context) (string, error)
	Enqueue(context.Context, string, int, time.Time) error
}

type Config struct {
	ID           string
	Concurrency  int
	PollInterval time.Duration
	JobTypes     []string
}

type Worker struct {
	config     Config
	repository jobRepository
	queue      workQueue
	handlers   map[string]Handler
	logger     *slog.Logger
}

func New(config Config, repository jobRepository, workQueue workQueue, handlers map[string]Handler, logger *slog.Logger) (*Worker, error) {
	if config.Concurrency < 1 {
		return nil, errors.New("worker concurrency must be greater than zero")
	}
	if config.PollInterval <= 0 {
		return nil, errors.New("worker poll interval must be greater than zero")
	}
	if strings.TrimSpace(config.ID) == "" {
		id, err := generateID()
		if err != nil {
			return nil, err
		}
		config.ID = id
	}
	selectedHandlers := make(map[string]Handler, len(config.JobTypes))
	for _, jobType := range config.JobTypes {
		handler, ok := handlers[jobType]
		if !ok {
			return nil, fmt.Errorf("worker does not have a handler for configured job type %q", jobType)
		}
		selectedHandlers[jobType] = handler
	}
	return &Worker{
		config:     config,
		repository: repository,
		queue:      workQueue,
		handlers:   selectedHandlers,
		logger:     logger.With("worker_id", config.ID),
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("worker started", "concurrency", w.config.Concurrency, "job_types", w.config.JobTypes)
	semaphore := make(chan struct{}, w.config.Concurrency)
	var active sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			active.Wait()
			w.logger.Info("worker stopped")
			return nil
		case semaphore <- struct{}{}:
		}

		jobID, err := w.queue.Dequeue(ctx)
		if errors.Is(err, queue.ErrEmpty) {
			<-semaphore
			if !waitForNextPoll(ctx, w.config.PollInterval) {
				active.Wait()
				w.logger.Info("worker stopped")
				return nil
			}
			continue
		}
		if err != nil {
			<-semaphore
			w.logger.Warn("job dequeue failed", "error", err)
			if !waitForNextPoll(ctx, w.config.PollInterval) {
				active.Wait()
				w.logger.Info("worker stopped")
				return nil
			}
			continue
		}

		candidate, err := w.repository.GetJob(ctx, jobID)
		if err != nil {
			<-semaphore
			if !errors.Is(err, jobs.ErrNotFound) && !errors.Is(err, context.Canceled) {
				w.logger.Warn("queued job lookup failed", "job_id", jobID, "error", err)
			}
			continue
		}
		handler, supported := w.handlers[candidate.Type]
		if !supported {
			<-semaphore
			if err := w.queue.Enqueue(ctx, candidate.ID, candidate.Priority, candidate.CreatedAt); err != nil {
				w.logger.Warn("unsupported job could not be returned to queue", "job_id", jobID, "job_type", candidate.Type, "error", err)
			}
			if !waitForNextPoll(ctx, w.config.PollInterval) {
				active.Wait()
				return nil
			}
			continue
		}

		claimed, err := w.repository.ClaimJob(ctx, jobID)
		if err != nil {
			<-semaphore
			if !errors.Is(err, jobs.ErrClaimConflict) && !errors.Is(err, context.Canceled) {
				w.logger.Warn("job claim failed", "job_id", jobID, "error", err)
			}
			continue
		}

		active.Add(1)
		go func() {
			defer active.Done()
			defer func() { <-semaphore }()
			w.execute(ctx, claimed, handler)
		}()
	}
}

func (w *Worker) execute(ctx context.Context, job jobs.Job, handler Handler) {
	startedAt := time.Now()
	result, executionErr := executeSafely(withAttempt(ctx, job.AttemptCount), handler, job.Payload)

	status := jobs.StatusSucceeded
	var encodedResult []byte
	lastError := ""
	if executionErr != nil {
		status = jobs.StatusFailed
		lastError = executionErr.Error()
		if len(lastError) > 4096 {
			lastError = lastError[:4096]
		}
	} else {
		var err error
		encodedResult, err = json.Marshal(result)
		if err != nil {
			status = jobs.StatusFailed
			lastError = fmt.Sprintf("encode job result: %v", err)
		}
	}

	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := w.repository.FinishJob(finishCtx, job.ID, status, encodedResult, lastError); err != nil {
		w.logger.Error("job result persistence failed",
			"job_id", job.ID,
			"attempt", job.AttemptCount,
			"status", status,
			"error", err,
		)
		return
	}

	attributes := []any{
		"job_id", job.ID,
		"job_type", job.Type,
		"attempt", job.AttemptCount,
		"status", status,
		"duration_ms", time.Since(startedAt).Milliseconds(),
	}
	if executionErr != nil {
		attributes = append(attributes, "error", executionErr)
		w.logger.Warn("job failed", attributes...)
		return
	}
	w.logger.Info("job completed", attributes...)
}

func executeSafely(ctx context.Context, handler Handler, payload json.RawMessage) (result any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("job handler panic: %v", recovered)
		}
	}()
	return handler.Execute(ctx, payload)
}

func waitForNextPoll(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func generateID() (string, error) {
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generate worker ID: %w", err)
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	return "worker-" + hostname + "-" + hex.EncodeToString(suffix[:]), nil
}
