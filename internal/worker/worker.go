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
	"sync/atomic"
	"time"

	"forgequeue/internal/jobs"
	"forgequeue/internal/jobtypes"
	"forgequeue/internal/queue"
	"forgequeue/internal/tenant"
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
	ID                 string
	Concurrency        int
	PollInterval       time.Duration
	LeaseRenewInterval time.Duration
	JobTimeout         time.Duration
	JobTypes           []string
	RequireTenant      bool
	MaxResultBytes     int
	Registry           *jobtypes.Registry
}

type Worker struct {
	config     Config
	broker     Broker
	handlers   map[string]Handler
	logger     *slog.Logger
	renewer    *LeaseRenewer
	activeJobs atomic.Int64
}

func (w *Worker) ID() string {
	return w.config.ID
}

func (w *Worker) ActiveJobs() int {
	return int(w.activeJobs.Load())
}

func New(config Config, repository jobRepository, workQueue workQueue, handlers map[string]Handler, logger *slog.Logger) (*Worker, error) {
	if repository == nil || workQueue == nil {
		return nil, errors.New("worker repository and queue are required")
	}
	return NewWithBroker(config, NewLocalBroker(repository, workQueue), handlers, logger)
}

func NewWithBroker(config Config, broker Broker, handlers map[string]Handler, logger *slog.Logger) (*Worker, error) {
	if config.Concurrency < 1 {
		return nil, errors.New("worker concurrency must be greater than zero")
	}
	if config.PollInterval <= 0 {
		return nil, errors.New("worker poll interval must be greater than zero")
	}
	if config.LeaseRenewInterval < 0 {
		return nil, errors.New("worker lease renewal interval cannot be negative")
	}
	if config.JobTimeout <= 0 {
		config.JobTimeout = 5 * time.Minute
	}
	if config.MaxResultBytes == 0 {
		config.MaxResultBytes = 1 << 20
	}
	if config.MaxResultBytes < 1 || config.MaxResultBytes > 1<<20 {
		return nil, errors.New("worker result limit must be between 1 and 1048576 bytes")
	}
	if strings.TrimSpace(config.ID) == "" {
		id, err := generateID()
		if err != nil {
			return nil, err
		}
		config.ID = id
	}
	if broker == nil {
		return nil, errors.New("worker broker is required")
	}
	selectedHandlers := make(map[string]Handler, len(config.JobTypes))
	for _, jobType := range config.JobTypes {
		if config.Registry != nil {
			if _, ok := config.Registry.Lookup(jobType); !ok {
				return nil, fmt.Errorf("worker lacks descriptor for job type %q", jobType)
			}
		}
		if _, exists := selectedHandlers[jobType]; exists {
			return nil, fmt.Errorf("duplicate worker job type %q", jobType)
		}
		handler, ok := handlers[jobType]
		if !ok || handler == nil {
			return nil, fmt.Errorf("worker does not have a handler for configured job type %q", jobType)
		}
		selectedHandlers[jobType] = handler
	}
	var renewer *LeaseRenewer
	if config.LeaseRenewInterval > 0 {
		var err error
		renewer, err = NewLeaseRenewer(broker, config.LeaseRenewInterval)
		if err != nil {
			return nil, err
		}
	}
	return &Worker{
		config:   config,
		broker:   broker,
		handlers: selectedHandlers,
		logger:   logger.With("worker_id", config.ID),
		renewer:  renewer,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("worker started", "concurrency", w.config.Concurrency, "job_types", w.config.JobTypes)
	semaphore := make(chan struct{}, w.config.Concurrency)
	var active sync.WaitGroup

	for {
		if ctx.Err() != nil {
			w.logger.Info("worker draining active jobs", "active_jobs", w.ActiveJobs())
			active.Wait()
			w.logger.Info("worker stopped")
			return nil
		}
		select {
		case <-ctx.Done():
			w.logger.Info("worker draining active jobs", "active_jobs", w.ActiveJobs())
			active.Wait()
			w.logger.Info("worker stopped")
			return nil
		case semaphore <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-semaphore
			active.Wait()
			w.logger.Info("worker stopped")
			return nil
		}

		claimed, err := w.broker.ClaimJob(ctx, w.config.ID, w.config.JobTypes)
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
			w.logger.Warn("job claim failed", "error", err)
			if !waitForNextPoll(ctx, w.config.PollInterval) {
				active.Wait()
				w.logger.Info("worker stopped")
				return nil
			}
			continue
		}

		handler, supported := w.handlers[claimed.Type]
		if !supported {
			<-semaphore
			w.logger.Error("coordinator returned unsupported job", "job_id", claimed.ID, "job_type", claimed.Type)
			finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_ = w.broker.FailJob(finishCtx, w.config.ID, claimed.ID, "worker has no handler for job type", false, claimed.LeaseGeneration)
			cancel()
			continue
		}

		active.Add(1)
		w.activeJobs.Add(1)
		go func() {
			defer active.Done()
			defer w.activeJobs.Add(-1)
			defer func() { <-semaphore }()
			w.execute(ctx, claimed, handler)
		}()
	}
}

func (w *Worker) execute(ctx context.Context, job jobs.Job, handler Handler) {
	startedAt := time.Now()
	jobCtx, cancelJob := context.WithTimeout(context.WithoutCancel(ctx), w.config.JobTimeout)
	defer cancelJob()
	executionCtx := withAttempt(jobCtx, job.AttemptCount)
	executionCtx = withMetadata(executionCtx, ExecutionMetadata{TenantID: job.TenantID, JobID: job.ID, JobType: job.Type, SubmissionKey: job.IdempotencyKey, Attempt: job.AttemptCount, LeaseGeneration: job.LeaseGeneration})
	if w.config.RequireTenant && job.TenantID == "" {
		w.logger.Error("job has no tenant identity; execution refused", "job_id", job.ID)
		return
	}
	if job.TenantID != "" {
		executionCtx = tenant.WithPrincipal(executionCtx, tenant.Principal{ID: job.TenantID})
	}
	var leaseSession *LeaseSession
	if w.renewer != nil {
		var err error
		leaseSession, err = w.renewer.Start(executionCtx, w.config.ID, job.ID, job.LeaseGeneration)
		if err != nil {
			w.logger.Error("lease renewal could not start", "job_id", job.ID, "error", err)
			return
		}
		executionCtx = leaseSession.Context()
	}
	var result any
	var executionErr error
	registry := w.config.Registry
	if registry == nil {
		registry = jobtypes.Fixtures()
	}
	if _, known := registry.Lookup(job.Type); known {
		if err := registry.Validate(job.Type, job.Payload); err != nil {
			executionErr = &HandlerError{Code: FailureInvalidPayload, Cause: err}
		}
	}
	if executionErr == nil {
		result, executionErr = executeSafely(executionCtx, handler, job.Payload)
	}
	if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
		executionErr = context.DeadlineExceeded
	}
	if leaseSession != nil {
		if err := leaseSession.Stop(); err != nil {
			w.logger.Warn("job lease lost; terminal result discarded",
				"job_id", job.ID,
				"attempt", job.AttemptCount,
				"error", err,
			)
			return
		}
	}

	status := jobs.StatusSucceeded
	var encodedResult []byte
	lastError := ""
	retryable := true
	if executionErr != nil {
		status = jobs.StatusFailed
		code, retry := classifyFailure(executionErr)
		lastError, retryable = string(code), retry
	} else {
		var err error
		encodedResult, err = json.Marshal(result)
		if err != nil || len(encodedResult) > w.config.MaxResultBytes {
			status = jobs.StatusFailed
			lastError, retryable = string(FailureResultInvalid), false
		}
	}

	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	var persistErr error
	if status == jobs.StatusSucceeded {
		persistErr = w.broker.CompleteJob(finishCtx, w.config.ID, job.ID, encodedResult, job.LeaseGeneration)
	} else {
		persistErr = w.broker.FailJob(finishCtx, w.config.ID, job.ID, lastError, retryable, job.LeaseGeneration)
	}
	if persistErr != nil {
		w.logger.Error("job result persistence failed",
			"job_id", job.ID,
			"attempt", job.AttemptCount,
			"status", status,
			"error", persistErr,
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
		// Handler errors may embed payloads or secrets; details stay in the
		// tenant-scoped durable result, not shared operational logs.
		w.logger.Warn("job failed", attributes...)
		return
	}
	w.logger.Info("job completed", attributes...)
}

func executeSafely(ctx context.Context, handler Handler, payload json.RawMessage) (result any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &HandlerError{Code: FailurePanic}
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
