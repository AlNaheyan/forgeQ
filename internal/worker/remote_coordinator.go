package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"
)

var ErrHeartbeatRejected = errors.New("coordinator rejected worker heartbeat")

// RegistrationRequest describes the worker capacity advertised to the
// coordinator.
type RegistrationRequest struct {
	WorkerID          string
	MaxConcurrency    int32
	SupportedJobTypes []string
}

// RemoteSettings contains coordinator-controlled worker timings.
type RemoteSettings struct {
	HeartbeatInterval time.Duration
	LeaseDuration     time.Duration
}

// HeartbeatMessage is the transport-neutral heartbeat sent by a worker.
type HeartbeatMessage struct {
	WorkerID       string
	ActiveJobs     int32
	MaxConcurrency int32
	Timestamp      time.Time
}

// CoordinatorRPC keeps registration and heartbeat behavior independent from
// the generated gRPC client, which also makes the retry loop easy to test.
type CoordinatorRPC interface {
	RegisterWorker(context.Context, RegistrationRequest) (RemoteSettings, error)
	Heartbeat(context.Context, HeartbeatMessage) (bool, error)
}

type RemoteConfig struct {
	WorkerID          string
	MaxConcurrency    int
	SupportedJobTypes []string
	InitialRetryDelay time.Duration
	MaxRetryDelay     time.Duration
	Now               func() time.Time
	Retryable         func(error) bool
}

// RemoteCoordinator manages a worker's registration and heartbeat lifecycle.
// It does not own the underlying gRPC connection.
type RemoteCoordinator struct {
	rpc               CoordinatorRPC
	workerID          string
	maxConcurrency    int32
	supportedJobTypes []string
	activeJobs        func() int
	initialRetryDelay time.Duration
	maxRetryDelay     time.Duration
	now               func() time.Time
	retryable         func(error) bool
	logger            *slog.Logger
}

func NewRemoteCoordinator(config RemoteConfig, rpc CoordinatorRPC, activeJobs func() int, logger *slog.Logger) (*RemoteCoordinator, error) {
	if rpc == nil {
		return nil, errors.New("coordinator RPC client is required")
	}
	if activeJobs == nil {
		return nil, errors.New("active job counter is required")
	}
	config.WorkerID = strings.TrimSpace(config.WorkerID)
	if config.WorkerID == "" {
		return nil, errors.New("worker ID is required")
	}
	if config.MaxConcurrency < 1 || config.MaxConcurrency > math.MaxInt32 {
		return nil, errors.New("worker max concurrency must be between 1 and 2147483647")
	}

	if config.InitialRetryDelay <= 0 {
		config.InitialRetryDelay = 100 * time.Millisecond
	}
	if config.MaxRetryDelay <= 0 {
		config.MaxRetryDelay = 5 * time.Second
	}
	if config.MaxRetryDelay < config.InitialRetryDelay {
		return nil, errors.New("maximum retry delay must not be less than initial retry delay")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Retryable == nil {
		config.Retryable = defaultRetryableCoordinatorError
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &RemoteCoordinator{
		rpc:               rpc,
		workerID:          config.WorkerID,
		maxConcurrency:    int32(config.MaxConcurrency),
		supportedJobTypes: append([]string(nil), config.SupportedJobTypes...),
		activeJobs:        activeJobs,
		initialRetryDelay: config.InitialRetryDelay,
		maxRetryDelay:     config.MaxRetryDelay,
		now:               config.Now,
		retryable:         config.Retryable,
		logger:            logger.With("worker_id", config.WorkerID),
	}, nil
}

// Register registers the worker, retrying transient failures with bounded
// exponential backoff until registration succeeds or the context ends.
func (r *RemoteCoordinator) Register(ctx context.Context) (RemoteSettings, error) {
	request := RegistrationRequest{
		WorkerID:          r.workerID,
		MaxConcurrency:    r.maxConcurrency,
		SupportedJobTypes: append([]string(nil), r.supportedJobTypes...),
	}
	delay := r.initialRetryDelay
	for {
		settings, err := r.rpc.RegisterWorker(ctx, request)
		if err == nil {
			if settings.HeartbeatInterval <= 0 {
				return RemoteSettings{}, errors.New("coordinator returned a non-positive heartbeat interval")
			}
			r.logger.Info("worker registered with coordinator",
				"heartbeat_interval", settings.HeartbeatInterval,
				"lease_duration", settings.LeaseDuration,
			)
			return settings, nil
		}
		if ctx.Err() != nil {
			return RemoteSettings{}, ctx.Err()
		}
		if !r.retryable(err) {
			return RemoteSettings{}, fmt.Errorf("register worker: %w", err)
		}
		r.logger.Warn("worker registration failed; retrying", "error", err, "retry_delay", delay)
		if !waitForCoordinatorRetry(ctx, delay) {
			return RemoteSettings{}, ctx.Err()
		}
		delay = nextRetryDelay(delay, r.maxRetryDelay)
	}
}

// RunHeartbeat sends an immediate heartbeat followed by periodic heartbeats.
// Transient failures are retried with bounded exponential backoff. A clean
// context cancellation stops the loop without an error.
func (r *RemoteCoordinator) RunHeartbeat(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("heartbeat interval must be greater than zero")
	}

	waitDelay := time.Duration(0)
	retryDelay := r.initialRetryDelay
	for {
		if waitDelay > 0 && !waitForCoordinatorRetry(ctx, waitDelay) {
			return nil
		}

		accepted, err := r.rpc.Heartbeat(ctx, HeartbeatMessage{
			WorkerID:       r.workerID,
			ActiveJobs:     boundedInt32(r.activeJobs()),
			MaxConcurrency: r.maxConcurrency,
			Timestamp:      r.now(),
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !r.retryable(err) {
				return fmt.Errorf("send worker heartbeat: %w", err)
			}
			waitDelay = retryDelay
			r.logger.Warn("worker heartbeat failed; retrying", "error", err, "retry_delay", waitDelay)
			retryDelay = nextRetryDelay(retryDelay, r.maxRetryDelay)
			continue
		}
		if !accepted {
			return ErrHeartbeatRejected
		}

		waitDelay = interval
		retryDelay = r.initialRetryDelay
	}
}

// Run performs registration and then maintains heartbeats using the interval
// selected by the coordinator.
func (r *RemoteCoordinator) Run(ctx context.Context) error {
	settings, err := r.Register(ctx)
	if err != nil {
		return err
	}
	return r.RunHeartbeat(ctx, settings.HeartbeatInterval)
}

type retryableCoordinatorError interface {
	Retryable() bool
}

func defaultRetryableCoordinatorError(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	var classified retryableCoordinatorError
	if errors.As(err, &classified) {
		return classified.Retryable()
	}
	return true
}

func waitForCoordinatorRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextRetryDelay(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

func boundedInt32(value int) int32 {
	if value <= 0 {
		return 0
	}
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(value)
}
