// Package coordinator contains the coordinator's worker state and scheduling
// primitives.
package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidRegistration = errors.New("invalid worker registration")
	ErrInvalidHeartbeat    = errors.New("invalid worker heartbeat")
	ErrWorkerNotFound      = errors.New("worker not found")
)

// Clock allows health transitions to be tested without waiting in real time.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Registration is the static worker information supplied when it connects.
type Registration struct {
	WorkerID          string
	MaxConcurrency    int
	SupportedJobTypes []string
}

// Heartbeat is a worker's current execution state. Timestamp is the time on
// the worker when it produced the heartbeat. Worker health uses the time the
// coordinator receives the heartbeat so clock skew cannot keep a dead worker
// healthy.
type Heartbeat struct {
	WorkerID       string
	ActiveJobs     int
	MaxConcurrency int
	Timestamp      time.Time
}

// WorkerSnapshot is an immutable point-in-time view of a registered worker.
type WorkerSnapshot struct {
	WorkerID          string
	MaxConcurrency    int
	SupportedJobTypes []string
	ActiveJobs        int
	AvailableCapacity int
	RegisteredAt      time.Time
	LastHeartbeatAt   time.Time
	ReportedAt        time.Time
	Health            Health
}

type workerState struct {
	workerID          string
	maxConcurrency    int
	supportedJobTypes []string
	activeJobs        int
	registeredAt      time.Time
	lastHeartbeatAt   time.Time
	reportedAt        time.Time
}

// Registry stores the coordinator's reconstructable, in-memory worker state.
// All methods are safe for concurrent use.
type Registry struct {
	mu         sync.RWMutex
	workers    map[string]workerState
	thresholds HealthThresholds
	clock      Clock
}

func NewRegistry(thresholds HealthThresholds, clock Clock) (*Registry, error) {
	if err := thresholds.validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		clock = systemClock{}
	}
	return &Registry{
		workers:    make(map[string]workerState),
		thresholds: thresholds,
		clock:      clock,
	}, nil
}

// Register adds or replaces a worker registration. Re-registering a worker is
// treated as a new session and resets its active-job count.
func (r *Registry) Register(registration Registration) (WorkerSnapshot, error) {
	jobTypes, err := validateRegistration(registration)
	if err != nil {
		return WorkerSnapshot{}, err
	}

	now := r.clock.Now()
	state := workerState{
		workerID:          registration.WorkerID,
		maxConcurrency:    registration.MaxConcurrency,
		supportedJobTypes: jobTypes,
		registeredAt:      now,
		lastHeartbeatAt:   now,
	}

	r.mu.Lock()
	r.workers[state.workerID] = state
	r.mu.Unlock()

	return r.snapshot(state, now), nil
}

// RecordHeartbeat updates a registered worker's liveness and capacity.
func (r *Registry) RecordHeartbeat(heartbeat Heartbeat) (WorkerSnapshot, error) {
	if err := validateHeartbeat(heartbeat); err != nil {
		return WorkerSnapshot{}, err
	}

	now := r.clock.Now()
	r.mu.Lock()
	state, ok := r.workers[heartbeat.WorkerID]
	if !ok {
		r.mu.Unlock()
		return WorkerSnapshot{}, fmt.Errorf("%w: %q", ErrWorkerNotFound, heartbeat.WorkerID)
	}
	if !state.reportedAt.IsZero() && heartbeat.Timestamp.Before(state.reportedAt) {
		r.mu.Unlock()
		return WorkerSnapshot{}, fmt.Errorf("%w: timestamp predates the previous heartbeat", ErrInvalidHeartbeat)
	}
	state.activeJobs = heartbeat.ActiveJobs
	state.maxConcurrency = heartbeat.MaxConcurrency
	state.lastHeartbeatAt = now
	state.reportedAt = heartbeat.Timestamp
	r.workers[state.workerID] = state
	r.mu.Unlock()

	return r.snapshot(state, now), nil
}

// Get returns the current snapshot for a worker.
func (r *Registry) Get(workerID string) (WorkerSnapshot, bool) {
	now := r.clock.Now()
	r.mu.RLock()
	state, ok := r.workers[workerID]
	r.mu.RUnlock()
	if !ok {
		return WorkerSnapshot{}, false
	}
	return r.snapshot(state, now), true
}

// List returns snapshots ordered by worker ID for deterministic API responses.
func (r *Registry) List() []WorkerSnapshot {
	now := r.clock.Now()
	r.mu.RLock()
	snapshots := make([]WorkerSnapshot, 0, len(r.workers))
	for _, state := range r.workers {
		snapshots = append(snapshots, r.snapshot(state, now))
	}
	r.mu.RUnlock()
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].WorkerID < snapshots[j].WorkerID
	})
	return snapshots
}

func (r *Registry) snapshot(state workerState, now time.Time) WorkerSnapshot {
	jobTypes := append([]string(nil), state.supportedJobTypes...)
	return WorkerSnapshot{
		WorkerID:          state.workerID,
		MaxConcurrency:    state.maxConcurrency,
		SupportedJobTypes: jobTypes,
		ActiveJobs:        state.activeJobs,
		AvailableCapacity: state.maxConcurrency - state.activeJobs,
		RegisteredAt:      state.registeredAt,
		LastHeartbeatAt:   state.lastHeartbeatAt,
		ReportedAt:        state.reportedAt,
		Health:            r.thresholds.health(now, state.lastHeartbeatAt),
	}
}

func validateRegistration(registration Registration) ([]string, error) {
	if registration.WorkerID == "" || strings.TrimSpace(registration.WorkerID) != registration.WorkerID {
		return nil, fmt.Errorf("%w: worker ID must be non-empty and have no surrounding whitespace", ErrInvalidRegistration)
	}
	if registration.MaxConcurrency <= 0 {
		return nil, fmt.Errorf("%w: max concurrency must be greater than zero", ErrInvalidRegistration)
	}
	if len(registration.SupportedJobTypes) == 0 {
		return nil, fmt.Errorf("%w: at least one supported job type is required", ErrInvalidRegistration)
	}

	jobTypes := append([]string(nil), registration.SupportedJobTypes...)
	sort.Strings(jobTypes)
	for i, jobType := range jobTypes {
		if jobType == "" || strings.TrimSpace(jobType) != jobType {
			return nil, fmt.Errorf("%w: job types must be non-empty and have no surrounding whitespace", ErrInvalidRegistration)
		}
		if i > 0 && jobTypes[i-1] == jobType {
			return nil, fmt.Errorf("%w: duplicate job type %q", ErrInvalidRegistration, jobType)
		}
	}
	return jobTypes, nil
}

func validateHeartbeat(heartbeat Heartbeat) error {
	if heartbeat.WorkerID == "" || strings.TrimSpace(heartbeat.WorkerID) != heartbeat.WorkerID {
		return fmt.Errorf("%w: worker ID must be non-empty and have no surrounding whitespace", ErrInvalidHeartbeat)
	}
	if heartbeat.MaxConcurrency <= 0 {
		return fmt.Errorf("%w: max concurrency must be greater than zero", ErrInvalidHeartbeat)
	}
	if heartbeat.ActiveJobs < 0 || heartbeat.ActiveJobs > heartbeat.MaxConcurrency {
		return fmt.Errorf("%w: active jobs must be between zero and max concurrency", ErrInvalidHeartbeat)
	}
	if heartbeat.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp is required", ErrInvalidHeartbeat)
	}
	return nil
}
