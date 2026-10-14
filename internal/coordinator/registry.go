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
	ErrWorkerUnhealthy     = errors.New("worker is unhealthy")
	ErrWorkerAtCapacity    = errors.New("worker is at capacity")
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
	JobsAssignedTotal uint64
}

type workerState struct {
	workerID          string
	maxConcurrency    int
	supportedJobTypes []string
	activeJobs        int
	// reservationsSinceHeartbeat counts claims accepted after the last
	// heartbeat was reconciled. RecordHeartbeat preserves these reservations
	// for one heartbeat boundary so a heartbeat produced just before a
	// concurrent claim cannot erase the newly consumed capacity.
	reservationsSinceHeartbeat int
	jobsAssignedTotal          uint64
	registeredAt               time.Time
	lastHeartbeatAt            time.Time
	reportedAt                 time.Time
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
// treated as a new session and resets its dynamic capacity state. The
// assignment counter is retained so it remains monotonic for a worker ID.
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
	if previous, ok := r.workers[state.workerID]; ok {
		state.jobsAssignedTotal = previous.jobsAssignedTotal
	}
	r.workers[state.workerID] = state
	r.mu.Unlock()

	return r.snapshot(state, now), nil
}

// RecordHeartbeat updates a registered worker's liveness and capacity.
//
// A heartbeat is authoritative for work the worker reports, while reservations
// accepted since the previous heartbeat are added conservatively. This may
// temporarily understate free capacity for one heartbeat interval when the
// worker already included a fresh assignment in its report, but it prevents a
// racing, older heartbeat from erasing a successful reservation.
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
	state.activeJobs = heartbeat.ActiveJobs + state.reservationsSinceHeartbeat
	if state.activeJobs > heartbeat.MaxConcurrency {
		state.activeJobs = heartbeat.MaxConcurrency
	}
	state.reservationsSinceHeartbeat = 0
	state.maxConcurrency = heartbeat.MaxConcurrency
	state.lastHeartbeatAt = now
	state.reportedAt = heartbeat.Timestamp
	r.workers[state.workerID] = state
	r.mu.Unlock()

	return r.snapshot(state, now), nil
}

// TryReserve atomically consumes one worker slot. Suspect workers may still be
// used, matching the coordinator's existing health policy; unhealthy workers
// and workers with no free capacity are rejected.
func (r *Registry) TryReserve(workerID string) (WorkerSnapshot, error) {
	now := r.clock.Now()
	r.mu.Lock()
	state, ok := r.workers[workerID]
	if !ok {
		r.mu.Unlock()
		return WorkerSnapshot{}, fmt.Errorf("%w: %q", ErrWorkerNotFound, workerID)
	}
	if r.thresholds.health(now, state.lastHeartbeatAt) == HealthUnhealthy {
		r.mu.Unlock()
		return WorkerSnapshot{}, fmt.Errorf("%w: %q", ErrWorkerUnhealthy, workerID)
	}
	if state.activeJobs >= state.maxConcurrency {
		r.mu.Unlock()
		return WorkerSnapshot{}, fmt.Errorf("%w: %q", ErrWorkerAtCapacity, workerID)
	}
	state.activeJobs++
	state.reservationsSinceHeartbeat++
	r.workers[workerID] = state
	r.mu.Unlock()

	return r.snapshot(state, now), nil
}

// ConfirmAssignment records that a reserved slot became a durable job lease.
// It is deliberately separate from TryReserve so failed lease attempts do not
// inflate the jobs-assigned metric.
func (r *Registry) ConfirmAssignment(workerID string) (WorkerSnapshot, error) {
	now := r.clock.Now()
	r.mu.Lock()
	state, ok := r.workers[workerID]
	if !ok {
		r.mu.Unlock()
		return WorkerSnapshot{}, fmt.Errorf("%w: %q", ErrWorkerNotFound, workerID)
	}
	state.jobsAssignedTotal++
	r.workers[workerID] = state
	r.mu.Unlock()

	return r.snapshot(state, now), nil
}

// RollbackReservation returns a slot when the durable lease was not created.
// Unlike Release, it also removes one fresh reservation from heartbeat
// reconciliation because the caller knows the failed lease corresponds to a
// TryReserve that has not become an assignment.
func (r *Registry) RollbackReservation(workerID string) (WorkerSnapshot, error) {
	now := r.clock.Now()
	r.mu.Lock()
	state, ok := r.workers[workerID]
	if !ok {
		r.mu.Unlock()
		return WorkerSnapshot{}, fmt.Errorf("%w: %q", ErrWorkerNotFound, workerID)
	}
	if state.activeJobs > 0 {
		state.activeJobs--
	}
	if state.reservationsSinceHeartbeat > 0 {
		state.reservationsSinceHeartbeat--
	}
	r.workers[workerID] = state
	r.mu.Unlock()

	return r.snapshot(state, now), nil
}

// Release returns one consumed slot after a durable assignment reaches a
// terminal state. It intentionally does not consume a fresh reservation: with
// aggregate worker counts, an older job may finish concurrently with a newer
// claim, and attributing that completion to the new reservation would allow a
// stale heartbeat to erase its capacity. Fast assignments that finish before
// their first heartbeat may therefore remain conservatively counted for one
// heartbeat interval. The count never drops below zero.
func (r *Registry) Release(workerID string) (WorkerSnapshot, error) {
	now := r.clock.Now()
	r.mu.Lock()
	state, ok := r.workers[workerID]
	if !ok {
		r.mu.Unlock()
		return WorkerSnapshot{}, fmt.Errorf("%w: %q", ErrWorkerNotFound, workerID)
	}
	if state.activeJobs > 0 {
		state.activeJobs--
	}
	r.workers[workerID] = state
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
	availableCapacity := state.maxConcurrency - state.activeJobs
	if availableCapacity < 0 {
		availableCapacity = 0
	}
	return WorkerSnapshot{
		WorkerID:          state.workerID,
		MaxConcurrency:    state.maxConcurrency,
		SupportedJobTypes: jobTypes,
		ActiveJobs:        state.activeJobs,
		AvailableCapacity: availableCapacity,
		RegisteredAt:      state.registeredAt,
		LastHeartbeatAt:   state.lastHeartbeatAt,
		ReportedAt:        state.reportedAt,
		Health:            r.thresholds.health(now, state.lastHeartbeatAt),
		JobsAssignedTotal: state.jobsAssignedTotal,
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
