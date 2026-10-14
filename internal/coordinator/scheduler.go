package coordinator

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"

	"forgequeue/internal/jobs"
)

// ErrNoEligibleWorker indicates that no healthy worker has both capacity and
// support for the job type being scheduled.
var ErrNoEligibleWorker = errors.New("no eligible worker")

// Scheduler selects a worker for a job from a point-in-time registry view.
// Implementations do not reserve worker capacity; the caller remains
// responsible for atomically assigning the job.
type Scheduler interface {
	SelectWorker(job jobs.Job, workers []WorkerSnapshot) (WorkerSnapshot, error)
}

// RoundRobin distributes jobs evenly across eligible workers. Its cursor is
// shared by all calls and is safe for concurrent use.
type RoundRobin struct {
	mu   sync.Mutex
	next uint64
}

func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

func (s *RoundRobin) SelectWorker(job jobs.Job, workers []WorkerSnapshot) (WorkerSnapshot, error) {
	eligible := eligibleWorkers(job, workers)
	if len(eligible) == 0 {
		return WorkerSnapshot{}, ErrNoEligibleWorker
	}

	s.mu.Lock()
	worker := eligible[s.next%uint64(len(eligible))]
	s.next++
	s.mu.Unlock()
	return worker, nil
}

// LeastLoaded selects the eligible worker with the greatest currently
// available capacity. Worker ID is the stable tie-breaker.
type LeastLoaded struct{}

func NewLeastLoaded() *LeastLoaded {
	return &LeastLoaded{}
}

func (*LeastLoaded) SelectWorker(job jobs.Job, workers []WorkerSnapshot) (WorkerSnapshot, error) {
	eligible := eligibleWorkers(job, workers)
	if len(eligible) == 0 {
		return WorkerSnapshot{}, ErrNoEligibleWorker
	}

	selected := eligible[0]
	for _, worker := range eligible[1:] {
		if worker.AvailableCapacity > selected.AvailableCapacity {
			selected = worker
		}
	}
	return selected, nil
}

// IntnSource supplies a uniformly distributed integer in [0, n). It allows
// weighted scheduling to use a deterministic source in tests.
type IntnSource interface {
	Intn(n int) int
}

// WeightedCapacity selects workers in proportion to their available
// capacity. Access to the random source is serialized because rand.Rand and
// arbitrary injected sources are not necessarily safe for concurrent use.
type WeightedCapacity struct {
	mu     sync.Mutex
	random IntnSource
}

func NewWeightedCapacity(random IntnSource) *WeightedCapacity {
	if random == nil {
		random = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &WeightedCapacity{random: random}
}

func (s *WeightedCapacity) SelectWorker(job jobs.Job, workers []WorkerSnapshot) (WorkerSnapshot, error) {
	eligible := eligibleWorkers(job, workers)
	if len(eligible) == 0 {
		return WorkerSnapshot{}, ErrNoEligibleWorker
	}

	totalCapacity := 0
	for _, worker := range eligible {
		if worker.AvailableCapacity > int(^uint(0)>>1)-totalCapacity {
			return WorkerSnapshot{}, ErrNoEligibleWorker
		}
		totalCapacity += worker.AvailableCapacity
	}

	s.mu.Lock()
	if s.random == nil {
		s.random = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	ticket := s.random.Intn(totalCapacity)
	s.mu.Unlock()
	if ticket < 0 || ticket >= totalCapacity {
		return WorkerSnapshot{}, ErrNoEligibleWorker
	}
	for _, worker := range eligible {
		if ticket < worker.AvailableCapacity {
			return worker, nil
		}
		ticket -= worker.AvailableCapacity
	}

	// An IntnSource must honor the documented range. Treat a broken injected
	// source as an inability to make a safe scheduling decision.
	return WorkerSnapshot{}, ErrNoEligibleWorker
}

// eligibleWorkers returns an independent, deterministically ordered slice.
// It never mutates the registry snapshot supplied by the caller.
func eligibleWorkers(job jobs.Job, workers []WorkerSnapshot) []WorkerSnapshot {
	eligible := make([]WorkerSnapshot, 0, len(workers))
	for _, worker := range workers {
		if worker.Health != HealthHealthy || worker.AvailableCapacity <= 0 {
			continue
		}
		if !supportsJobType(worker.SupportedJobTypes, job.Type) {
			continue
		}
		eligible = append(eligible, worker)
	}
	sort.Slice(eligible, func(i, j int) bool {
		return eligible[i].WorkerID < eligible[j].WorkerID
	})
	return eligible
}

func supportsJobType(supported []string, jobType string) bool {
	for _, candidate := range supported {
		if candidate == jobType {
			return true
		}
	}
	return false
}
