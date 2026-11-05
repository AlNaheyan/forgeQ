package coordinator

import (
	"errors"
	"math/rand"
	"sync"
	"testing"

	"github.com/AlNaheyan/forgeQ/internal/jobs"
)

func TestSchedulersRejectWhenNoWorkerIsEligible(t *testing.T) {
	t.Parallel()

	job := jobs.Job{Type: "sleep"}
	workers := []WorkerSnapshot{
		{WorkerID: "unhealthy", Health: HealthUnhealthy, AvailableCapacity: 3, SupportedJobTypes: []string{"sleep"}},
		{WorkerID: "full", Health: HealthHealthy, AvailableCapacity: 0, SupportedJobTypes: []string{"sleep"}},
		{WorkerID: "unsupported", Health: HealthHealthy, AvailableCapacity: 3, SupportedJobTypes: []string{"fail"}},
	}
	schedulers := map[string]Scheduler{
		"round robin":       NewRoundRobin(),
		"least loaded":      NewLeastLoaded(),
		"weighted capacity": NewWeightedCapacity(rand.New(rand.NewSource(1))),
	}

	for name, scheduler := range schedulers {
		scheduler := scheduler
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := scheduler.SelectWorker(job, workers)
			if !errors.Is(err, ErrNoEligibleWorker) {
				t.Fatalf("SelectWorker() error = %v, want %v", err, ErrNoEligibleWorker)
			}
		})
	}
}

func TestRoundRobinIsDeterministicAndDoesNotMutateInput(t *testing.T) {
	t.Parallel()

	scheduler := NewRoundRobin()
	job := jobs.Job{Type: "sleep"}
	workers := []WorkerSnapshot{
		eligibleWorker("worker-c", 1),
		eligibleWorker("worker-a", 1),
		eligibleWorker("worker-b", 1),
	}
	want := []string{"worker-a", "worker-b", "worker-c", "worker-a", "worker-b"}
	for i, workerID := range want {
		worker, err := scheduler.SelectWorker(job, workers)
		if err != nil {
			t.Fatalf("selection %d: %v", i, err)
		}
		if worker.WorkerID != workerID {
			t.Fatalf("selection %d = %q, want %q", i, worker.WorkerID, workerID)
		}
	}
	if workers[0].WorkerID != "worker-c" || workers[1].WorkerID != "worker-a" || workers[2].WorkerID != "worker-b" {
		t.Fatalf("SelectWorker mutated input order: %#v", workers)
	}
}

func TestRoundRobinConcurrentSelectionsAreEven(t *testing.T) {
	t.Parallel()

	const calls = 300
	scheduler := NewRoundRobin()
	workers := []WorkerSnapshot{
		eligibleWorker("worker-a", 1),
		eligibleWorker("worker-b", 1),
		eligibleWorker("worker-c", 1),
	}
	counts := make(map[string]int)
	var countsMu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker, err := scheduler.SelectWorker(jobs.Job{Type: "sleep"}, workers)
			if err != nil {
				t.Errorf("SelectWorker() error = %v", err)
				return
			}
			countsMu.Lock()
			counts[worker.WorkerID]++
			countsMu.Unlock()
		}()
	}
	wg.Wait()

	for _, workerID := range []string{"worker-a", "worker-b", "worker-c"} {
		if counts[workerID] != calls/len(workers) {
			t.Errorf("%s selected %d times, want %d", workerID, counts[workerID], calls/len(workers))
		}
	}
}

func TestLeastLoadedUsesFreeCapacityAndWorkerIDTieBreak(t *testing.T) {
	t.Parallel()

	scheduler := NewLeastLoaded()
	job := jobs.Job{Type: "sleep"}
	tests := []struct {
		name    string
		workers []WorkerSnapshot
		want    string
	}{
		{
			name: "greatest free capacity",
			workers: []WorkerSnapshot{
				eligibleWorker("small", 2),
				eligibleWorker("large", 7),
				eligibleWorker("medium", 4),
			},
			want: "large",
		},
		{
			name: "worker ID breaks tie regardless of input order",
			workers: []WorkerSnapshot{
				eligibleWorker("worker-z", 4),
				eligibleWorker("worker-a", 4),
			},
			want: "worker-a",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			worker, err := scheduler.SelectWorker(job, test.workers)
			if err != nil {
				t.Fatal(err)
			}
			if worker.WorkerID != test.want {
				t.Fatalf("SelectWorker() = %q, want %q", worker.WorkerID, test.want)
			}
		})
	}
}

func TestWeightedCapacityUsesCapacityRangesInStableOrder(t *testing.T) {
	t.Parallel()

	random := &sequenceIntn{values: []int{0, 1, 2, 5}}
	scheduler := NewWeightedCapacity(random)
	workers := []WorkerSnapshot{
		eligibleWorker("worker-b", 4),
		eligibleWorker("worker-a", 2),
	}
	want := []string{"worker-a", "worker-a", "worker-b", "worker-b"}
	for i, workerID := range want {
		worker, err := scheduler.SelectWorker(jobs.Job{Type: "sleep"}, workers)
		if err != nil {
			t.Fatalf("selection %d: %v", i, err)
		}
		if worker.WorkerID != workerID {
			t.Fatalf("selection %d = %q, want %q", i, worker.WorkerID, workerID)
		}
	}
	if random.boundsCalls != 4 {
		t.Fatalf("Intn called %d times, want 4", random.boundsCalls)
	}
	for i, bound := range random.bounds {
		if bound != 6 {
			t.Errorf("Intn bound %d = %d, want 6", i, bound)
		}
	}
}

func TestWeightedCapacityApproximatesCapacityRatio(t *testing.T) {
	t.Parallel()

	scheduler := NewWeightedCapacity(rand.New(rand.NewSource(42)))
	workers := []WorkerSnapshot{
		eligibleWorker("one", 1),
		eligibleWorker("three", 3),
	}
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		worker, err := scheduler.SelectWorker(jobs.Job{Type: "sleep"}, workers)
		if err != nil {
			t.Fatal(err)
		}
		counts[worker.WorkerID]++
	}
	if counts["one"] < 2350 || counts["one"] > 2650 {
		t.Fatalf("capacity-1 worker selected %d times, want approximately 2500", counts["one"])
	}
}

func TestWeightedCapacityZeroValueIsUsable(t *testing.T) {
	t.Parallel()

	var scheduler WeightedCapacity
	worker, err := scheduler.SelectWorker(jobs.Job{Type: "sleep"}, []WorkerSnapshot{eligibleWorker("only", 1)})
	if err != nil {
		t.Fatal(err)
	}
	if worker.WorkerID != "only" {
		t.Fatalf("SelectWorker() = %q, want only", worker.WorkerID)
	}
}

func TestWeightedCapacityRejectsBrokenRandomSource(t *testing.T) {
	t.Parallel()

	scheduler := NewWeightedCapacity(&sequenceIntn{values: []int{2}})
	_, err := scheduler.SelectWorker(jobs.Job{Type: "sleep"}, []WorkerSnapshot{eligibleWorker("only", 2)})
	if !errors.Is(err, ErrNoEligibleWorker) {
		t.Fatalf("SelectWorker() error = %v, want %v", err, ErrNoEligibleWorker)
	}
}

func TestEligibilityReturnsIndependentSortedSlice(t *testing.T) {
	t.Parallel()

	workers := []WorkerSnapshot{
		eligibleWorker("worker-b", 1),
		eligibleWorker("worker-a", 1),
	}
	got := eligibleWorkers(jobs.Job{Type: "sleep"}, workers)
	if len(got) != 2 || got[0].WorkerID != "worker-a" || got[1].WorkerID != "worker-b" {
		t.Fatalf("eligibleWorkers() = %#v", got)
	}
	got[0].WorkerID = "changed"
	if workers[1].WorkerID != "worker-a" {
		t.Fatal("eligibleWorkers shares its slice with the caller")
	}
}

func eligibleWorker(workerID string, capacity int) WorkerSnapshot {
	return WorkerSnapshot{
		WorkerID:          workerID,
		MaxConcurrency:    capacity,
		SupportedJobTypes: []string{"sleep"},
		AvailableCapacity: capacity,
		Health:            HealthHealthy,
	}
}

type sequenceIntn struct {
	values      []int
	next        int
	bounds      []int
	boundsCalls int
}

func (s *sequenceIntn) Intn(bound int) int {
	s.boundsCalls++
	s.bounds = append(s.bounds, bound)
	value := s.values[s.next]
	s.next++
	return value
}
