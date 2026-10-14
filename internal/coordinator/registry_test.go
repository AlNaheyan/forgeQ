package coordinator

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func newTestRegistry(t *testing.T, clock Clock) *Registry {
	t.Helper()
	registry, err := NewRegistry(HealthThresholds{
		SuspectAfter:   10 * time.Second,
		UnhealthyAfter: 15 * time.Second,
	}, clock)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	return registry
}

func TestRegisterAndGetWorker(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	registry := newTestRegistry(t, clock)
	jobTypes := []string{"sleep", "compute"}

	snapshot, err := registry.Register(Registration{
		WorkerID:          "worker-2",
		MaxConcurrency:    4,
		SupportedJobTypes: jobTypes,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	jobTypes[0] = "changed"

	if snapshot.WorkerID != "worker-2" || snapshot.MaxConcurrency != 4 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if snapshot.ActiveJobs != 0 || snapshot.AvailableCapacity != 4 {
		t.Fatalf("unexpected capacity: %+v", snapshot)
	}
	if snapshot.Health != HealthHealthy || !snapshot.RegisteredAt.Equal(now) || !snapshot.LastHeartbeatAt.Equal(now) {
		t.Fatalf("unexpected initial liveness: %+v", snapshot)
	}
	if want := []string{"compute", "sleep"}; !reflect.DeepEqual(snapshot.SupportedJobTypes, want) {
		t.Fatalf("job types = %v, want %v", snapshot.SupportedJobTypes, want)
	}

	snapshot.SupportedJobTypes[0] = "mutated"
	stored, ok := registry.Get("worker-2")
	if !ok {
		t.Fatal("registered worker not found")
	}
	if want := []string{"compute", "sleep"}; !reflect.DeepEqual(stored.SupportedJobTypes, want) {
		t.Fatalf("registry leaked its job type slice: %v", stored.SupportedJobTypes)
	}
	if _, ok := registry.Get("missing"); ok {
		t.Fatal("missing worker was found")
	}
}

func TestRegisterRejectsInvalidInput(t *testing.T) {
	registry := newTestRegistry(t, &fakeClock{now: time.Now()})
	tests := []struct {
		name         string
		registration Registration
	}{
		{name: "empty ID", registration: Registration{MaxConcurrency: 1, SupportedJobTypes: []string{"sleep"}}},
		{name: "padded ID", registration: Registration{WorkerID: " worker", MaxConcurrency: 1, SupportedJobTypes: []string{"sleep"}}},
		{name: "zero concurrency", registration: Registration{WorkerID: "worker", SupportedJobTypes: []string{"sleep"}}},
		{name: "no job types", registration: Registration{WorkerID: "worker", MaxConcurrency: 1}},
		{name: "empty job type", registration: Registration{WorkerID: "worker", MaxConcurrency: 1, SupportedJobTypes: []string{""}}},
		{name: "padded job type", registration: Registration{WorkerID: "worker", MaxConcurrency: 1, SupportedJobTypes: []string{" sleep"}}},
		{name: "duplicate job type", registration: Registration{WorkerID: "worker", MaxConcurrency: 1, SupportedJobTypes: []string{"sleep", "sleep"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := registry.Register(test.registration)
			if !errors.Is(err, ErrInvalidRegistration) {
				t.Fatalf("error = %v, want ErrInvalidRegistration", err)
			}
		})
	}
}

func TestHeartbeatUpdatesStateAndHealthTransitions(t *testing.T) {
	start := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: start}
	registry := newTestRegistry(t, clock)
	_, err := registry.Register(Registration{WorkerID: "worker", MaxConcurrency: 4, SupportedJobTypes: []string{"sleep"}})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	clock.Advance(3 * time.Second)
	reportedAt := start.Add(2 * time.Second)
	snapshot, err := registry.RecordHeartbeat(Heartbeat{
		WorkerID:       "worker",
		ActiveJobs:     2,
		MaxConcurrency: 5,
		Timestamp:      reportedAt,
	})
	if err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}
	if snapshot.ActiveJobs != 2 || snapshot.MaxConcurrency != 5 || snapshot.AvailableCapacity != 3 {
		t.Fatalf("unexpected capacity after heartbeat: %+v", snapshot)
	}
	if !snapshot.ReportedAt.Equal(reportedAt) || !snapshot.LastHeartbeatAt.Equal(start.Add(3*time.Second)) {
		t.Fatalf("heartbeat timestamps not retained: %+v", snapshot)
	}

	clock.Advance(10*time.Second - time.Nanosecond)
	assertWorkerHealth(t, registry, HealthHealthy)
	clock.Advance(time.Nanosecond)
	assertWorkerHealth(t, registry, HealthSuspect)
	clock.Advance(5*time.Second - time.Nanosecond)
	assertWorkerHealth(t, registry, HealthSuspect)
	clock.Advance(time.Nanosecond)
	assertWorkerHealth(t, registry, HealthUnhealthy)

	current := clock.Now()
	_, err = registry.RecordHeartbeat(Heartbeat{WorkerID: "worker", MaxConcurrency: 5, Timestamp: current})
	if err != nil {
		t.Fatalf("record recovery heartbeat: %v", err)
	}
	assertWorkerHealth(t, registry, HealthHealthy)
}

func TestHeartbeatRejectsInvalidInput(t *testing.T) {
	registry := newTestRegistry(t, &fakeClock{now: time.Now()})
	_, err := registry.Register(Registration{WorkerID: "worker", MaxConcurrency: 2, SupportedJobTypes: []string{"sleep"}})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	now := time.Now()
	tests := []struct {
		name      string
		heartbeat Heartbeat
		want      error
	}{
		{name: "unknown worker", heartbeat: Heartbeat{WorkerID: "missing", MaxConcurrency: 1, Timestamp: now}, want: ErrWorkerNotFound},
		{name: "empty ID", heartbeat: Heartbeat{MaxConcurrency: 1, Timestamp: now}, want: ErrInvalidHeartbeat},
		{name: "padded ID", heartbeat: Heartbeat{WorkerID: " worker", MaxConcurrency: 1, Timestamp: now}, want: ErrInvalidHeartbeat},
		{name: "negative jobs", heartbeat: Heartbeat{WorkerID: "worker", ActiveJobs: -1, MaxConcurrency: 1, Timestamp: now}, want: ErrInvalidHeartbeat},
		{name: "jobs exceed capacity", heartbeat: Heartbeat{WorkerID: "worker", ActiveJobs: 2, MaxConcurrency: 1, Timestamp: now}, want: ErrInvalidHeartbeat},
		{name: "zero capacity", heartbeat: Heartbeat{WorkerID: "worker", Timestamp: now}, want: ErrInvalidHeartbeat},
		{name: "zero timestamp", heartbeat: Heartbeat{WorkerID: "worker", MaxConcurrency: 1}, want: ErrInvalidHeartbeat},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := registry.RecordHeartbeat(test.heartbeat)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestHeartbeatRejectsOutOfOrderTimestamp(t *testing.T) {
	start := time.Now()
	registry := newTestRegistry(t, &fakeClock{now: start})
	_, err := registry.Register(Registration{WorkerID: "worker", MaxConcurrency: 2, SupportedJobTypes: []string{"sleep"}})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err = registry.RecordHeartbeat(Heartbeat{WorkerID: "worker", ActiveJobs: 1, MaxConcurrency: 2, Timestamp: start})
	if err != nil {
		t.Fatalf("first heartbeat: %v", err)
	}
	_, err = registry.RecordHeartbeat(Heartbeat{WorkerID: "worker", MaxConcurrency: 2, Timestamp: start.Add(-time.Second)})
	if !errors.Is(err, ErrInvalidHeartbeat) {
		t.Fatalf("error = %v, want ErrInvalidHeartbeat", err)
	}
	stored, _ := registry.Get("worker")
	if stored.ActiveJobs != 1 {
		t.Fatalf("rejected heartbeat changed state: %+v", stored)
	}
}

func TestListIsDeterministic(t *testing.T) {
	registry := newTestRegistry(t, &fakeClock{now: time.Now()})
	for _, id := range []string{"worker-c", "worker-a", "worker-b"} {
		_, err := registry.Register(Registration{WorkerID: id, MaxConcurrency: 1, SupportedJobTypes: []string{"sleep"}})
		if err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	snapshots := registry.List()
	want := []string{"worker-a", "worker-b", "worker-c"}
	for index, id := range want {
		if snapshots[index].WorkerID != id {
			t.Fatalf("worker at index %d = %q, want %q", index, snapshots[index].WorkerID, id)
		}
	}
}

func TestReregisterStartsNewSession(t *testing.T) {
	start := time.Now()
	clock := &fakeClock{now: start}
	registry := newTestRegistry(t, clock)
	registration := Registration{WorkerID: "worker", MaxConcurrency: 2, SupportedJobTypes: []string{"sleep"}}
	if _, err := registry.Register(registration); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := registry.RecordHeartbeat(Heartbeat{WorkerID: "worker", ActiveJobs: 2, MaxConcurrency: 2, Timestamp: start}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if _, err := registry.TryReserve("worker"); !errors.Is(err, ErrWorkerAtCapacity) {
		t.Fatalf("reserve full worker: %v", err)
	}
	if _, err := registry.Release("worker"); err != nil {
		t.Fatalf("release heartbeat capacity: %v", err)
	}
	if _, err := registry.TryReserve("worker"); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := registry.ConfirmAssignment("worker"); err != nil {
		t.Fatalf("confirm assignment: %v", err)
	}
	clock.Advance(time.Second)

	snapshot, err := registry.Register(Registration{WorkerID: "worker", MaxConcurrency: 3, SupportedJobTypes: []string{"compute"}})
	if err != nil {
		t.Fatalf("reregister: %v", err)
	}
	if snapshot.ActiveJobs != 0 || snapshot.AvailableCapacity != 3 || !snapshot.ReportedAt.IsZero() {
		t.Fatalf("new session did not reset dynamic state: %+v", snapshot)
	}
	if snapshot.JobsAssignedTotal != 1 {
		t.Fatalf("assignment total = %d, want 1", snapshot.JobsAssignedTotal)
	}
	if !snapshot.RegisteredAt.Equal(start.Add(time.Second)) {
		t.Fatalf("registration time = %v", snapshot.RegisteredAt)
	}
}

func TestTryReserveAndRelease(t *testing.T) {
	registry := newTestRegistry(t, &fakeClock{now: time.Now()})
	if _, err := registry.Register(Registration{WorkerID: "worker", MaxConcurrency: 2, SupportedJobTypes: []string{"sleep"}}); err != nil {
		t.Fatalf("register: %v", err)
	}

	first, err := registry.TryReserve("worker")
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	if first.ActiveJobs != 1 || first.AvailableCapacity != 1 || first.JobsAssignedTotal != 0 {
		t.Fatalf("first reservation snapshot: %+v", first)
	}
	first, err = registry.ConfirmAssignment("worker")
	if err != nil {
		t.Fatalf("confirm first assignment: %v", err)
	}
	if first.JobsAssignedTotal != 1 {
		t.Fatalf("first confirmed assignment total = %d, want 1", first.JobsAssignedTotal)
	}
	second, err := registry.TryReserve("worker")
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if second.ActiveJobs != 2 || second.AvailableCapacity != 0 || second.JobsAssignedTotal != 1 {
		t.Fatalf("second reservation snapshot: %+v", second)
	}
	second, err = registry.ConfirmAssignment("worker")
	if err != nil {
		t.Fatalf("confirm second assignment: %v", err)
	}
	if second.JobsAssignedTotal != 2 {
		t.Fatalf("second confirmed assignment total = %d, want 2", second.JobsAssignedTotal)
	}
	if _, err := registry.TryReserve("worker"); !errors.Is(err, ErrWorkerAtCapacity) {
		t.Fatalf("third reserve error = %v, want ErrWorkerAtCapacity", err)
	}

	released, err := registry.Release("worker")
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released.ActiveJobs != 1 || released.AvailableCapacity != 1 || released.JobsAssignedTotal != 2 {
		t.Fatalf("released snapshot: %+v", released)
	}
	if _, err := registry.Release("worker"); err != nil {
		t.Fatalf("second release: %v", err)
	}
	empty, err := registry.Release("worker")
	if err != nil {
		t.Fatalf("idempotent empty release: %v", err)
	}
	if empty.ActiveJobs != 0 || empty.AvailableCapacity != 2 || empty.JobsAssignedTotal != 2 {
		t.Fatalf("empty release snapshot: %+v", empty)
	}
	if _, err := registry.Release("missing"); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("missing release error = %v, want ErrWorkerNotFound", err)
	}
}

func TestTryReserveRejectsMissingUnhealthyAndFullWorkers(t *testing.T) {
	start := time.Now()
	clock := &fakeClock{now: start}
	registry := newTestRegistry(t, clock)
	if _, err := registry.Register(Registration{WorkerID: "worker", MaxConcurrency: 1, SupportedJobTypes: []string{"sleep"}}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := registry.TryReserve("missing"); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("missing reserve error = %v, want ErrWorkerNotFound", err)
	}
	if _, err := registry.TryReserve("worker"); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := registry.TryReserve("worker"); !errors.Is(err, ErrWorkerAtCapacity) {
		t.Fatalf("full reserve error = %v, want ErrWorkerAtCapacity", err)
	}
	if _, err := registry.RollbackReservation("worker"); err != nil {
		t.Fatalf("rollback reservation: %v", err)
	}
	clock.Advance(15 * time.Second)
	if _, err := registry.TryReserve("worker"); !errors.Is(err, ErrWorkerUnhealthy) {
		t.Fatalf("unhealthy reserve error = %v, want ErrWorkerUnhealthy", err)
	}
}

func TestConfirmAssignmentRejectsMissingWorker(t *testing.T) {
	registry := newTestRegistry(t, &fakeClock{now: time.Now()})
	if _, err := registry.ConfirmAssignment("missing"); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("confirm missing worker error = %v, want ErrWorkerNotFound", err)
	}
}

func TestHeartbeatPreservesConcurrentReservations(t *testing.T) {
	start := time.Now()
	registry := newTestRegistry(t, ClockFunc(func() time.Time { return start }))
	if _, err := registry.Register(Registration{WorkerID: "worker", MaxConcurrency: 4, SupportedJobTypes: []string{"sleep"}}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := registry.TryReserve("worker"); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	// This heartbeat may have been produced immediately before the reservation.
	// The reserved slot must survive this reconciliation boundary.
	snapshot, err := registry.RecordHeartbeat(Heartbeat{
		WorkerID:       "worker",
		ActiveJobs:     0,
		MaxConcurrency: 4,
		Timestamp:      start,
	})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if snapshot.ActiveJobs != 1 || snapshot.AvailableCapacity != 3 {
		t.Fatalf("heartbeat erased reservation: %+v", snapshot)
	}

	// Once the worker reports the assignment, the heartbeat becomes the
	// authoritative count rather than adding the reservation a second time.
	snapshot, err = registry.RecordHeartbeat(Heartbeat{
		WorkerID:       "worker",
		ActiveJobs:     1,
		MaxConcurrency: 4,
		Timestamp:      start.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("second heartbeat: %v", err)
	}
	if snapshot.ActiveJobs != 1 || snapshot.AvailableCapacity != 3 {
		t.Fatalf("reservation was double counted: %+v", snapshot)
	}
}

func TestOlderJobReleaseDoesNotEraseNewReservation(t *testing.T) {
	start := time.Now()
	registry := newTestRegistry(t, ClockFunc(func() time.Time { return start }))
	if _, err := registry.Register(Registration{WorkerID: "worker", MaxConcurrency: 3, SupportedJobTypes: []string{"sleep"}}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := registry.RecordHeartbeat(Heartbeat{
		WorkerID:       "worker",
		ActiveJobs:     1,
		MaxConcurrency: 3,
		Timestamp:      start,
	}); err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}
	if _, err := registry.TryReserve("worker"); err != nil {
		t.Fatalf("reserve newer job: %v", err)
	}

	// The older reported job completes while the newer reservation has not yet
	// appeared in a worker heartbeat. Release must not consume that reservation.
	if _, err := registry.Release("worker"); err != nil {
		t.Fatalf("release older job: %v", err)
	}
	snapshot, err := registry.RecordHeartbeat(Heartbeat{
		WorkerID:       "worker",
		ActiveJobs:     1, // stale report still containing the older job
		MaxConcurrency: 3,
		Timestamp:      start.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("stale heartbeat: %v", err)
	}
	if snapshot.ActiveJobs != 2 || snapshot.AvailableCapacity != 1 {
		t.Fatalf("older release erased newer reservation: %+v", snapshot)
	}

	// The next heartbeat observes the new job and removes the conservative
	// overlap with the completed old job.
	snapshot, err = registry.RecordHeartbeat(Heartbeat{
		WorkerID:       "worker",
		ActiveJobs:     1,
		MaxConcurrency: 3,
		Timestamp:      start.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("fresh heartbeat: %v", err)
	}
	if snapshot.ActiveJobs != 1 || snapshot.AvailableCapacity != 2 {
		t.Fatalf("fresh heartbeat did not reconcile capacity: %+v", snapshot)
	}
}

func TestRollbackReservationRemovesFreshCapacity(t *testing.T) {
	start := time.Now()
	registry := newTestRegistry(t, ClockFunc(func() time.Time { return start }))
	if _, err := registry.Register(Registration{WorkerID: "worker", MaxConcurrency: 2, SupportedJobTypes: []string{"sleep"}}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := registry.TryReserve("worker"); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	snapshot, err := registry.RollbackReservation("worker")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if snapshot.ActiveJobs != 0 || snapshot.AvailableCapacity != 2 || snapshot.JobsAssignedTotal != 0 {
		t.Fatalf("rollback snapshot: %+v", snapshot)
	}
	snapshot, err = registry.RecordHeartbeat(Heartbeat{
		WorkerID:       "worker",
		ActiveJobs:     0,
		MaxConcurrency: 2,
		Timestamp:      start,
	})
	if err != nil {
		t.Fatalf("heartbeat after rollback: %v", err)
	}
	if snapshot.ActiveJobs != 0 || snapshot.AvailableCapacity != 2 {
		t.Fatalf("rolled-back reservation survived heartbeat: %+v", snapshot)
	}
	if _, err := registry.RollbackReservation("missing"); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("missing rollback error = %v, want ErrWorkerNotFound", err)
	}
}

func TestConcurrentReservationsNeverOversubscribe(t *testing.T) {
	registry := newTestRegistry(t, &fakeClock{now: time.Now()})
	const capacity = 8
	if _, err := registry.Register(Registration{WorkerID: "worker", MaxConcurrency: capacity, SupportedJobTypes: []string{"sleep"}}); err != nil {
		t.Fatalf("register: %v", err)
	}

	const contenders = 256
	start := make(chan struct{})
	results := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := registry.TryReserve("worker")
			if err == nil {
				_, err = registry.ConfirmAssignment("worker")
			}
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	succeeded := 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrWorkerAtCapacity):
		default:
			t.Fatalf("unexpected reservation error: %v", err)
		}
	}
	if succeeded != capacity {
		t.Fatalf("successful reservations = %d, want %d", succeeded, capacity)
	}
	snapshot, _ := registry.Get("worker")
	if snapshot.ActiveJobs != capacity || snapshot.AvailableCapacity != 0 || snapshot.JobsAssignedTotal != capacity {
		t.Fatalf("final capacity: %+v", snapshot)
	}
}

func TestRegistryConcurrentAccess(t *testing.T) {
	start := time.Now()
	registry := newTestRegistry(t, ClockFunc(func() time.Time { return start }))
	const workerCount = 32

	var registrations sync.WaitGroup
	for index := 0; index < workerCount; index++ {
		registrations.Add(1)
		go func(index int) {
			defer registrations.Done()
			id := fmt.Sprintf("worker-%02d", index)
			if _, err := registry.Register(Registration{WorkerID: id, MaxConcurrency: 4, SupportedJobTypes: []string{"sleep"}}); err != nil {
				t.Errorf("register %s: %v", id, err)
			}
		}(index)
	}
	registrations.Wait()

	var accesses sync.WaitGroup
	for index := 0; index < workerCount; index++ {
		accesses.Add(2)
		go func(index int) {
			defer accesses.Done()
			id := fmt.Sprintf("worker-%02d", index)
			for sequence := 0; sequence < 100; sequence++ {
				_, err := registry.RecordHeartbeat(Heartbeat{
					WorkerID:       id,
					ActiveJobs:     sequence % 4,
					MaxConcurrency: 4,
					Timestamp:      start.Add(time.Duration(sequence) * time.Second),
				})
				if err != nil {
					t.Errorf("heartbeat %s: %v", id, err)
					return
				}
			}
		}(index)
		go func() {
			defer accesses.Done()
			for sequence := 0; sequence < 100; sequence++ {
				_ = registry.List()
			}
		}()
	}
	accesses.Wait()

	if got := len(registry.List()); got != workerCount {
		t.Fatalf("worker count = %d, want %d", got, workerCount)
	}
}

func TestNewRegistryRejectsInvalidThresholds(t *testing.T) {
	tests := []HealthThresholds{
		{},
		{SuspectAfter: -time.Second, UnhealthyAfter: time.Second},
		{SuspectAfter: time.Second, UnhealthyAfter: time.Second},
		{SuspectAfter: 2 * time.Second, UnhealthyAfter: time.Second},
	}
	for _, thresholds := range tests {
		if _, err := NewRegistry(thresholds, nil); err == nil {
			t.Fatalf("NewRegistry(%+v) succeeded", thresholds)
		}
	}
}

func assertWorkerHealth(t *testing.T, registry *Registry, want Health) {
	t.Helper()
	snapshot, ok := registry.Get("worker")
	if !ok {
		t.Fatal("worker not found")
	}
	if snapshot.Health != want {
		t.Fatalf("health = %q, want %q", snapshot.Health, want)
	}
}
