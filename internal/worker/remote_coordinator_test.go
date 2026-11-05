package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type fakeCoordinatorRPC struct {
	mu                sync.Mutex
	registerFailures  int
	heartbeatFailures int
	rejectHeartbeat   bool
	loseRegistration  bool
	settings          RemoteSettings
	registrations     []RegistrationRequest
	heartbeats        []HeartbeatMessage
	onHeartbeat       func(int)
}

func (f *fakeCoordinatorRPC) RegisterWorker(_ context.Context, request RegistrationRequest) (RemoteSettings, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registrations = append(f.registrations, request)
	if len(f.registrations) <= f.registerFailures {
		return RemoteSettings{}, errors.New("coordinator unavailable")
	}
	return f.settings, nil
}

func (f *fakeCoordinatorRPC) Heartbeat(_ context.Context, heartbeat HeartbeatMessage) (bool, error) {
	f.mu.Lock()
	f.heartbeats = append(f.heartbeats, heartbeat)
	count := len(f.heartbeats)
	failure := count <= f.heartbeatFailures
	reject := f.rejectHeartbeat
	lost := f.loseRegistration && count == 1
	callback := f.onHeartbeat
	f.mu.Unlock()
	if callback != nil {
		callback(count)
	}
	if failure {
		return false, errors.New("coordinator unavailable")
	}
	if lost {
		return false, ErrWorkerNotRegistered
	}
	return !reject, nil
}

func TestHeartbeatReregistersAfterCoordinatorLosesWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rpc := &fakeCoordinatorRPC{loseRegistration: true, settings: RemoteSettings{HeartbeatInterval: time.Millisecond, LeaseDuration: time.Second}}
	rpc.onHeartbeat = func(count int) {
		if count == 2 {
			cancel()
		}
	}
	remote := newTestRemote(t, rpc, func() int { return 0 })
	if err := remote.RunHeartbeat(ctx, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	registrations, heartbeats := rpc.snapshot()
	if len(registrations) != 1 || len(heartbeats) != 2 {
		t.Fatalf("registrations=%d heartbeats=%d", len(registrations), len(heartbeats))
	}
}

func TestReregistrationRevalidatesCoordinatorLease(t *testing.T) {
	rpc := &fakeCoordinatorRPC{loseRegistration: true, settings: RemoteSettings{HeartbeatInterval: time.Millisecond, LeaseDuration: time.Second}}
	invalid := errors.New("lease no longer safe")
	checks := 0
	r, err := NewRemoteCoordinator(RemoteConfig{WorkerID: "worker", MaxConcurrency: 1, ValidateSettings: func(RemoteSettings) error {
		checks++
		if checks > 1 {
			return invalid
		}
		return nil
	}}, rpc, func() int { return 0 }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = r.RunHeartbeat(context.Background(), time.Millisecond); !errors.Is(err, invalid) {
		t.Fatalf("re-registration error=%v", err)
	}
	if checks != 2 {
		t.Fatalf("settings checks=%d", checks)
	}
}

func (f *fakeCoordinatorRPC) snapshot() ([]RegistrationRequest, []HeartbeatMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	registrations := append([]RegistrationRequest(nil), f.registrations...)
	heartbeats := append([]HeartbeatMessage(nil), f.heartbeats...)
	return registrations, heartbeats
}

func newTestRemote(t *testing.T, rpc CoordinatorRPC, activeJobs func() int) *RemoteCoordinator {
	t.Helper()
	remote, err := NewRemoteCoordinator(RemoteConfig{
		WorkerID:          "worker-test",
		MaxConcurrency:    4,
		SupportedJobTypes: []string{"sleep", "compute"},
		InitialRetryDelay: time.Millisecond,
		MaxRetryDelay:     2 * time.Millisecond,
		Now:               func() time.Time { return time.Unix(123, 0).UTC() },
	}, rpc, activeJobs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewRemoteCoordinator: %v", err)
	}
	return remote
}

func TestRemoteCoordinatorRegisterRetriesAndReportsCapacity(t *testing.T) {
	rpc := &fakeCoordinatorRPC{
		registerFailures: 2,
		settings: RemoteSettings{
			HeartbeatInterval: 5 * time.Second,
			LeaseDuration:     30 * time.Second,
		},
	}
	remote := newTestRemote(t, rpc, func() int { return 0 })

	settings, err := remote.Register(context.Background())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if settings != rpc.settings {
		t.Fatalf("settings = %+v, want %+v", settings, rpc.settings)
	}
	registrations, _ := rpc.snapshot()
	if len(registrations) != 3 {
		t.Fatalf("registration attempts = %d, want 3", len(registrations))
	}
	request := registrations[0]
	if request.WorkerID != "worker-test" || request.MaxConcurrency != 4 {
		t.Fatalf("registration = %+v", request)
	}
	if len(request.SupportedJobTypes) != 2 || request.SupportedJobTypes[0] != "sleep" {
		t.Fatalf("supported job types = %v", request.SupportedJobTypes)
	}
}

func TestRemoteCoordinatorRegisterStopsForPermanentFailure(t *testing.T) {
	rpc := &fakeCoordinatorRPC{registerFailures: 10}
	remote, err := NewRemoteCoordinator(RemoteConfig{
		WorkerID:       "worker-test",
		MaxConcurrency: 1,
		Retryable:      func(error) bool { return false },
	}, rpc, func() int { return 0 }, nil)
	if err != nil {
		t.Fatalf("NewRemoteCoordinator: %v", err)
	}
	if _, err := remote.Register(context.Background()); err == nil {
		t.Fatal("Register returned nil error")
	}
	registrations, _ := rpc.snapshot()
	if len(registrations) != 1 {
		t.Fatalf("registration attempts = %d, want 1", len(registrations))
	}
}

func TestRemoteCoordinatorHeartbeatReportsLoadAndRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rpc := &fakeCoordinatorRPC{heartbeatFailures: 2}
	rpc.onHeartbeat = func(count int) {
		if count == 4 {
			cancel()
		}
	}
	remote := newTestRemote(t, rpc, func() int { return 3 })

	if err := remote.RunHeartbeat(ctx, time.Millisecond); err != nil {
		t.Fatalf("RunHeartbeat: %v", err)
	}
	_, heartbeats := rpc.snapshot()
	if len(heartbeats) != 4 {
		t.Fatalf("heartbeat attempts = %d, want 4", len(heartbeats))
	}
	for _, heartbeat := range heartbeats {
		if heartbeat.WorkerID != "worker-test" || heartbeat.ActiveJobs != 3 || heartbeat.MaxConcurrency != 4 {
			t.Fatalf("heartbeat = %+v", heartbeat)
		}
		if !heartbeat.Timestamp.Equal(time.Unix(123, 0)) {
			t.Fatalf("heartbeat timestamp = %v", heartbeat.Timestamp)
		}
	}
}

func TestRemoteCoordinatorHeartbeatRejectionIsReturned(t *testing.T) {
	rpc := &fakeCoordinatorRPC{rejectHeartbeat: true}
	remote := newTestRemote(t, rpc, func() int { return 0 })
	if err := remote.RunHeartbeat(context.Background(), time.Second); !errors.Is(err, ErrHeartbeatRejected) {
		t.Fatalf("RunHeartbeat error = %v, want ErrHeartbeatRejected", err)
	}
}

func TestNewRemoteCoordinatorValidatesConfiguration(t *testing.T) {
	rpc := &fakeCoordinatorRPC{}
	tests := []struct {
		name       string
		config     RemoteConfig
		activeJobs func() int
	}{
		{name: "worker ID", config: RemoteConfig{MaxConcurrency: 1}, activeJobs: func() int { return 0 }},
		{name: "concurrency", config: RemoteConfig{WorkerID: "worker"}, activeJobs: func() int { return 0 }},
		{name: "active job counter", config: RemoteConfig{WorkerID: "worker", MaxConcurrency: 1}},
		{name: "retry bounds", config: RemoteConfig{WorkerID: "worker", MaxConcurrency: 1, InitialRetryDelay: time.Second, MaxRetryDelay: time.Millisecond}, activeJobs: func() int { return 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRemoteCoordinator(test.config, rpc, test.activeJobs, nil); err == nil {
				t.Fatal("NewRemoteCoordinator returned nil error")
			}
		})
	}
}
