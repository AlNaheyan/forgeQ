package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type leaseRenewalBrokerFunc func(context.Context, string, string) (time.Time, error)

func (f leaseRenewalBrokerFunc) RenewLease(ctx context.Context, workerID, jobID string, _ int64) (time.Time, error) {
	return f(ctx, workerID, jobID)
}

func TestLeaseRenewerRenewsUntilStopped(t *testing.T) {
	calls := make(chan struct{}, 4)
	broker := leaseRenewalBrokerFunc(func(_ context.Context, workerID, jobID string) (time.Time, error) {
		if workerID != "worker-1" || jobID != "job-1" {
			t.Errorf("RenewLease(%q, %q), want worker-1, job-1", workerID, jobID)
		}
		select {
		case calls <- struct{}{}:
		default:
		}
		return time.Now().Add(time.Minute), nil
	})
	renewer, err := NewLeaseRenewer(broker, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("NewLeaseRenewer() error = %v", err)
	}

	session, err := renewer.Start(context.Background(), "worker-1", "job-1", 1)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	for range 2 {
		select {
		case <-calls:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for lease renewal")
		}
	}

	if err := session.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := session.Stop(); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
	select {
	case <-session.Done():
	default:
		t.Fatal("Done() remained open after Stop")
	}
	if !errors.Is(session.Context().Err(), context.Canceled) {
		t.Fatalf("Context().Err() = %v, want context.Canceled", session.Context().Err())
	}
	for len(calls) > 0 {
		<-calls
	}

	select {
	case <-calls:
		t.Fatal("renewal continued after Stop")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestLeaseRenewerCancelsHandlerOnLeaseLoss(t *testing.T) {
	renewalFailure := errors.New("lease belongs to another worker")
	broker := leaseRenewalBrokerFunc(func(context.Context, string, string) (time.Time, error) {
		return time.Time{}, renewalFailure
	})
	renewer, err := NewLeaseRenewer(broker, time.Millisecond)
	if err != nil {
		t.Fatalf("NewLeaseRenewer() error = %v", err)
	}
	session, err := renewer.Start(context.Background(), "worker-1", "job-1", 1)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case <-session.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("handler context was not canceled after renewal failure")
	}
	select {
	case <-session.Done():
	case <-time.After(time.Second):
		t.Fatal("renewal goroutine did not exit after lease loss")
	}

	err = session.Stop()
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Stop() error = %v, want ErrLeaseLost", err)
	}
	if !errors.Is(err, renewalFailure) {
		t.Fatalf("Stop() error = %v, want wrapped renewal failure", err)
	}
}

func TestLeaseRenewerRejectsExpiredRenewal(t *testing.T) {
	broker := leaseRenewalBrokerFunc(func(context.Context, string, string) (time.Time, error) {
		return time.Unix(99, 0), nil
	})
	renewer, err := NewLeaseRenewer(broker, time.Millisecond)
	if err != nil {
		t.Fatalf("NewLeaseRenewer() error = %v", err)
	}
	renewer.now = func() time.Time { return time.Unix(100, 0) }
	session, err := renewer.Start(context.Background(), "worker-1", "job-1", 1)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case <-session.Done():
	case <-time.After(time.Second):
		t.Fatal("renewal goroutine did not reject expired lease")
	}
	if err := session.Stop(); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Stop() error = %v, want ErrLeaseLost", err)
	}
}

func TestLeaseRenewerParentCancellationStopsInflightRenewal(t *testing.T) {
	started := make(chan struct{})
	exited := make(chan struct{})
	broker := leaseRenewalBrokerFunc(func(ctx context.Context, _, _ string) (time.Time, error) {
		close(started)
		<-ctx.Done()
		close(exited)
		return time.Time{}, ctx.Err()
	})
	renewer, err := NewLeaseRenewer(broker, time.Millisecond)
	if err != nil {
		t.Fatalf("NewLeaseRenewer() error = %v", err)
	}
	parent, cancel := context.WithCancel(context.Background())
	session, err := renewer.Start(parent, "worker-1", "job-1", 1)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("renewal call did not start")
	}
	cancel()
	if err := session.Stop(); err != nil {
		t.Fatalf("Stop() error after parent cancellation = %v", err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("in-flight renewal did not observe cancellation")
	}
}

func TestLeaseRenewerValidation(t *testing.T) {
	broker := leaseRenewalBrokerFunc(func(context.Context, string, string) (time.Time, error) {
		return time.Now().Add(time.Minute), nil
	})
	if _, err := NewLeaseRenewer(nil, time.Second); err == nil {
		t.Fatal("NewLeaseRenewer(nil) error = nil")
	}
	if _, err := NewLeaseRenewer(broker, 0); err == nil {
		t.Fatal("NewLeaseRenewer(interval=0) error = nil")
	}
	renewer, err := NewLeaseRenewer(broker, time.Second)
	if err != nil {
		t.Fatalf("NewLeaseRenewer() error = %v", err)
	}

	tests := []struct {
		name     string
		ctx      context.Context
		workerID string
		jobID    string
	}{
		{name: "nil context", workerID: "worker", jobID: "job"},
		{name: "missing worker", ctx: context.Background(), jobID: "job"},
		{name: "missing job", ctx: context.Background(), workerID: "worker"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := renewer.Start(test.ctx, test.workerID, test.jobID, 1); err == nil {
				t.Fatal("Start() error = nil")
			}
		})
	}
}

func TestLeaseRenewerStopPreventsFutureCalls(t *testing.T) {
	var calls atomic.Int32
	broker := leaseRenewalBrokerFunc(func(context.Context, string, string) (time.Time, error) {
		calls.Add(1)
		return time.Now().Add(time.Minute), nil
	})
	renewer, err := NewLeaseRenewer(broker, time.Hour)
	if err != nil {
		t.Fatalf("NewLeaseRenewer() error = %v", err)
	}
	session, err := renewer.Start(context.Background(), "worker-1", "job-1", 1)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := session.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("renewal calls = %d, want 0", got)
	}
}
