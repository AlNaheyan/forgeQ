package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ErrLeaseLost marks a renewal failure after which a worker must stop using
// the job lease and must not report a terminal result for that lease.
var ErrLeaseLost = errors.New("job lease lost")

// leaseRenewalBroker is deliberately narrower than Broker so the renewal loop
// can be tested and reused independently of claiming and result persistence.
// Broker can satisfy it by adding a method with this signature.
type leaseRenewalBroker interface {
	RenewLease(context.Context, string, string, int64) (time.Time, error)
}

// LeaseRenewer periodically extends one running job lease. A renewal failure
// is treated as loss of ownership: the session context is canceled immediately
// so a cooperative handler can stop before producing more side effects.
type LeaseRenewer struct {
	broker   leaseRenewalBroker
	interval time.Duration
	now      func() time.Time
}

func NewLeaseRenewer(broker leaseRenewalBroker, interval time.Duration) (*LeaseRenewer, error) {
	if broker == nil {
		return nil, errors.New("lease renewal broker is required")
	}
	if interval <= 0 {
		return nil, errors.New("lease renewal interval must be greater than zero")
	}
	return &LeaseRenewer{
		broker:   broker,
		interval: interval,
		now:      time.Now,
	}, nil
}

// LeaseSession owns the renewal goroutine and the context passed to a handler.
// Call Stop after the handler returns; Stop is safe to call more than once and
// waits for the renewal goroutine to exit.
type LeaseSession struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu       sync.RWMutex
	err      error
	stopOnce sync.Once
}

// Start begins renewing a lease after each renewal interval. The existing
// lease is assumed to be active when Start is called.
func (r *LeaseRenewer) Start(parent context.Context, workerID, jobID string, generation int64) (*LeaseSession, error) {
	if parent == nil {
		return nil, errors.New("lease parent context is required")
	}
	workerID = strings.TrimSpace(workerID)
	jobID = strings.TrimSpace(jobID)
	if workerID == "" || jobID == "" {
		return nil, errors.New("worker ID and job ID are required to renew a lease")
	}

	ctx, cancel := context.WithCancel(parent)
	session := &LeaseSession{
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go r.run(session, workerID, jobID, generation)
	return session, nil
}

// Context returns the context a job handler should use. It is canceled when
// the parent is canceled, Stop is called, or lease renewal fails.
func (s *LeaseSession) Context() context.Context {
	return s.ctx
}

// Done closes after the renewal goroutine exits. When it closes unexpectedly,
// Err reports whether ownership was lost.
func (s *LeaseSession) Done() <-chan struct{} {
	return s.done
}

// Err returns the lease-loss error, if any. It intentionally does not return
// context.Canceled for an orderly Stop or parent cancellation.
func (s *LeaseSession) Err() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

// Stop cancels renewal and waits for the goroutine to exit. If the lease was
// already lost, Stop returns the lease-loss error.
func (s *LeaseSession) Stop() error {
	s.stopOnce.Do(func() {
		s.cancel()
		<-s.done
	})
	return s.Err()
}

func (r *LeaseRenewer) run(session *LeaseSession, workerID, jobID string, generation int64) {
	defer close(session.done)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-session.ctx.Done():
			return
		case <-ticker.C:
		}

		// Bound every call so a stuck transport cannot outlive the next
		// renewal opportunity. Stop and parent cancellation also cancel it.
		renewCtx, cancel := context.WithTimeout(session.ctx, r.interval)
		expiresAt, err := r.broker.RenewLease(renewCtx, workerID, jobID, generation)
		cancel()
		if err != nil {
			if session.ctx.Err() != nil {
				return
			}
			session.lose(fmt.Errorf("%w: renew job %q: %w", ErrLeaseLost, jobID, err))
			return
		}
		if !expiresAt.After(r.now()) {
			session.lose(fmt.Errorf("%w: renewal for job %q returned non-future expiry %s", ErrLeaseLost, jobID, expiresAt.UTC().Format(time.RFC3339Nano)))
			return
		}
	}
}

func (s *LeaseSession) lose(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	s.cancel()
}
