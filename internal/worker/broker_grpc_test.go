package worker

import (
	"context"
	"testing"
	"time"

	"forgequeue/proto/workerpb"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type renewalCoordinatorClient struct {
	workerpb.WorkerCoordinatorClient
	renew func(*workerpb.RenewLeaseRequest) (*workerpb.RenewLeaseResponse, error)
	fail  func(*workerpb.FailJobRequest) (*workerpb.FailJobResponse, error)
}

func (c *renewalCoordinatorClient) FailJob(
	_ context.Context,
	request *workerpb.FailJobRequest,
	_ ...grpc.CallOption,
) (*workerpb.FailJobResponse, error) {
	return c.fail(request)
}

func (c *renewalCoordinatorClient) RenewLease(
	_ context.Context,
	request *workerpb.RenewLeaseRequest,
	_ ...grpc.CallOption,
) (*workerpb.RenewLeaseResponse, error) {
	return c.renew(request)
}

func TestGRPCBrokerRenewsLease(t *testing.T) {
	wantExpiry := time.Now().Add(time.Minute).UTC().Truncate(time.Microsecond)
	client := &renewalCoordinatorClient{}
	client.renew = func(request *workerpb.RenewLeaseRequest) (*workerpb.RenewLeaseResponse, error) {
		if request.GetWorkerId() != "worker-1" || request.GetJobId() != "job-1" {
			t.Fatalf("renew request = %+v", request)
		}
		return &workerpb.RenewLeaseResponse{LeaseExpiresAt: timestamppb.New(wantExpiry)}, nil
	}
	broker, err := NewGRPCBroker(client)
	if err != nil {
		t.Fatalf("NewGRPCBroker: %v", err)
	}

	expiresAt, err := broker.RenewLease(context.Background(), "worker-1", "job-1")
	if err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	if !expiresAt.Equal(wantExpiry) {
		t.Fatalf("expiry = %v, want %v", expiresAt, wantExpiry)
	}
}

func TestGRPCBrokerRejectsMissingLeaseExpiry(t *testing.T) {
	client := &renewalCoordinatorClient{renew: func(*workerpb.RenewLeaseRequest) (*workerpb.RenewLeaseResponse, error) {
		return &workerpb.RenewLeaseResponse{}, nil
	}}
	broker, err := NewGRPCBroker(client)
	if err != nil {
		t.Fatalf("NewGRPCBroker: %v", err)
	}
	if _, err := broker.RenewLease(context.Background(), "worker-1", "job-1"); err == nil {
		t.Fatal("RenewLease error = nil, want missing expiry error")
	}
}

func TestGRPCBrokerPassesFailureClassification(t *testing.T) {
	client := &renewalCoordinatorClient{fail: func(request *workerpb.FailJobRequest) (*workerpb.FailJobResponse, error) {
		if request.GetWorkerId() != "worker-1" || request.GetJobId() != "job-1" || request.GetError() != "boom" || request.GetRetryable() {
			t.Fatalf("fail request = %+v", request)
		}
		return &workerpb.FailJobResponse{}, nil
	}}
	broker, err := NewGRPCBroker(client)
	if err != nil {
		t.Fatalf("NewGRPCBroker: %v", err)
	}
	if err := broker.FailJob(context.Background(), "worker-1", "job-1", "boom", false); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
}

func TestBoundedRPCContextAddsDeadline(t *testing.T) {
	ctx, cancel := boundedRPCContext(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("RPC context has no deadline")
	}
}
