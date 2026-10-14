package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"forgequeue/proto/workerpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type fakeGeneratedCoordinatorClient struct {
	workerpb.WorkerCoordinatorClient
	register  func(*workerpb.RegisterWorkerRequest) (*workerpb.RegisterWorkerResponse, error)
	heartbeat func(*workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error)
}

func (f *fakeGeneratedCoordinatorClient) RegisterWorker(_ context.Context, request *workerpb.RegisterWorkerRequest, _ ...grpc.CallOption) (*workerpb.RegisterWorkerResponse, error) {
	return f.register(request)
}

func (f *fakeGeneratedCoordinatorClient) Heartbeat(_ context.Context, request *workerpb.HeartbeatRequest, _ ...grpc.CallOption) (*workerpb.HeartbeatResponse, error) {
	return f.heartbeat(request)
}

func TestGRPCCoordinatorRPCMapsRegistration(t *testing.T) {
	client := &fakeGeneratedCoordinatorClient{}
	client.register = func(request *workerpb.RegisterWorkerRequest) (*workerpb.RegisterWorkerResponse, error) {
		if request.GetWorkerId() != "worker-test" || request.GetMaxConcurrency() != 4 {
			t.Fatalf("registration request = %+v", request)
		}
		if len(request.GetSupportedJobTypes()) != 1 || request.GetSupportedJobTypes()[0] != "sleep" {
			t.Fatalf("supported job types = %v", request.GetSupportedJobTypes())
		}
		return &workerpb.RegisterWorkerResponse{
			HeartbeatInterval: durationpb.New(5 * time.Second),
			LeaseDuration:     durationpb.New(30 * time.Second),
		}, nil
	}
	client.heartbeat = func(*workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error) {
		return nil, errors.New("unexpected heartbeat")
	}

	settings, err := NewGRPCCoordinatorRPC(client).RegisterWorker(context.Background(), RegistrationRequest{
		WorkerID:          "worker-test",
		MaxConcurrency:    4,
		SupportedJobTypes: []string{"sleep"},
	})
	if err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if settings.HeartbeatInterval != 5*time.Second || settings.LeaseDuration != 30*time.Second {
		t.Fatalf("settings = %+v", settings)
	}
}

func TestGRPCCoordinatorRPCMapsHeartbeat(t *testing.T) {
	wantTime := time.Unix(123, 456).UTC()
	client := &fakeGeneratedCoordinatorClient{}
	client.register = func(*workerpb.RegisterWorkerRequest) (*workerpb.RegisterWorkerResponse, error) {
		return nil, errors.New("unexpected registration")
	}
	client.heartbeat = func(request *workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error) {
		if request.GetWorkerId() != "worker-test" || request.GetActiveJobs() != 2 || request.GetMaxConcurrency() != 4 {
			t.Fatalf("heartbeat request = %+v", request)
		}
		if got := request.GetTimestamp().AsTime(); !got.Equal(wantTime) {
			t.Fatalf("heartbeat timestamp = %v, want %v", got, wantTime)
		}
		return &workerpb.HeartbeatResponse{Accepted: true}, nil
	}

	accepted, err := NewGRPCCoordinatorRPC(client).Heartbeat(context.Background(), HeartbeatMessage{
		WorkerID:       "worker-test",
		ActiveJobs:     2,
		MaxConcurrency: 4,
		Timestamp:      wantTime,
	})
	if err != nil || !accepted {
		t.Fatalf("Heartbeat: accepted=%t err=%v", accepted, err)
	}
}

func TestGRPCCoordinatorRPCClassifiesErrors(t *testing.T) {
	if err := classifyGRPCError(status.Error(codes.Unavailable, "down")); !defaultRetryableCoordinatorError(err) {
		t.Fatal("Unavailable should be retryable")
	}
	if err := classifyGRPCError(status.Error(codes.InvalidArgument, "bad request")); defaultRetryableCoordinatorError(err) {
		t.Fatal("InvalidArgument should not be retryable")
	}
}
