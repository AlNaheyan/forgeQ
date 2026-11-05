package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AlNaheyan/forgeQ/proto/workerpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GRPCCoordinatorRPC adapts the generated worker coordinator client to the
// transport-neutral CoordinatorRPC interface.
type GRPCCoordinatorRPC struct {
	client  workerpb.WorkerCoordinatorClient
	timeout time.Duration
}

func (r *GRPCCoordinatorRPC) WithTimeout(timeout time.Duration) *GRPCCoordinatorRPC {
	r.timeout = timeout
	return r
}

func NewGRPCCoordinatorRPC(client workerpb.WorkerCoordinatorClient) *GRPCCoordinatorRPC {
	return &GRPCCoordinatorRPC{client: client}
}

func (r *GRPCCoordinatorRPC) RegisterWorker(ctx context.Context, registration RegistrationRequest) (RemoteSettings, error) {
	if r == nil || r.client == nil {
		return RemoteSettings{}, nonRetryableCoordinatorError(errors.New("gRPC coordinator client is required"))
	}
	ctx, cancel := boundedRPCContext(ctx, r.timeout)
	defer cancel()
	response, err := r.client.RegisterWorker(ctx, &workerpb.RegisterWorkerRequest{
		WorkerId:          registration.WorkerID,
		MaxConcurrency:    registration.MaxConcurrency,
		SupportedJobTypes: append([]string(nil), registration.SupportedJobTypes...),
	})
	if err != nil {
		return RemoteSettings{}, classifyGRPCError(err)
	}
	if response == nil {
		return RemoteSettings{}, nonRetryableCoordinatorError(errors.New("register worker returned an empty response"))
	}

	heartbeatInterval, err := checkedDuration(response.GetHeartbeatInterval(), "heartbeat interval")
	if err != nil {
		return RemoteSettings{}, nonRetryableCoordinatorError(err)
	}
	leaseDuration, err := checkedDuration(response.GetLeaseDuration(), "lease duration")
	if err != nil {
		return RemoteSettings{}, nonRetryableCoordinatorError(err)
	}
	return RemoteSettings{
		HeartbeatInterval: heartbeatInterval,
		LeaseDuration:     leaseDuration,
	}, nil
}

func (r *GRPCCoordinatorRPC) Heartbeat(ctx context.Context, heartbeat HeartbeatMessage) (bool, error) {
	if r == nil || r.client == nil {
		return false, nonRetryableCoordinatorError(errors.New("gRPC coordinator client is required"))
	}
	ctx, cancel := boundedRPCContext(ctx, r.timeout)
	defer cancel()
	timestamp := timestamppb.New(heartbeat.Timestamp)
	if err := timestamp.CheckValid(); err != nil {
		return false, nonRetryableCoordinatorError(fmt.Errorf("heartbeat timestamp: %w", err))
	}
	response, err := r.client.Heartbeat(ctx, &workerpb.HeartbeatRequest{
		WorkerId:       heartbeat.WorkerID,
		ActiveJobs:     heartbeat.ActiveJobs,
		MaxConcurrency: heartbeat.MaxConcurrency,
		Timestamp:      timestamp,
	})
	if err != nil {
		return false, classifyGRPCError(err)
	}
	if response == nil {
		return false, nonRetryableCoordinatorError(errors.New("heartbeat returned an empty response"))
	}
	return response.GetAccepted(), nil
}

type classifiedCoordinatorRPCError struct {
	err       error
	retryable bool
}

func (e classifiedCoordinatorRPCError) Error() string   { return e.err.Error() }
func (e classifiedCoordinatorRPCError) Unwrap() error   { return e.err }
func (e classifiedCoordinatorRPCError) Retryable() bool { return e.retryable }

func nonRetryableCoordinatorError(err error) error {
	return classifiedCoordinatorRPCError{err: err}
}

func classifyGRPCError(err error) error {
	if errors.Is(err, context.Canceled) {
		return err
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return classifiedCoordinatorRPCError{err: err, retryable: true}
	default:
		return nonRetryableCoordinatorError(err)
	}
}

func checkedDuration(value *durationpb.Duration, field string) (time.Duration, error) {
	if value == nil {
		return 0, fmt.Errorf("register worker response omitted %s", field)
	}
	if err := value.CheckValid(); err != nil {
		return 0, fmt.Errorf("register worker response has invalid %s: %w", field, err)
	}
	return value.AsDuration(), nil
}
