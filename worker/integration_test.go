package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/proto/workerpb"
	"github.com/AlNaheyan/forgeQ/worker"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type coordinatorFixture struct {
	workerpb.UnimplementedWorkerCoordinatorServer
	claimed    atomic.Bool
	completed  chan *workerpb.CompleteJobRequest
	registered chan []string
}

func (s *coordinatorFixture) RegisterWorker(_ context.Context, req *workerpb.RegisterWorkerRequest) (*workerpb.RegisterWorkerResponse, error) {
	s.registered <- req.GetSupportedJobTypes()
	return &workerpb.RegisterWorkerResponse{HeartbeatInterval: durationpb.New(time.Second), LeaseDuration: durationpb.New(time.Minute)}, nil
}
func (*coordinatorFixture) Heartbeat(context.Context, *workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error) {
	return &workerpb.HeartbeatResponse{Accepted: true}, nil
}
func (s *coordinatorFixture) ClaimJob(context.Context, *workerpb.ClaimJobRequest) (*workerpb.ClaimJobResponse, error) {
	if !s.claimed.CompareAndSwap(false, true) {
		return &workerpb.ClaimJobResponse{}, nil
	}
	return &workerpb.ClaimJobResponse{Job: fixtureJob()}, nil
}
func (*coordinatorFixture) StartJob(context.Context, *workerpb.StartJobRequest) (*workerpb.StartJobResponse, error) {
	return &workerpb.StartJobResponse{Job: fixtureJob()}, nil
}
func (s *coordinatorFixture) CompleteJob(_ context.Context, req *workerpb.CompleteJobRequest) (*workerpb.CompleteJobResponse, error) {
	s.completed <- req
	return &workerpb.CompleteJobResponse{}, nil
}
func fixtureJob() *workerpb.Job {
	now := timestamppb.Now()
	return &workerpb.Job{Id: "job-1", TenantId: "tenant-1", Type: "invoice.send", PayloadJson: []byte(`{"invoice_id":"inv-1"}`), AttemptCount: 1, LeaseGeneration: 2, ScheduledAt: now, CreatedAt: now, UpdatedAt: now}
}

func TestPublicWorkerRegistersRunsHandlerAndDrains(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	fixture := &coordinatorFixture{completed: make(chan *workerpb.CompleteJobRequest, 1), registered: make(chan []string, 1)}
	workerpb.RegisterWorkerCoordinatorServer(server, fixture)
	go server.Serve(listener)
	defer server.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx, worker.Config{CoordinatorAddr: listener.Addr().String(), WorkerID: "worker-1", Concurrency: 2, PollInterval: time.Millisecond, AllowInsecure: true, DevelopmentToken: strings.Repeat("x", 32), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, worker.Task{Name: "invoice.send", Handle: func(ctx context.Context, payload json.RawMessage) (any, error) {
			m, ok := worker.MetadataFromContext(ctx)
			if !ok || m.TenantID != "tenant-1" || m.JobID != "job-1" {
				t.Errorf("metadata=%+v ok=%v", m, ok)
			}
			return map[string]string{"invoice_id": "inv-1", "state": "sent"}, nil
		}})
	}()
	select {
	case names := <-fixture.registered:
		if len(names) != 1 || names[0] != "invoice.send" {
			t.Fatalf("registered=%v", names)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not register")
	}
	select {
	case result := <-fixture.completed:
		if string(result.GetResultJson()) != `{"invoice_id":"inv-1","state":"sent"}` {
			t.Fatalf("result=%s", result.GetResultJson())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not complete")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not drain")
	}
}
