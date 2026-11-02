package security

import (
	"context"
	"net"
	"testing"
	"time"

	"forgequeue/proto/workerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type testCoordinator struct {
	workerpb.UnimplementedWorkerCoordinatorServer
}

func (testCoordinator) ListWorkers(context.Context, *workerpb.ListWorkersRequest) (*workerpb.ListWorkersResponse, error) {
	return &workerpb.ListWorkersResponse{}, nil
}
func (testCoordinator) RegisterWorker(context.Context, *workerpb.RegisterWorkerRequest) (*workerpb.RegisterWorkerResponse, error) {
	return &workerpb.RegisterWorkerResponse{}, nil
}
func TestVerifiedRPCPaths(t *testing.T) {
	ca := makeCA(t)
	sc, sk := ca.issue(t, "", time.Now().Add(time.Hour))
	serverTLS, err := ServiceTLS(ca.path, sc, sk, "", true)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)), grpc.UnaryInterceptor(UnaryAuthorization), grpc.StreamInterceptor(StreamAuthorization))
	workerpb.RegisterWorkerCoordinatorServer(server, testCoordinator{})
	go server.Serve(listener)
	defer server.Stop()
	for _, tc := range []struct {
		name, identity, serverName, worker string
		wrongCA                            bool
		want                               codes.Code
	}{
		{"API reader", "urn:forgeq:api:a", "localhost", "", false, codes.OK},
		{"worker registration", "urn:forgeq:worker:w", "localhost", "w", false, codes.OK},
		{"API mutation denied", "urn:forgeq:api:a", "localhost", "w", false, codes.PermissionDenied},
		{"worker spoof denied", "urn:forgeq:worker:w", "localhost", "other", false, codes.PermissionDenied},
		{"wrong DNS", "urn:forgeq:api:a", "wrong.invalid", "", false, codes.Unavailable},
		{"wrong CA", "urn:forgeq:api:a", "localhost", "", true, codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc, ck := ca.issue(t, tc.identity, time.Now().Add(time.Hour))
			root := ca.path
			if tc.wrongCA {
				root = makeCA(t).path
			}
			cfg, err := ServiceTLS(root, cc, ck, tc.serverName, false)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := grpc.NewClient("passthrough:///fixture", grpc.WithTransportCredentials(credentials.NewTLS(cfg)), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			client := workerpb.NewWorkerCoordinatorClient(conn)
			if tc.worker == "" {
				_, err = client.ListWorkers(ctx, &workerpb.ListWorkersRequest{})
			} else {
				_, err = client.RegisterWorker(ctx, &workerpb.RegisterWorkerRequest{WorkerId: tc.worker})
			}
			if status.Code(err) != tc.want {
				t.Fatalf("want %s got %v", tc.want, err)
			}
		})
	}
}
