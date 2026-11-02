package security

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"forgequeue/proto/workerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"net/url"
	"testing"
)

func identityContext(identity string) context.Context {
	u, _ := url.Parse(identity)
	cert := &x509.Certificate{URIs: []*url.URL{u}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}}})
}
func TestAuthorizationMatrix(t *testing.T) {
	prefix := "/forgequeue.worker.v1.WorkerCoordinator/"
	for _, tc := range []struct {
		identity, method, id string
		want                 codes.Code
	}{
		{"urn:forgeq:api:a", "ListWorkers", "", codes.OK},
		{"urn:forgeq:api:a", "RegisterWorker", "w", codes.PermissionDenied},
		{"urn:forgeq:worker:w", "RegisterWorker", "w", codes.OK},
		{"urn:forgeq:worker:w", "RegisterWorker", "other", codes.PermissionDenied},
		{"urn:forgeq:worker:w", "ListWorkers", "", codes.PermissionDenied},
		{"urn:forgeq:monitor:m", "ListWorkers", "", codes.PermissionDenied},
		{"urn:forgeq:api:a", "Unknown", "", codes.PermissionDenied},
		{"urn:forgeq:api:bad/id", "ListWorkers", "", codes.Unauthenticated},
		{"fq_tenant_key", "ListWorkers", "", codes.Unauthenticated},
	} {
		err := authorizeMethod(identityContext(tc.identity), prefix+tc.method, &workerpb.RegisterWorkerRequest{WorkerId: tc.id})
		if status.Code(err) != tc.want {
			t.Errorf("%+v: %v", tc, err)
		}
	}
	for _, role := range []string{"api", "worker", "monitor"} {
		if err := authorizeMethod(identityContext("urn:forgeq:"+role+":id"), "/grpc.health.v1.Health/Watch", nil); err != nil {
			t.Fatal(err)
		}
	}
	if status.Code(authorizeMethod(context.Background(), prefix+"ListWorkers", nil)) != codes.Unauthenticated {
		t.Fatal("unverified peer accepted")
	}
}
