package security

import (
	"context"
	"crypto/x509"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/url"
	"testing"
)

type contextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s contextStream) Context() context.Context { return s.ctx }
func TestStreamingAndAmbiguousIdentityFailClosed(t *testing.T) {
	for _, tc := range []struct {
		identity, method string
		want             codes.Code
	}{
		{"urn:forgeq:monitor:m", "/grpc.health.v1.Health/Watch", codes.OK},
		{"urn:forgeq:monitor:m", "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo", codes.PermissionDenied},
		{"urn:forgeq:api:a", "/forgequeue.worker.v1.WorkerCoordinator/ClaimJob", codes.PermissionDenied},
		{"invalid", "/grpc.health.v1.Health/Watch", codes.Unauthenticated},
	} {
		called := false
		err := StreamAuthorization(nil, contextStream{ctx: identityContext(tc.identity)}, &grpc.StreamServerInfo{FullMethod: tc.method}, func(any, grpc.ServerStream) error { called = true; return nil })
		if status.Code(err) != tc.want || called != (tc.want == codes.OK) {
			t.Fatalf("stream %s: %v", tc.method, err)
		}
	}
	u, _ := url.Parse("urn:forgeq:api:a")
	v, _ := url.Parse("urn:forgeq:worker:w")
	for _, uris := range [][]*url.URL{nil, {u, u}, {u, v}} {
		if _, err := CertificateIdentity(&x509.Certificate{URIs: uris}); err == nil {
			t.Fatal("ambiguous/missing identity accepted")
		}
	}
}
