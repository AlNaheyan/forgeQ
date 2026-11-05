package tenant

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"github.com/AlNaheyan/forgeQ/internal/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"os"
)

// RPCAuth is a service credential, never a tenant credential. Plaintext transport
// is development/test compatibility only; production uses verified mTLS.
type RPCAuth struct{ token string }

func LoadRPCAuth() (RPCAuth, error) {
	if os.Getenv("APP_ENV") == "production" {
		return RPCAuth{}, fmt.Errorf("token RPC authentication is development/test only")
	}
	token, err := security.Secret("CONTROL_PLANE_TOKEN", os.Getenv("CONTROL_PLANE_TOKEN"), os.Getenv("CONTROL_PLANE_TOKEN_FILE"))
	if err != nil {
		return RPCAuth{}, err
	}
	if len(token) < 32 {
		return RPCAuth{}, fmt.Errorf("CONTROL_PLANE_TOKEN must contain at least 32 bytes")
	}
	return RPCAuth{token: token}, nil
}
func (a RPCAuth) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + a.token}, nil
}
func (a RPCAuth) RequireTransportSecurity() bool { return false }
func (a RPCAuth) Intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	if err := a.authorize(ctx); err != nil {
		return nil, err
	}
	return next(ctx, req)
}
func (a RPCAuth) InterceptStream(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	if err := a.authorize(stream.Context()); err != nil {
		return err
	}
	return next(srv, stream)
}
func (a RPCAuth) authorize(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || len(a.token) < 32 {
		return status.Error(codes.Unauthenticated, "service credential required")
	}
	got := sha256.Sum256([]byte(values[0]))
	want := sha256.Sum256([]byte("Bearer " + a.token))
	if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
		return status.Error(codes.Unauthenticated, "service credential required")
	}
	return nil
}
