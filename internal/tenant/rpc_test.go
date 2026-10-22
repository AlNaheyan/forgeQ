package tenant

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
)

func TestRPCRequiresServiceCredential(t *testing.T) {
	t.Setenv("CONTROL_PLANE_TOKEN", "")
	if _, err := LoadRPCAuth(); err == nil {
		t.Fatal("empty service credential accepted")
	}
	token := strings.Repeat("service-test-", 4)
	t.Setenv("CONTROL_PLANE_TOKEN", token)
	auth, err := LoadRPCAuth()
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "Bearer fq_" + strings.Repeat("a", 64), "Bearer wrong", "Bearer " + token} {
		called := false
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", value))
		_, err := auth.Intercept(ctx, nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { called = true; return nil, nil })
		if value == "Bearer "+token {
			if err != nil || !called {
				t.Fatal("service credential rejected")
			}
		} else if status.Code(err) != codes.Unauthenticated || called {
			t.Fatal("unauthorized RPC accepted")
		}
	}
}
