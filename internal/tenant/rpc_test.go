package tenant

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"os"
	"path/filepath"
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

func TestRPCAuthSecretFileAndProductionGuard(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	t.Setenv("CONTROL_PLANE_TOKEN", "")
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(strings.Repeat("z", 32)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONTROL_PLANE_TOKEN_FILE", path)
	if _, err := LoadRPCAuth(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONTROL_PLANE_TOKEN", strings.Repeat("x", 32))
	if _, err := LoadRPCAuth(); err == nil {
		t.Fatal("conflicting sources accepted")
	}
	t.Setenv("APP_ENV", "production")
	if _, err := LoadRPCAuth(); err == nil {
		t.Fatal("production token mode accepted")
	}
}
