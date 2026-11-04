package worker

import (
	"context"
	"testing"
)

func TestOperationKeyIsTenantScopedAndStableAcrossAttempts(t *testing.T) {
	m := ExecutionMetadata{TenantID: "tenant/a", JobID: "job-1", Attempt: 1, LeaseGeneration: 1}
	first, err := m.OperationKey("export")
	if err != nil {
		t.Fatal(err)
	}
	m.Attempt, m.LeaseGeneration = 2, 7
	second, err := m.OperationKey("export")
	if err != nil || first != second {
		t.Fatalf("key changed across attempts: %q %q %v", first, second, err)
	}
	m.TenantID = "tenant/b"
	third, err := m.OperationKey("export")
	if err != nil || third == first {
		t.Fatalf("tenant keys collide: %q %q %v", first, third, err)
	}
	ctx := withMetadata(context.Background(), m)
	got, ok := MetadataFromContext(ctx)
	if !ok || got != m {
		t.Fatalf("metadata = %+v, %v", got, ok)
	}
	if _, err := (ExecutionMetadata{}).OperationKey("export"); err == nil {
		t.Fatal("missing identity accepted")
	}
}
