package worker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"strings"
	"testing"
)

func TestRunRejectsInvalidTaskAndTransportConfiguration(t *testing.T) {
	h := func(context.Context, json.RawMessage) (any, error) { return nil, nil }
	base := Config{CoordinatorAddr: "localhost:9090", WorkerID: "worker-1", AllowInsecure: true, DevelopmentToken: strings.Repeat("x", 32)}
	for _, tc := range []struct {
		name  string
		cfg   Config
		tasks []Task
	}{
		{"no tasks", base, nil},
		{"nil handler", base, []Task{{Name: "example"}}},
		{"duplicate", base, []Task{{Name: "example", Handle: h}, {Name: "example", Handle: h}}},
		{"invalid name", base, []Task{{Name: "bad/type", Handle: h}}},
		{"invalid limit", base, []Task{{Name: "example", Handle: h, MaxPayloadBytes: 2 << 20}}},
		{"missing credentials", Config{CoordinatorAddr: "localhost:9090", WorkerID: "worker-1"}, []Task{{Name: "example", Handle: h}}},
		{"insecure TLS", Config{CoordinatorAddr: "localhost:9090", WorkerID: "worker-1", TLSConfig: &tls.Config{InsecureSkipVerify: true}}, []Task{{Name: "example", Handle: h}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := Run(context.Background(), tc.cfg, tc.tasks...); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestOperationKeyAndJSONValidation(t *testing.T) {
	m := Metadata{TenantID: "tenant", JobID: "job", Attempt: 1, LeaseGeneration: 2}
	first, err := m.OperationKey("effect")
	if err != nil {
		t.Fatal(err)
	}
	m.Attempt = 2
	m.LeaseGeneration = 3
	second, err := m.OperationKey("effect")
	if err != nil || first != second {
		t.Fatalf("unstable key: %q %q %v", first, second, err)
	}
	if err := ObjectPayload(json.RawMessage(`{"name":"invoice"}`)); err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{`null`, `[]`, `"string"`, `{`} {
		if ObjectPayload(json.RawMessage(input)) == nil {
			t.Fatalf("accepted %s", input)
		}
	}
}
