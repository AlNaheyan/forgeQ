package jobtypes

import (
	"encoding/json"
	"testing"
)

func TestRegistryRejectsInvalidAndDuplicateDescriptors(t *testing.T) {
	valid := Descriptor{Name: "example", MaxPayloadBytes: 10, Validate: func(json.RawMessage) error { return nil }}
	for _, descriptors := range [][]Descriptor{{valid, valid}, {{Name: "missing-validator", MaxPayloadBytes: 1}}, {{Name: "bad-limit", MaxPayloadBytes: MaxPayloadBytes + 1, Validate: valid.Validate}}} {
		if _, err := NewRegistry(descriptors); err == nil {
			t.Fatal("accepted invalid descriptors")
		}
	}
}

func TestFixtureValidationAndUnknownType(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		valid         bool
	}{
		{"sleep", `{"duration_ms":15}`, true},
		{"sleep", `{"duration_ms":3600001}`, false},
		{"fail", `{"failures_before_success":-1}`, false},
		{"fail", `{}`, true},
		{"unknown", `{}`, false},
	} {
		if err := Fixtures().Validate(tc.name, json.RawMessage(tc.payload)); (err == nil) != tc.valid {
			t.Errorf("%s %s: %v", tc.name, tc.payload, err)
		}
	}
}
