// Package jobtypes defines immutable job descriptors shared by submission and workers.
package jobtypes

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const MaxPayloadBytes = 1 << 20

type Descriptor struct {
	Name            string
	MaxPayloadBytes int
	Validate        func(json.RawMessage) error
}

type Registry struct{ descriptors map[string]Descriptor }

func NewRegistry(descriptors []Descriptor) (*Registry, error) {
	r := &Registry{descriptors: make(map[string]Descriptor, len(descriptors))}
	for _, d := range descriptors {
		if d.Name == "" || d.Name != strings.TrimSpace(d.Name) || d.Validate == nil || d.MaxPayloadBytes < 1 || d.MaxPayloadBytes > MaxPayloadBytes {
			return nil, errors.New("invalid job type descriptor")
		}
		if _, exists := r.descriptors[d.Name]; exists {
			return nil, fmt.Errorf("duplicate job type %q", d.Name)
		}
		r.descriptors[d.Name] = d
	}
	return r, nil
}

func (r *Registry) Lookup(name string) (Descriptor, bool) {
	if r == nil {
		return Descriptor{}, false
	}
	d, ok := r.descriptors[name]
	return d, ok
}

func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.descriptors))
	for name := range r.descriptors {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Registry) Validate(name string, payload json.RawMessage) error {
	d, ok := r.Lookup(name)
	if !ok {
		return fmt.Errorf("unsupported job type %q", name)
	}
	if len(payload) > d.MaxPayloadBytes {
		return errors.New("payload exceeds job type limit")
	}
	return d.Validate(payload)
}

func fixtureObject(payload json.RawMessage) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil || obj == nil {
		return errors.New("payload must be a JSON object")
	}
	return nil
}

func validateSleep(payload json.RawMessage) error {
	if err := fixtureObject(payload); err != nil {
		return err
	}
	var v struct {
		DurationMS int64 `json:"duration_ms"`
	}
	if err := json.Unmarshal(payload, &v); err != nil || v.DurationMS < 0 || v.DurationMS > 3600000 {
		return errors.New("duration_ms must be an integer between 0 and 3600000")
	}
	return nil
}

func validateFail(payload json.RawMessage) error {
	if err := fixtureObject(payload); err != nil {
		return err
	}
	var v struct {
		Failures int `json:"failures_before_success"`
	}
	if err := json.Unmarshal(payload, &v); err != nil || v.Failures < 0 {
		return errors.New("failures_before_success must be a non-negative integer")
	}
	return nil
}

var fixtures = func() *Registry {
	r, err := NewRegistry([]Descriptor{{Name: "sleep", MaxPayloadBytes: MaxPayloadBytes, Validate: validateSleep}, {Name: "fail", MaxPayloadBytes: MaxPayloadBytes, Validate: validateFail}})
	if err != nil {
		panic(err)
	}
	return r
}()

// Fixtures is the shared registry of built-in development job types.
func Fixtures() *Registry { return fixtures }
