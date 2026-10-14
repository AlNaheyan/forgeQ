package idempotency

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestExecuteOnceRejectsInvalidInputBeforeDatabaseAccess(t *testing.T) {
	executor := NewExecutor(nil)

	tests := []struct {
		name string
		key  string
		fn   func(context.Context) error
		err  error
	}{
		{name: "empty", key: "", fn: func(context.Context) error { return nil }, err: ErrInvalidKey},
		{name: "whitespace", key: "  \t", fn: func(context.Context) error { return nil }, err: ErrInvalidKey},
		{name: "too long", key: strings.Repeat("k", maxKeyBytes+1), fn: func(context.Context) error { return nil }, err: ErrInvalidKey},
		{name: "invalid UTF-8", key: string([]byte{0xff}), fn: func(context.Context) error { return nil }, err: ErrInvalidKey},
		{name: "NUL", key: "bad\x00key", fn: func(context.Context) error { return nil }, err: ErrInvalidKey},
		{name: "nil callback", key: "valid", fn: nil, err: ErrNilCallback},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := executor.ExecuteOnce(context.Background(), test.key, test.fn)
			if !errors.Is(err, test.err) {
				t.Fatalf("expected %v, got %v", test.err, err)
			}
		})
	}
}

func TestExecuteOnceTxRejectsNilCallback(t *testing.T) {
	executor := NewExecutor(nil)
	if _, err := executor.ExecuteOnceTx(context.Background(), "valid", nil); !errors.Is(err, ErrNilCallback) {
		t.Fatalf("expected ErrNilCallback, got %v", err)
	}
}
