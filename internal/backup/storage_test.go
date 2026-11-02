package backup

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestScopeAndCollision(t *testing.T) {
	s, e := NewLocal(t.TempDir(), "owned")
	if e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	for _, k := range []string{"../bad", "other/x", "owned/../bad", "owned/x/y"} {
		if s.Put(ctx, k, strings.NewReader("x")) == nil {
			t.Fatal(k)
		}
	}
	if e = s.Put(ctx, "owned/a", strings.NewReader("x")); e != nil {
		t.Fatal(e)
	}
	if s.Put(ctx, "owned/a", strings.NewReader("y")) == nil {
		t.Fatal("overwrite")
	}
}
func TestSchedulerLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Schedule(ctx, dir, time.Hour, func(context.Context) error { close(entered); <-ctx.Done(); return nil })
	}()
	<-entered
	if Schedule(ctx, dir, time.Hour, func(context.Context) error { return nil }) == nil {
		t.Fatal("overlap")
	}
	cancel()
	<-done
}
