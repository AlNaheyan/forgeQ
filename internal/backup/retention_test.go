package backup

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"
)

func TestRetentionKeepsUsableAndDryRun(t *testing.T) {
	r, _ := fixture(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		m, e := r.Backup(ctx, func(_ context.Context, w io.Writer) error { _, e := io.WriteString(w, "snapshot"); return e })
		if e != nil {
			t.Fatal(e)
		}
		r.Store.Delete(ctx, m.Object+".json")
		m.Snapshot = time.Now().Add(-time.Duration(i+1) * 24 * time.Hour)
		b, _ := json.Marshal(m)
		r.Store.Put(ctx, m.Object+".json", bytesReader(b))
	}
	plan, e := Retain(ctx, r.Store, "owned", Retention{1, 0, 4}, false)
	if e != nil || len(plan) != 4 {
		t.Fatal(plan, e)
	}
	keys, _ := r.Store.List(ctx)
	if len(keys) != 10 {
		t.Fatal("dry run deleted")
	}
	for i := 0; i < 3; i++ {
		if _, e = Retain(ctx, r.Store, "owned", Retention{1, 0, 4}, true); e != nil {
			t.Fatal(e)
		}
	}
	keys, _ = r.Store.List(ctx)
	if len(keys) != 2 {
		t.Fatal("last usable lost")
	}
	if _, e = PlanRetention(ctx, r.Store, "foreign", Retention{1, 0, 4}); e == nil {
		t.Fatal("foreign prefix")
	}
}
