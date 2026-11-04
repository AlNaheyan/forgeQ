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
		if i == 0 {
			if e = recordRestore(ctx, r.Store, m); e != nil {
				t.Fatal(e)
			}
		}
	}
	plan, e := Retain(ctx, r.Store, "owned", Retention{1, 0, 4}, false)
	if e != nil || len(plan) != 4 {
		t.Fatal(plan, e)
	}
	keys, _ := r.Store.List(ctx)
	if len(keys) != 11 {
		t.Fatal("dry run deleted")
	}
	for i := 0; i < 3; i++ {
		if _, e = Retain(ctx, r.Store, "owned", Retention{1, 0, 4}, true); e != nil {
			t.Fatal(e)
		}
	}
	keys, _ = r.Store.List(ctx)
	if len(keys) != 3 {
		t.Fatal("last usable lost")
	}
	if _, e = PlanRetention(ctx, r.Store, "foreign", Retention{1, 0, 4}); e == nil {
		t.Fatal("foreign prefix")
	}
}

func TestRetentionRequiresAndPreservesRestoreProof(t *testing.T) {
	r, _ := fixture(t)
	ctx := context.Background()
	var snapshots []Manifest
	for i := 0; i < 3; i++ {
		m, e := r.Backup(ctx, func(_ context.Context, w io.Writer) error { _, e := io.WriteString(w, "snapshot"); return e })
		if e != nil {
			t.Fatal(e)
		}
		snapshots = append(snapshots, m)
	}
	p := Retention{Daily: 1, MaxDelete: 100}
	plan, e := Retain(ctx, r.Store, "owned", p, true)
	if e != nil || len(plan) != 0 {
		t.Fatal("deleted without restore proof", e)
	}
	if e = recordRestore(ctx, r.Store, snapshots[0]); e != nil {
		t.Fatal(e)
	}
	if _, e = Retain(ctx, r.Store, "owned", p, true); e != nil {
		t.Fatal(e)
	}
	for _, m := range []Manifest{snapshots[0], snapshots[2]} {
		if e = Verify(ctx, r.Store, m); e != nil {
			t.Fatal("deleted restored or newest archive", e)
		}
	}
	if e = Verify(ctx, r.Store, snapshots[1]); e == nil {
		t.Fatal("unneeded intermediate retained")
	}
}
