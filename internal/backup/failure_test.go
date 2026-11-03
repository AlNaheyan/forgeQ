package backup

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type failingStore struct {
	Storage
	mode string
}

func (s failingStore) Put(ctx context.Context, k string, r io.Reader) error {
	switch s.mode {
	case "denied":
		return errors.New("denied with secret")
	case "truncated":
		if strings.HasSuffix(k, ".age") {
			return s.Storage.Put(ctx, k, io.LimitReader(r, 20))
		}
	case "manifest":
		if strings.HasSuffix(k, ".json") {
			return errors.New("manifest unavailable")
		}
	}
	return s.Storage.Put(ctx, k, r)
}
func TestUploadFailuresNeverPublishComplete(t *testing.T) {
	for _, mode := range []string{"denied", "truncated", "manifest"} {
		t.Run(mode, func(t *testing.T) {
			r, _ := fixture(t)
			r.Store = failingStore{r.Store, mode}
			observed := false
			r.Observe = func(_ Manifest, _ time.Duration, e error) { observed = e != nil }
			_, e := r.Backup(context.Background(), func(_ context.Context, w io.Writer) error { _, e := io.WriteString(w, "snapshot"); return e })
			if e == nil || strings.Contains(e.Error(), "secret") || !observed {
				t.Fatal(e)
			}
			keys, _ := r.Store.List(context.Background())
			for _, k := range keys {
				if strings.HasSuffix(k, ".json") {
					t.Fatal("complete manifest published")
				}
			}
		})
	}
}

func TestUnavailableArchiveAndCorruptRetention(t *testing.T) {
	r, id := fixture(t)
	ctx := context.Background()
	m, e := r.Backup(ctx, func(_ context.Context, w io.Writer) error { _, e := io.WriteString(w, "snapshot"); return e })
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Store.Delete(ctx, m.Object); e != nil {
		t.Fatal(e)
	}
	if _, e = Decrypt(ctx, r.Store, m, t.TempDir(), id); e == nil {
		t.Fatal("missing archive accepted")
	}
	if e = r.Store.Put(ctx, m.Object, strings.NewReader("truncated ciphertext")); e != nil {
		t.Fatal(e)
	}
	if _, e = Decrypt(ctx, r.Store, m, t.TempDir(), id); e == nil {
		t.Fatal("corrupt archive accepted")
	}
	plan, e := Retain(ctx, r.Store, "owned", Retention{Daily: 1, MaxDelete: 2}, true)
	if e != nil || len(plan) != 0 {
		t.Fatal("corrupt object automatically deleted", e)
	}
}
