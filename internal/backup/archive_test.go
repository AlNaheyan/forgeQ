package backup

import (
	"context"
	"errors"
	"filippo.io/age"
	"io"
	"strings"
	"testing"
)

func fixture(t *testing.T) (*Runner, *age.X25519Identity) {
	t.Helper()
	id, e := age.GenerateX25519Identity()
	if e != nil {
		t.Fatal(e)
	}
	s, e := NewLocal(t.TempDir(), "owned")
	if e != nil {
		t.Fatal(e)
	}
	return &Runner{Store: s, Prefix: "owned", TempDir: t.TempDir(), Recipients: []age.Recipient{id.Recipient()}, KeyID: "test", Revision: "fixture", Schema: "009", DatabaseMajor: "16"}, id
}
func TestEncryptedRoundTrip(t *testing.T) {
	r, id := fixture(t)
	ctx := context.Background()
	m, e := r.Backup(ctx, func(_ context.Context, w io.Writer) error { _, e := io.WriteString(w, "synthetic snapshot"); return e })
	if e != nil {
		t.Fatal(e)
	}
	m, e = ReadManifest(ctx, r.Store, m.Object+".json")
	if e != nil {
		t.Fatal(e)
	}
	f, e := Decrypt(ctx, r.Store, m, t.TempDir(), id)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	b, _ := io.ReadAll(f)
	if string(b) != "synthetic snapshot" {
		t.Fatal("roundtrip")
	}
	wrong, _ := age.GenerateX25519Identity()
	if _, e = Decrypt(ctx, r.Store, m, t.TempDir(), wrong); e == nil {
		t.Fatal("wrong key")
	}
	if _, e = Decrypt(ctx, r.Store, m, t.TempDir()); e == nil {
		t.Fatal("missing key")
	}
	m.SHA256 = strings.Repeat("0", 64)
	if Verify(ctx, r.Store, m) == nil {
		t.Fatal("checksum")
	}
}
func TestInterruptedDump(t *testing.T) {
	r, _ := fixture(t)
	_, e := r.Backup(context.Background(), func(_ context.Context, w io.Writer) error {
		io.WriteString(w, "partial")
		return errors.New("password secret")
	})
	if e == nil || strings.Contains(e.Error(), "secret") {
		t.Fatal(e)
	}
	keys, _ := r.Store.List(context.Background())
	if len(keys) != 0 {
		t.Fatal("published failed dump")
	}
}
