package backup

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStatusObserverKeepsLastGoodSnapshotAndCountsFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "backup-status.json")
	observe := StatusObserver(path)
	snapshot := time.Unix(100, 0).UTC()
	uploaded := time.Unix(200, 0).UTC()
	if err := observe(Manifest{Snapshot: snapshot, Uploaded: uploaded, Size: 123}, time.Second, nil); err != nil {
		t.Fatal(err)
	}
	if err := observe(Manifest{}, 2*time.Second, os.ErrInvalid); err != nil {
		t.Fatal(err)
	}
	s, err := ReadStatus(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.LastSnapshot != 100 || s.LastSuccess != 200 || s.ArchiveBytes != 123 || s.Successes != 1 || s.Failures != 1 || s.DurationSeconds != 2 {
		t.Fatalf("unexpected status: %+v", s)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0640 {
		t.Fatalf("status permissions=%o", info.Mode().Perm())
	}
}

func TestReadStatusRejectsMalformedAndOversizedFiles(t *testing.T) {
	for _, content := range []string{"{}", `{"version":2}`, string(make([]byte, 17*1024))} {
		path := filepath.Join(t.TempDir(), "status.json")
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadStatus(path); err == nil {
			t.Fatalf("accepted %d-byte invalid status", len(content))
		}
	}
}

func TestStatusObserverReportsPublicationErrors(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := StatusObserver(filepath.Join(blocker, "status"))(Manifest{}, time.Second, nil); err == nil {
		t.Fatal("expected publication failure")
	}
	bad := filepath.Join(dir, "invalid.json")
	if err := os.WriteFile(bad, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := StatusObserver(bad)(Manifest{}, time.Second, nil); err == nil {
		t.Fatal("expected invalid status failure")
	}
}
