package backup

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// Status is a small durable summary safe to share read-only with the metrics
// process. It contains no tenant data, credentials, object names or error text.
type Status struct {
	Version         int     `json:"version"`
	LastSnapshot    int64   `json:"last_snapshot_timestamp_seconds"`
	LastSuccess     int64   `json:"last_success_timestamp_seconds"`
	ArchiveBytes    int64   `json:"archive_bytes"`
	DurationSeconds float64 `json:"duration_seconds"`
	Successes       uint64  `json:"successes"`
	Failures        uint64  `json:"failures"`
}

func ReadStatus(path string) (Status, error) {
	var s Status
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	if len(b) == 0 || len(b) > 16*1024 {
		return s, errors.New("invalid backup status size")
	}
	if err = json.Unmarshal(b, &s); err != nil || s.Version != 1 || s.Successes > 1<<53 || s.Failures > 1<<53 || s.ArchiveBytes < 0 || s.DurationSeconds < 0 {
		return Status{}, errors.New("invalid backup status")
	}
	return s, nil
}

func StatusObserver(path string) func(Manifest, time.Duration, error) error {
	return func(m Manifest, elapsed time.Duration, operationErr error) error {
		if path == "" {
			return nil
		}
		s, err := ReadStatus(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		s.Version = 1
		s.DurationSeconds = elapsed.Seconds()
		if operationErr != nil {
			s.Failures++
			return writeStatus(path, s)
		}
		s.Successes++
		s.LastSnapshot = m.Snapshot.Unix()
		s.LastSuccess = m.Uploaded.Unix()
		s.ArchiveBytes = m.Size
		return writeStatus(path, s)
	}
}

func writeStatus(path string, s Status) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".backup-status-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0640); err != nil {
		f.Close()
		return err
	}
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
