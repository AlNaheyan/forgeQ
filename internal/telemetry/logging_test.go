package telemetry

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestNewLoggerIncludesRequiredFields(t *testing.T) {
	var output bytes.Buffer
	logger, err := NewLogger(&output, "worker", "info")
	if err != nil {
		t.Fatalf("create logger: %v", err)
	}

	logger.Info("service started", "worker_id", "worker-1")

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("decode log record: %v", err)
	}
	for _, key := range []string{"timestamp", "level", "service", "message"} {
		if _, ok := record[key]; !ok {
			t.Errorf("required field %q is missing from %v", key, record)
		}
	}
	if record["service"] != "worker" || record["message"] != "service started" {
		t.Fatalf("unexpected log context: %v", record)
	}
}

func TestNewLoggerHonorsLevel(t *testing.T) {
	var output bytes.Buffer
	logger, err := NewLogger(&output, "api", "warn")
	if err != nil {
		t.Fatalf("create logger: %v", err)
	}

	logger.Info("hidden")
	logger.Warn("visible")
	if bytes.Contains(output.Bytes(), []byte("hidden")) || !bytes.Contains(output.Bytes(), []byte("visible")) {
		t.Fatalf("unexpected level filtering: %s", output.String())
	}
}
