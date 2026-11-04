package telemetry

import (
	dto "github.com/prometheus/client_model/go"
	"os"
	"path/filepath"
	"testing"
)

func TestBackupStatusCollectorExportsSuccessAndFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup-status.json")
	metrics := NewMetrics()
	if err := metrics.SetBackupStatusFile(path); err != nil {
		t.Fatal(err)
	}
	before, err := metrics.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	if got := metricValue(before, "forgequeue_backup_status_available"); got != 0 {
		t.Fatalf("missing status availability=%v", got)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"last_snapshot_timestamp_seconds":10,"last_success_timestamp_seconds":20,"archive_bytes":30,"duration_seconds":4.5,"successes":2,"failures":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	gathered, err := metrics.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	if got := metricValue(gathered, "forgequeue_backup_status_available"); got != 1 {
		t.Fatalf("status availability=%v", got)
	}
	if got := counterValue(gathered, "forgequeue_backup_runs_total", "success"); got != 2 {
		t.Fatalf("successes=%v", got)
	}
	if got := counterValue(gathered, "forgequeue_backup_runs_total", "failure"); got != 1 {
		t.Fatalf("failures=%v", got)
	}
}

func metricValue(families []*dto.MetricFamily, name string) float64 {
	for _, family := range families {
		if family.GetName() == name && len(family.Metric) > 0 {
			return family.Metric[0].GetGauge().GetValue()
		}
	}
	return -1
}
func counterValue(families []*dto.MetricFamily, name, outcome string) float64 {
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if len(metric.Label) == 1 && metric.Label[0].GetValue() == outcome {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return -1
}
