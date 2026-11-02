package telemetry

import (
	"testing"
	"time"
)

func TestLoadedCertificateExpiry(t *testing.T) {
	m := NewMetrics()
	expiry := time.Now().Add(time.Hour).Unix()
	m.TLSCertificateNotAfter.WithLabelValues("http", "server").Set(float64(expiry))
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "forgequeue_tls_certificate_not_after_timestamp_seconds" {
			if len(family.Metric) != 1 || family.Metric[0].GetGauge().GetValue() != float64(expiry) {
				t.Fatal("invalid expiry")
			}
			return
		}
	}
	t.Fatal("expiry collector missing")
}
