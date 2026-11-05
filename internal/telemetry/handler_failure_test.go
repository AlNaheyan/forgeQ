package telemetry

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestHandlerFailureLabelsAreBounded(t *testing.T) {
	m := NewMetrics()
	m.RecordHandlerFailure("invalid_payload")
	m.RecordHandlerFailure("secret-tenant-value")
	want := `# HELP forgequeue_handler_failures_total Failed job attempts by bounded classification.
# TYPE forgequeue_handler_failures_total counter
forgequeue_handler_failures_total{code="invalid_payload"} 1
forgequeue_handler_failures_total{code="unclassified"} 1
`
	if err := testutil.CollectAndCompare(m.HandlerFailures, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}
