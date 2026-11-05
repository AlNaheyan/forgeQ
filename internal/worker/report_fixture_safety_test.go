package worker

import (
	"context"
	"encoding/json"
	"github.com/AlNaheyan/forgeQ/internal/jobtypes"
	"strings"
	"testing"
)

func TestReportFixtureBoundsAndRejectsExternalTargets(t *testing.T) {
	d := jobtypes.ReportFixtureDescriptor()
	for _, payload := range []string{`{"report_id":"x","values":[1],"url":"https://example.com"}`, `{"report_id":"../x","values":[1]}`, `{"report_id":"x","values":[1000001]}`, `{"report_id":"x","values":[]}`} {
		if err := d.Validate(json.RawMessage(payload)); err == nil {
			t.Fatalf("accepted %s", payload)
		}
	}
	if _, err := jobtypes.DecodeReportFixture(json.RawMessage(`{"report_id":"` + strings.Repeat("x", 70<<10) + `","values":[1]}`)); err == nil {
		t.Fatal("oversized payload accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h := ReportExportHandler{Sink: &fakeReportSink{records: make(map[string]ReportFixture)}}
	m := ExecutionMetadata{TenantID: "tenant", JobID: "job"}
	if _, err := h.Execute(withMetadata(ctx, m), json.RawMessage(`{"report_id":"r","values":[1]}`)); err == nil {
		t.Fatal("canceled request succeeded")
	}
}
