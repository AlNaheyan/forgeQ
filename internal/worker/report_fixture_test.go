package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

type fakeReportSink struct {
	mu      sync.Mutex
	records map[string]ReportFixture
	writes  int
	loseAck bool
}

func (s *fakeReportSink) PutIfAbsent(ctx context.Context, tenant, key string, report ReportFixture) (ReportFixture, error) {
	if err := ctx.Err(); err != nil {
		return ReportFixture{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if saved, ok := s.records[key]; ok {
		return saved, nil
	}
	s.records[key] = report
	s.writes++
	if s.loseAck {
		s.loseAck = false
		return ReportFixture{}, errors.New("simulated lost acknowledgment")
	}
	return report, nil
}

func TestReportFixtureRetryUsesStableTenantScopedEffect(t *testing.T) {
	sink := &fakeReportSink{records: make(map[string]ReportFixture), loseAck: true}
	h := ReportExportHandler{Sink: sink}
	payload := json.RawMessage(`{"report_id":"report-1","values":[4,7,-2]}`)
	m := ExecutionMetadata{TenantID: "tenant-a", JobID: "job-1", Attempt: 1, LeaseGeneration: 1}
	if _, err := h.Execute(withMetadata(context.Background(), m), payload); err == nil {
		t.Fatal("expected lost acknowledgment")
	}
	m.Attempt, m.LeaseGeneration = 2, 2
	result, err := h.Execute(withMetadata(context.Background(), m), payload)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.(ReportFixture); got.Count != 3 || got.Sum != 9 {
		t.Fatalf("unexpected report: %+v", got)
	}
	if sink.writes != 1 {
		t.Fatalf("retry repeated effect: %d", sink.writes)
	}
	m.TenantID = "tenant-b"
	if _, err := h.Execute(withMetadata(context.Background(), m), payload); err != nil {
		t.Fatal(err)
	}
	if sink.writes != 2 {
		t.Fatalf("tenants shared an effect: %d", sink.writes)
	}
}
