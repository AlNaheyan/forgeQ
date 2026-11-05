package worker

import (
	"context"
	"encoding/json"
	"errors"

	"forgequeue/internal/jobtypes"
)

// ReportFixture is a synthetic aggregation example; it does not fetch URLs or
// access customer systems. A real sink must implement durable idempotency.
type ReportFixture struct {
	ReportID string `json:"report_id"`
	Count    int    `json:"count"`
	Sum      int64  `json:"sum"`
}

// ReportSink atomically stores a result only if the tenant-scoped key is absent,
// returning the existing result for a duplicate call.
type ReportSink interface {
	PutIfAbsent(context.Context, string, string, ReportFixture) (ReportFixture, error)
}

type ReportExportHandler struct{ Sink ReportSink }

func (h ReportExportHandler) Execute(ctx context.Context, payload json.RawMessage) (any, error) {
	if h.Sink == nil {
		return nil, &HandlerError{Code: FailurePermanent, Cause: errors.New("report sink required")}
	}
	p, err := jobtypes.DecodeReportFixture(payload)
	if err != nil {
		return nil, &HandlerError{Code: FailureInvalidPayload, Cause: err}
	}
	m, ok := MetadataFromContext(ctx)
	if !ok {
		return nil, &HandlerError{Code: FailurePermanent, Cause: errors.New("execution metadata required")}
	}
	key, err := m.OperationKey("report_export")
	if err != nil {
		return nil, &HandlerError{Code: FailurePermanent, Cause: err}
	}
	result := ReportFixture{ReportID: p.ReportID, Count: len(p.Values)}
	for _, value := range p.Values {
		result.Sum += value
	}
	stored, err := h.Sink.PutIfAbsent(ctx, m.TenantID, key, result)
	if err != nil {
		return nil, &HandlerError{Code: FailureDownstreamUnavailable, Cause: err}
	}
	return stored, nil
}
