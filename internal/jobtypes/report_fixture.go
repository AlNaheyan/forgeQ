package jobtypes

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// ReportFixturePayload is a synthetic report fixture, not a product schema.
type ReportFixturePayload struct {
	ReportID string  `json:"report_id"`
	Values   []int64 `json:"values"`
}

func DecodeReportFixture(data json.RawMessage) (ReportFixturePayload, error) {
	var p ReportFixturePayload
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return p, errors.New("invalid report fixture payload")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return p, errors.New("invalid report fixture payload")
	}
	if len(p.ReportID) < 1 || len(p.ReportID) > 64 || len(p.Values) < 1 || len(p.Values) > 1000 {
		return p, errors.New("invalid report fixture fields")
	}
	for _, c := range p.ReportID {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return p, errors.New("invalid report identifier")
		}
	}
	for _, value := range p.Values {
		if value < -1000000 || value > 1000000 {
			return p, errors.New("report value out of bounds")
		}
	}
	return p, nil
}

// ReportFixtureDescriptor is opt-in and must be installed on both API and worker.
func ReportFixtureDescriptor() Descriptor {
	return Descriptor{Name: "report_export.v1", MaxPayloadBytes: 64 << 10, Validate: func(data json.RawMessage) error { _, err := DecodeReportFixture(data); return err }}
}
