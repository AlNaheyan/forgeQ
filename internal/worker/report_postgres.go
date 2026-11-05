package worker

import (
	"context"
	"database/sql"
	"errors"
)

// PostgresReportSink uses a separately provisioned application database. Its
// unique tenant/operation key makes the insert and deduplication one database
// operation, including when the client loses the acknowledgment.
type PostgresReportSink struct{ DB *sql.DB }

func (s PostgresReportSink) PutIfAbsent(ctx context.Context, tenantID, key string, report ReportFixture) (ReportFixture, error) {
	if s.DB == nil || tenantID == "" || key == "" {
		return ReportFixture{}, errors.New("report database and identity are required")
	}
	var stored ReportFixture
	err := s.DB.QueryRowContext(ctx, `
		INSERT INTO forgeq_report_fixture.reports (tenant_id, operation_key, report_id, sample_count, sample_sum)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT (tenant_id, operation_key) DO NOTHING
		RETURNING report_id, sample_count, sample_sum`, tenantID, key, report.ReportID, report.Count, report.Sum).Scan(&stored.ReportID, &stored.Count, &stored.Sum)
	if errors.Is(err, sql.ErrNoRows) {
		err = s.DB.QueryRowContext(ctx, `SELECT report_id, sample_count, sample_sum FROM forgeq_report_fixture.reports WHERE tenant_id=$1 AND operation_key=$2`, tenantID, key).Scan(&stored.ReportID, &stored.Count, &stored.Sum)
	}
	return stored, err
}
