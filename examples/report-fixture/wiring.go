// Package reportfixture demonstrates opt-in wiring inside the ForgeQ module.
package reportfixture

import (
	"database/sql"
	"errors"

	"github.com/AlNaheyan/forgeQ/internal/jobtypes"
	"github.com/AlNaheyan/forgeQ/internal/worker"
)

// Registrations returns the same descriptor set and executable handlers that
// the API and worker must use. db must be a separate application database.
func Registrations(db *sql.DB) (*jobtypes.Registry, map[string]worker.Handler, error) {
	if db == nil {
		return nil, nil, errors.New("application database required")
	}
	descriptors := append(jobtypes.Fixtures().Descriptors(), jobtypes.ReportFixtureDescriptor())
	registry, err := jobtypes.NewRegistry(descriptors)
	if err != nil {
		return nil, nil, err
	}
	handlers := worker.BuiltInHandlers()
	handlers["report_export.v1"] = worker.ReportExportHandler{Sink: worker.PostgresReportSink{DB: db}}
	return registry, handlers, nil
}
