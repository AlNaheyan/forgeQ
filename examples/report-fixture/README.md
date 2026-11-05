# Synthetic report fixture

This opt-in example aggregates supplied integers and stores the result in a
separate PostgreSQL application schema. It is a synthetic fixture for exploring
the handler contract; ForgeQ does not enable it by default and the product's real
workload has not yet been selected.

The payload `{"report_id":"sample","values":[4,7,-2]}` produces
`{"report_id":"sample","count":3,"sum":9}`. Inputs contain no URL or file
path: unknown fields, more than 1,000 values, values outside ±1,000,000, and
payloads over 64 KiB are rejected before execution.

Provision a separate application database and apply [schema.sql](schema.sql)
using a migration role. The handler's runtime account needs `USAGE` on the
schema and `SELECT` and `INSERT` on its `reports` table. It does not need ForgeQ
control-plane database credentials or schema-creation privileges. Use verified
TLS and a dedicated secret when connecting to the application database.

`Registrations(db)` in [wiring.go](wiring.go) creates one descriptor registry
and its handlers. Install that same registry on the API's `jobs.Service` with
`WithJobTypes(registry)` and on `worker.Config.Registry`; pass its handler map to
`worker.NewWithBroker` and include `report_export.v1` in the worker's selected
job types. Worker registration advertises only selected handlers. The example
imports ForgeQ's `internal` packages and must currently live inside this module;
a public application SDK is planned in Wave 26.

Each effect uses a key built from tenant, job ID, and effect name. Attempt number
and lease generation never change that key. A unique database constraint makes
the sample report insertion idempotent across retries and lost acknowledgments.
Keep the row for the whole replay window. This protects this database insert;
effects in other systems require their own idempotency mechanism. ForgeQ runs
trusted handlers with the worker's privileges and does not sandbox uploaded code.
