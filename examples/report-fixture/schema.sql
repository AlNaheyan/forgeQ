-- Apply only to a separately provisioned application database for this synthetic fixture.
CREATE SCHEMA IF NOT EXISTS forgeq_report_fixture;
CREATE TABLE IF NOT EXISTS forgeq_report_fixture.reports (
  tenant_id text NOT NULL,
  operation_key text NOT NULL,
  report_id text NOT NULL,
  sample_count integer NOT NULL,
  sample_sum bigint NOT NULL,
  PRIMARY KEY (tenant_id, operation_key)
);
