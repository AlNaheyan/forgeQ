-- Apply using a migration role in a separate application database or schema.
CREATE SCHEMA IF NOT EXISTS invoice_app;
CREATE TABLE IF NOT EXISTS invoice_app.invoices (
  tenant_id text NOT NULL,
  operation_key text NOT NULL,
  invoice_id text NOT NULL,
  amount_cents bigint NOT NULL CHECK (amount_cents > 0),
  PRIMARY KEY (tenant_id, operation_key)
);
