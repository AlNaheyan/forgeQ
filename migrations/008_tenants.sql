CREATE TABLE tenants (
 id UUID PRIMARY KEY,
 name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
 max_pending INTEGER NOT NULL CHECK (max_pending > 0),
 max_concurrent INTEGER NOT NULL CHECK (max_concurrent > 0),
 per_minute INTEGER NOT NULL CHECK (per_minute > 0),
 max_payload_bytes INTEGER NOT NULL CHECK (max_payload_bytes BETWEEN 1 AND 1048576),
 window_start TIMESTAMPTZ NOT NULL DEFAULT date_trunc('minute', clock_timestamp()),
 window_count INTEGER NOT NULL DEFAULT 0,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
-- Preserve existing data without granting any public access to it.
INSERT INTO tenants(id,name,max_pending,max_concurrent,per_minute,max_payload_bytes)
 VALUES ('00000000-0000-4000-8000-000000000001','legacy',1000000,100000,1000000,1048576);
CREATE TABLE api_keys (
 id UUID PRIMARY KEY,
 tenant_id UUID NOT NULL REFERENCES tenants(id),
 verifier BYTEA NOT NULL UNIQUE,
 operator BOOLEAN NOT NULL DEFAULT FALSE,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 revoked_at TIMESTAMPTZ
);
ALTER TABLE jobs ADD COLUMN tenant_id UUID NOT NULL DEFAULT '00000000-0000-4000-8000-000000000001' REFERENCES tenants(id);
CREATE INDEX jobs_tenant_state_idx ON jobs(tenant_id,status);
CREATE INDEX jobs_tenant_created_idx ON jobs(tenant_id,created_at DESC,id DESC);
ALTER TABLE idempotency_keys ADD COLUMN tenant_id UUID NOT NULL DEFAULT '00000000-0000-4000-8000-000000000001' REFERENCES tenants(id);
ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_pkey;
ALTER TABLE idempotency_keys ADD PRIMARY KEY(tenant_id,key);
ALTER TABLE execution_idempotency_keys ADD COLUMN tenant_id UUID NOT NULL DEFAULT '00000000-0000-4000-8000-000000000001' REFERENCES tenants(id);
ALTER TABLE execution_idempotency_keys DROP CONSTRAINT execution_idempotency_keys_pkey;
ALTER TABLE execution_idempotency_keys ADD PRIMARY KEY(tenant_id,key);

-- Worker-supplied errors can contain sensitive payload data. Remove them from
-- newly captured events without changing job results visible to their owner.
CREATE FUNCTION forgequeue_redact_event() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 NEW.payload := NEW.payload - 'last_error';
 RETURN NEW;
END $$;
CREATE TRIGGER redact_event_payload BEFORE INSERT ON event_outbox
 FOR EACH ROW EXECUTE FUNCTION forgequeue_redact_event();
