CREATE TABLE execution_idempotency_keys (
    key TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT execution_idempotency_key_not_empty CHECK (length(btrim(key)) > 0),
    CONSTRAINT execution_idempotency_key_size CHECK (octet_length(key) <= 1024)
);

