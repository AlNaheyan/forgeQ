-- A generation survives requeue and increments on every claim, including
-- reclaims by the same worker. Old clients fail closed until upgraded.
ALTER TABLE jobs ADD COLUMN lease_generation BIGINT NOT NULL DEFAULT 0
    CHECK (lease_generation >= 0);

-- Reconciliation scans by ID so a full first page cannot hide later jobs.
CREATE INDEX jobs_queued_reconciliation_idx ON jobs (id)
    WHERE status = 'queued';
