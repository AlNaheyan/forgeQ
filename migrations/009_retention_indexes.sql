-- Additive indexes only. Keep keyed jobs and execution markers indefinitely.
CREATE INDEX jobs_retention_idx ON jobs(updated_at,id) WHERE status IN ('succeeded','canceled');
CREATE INDEX event_outbox_retention_idx ON event_outbox(published_at,id) WHERE published_at IS NOT NULL AND lease_owner IS NULL;
