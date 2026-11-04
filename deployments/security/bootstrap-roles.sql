-- Run as the separately authorized cluster/database administrator AFTER migrations.
-- No passwords are embedded: provision login credentials out of band. Runtime
-- roles must not be members of another privileged role. This is intentionally
-- separate from transactional schema migrations and applies only to this DB.
\set ON_ERROR_STOP on
BEGIN;
CREATE ROLE forgeq_api NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE forgeq_coordinator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
DO $$ BEGIN EXECUTE format('REVOKE CREATE,TEMPORARY ON DATABASE %I FROM PUBLIC',current_database()); END $$;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO forgeq_api, forgeq_coordinator;
GRANT SELECT ON schema_migrations,event_stream_config,tenants TO forgeq_api,forgeq_coordinator;
GRANT SELECT ON api_keys TO forgeq_api;
GRANT UPDATE(window_start,window_count) ON tenants TO forgeq_api;
-- SELECT FOR UPDATE requires an UPDATE column even when coordinator only locks.
GRANT UPDATE(window_count) ON tenants TO forgeq_coordinator;
GRANT SELECT,INSERT,UPDATE,DELETE ON jobs,job_attempts,idempotency_keys,execution_idempotency_keys TO forgeq_api;
GRANT SELECT,UPDATE ON jobs TO forgeq_coordinator;
GRANT SELECT,INSERT,UPDATE ON job_attempts,execution_idempotency_keys TO forgeq_coordinator;
GRANT SELECT,INSERT,UPDATE,DELETE ON event_outbox TO forgeq_api;
GRANT SELECT,INSERT ON event_outbox TO forgeq_coordinator;
GRANT USAGE ON SEQUENCE event_outbox_id_seq TO forgeq_api,forgeq_coordinator;
GRANT EXECUTE ON FUNCTION forgequeue_record_event(TEXT,TEXT,TEXT,JSONB),forgequeue_job_event_trigger(),forgequeue_redact_event() TO forgeq_api,forgeq_coordinator;
CREATE ROLE forgeq_backup NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT USAGE ON SCHEMA public TO forgeq_backup;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO forgeq_backup;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO forgeq_backup;
COMMIT;
