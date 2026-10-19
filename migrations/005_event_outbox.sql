CREATE TABLE event_stream_config (
	singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
	enabled BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO event_stream_config (singleton, enabled)
VALUES (TRUE, FALSE)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE event_outbox (
	id BIGSERIAL PRIMARY KEY,
	event_type TEXT NOT NULL CHECK (event_type IN (
		'JobCreated', 'JobLeased', 'JobStarted', 'JobSucceeded', 'JobFailed',
		'JobRetried', 'JobDeadLettered', 'WorkerRegistered', 'WorkerLost'
	)),
	aggregate_type TEXT NOT NULL,
	aggregate_id TEXT NOT NULL,
	occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
	payload JSONB NOT NULL,
	attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
	next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
	lease_owner TEXT,
	lease_until TIMESTAMPTZ,
	published_at TIMESTAMPTZ,
	last_error TEXT,
	CHECK ((lease_owner IS NULL) = (lease_until IS NULL))
);

CREATE INDEX event_outbox_pending_idx
	ON event_outbox (next_attempt_at, id)
	WHERE published_at IS NULL;

CREATE FUNCTION forgequeue_record_event(
	p_event_type TEXT,
	p_aggregate_type TEXT,
	p_aggregate_id TEXT,
	p_payload JSONB
) RETURNS VOID
LANGUAGE SQL
AS $$
	INSERT INTO event_outbox (event_type, aggregate_type, aggregate_id, payload)
	SELECT p_event_type, p_aggregate_type, p_aggregate_id, p_payload
	WHERE EXISTS (
		SELECT 1 FROM event_stream_config WHERE singleton = TRUE AND enabled = TRUE
	)
$$;

CREATE FUNCTION forgequeue_job_event_trigger() RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
	job_payload JSONB;
BEGIN
	job_payload := jsonb_build_object(
		'job_id', NEW.id,
		'job_type', NEW.type,
		'status', NEW.status,
		'attempt_count', NEW.attempt_count,
		'max_attempts', NEW.max_attempts,
		'priority', NEW.priority,
		'scheduled_at', NEW.scheduled_at,
		'lease_owner', NEW.lease_owner,
		'last_error', NEW.last_error
	);

	IF TG_OP = 'INSERT' THEN
		PERFORM forgequeue_record_event('JobCreated', 'job', NEW.id::TEXT, job_payload);
		RETURN NEW;
	END IF;

	IF OLD.status IS DISTINCT FROM NEW.status THEN
		CASE NEW.status
			WHEN 'leased' THEN
				PERFORM forgequeue_record_event('JobLeased', 'job', NEW.id::TEXT, job_payload);
			WHEN 'running' THEN
				PERFORM forgequeue_record_event('JobStarted', 'job', NEW.id::TEXT, job_payload);
			WHEN 'succeeded' THEN
				PERFORM forgequeue_record_event('JobSucceeded', 'job', NEW.id::TEXT, job_payload);
			WHEN 'failed' THEN
				PERFORM forgequeue_record_event('JobFailed', 'job', NEW.id::TEXT, job_payload);
			WHEN 'retry_wait' THEN
				PERFORM forgequeue_record_event('JobFailed', 'job', NEW.id::TEXT, job_payload);
				PERFORM forgequeue_record_event('JobRetried', 'job', NEW.id::TEXT, job_payload);
			WHEN 'dead_letter' THEN
				PERFORM forgequeue_record_event('JobFailed', 'job', NEW.id::TEXT, job_payload);
				PERFORM forgequeue_record_event('JobDeadLettered', 'job', NEW.id::TEXT, job_payload);
			WHEN 'queued' THEN
				IF OLD.status IN ('running', 'leased', 'dead_letter') THEN
					PERFORM forgequeue_record_event('JobRetried', 'job', NEW.id::TEXT, job_payload);
				END IF;
		END CASE;
	END IF;
	RETURN NEW;
END
$$;

CREATE TRIGGER jobs_event_outbox_insert_trigger
	AFTER INSERT ON jobs
	FOR EACH ROW EXECUTE FUNCTION forgequeue_job_event_trigger();

CREATE TRIGGER jobs_event_outbox_status_trigger
	AFTER UPDATE OF status ON jobs
	FOR EACH ROW EXECUTE FUNCTION forgequeue_job_event_trigger();
