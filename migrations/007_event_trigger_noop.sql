-- Unlisted transitions (notably cancellation) must not abort job mutations.
-- Replace the function rather than editing an already-applied migration.
CREATE OR REPLACE FUNCTION forgequeue_job_event_trigger() RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    job_payload JSONB;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM event_stream_config WHERE singleton AND enabled) THEN
        RETURN NEW;
    END IF;
    job_payload := jsonb_build_object(
        'job_id', NEW.id, 'job_type', NEW.type, 'status', NEW.status,
        'attempt_count', NEW.attempt_count, 'max_attempts', NEW.max_attempts,
        'priority', NEW.priority, 'scheduled_at', NEW.scheduled_at,
        'lease_owner', NEW.lease_owner, 'lease_generation', NEW.lease_generation,
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
            ELSE NULL;
        END CASE;
    END IF;
    RETURN NEW;
END
$$;
