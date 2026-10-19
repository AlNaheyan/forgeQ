# Optional Kafka event stream

Wave 14 adds an optional Kafka stream for job and worker lifecycle events.
PostgreSQL remains the source of truth; Redis remains the queue-hint layer.
With `EVENTS_ENABLED=false` (the default), job transitions do not create outbox
rows and Kafka is not contacted.

## Event contract

Events are JSON records in `forgequeue.events`. Each record has an `event_id`,
`event_type`, `aggregate_type`, `aggregate_id`, `occurred_at`, and `payload`.
The Kafka key is the aggregate ID, keeping events for a given job or worker on
the same partition. Job events include creation, lease/start, success, failure,
retry, and dead-letter transitions. Worker events include registration and a
detected transition to unhealthy. Payloads intentionally omit the job's input
payload and result.

PostgreSQL writes job events through a status trigger in the same transaction as
the corresponding state change. A publisher claims outbox rows using leases,
publishes synchronously, and marks them delivered only after Kafka acknowledges
the write. Failed writes are scheduled for retry with capped exponential delay;
older unpublished events block later events for the same aggregate. This is
at-least-once delivery: if Kafka accepts a record but the publisher loses its
connection before it records success in PostgreSQL, that event can be delivered
again. Consumers should deduplicate by `event_id`.

Worker registration/loss events are recorded by the coordinator. The event
stream can be enabled from the API and coordinator configuration with
`EVENTS_ENABLED=true` and a comma-separated `KAFKA_BROKERS` list. Set
`KAFKA_TOPIC` to override the topic. The API owns the outbox publisher; the
coordinator records worker events into the shared outbox.

## Local Kafka (optional)

Kafka is not included in the ordinary Compose stack. To opt in, use the overlay
when starting services:

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.kafka.yml up --build
```

This downloads and runs a Kafka container. Omit the overlay to keep the local
stack light and the stream disabled. For services run directly with `go run`,
start Kafka separately, then set `EVENTS_ENABLED=true` and
`KAFKA_BROKERS=localhost:9092` for both API and coordinator.

Inspect the topic from the Compose stack:

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.kafka.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic forgequeue.events --from-beginning
```

The durable outbox is also directly inspectable in PostgreSQL:

```sql
SELECT id, event_type, aggregate_type, aggregate_id, occurred_at,
       attempts, published_at, last_error
FROM event_outbox
ORDER BY id DESC
LIMIT 50;
```
