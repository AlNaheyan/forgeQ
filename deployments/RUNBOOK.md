# Deployment notes

ForgeQ supports a private, single-host Compose deployment with one coordinator
and trusted workers. It is not a highly available or untrusted-code hosting
platform. Execution is at least once: external effects need idempotency.

## Configuration

- The base Compose configuration is for local development. Its control-plane
  token is separate from tenant API keys, which can be issued with
  `go run ./cmd/tenant`.
- Production requires HTTPS, authenticated TLS for worker RPCs, PostgreSQL and
  Redis, and service-specific secret files. Follow [network security](security/NETWORK.md),
  [database roles](security/DATABASE-ROLES.md), and [secret rotation](security/ROTATION.md).
  Keep service endpoints and metrics private.
- Run the privileged migration job before starting production services. Runtime
  services validate the schema using their restricted database roles. Local
  development may apply migrations automatically.
- Configure an operator API key for authenticated Prometheus scrapes and Grafana
  login through the production secrets and operations overlay. Keep secrets out
  of the repository.
- Configure `FORGEQ_JOB_TYPES` on the API for the handlers your application
  workers support. The bundled example worker handles only `sleep` and `fail`;
  application workers register their own handlers through the Go worker SDK.
- Kafka is optional, via `docker-compose.kafka.yml`; it carries outbox lifecycle
  events, not job dispatch. Consumers must tolerate duplicates.

## Backups and upgrades

Use `bash scripts/backup-postgres.sh once`, `schedule`, `retention`, or `restore`
with the required configuration in the [backup guide](backup/README.md). The
implemented storage target is local; operators must arrange encrypted off-host
storage and test access to recovery keys. Restore into a new, empty, isolated
database and verify jobs, tenant access, results, and idempotency records before
cutover. Restore durable state together and rebuild Redis hints through
reconciliation. Effects after the backup snapshot may repeat.

Before upgrading, back up and rehearse with `make test-operations`. Stop
admission and drain/stop services, then run the production migration job and
deploy the reviewed images together. Prefer forward fixes over schema
downgrades; never delete migration-history rows to force rollback. The default
worker shutdown deadline is eight seconds, so longer jobs can be recovered and
re-executed. `/readyz` checks PostgreSQL/Redis, not available worker capacity.

## Target or dependency down

Check containers, `/readyz`, disk space, connection limits, and service logs.
For scrape-only failures, check the Prometheus operator key and file permissions.
Restore Redis connectivity and verify reconciliation repopulates ready hints.
Do not delete database volumes. `forgequeue_durable_health=0` means its last
database snapshot failed; associated queue/age gauges may be stale.

## Queue stalled

Compare durable ready age/depth with Redis hints, worker capacity, supported
types, and tenant concurrency limits. Repair connectivity before scaling workers.
Confirm a canary job completes and oldest ready age decreases. Never delete
durable jobs to clear an alert.

## Worker loss

Check service credentials, coordinator reachability, heartbeat timestamps,
memory kills, and duplicate worker IDs. Restart affected workers and allow
expired leases to recover; do not manually extend abandoned leases.

## Retry or dead-letter growth

Inspect affected tenant jobs and fix handler/downstream failures before retrying.
Retry a small sample first. Manual retries consume admission capacity and may
repeat external effects without downstream idempotency.

## Event publisher lag

Check Kafka connectivity, event capture configuration, and publisher health.
Restore publishing and confirm pending-event age falls. Do not delete unpublished
events or mark them published to silence an alert.

## Retention and disk growth

Cleanup defaults off. `RETENTION_ENABLED=true` enables bounded cleanup, normally
100 parent rows per kind per minute: unkeyed succeeded/canceled jobs after 720h
and published, unleased events after 168h. Attempts cascade with deleted jobs.
Active, failed/dead-letter, and keyed jobs plus execution-idempotency markers
are retained. Inspect `forgequeue_retention_errors_total` and
`forgequeue_retention_last_success_timestamp_seconds`; do not delete markers to
free space. They prevent repeated effects. Backups and external audit/log/Kafka
retention remain operator responsibilities.

Prometheus evaluates the supplied alert rules, but external paging/Alertmanager
routes notifications to the local mock receiver in development. Start it with
`python3 deployments/alertmanager/mock-alert-receiver.py` before using the local
monitoring stack, then inspect sanitized firing and resolution summaries at
`http://127.0.0.1:8088/events`. This receiver is a test sink, not an incident
destination. Production Compose selects `deployments/security/alertmanager.yml`,
which reads an approved HTTPS webhook URL from the monitoring secret directory.
Set the recipient, on-call owner and escalation route before relying on paging.

## Backup stale or failed

Check the backup command exit status, completed manifest timestamp, storage
availability and encryption key access. A stale-success alert means no upload
has completed within 26 hours; compare snapshot and upload timestamps separately.
If `ForgeQueueBackupStatusUnavailable` fires, check that the backup process and
coordinator share the configured status volume and that the coordinator has
read-only access to `status.json`.
Retry only after resolving the underlying cause. Never delete an older restored
checkpoint to silence this alert. A checksum alone does not prove a backup can
restore; use the isolated restore drill and its recovery receipt.

## Certificate expiry

Identify the expiring endpoint and role from bounded labels, inspect the mounted
certificate chain and issuer, then stage a replacement with overlapping trust.
This deployment rotates certificates by coordinated restart and can interrupt
active channels. Verify each service's health and authenticated scrape after
restart; do not remove the old CA until all clients have moved.
