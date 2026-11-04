# Deployment notes

ForgeQ supports a private, single-host Compose deployment with one coordinator
and trusted workers. It is not a highly available or untrusted-code hosting
platform. Execution is at least once: external effects need idempotency.

## Configuration

- Keep PostgreSQL, Redis, worker RPCs, and metrics private. Use HTTPS termination
  for public HTTP; internal RPC transport currently assumes a trusted network.
- API, coordinator, and workers need the same random `CONTROL_PLANE_TOKEN`.
  Tenant API keys are separate; issue them with `go run ./cmd/tenant`.
- The optional `docker-compose.operations.yml` overlay adds resource limits
  and requires `POSTGRES_PASSWORD` and `GRAFANA_ADMIN_PASSWORD`. Use a random hex
  database password so it is safe inside the connection URL. Existing database
  volumes require explicit role-password rotation, not just an environment edit.
- Prometheus needs a dedicated operator API key saved as token-only text in
  `deployments/secrets/prometheus-api-key`, readable by the container user.
  This directory must remain untracked. The base Grafana anonymous-admin setup
  is local-only; the operations overlay enables login.
- Kafka is optional, via `docker-compose.kafka.yml`; it carries outbox lifecycle
  events, not job dispatch. Consumers must tolerate duplicates.

## Backups and upgrades

Run `bash scripts/backup-postgres.sh /absolute/new/backup.dump`. Encrypt backups
and store them off-host; they contain tenant data and credential verifiers.
Restore with PostgreSQL 16 `pg_restore --single-transaction --exit-on-error
--no-owner --no-acl` into a new, empty, isolated database. Never restore over
running production data. Verify jobs, tenant access, results, and idempotency
records before cutover. Restore all durable state together and rebuild Redis
hints through reconciliation. Effects after the backup snapshot may repeat.

Before upgrading, back up and rehearse with `make test-operations`. Stop
admission and drain/stop services, then deploy the reviewed images together.
The API applies migrations before readiness. Prefer forward fixes over schema
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
Retry only after resolving the underlying cause. Never delete an older restored
checkpoint to silence this alert. A checksum alone does not prove a backup can
restore; use the isolated restore drill and its recovery receipt.

## Certificate expiry

Identify the expiring endpoint and role from bounded labels, inspect the mounted
certificate chain and issuer, then stage a replacement with overlapping trust.
This deployment rotates certificates by coordinated restart and can interrupt
active channels. Verify each service's health and authenticated scrape after
restart; do not remove the old CA until all clients have moved.
