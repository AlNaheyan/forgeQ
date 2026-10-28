# Operations and lifecycle — Wave 18

## Supported deployment

The supported starting point is one private Linux/Docker Compose host, one API,
one coordinator, PostgreSQL 16, Redis 7, and one or more trusted Go workers.
This is not a highly available deployment. A host failure requires restore;
multiple coordinators are not yet a supported scheduling topology. Kafka remains
optional. Public HTTP needs a TLS reverse proxy; databases, service RPCs, metrics,
and administration ports must remain private. All published demo ports bind to
loopback. See [tenant security](tenant-security.md) for the trust boundary.

Start with 2 CPU cores and 4 GiB allocated to Docker; allow more for monitoring,
image builds, or heavier handlers. These are starting resource assumptions, not
capacity claims. The operations overlay caps PostgreSQL at 512 MiB, Redis at
192 MiB, each application at 256 MiB, Prometheus at 384 MiB, and Grafana at
256 MiB. Workers default to concurrency 2. Test with your actual payloads before
raising concurrency. Redis uses `noeviction`: allocation errors must surface,
not silently drop queue hints. PostgreSQL remains authoritative.

### Fresh installation

Use a reviewed source revision. Generate unique random hexadecimal database and
service passwords, and a separate Grafana administrator password. Export
`POSTGRES_PASSWORD`, `CONTROL_PLANE_TOKEN`, and `GRAFANA_ADMIN_PASSWORD`, or put
them in an access-restricted, ignored environment file. Never commit them.
Existing PostgreSQL volumes do not change passwords when environment variables
change: rotate the database role separately during a planned change.

```sh
export FORGEQUEUE_VERSION=$(git rev-parse HEAD)
docker compose -p forgequeue -f deployments/docker-compose.yml -f deployments/docker-compose.operations.yml config --quiet
docker compose -p forgequeue -f deployments/docker-compose.yml -f deployments/docker-compose.operations.yml up -d postgres redis
```

Export `DATABASE_URL` pointing to loopback port 55432 using that database
password. Run `go run ./cmd/tenant create --name local --operator` to apply
migrations and provision the initial operator. Save the credential securely.
Issue a dedicated operator key for Prometheus and write only its token to
`deployments/secrets/prometheus-api-key`; permit read access to the container's
Prometheus UID without making the host directory public.

```sh
docker compose -p forgequeue -f deployments/docker-compose.yml -f deployments/docker-compose.operations.yml up -d --build api coordinator worker
curl --fail http://localhost:8080/readyz
docker compose -p forgequeue -f deployments/docker-compose.yml -f deployments/docker-compose.operations.yml up -d prometheus grafana
```

Connect to `/dashboard` with the operator key, submit one short sleep job, and
verify success and worker health. Verify both Prometheus targets and alert rules.
The API readiness probe checks PostgreSQL and Redis; coordinator `/healthz` is
liveness only. Worker health comes from coordinator heartbeats, not an invented
container readiness signal. API readiness does not guarantee an eligible worker.

Application containers are unprivileged, read-only, drop capabilities, and deny
privilege escalation. The service credential is mandatory. Environment-based
secrets remain visible to trusted Docker administrators: use a managed secret
delivery system before widening the operator boundary. Do not publish rendered
`docker compose config` output, which may contain credentials.

### Versioned releases

`bash scripts/package-release.sh /absolute/new/release-directory` builds the
three service images tagged with the clean Git commit, saves the exact app and
dependency images, tracked source archive, image metadata, and SHA-256 checksums.
It requires the same operations environment. Archive it encrypted off-host.
Tags alone are mutable; the saved image archive plus checksums is the deployment
artifact. Initial builds are not claimed bit-for-bit reproducible across time.

To redeploy on the same CPU architecture: verify `SHA256SUMS`, extract source
into a new directory, `docker image load -i images.tar`, set `FORGEQUEUE_VERSION`
from `revision.txt`, provision secrets separately, and run the operations Compose
commands with `up -d --no-build --pull never`. Never mix an archive from one
revision with Compose configuration from another. Packaging/building all images
is an operator release step, not part of the modest-machine correctness suite.

## Backup and restore

Run `bash scripts/backup-postgres.sh /absolute/new/backup.dump` from the deployment
checkout. It uses `pg_dump` custom format and refuses existing destinations.
A failed dump leaves `.partial`, not a falsely named successful backup. The
script prints a checksum; record it with PostgreSQL major version, source Git
revision, timestamp, and image manifest. Dumps contain tenant payloads, results,
credential verifiers, and idempotency state: restrict access, encrypt, and copy
off-host. Back up credentials and deployment configuration separately.

Take at least daily backups, retaining seven daily and four weekly copies as an
initial operator policy, adjusted to your needs. A logical backup's recovery
point is its snapshot time; this is not continuous WAL/PITR protection. Measure
restore time on your data. The demo does not promise an RPO or RTO.

Restore only to a **new, empty, isolated database** using PostgreSQL 16 tools:

```sh
createdb --maintenance-db="$RESTORE_ADMIN_URL" forgequeue_restore
pg_restore --dbname="$RESTORE_DATABASE_URL" --single-transaction --exit-on-error \
  --no-owner --no-acl /secure/path/backup.dump
```

Both URLs must point to the isolated destination, never the running source.
Provision the destination role first. Do not use `--clean` against an existing
database. Verify tenants, credential authentication, job counts/status/results,
submission markers, execution markers, and attempts. Apply the target binary's
migrations to that copy, then run a short job with a separate empty Redis.
Do not start restored workers against production external side effects during
a drill. Retain the original backup until verification and cutover are complete.

For actual disaster recovery, stop old writers/workers before cutover. Restore
all PostgreSQL data together, point services at the restored DB and fresh Redis,
start the API/coordinator, allow reconciliation and expired-lease recovery, then
start workers. Previously completed external effects after the backup snapshot
may run again: reconcile those effects using downstream idempotency records.
Do not restore Redis hints as authoritative job state.

`make test-operations` runs the automated drill in disposable PostgreSQL/Redis:
create a schema-008 database, seed a tenant/key/job/result/submission marker,
dump it, restore into a second database, upgrade to 009 twice, authenticate the
restored key, and verify the recovered job. It also exercises retention and the
existing process/dependency outage tests. No user database is touched.

## Upgrade and rollback

1. Record deployed image IDs and take/verify a backup. Rehearse the upgrade on
   a restored copy. Read every new migration before deployment.
2. Stop new admission, drain workers within your chosen deadline, then stop
   workers/coordinator/API. The supplied worker shutdown window is 8 seconds
   inside a 10-second container grace period; longer work may be recovered and
   re-executed after termination. This is not a five-minute drain guarantee.
3. Deploy the reviewed image set together. API startup applies migrations before
   readiness; the migration runner serializes instances with a PostgreSQL
   advisory transaction lock and commits the migration batch atomically.
4. Check readiness, authenticated reads, a short canary job, fleet health,
   dependency errors, and outbox progress before reopening admission.

009 adds indexes only; there is no destructive schema rewrite. Existing data
and tenant semantics are unchanged. Index creation can block writes and take
time on large databases: use the maintenance window and rehearse at scale.
Future changes must use expand/migrate/contract: add compatible structures,
deploy dual-compatible code, backfill in bounded batches, then remove obsolete
structures in a later reviewed release. Do not edit already-applied SQL files.

The new runner refuses unknown/newer migration versions rather than silently
running against an unexpected schema. This is not a universal compatibility
guarantee for old binaries, which may lack that guard. Prefer a forward fix.
Do not delete schema-history rows to force a downgrade. If rollback requires
restoring a snapshot, explicitly accept the loss of writes after that snapshot
and reconcile external effects. There are no automatic down migrations.

## Retention and disk growth

Cleanup is **disabled by default**. Set `RETENTION_ENABLED=true` only after
reviewing the policy and backups. Defaults are 720h for eligible jobs, 168h for
published events, and batch size 100 (maximum 1000), once per minute. Each tick
has a five-second deadline, a cross-instance advisory lock, and row locks with
`SKIP LOCKED`. Both deletions commit together or roll back. The bound is on
parent records; deleting jobs also cascades to attempts, so storage cost is not
a strict bound of 100 physical rows. Large histories can hit the deadline.

| Data | Policy |
| --- | --- |
| Unkeyed succeeded/canceled jobs | Delete after job age, including payload/result and cascading attempts |
| Active/retry-wait/failed/dead-letter jobs | Preserve for execution or operator investigation |
| Jobs referenced by submission idempotency keys | Preserve indefinitely, including replay response |
| Execution-idempotency markers | Preserve indefinitely; deleting them can repeat effects |
| Published, unleased outbox rows | Delete after publication age |
| Unpublished or leased outbox rows | Never automatically delete |
| Tenant/key metadata | Preserve; revoked keys remain revoked |
| External audit/log/Kafka data | Operator-owned retention; not deleted by this janitor |

There is no dedicated security-audit event store yet; lifecycle events are not a
compliance audit trail. Export required audit evidence before outbox retention.
Keyed jobs and markers can grow forever intentionally: plan storage and a
separately reviewed archival/replay-window design before changing that contract.

Watch `forgequeue_retention_deleted_total{kind="jobs|events"}`,
`forgequeue_retention_errors_total`, and
`forgequeue_retention_last_success_timestamp_seconds`. A zero success timestamp
is expected when cleanup is disabled. SQL failures roll back; inspect database
availability, permissions, statement duration, and locks before increasing the
batch. Never fix disk pressure by deleting active rows or idempotency markers.

## Alert runbooks

Rules live in [alerts.yml](../deployments/prometheus/alerts.yml); thresholds are
starting points, not SLOs. Each alert's `runbook` annotation identifies a section
below. Publish these docs at your internal runbook URL when wiring notifications.
Prometheus evaluates rules locally; Alertmanager/paging routing is not configured
by this repository. Validate with `promtool check rules alerts.yml` and
`promtool test rules alerts.test.yml` from `deployments/prometheus`.

### Target or dependency down

Check container status and `/readyz`. If API scrape alone fails, check the
Prometheus operator key and file permissions (rotation/revocation produces
401). For PostgreSQL failures, inspect connectivity, disk, connection limits,
and restart logs; do not replace a volume. For Redis failure, restore connectivity
or restart Redis, then confirm reconciliation repopulates due queued jobs.
`forgequeue_durable_health=0` means the last DB snapshot failed; its old depth/age
values are stale. It does not independently diagnose Redis. Inspect sustained
HTTP 5xx and request timeouts before retrying submissions with the same keys.

### Queue stalled

Compare durable ready depth/oldest age with Redis depth, worker capacity, tenant
concurrency, supported job types, and lease expirations. Confirm dependencies and
reconciliation are healthy. Scale workers modestly only when CPU/DB capacity and
tenant limits allow it. Do not delete queue rows to clear an alert. After fixing
the cause, confirm oldest due age decreases and a canary job completes.

### Worker loss

Check service credentials, coordinator reachability, heartbeat timestamps,
container memory kills, and duplicate worker IDs. Restart only affected workers.
Wait for abandoned leases to expire and verify attempts recover. Never extend
leases manually to hide a dead worker; that delays safe recovery.

### Retry or dead-letter growth

Inspect affected tenant-owned jobs and handler/dependency failures. Fix the
underlying issue before retrying. Retry a small sample, preserving idempotency,
then watch completion and failure rates. Repeated retries consume admission
capacity and can repeat non-idempotent external effects.

### Event publisher lag

Check whether event capture is enabled, Kafka is reachable, publisher processes
are running, and pending outbox leases expire. Restore the publisher/broker and
verify pending age falls. Do not mark unpublished events as published or delete
them to silence the alert. Consumers must tolerate duplicates after ambiguous
publish acknowledgments. Disabling capture does not drain existing events.

## Verification evidence

On 2026-10-28, the isolated suite passed **142 top-level tests**, with no failures
or skips. The schema-008 dump/restore/009-upgrade drill took 0.42 seconds on its
small fixture; retention checks took 0.11 seconds. These are correctness tests,
not production restore-time or throughput estimates. See the retained
[test output](../artifacts/recovery/forgequeue-recovery-1793227045-49905/tests.txt)
and [environment](../artifacts/recovery/forgequeue-recovery-1793227045-49905/environment.txt).
The disposable containers and test volume were removed afterward.

`go vet ./...`, shell syntax checks, operations Compose configuration validation,
and Prometheus 3.6.0 `promtool test rules alerts.test.yml` (firing, delay, and
recovery cases) also passed. Full release image packaging and a complete monitoring
stack rollout were not run as part of this modest-machine verification. Operators
must rehearse those steps on the target host and verify their alert delivery
integration before serving real users.
