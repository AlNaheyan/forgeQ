# Tenant safety — Wave 17

Every `/v1` operation and API `/metrics` requires `Authorization: Bearer <api_key>`.
Only `/`, `/dashboard` (an empty shell), `/healthz`, and `/readyz` are public.
There is no development authentication bypass and no public administration API.

## Bootstrap and credential lifecycle

Start PostgreSQL/Redis, export `DATABASE_URL`, then use the DB-admin CLI:

```bash
export CONTROL_PLANE_TOKEN="$(openssl rand -hex 32)"
export DATABASE_URL='postgres://forgequeue:forgequeue@localhost:55432/forgequeue?sslmode=disable'
docker compose up -d postgres redis
go run ./cmd/tenant create --name local --operator
```

The output includes `tenant_id`, `key_id`, and `api_key`. The secret is displayed
once; put it in a password manager. CLI stdout is sensitive: do not capture it
in shared CI logs. Export `FORGEQUEUE_API_KEY` for curl/k6/health-polling tools,
or paste it into the dashboard password field. The dashboard holds it only in
memory, clears the input after connecting, and reloads to clear/switch sessions.
No URL parameters, cookies, or browser storage hold credentials.

```bash
go run ./cmd/tenant key --tenant-id TENANT_UUID
go run ./cmd/tenant rotate --key-id KEY_UUID
go run ./cmd/tenant revoke --key-id KEY_UUID
go run ./cmd/tenant limits --tenant-id TENANT_UUID \
  --max-pending 1000 --max-concurrent 4 --per-minute 60 --max-payload-bytes 65536
```

`key` creates an additional key; `rotate` atomically revokes the old key and
issues its replacement, preserving tenant and operator permissions. Revocation
is checked on every request, with no cache. Already authenticated in-flight
requests may finish. Lost replacement output requires issuing another key.
Keys contain 256 random bits; PostgreSQL stores only their SHA-256 verifiers,
not the bearer secret. Tenant keys have no automatic expiration in this version.

An operator key permits **global fleet and metrics views**, but job reads and
mutations remain scoped to its own tenant. Create separate regular keys for
applications; operator credentials are for trusted deployment administrators.

## Isolation and quotas

Repository predicates scope job lists (including pagination), reads, cancellation,
and retries to the authenticated tenant. Cross-tenant job IDs return 404, just
like missing IDs. Submission and execution idempotency use `(tenant_id, key)`;
the coordinator carries tenant identity to the worker's handler context.
Clients cannot choose identity through the JSON request body.

Defaults: 1,000 outstanding jobs, 4 concurrent leases, 60 admissions per fixed
database-clock minute, and 65,536 payload bytes. The global request cap remains
1 MiB including JSON envelope. PostgreSQL tenant-row locks serialize admission
and lease acquisition across API/coordinator instances. Outstanding means
`queued`, `retry_wait`, `leased`, or `running`; terminal jobs do not count.
Submission and manual dead-letter retry consume admission capacity. Automatic
retries retain their existing outstanding slot. Idempotent submission replays
do not consume another slot or rate allowance.

The minute counter resets at a clock-minute boundary, not a sliding window;
boundary bursts are possible. Concurrency counts unexpired durable leases, not
uncooperative external side effects after expiry. Lowering limits does not kill
existing jobs; new admissions/leases wait until usage permits them. A saturated
tenant's queue hints are deferred until the end of a bounded claim scan, allowing
other candidates in that scan to run. This is not weighted fair scheduling.
The existing global Redis `MAX_QUEUE_DEPTH` remains a process-local soft guard;
tenant quotas are the cross-replica durable bounds.

| Response | Meaning |
| --- | --- |
| 401 | Missing, invalid, rotated, or revoked key |
| 403 | Valid tenant key lacks operator permission |
| 404 | Job missing or belongs to a different tenant |
| 413 | Tenant payload or global request-body cap exceeded |
| 429 | Admission rate/outstanding quota or global queue guard; inspect error code and `Retry-After` |
| 503 | Credential lookup unavailable; fails closed |

`Retry-After: 60` is advisory for tenant quotas: an outstanding-job quota can
remain full longer than a minute. Concurrency saturation leaves jobs queued;
it does not reject already admitted jobs.

## Monitoring and deployment boundary

Prometheus's API scrape now requires an operator key. Issue a dedicated one,
then save **only the secret token** in `deployments/secrets/prometheus-api-key`.
This directory is gitignored and mounted read-only into Prometheus. Restrict
host file access while allowing the container's Prometheus user to read it.
Until provisioned, API scraping fails closed; readiness remains usable.
Rotate this key with the CLI and replace the file. Coordinator metrics are
private-network-only. Prometheus/Grafana host ports bind to loopback; the demo
Grafana anonymous-admin configuration is strictly local, not internet-ready.

API, worker, and coordinator require the same independently generated
`CONTROL_PLANE_TOKEN` of at least 32 bytes. Internal RPCs reject tenant API keys.
Rotate it through a coordinated service restart. This authenticates a trusted
service pool, not individual workers. The current service transport is plaintext
on a private network; use TLS termination for public HTTP and an encrypted,
restricted service network (or add mTLS) before remote deployment. Never expose
PostgreSQL, Redis, worker RPC, or internal metrics to untrusted clients.
The Compose defaults are a local demonstration, not a production security profile.

Database access, administrators, and worker code are trusted across tenants.
This is application-level isolation, not PostgreSQL row-level security or a
sandbox for user-supplied code. Custom handlers must use tenant-scoped external
resource identifiers and idempotency keys. There is no signup/billing system,
per-tenant CPU sandbox, generic read-rate limiter, or DDoS defense yet.

## Upgrade and telemetry

Migration 008 assigns existing jobs and idempotency records to the legacy tenant
`00000000-0000-4000-8000-000000000001`, with generous compatibility quotas and
**no API key**. An administrator can explicitly issue a key for that tenant and
then lower its quotas. New tenants use the safer defaults above. Upgrade all
service binaries together: old workers do not carry the tenant context needed
for execution idempotency. Take a database backup before applying migrations.

No new tenant/key/payload metric labels are introduced. Handler error details
remain in tenant-scoped durable job results, not worker completion logs. New
outbox events omit `last_error` (which may contain secrets); job payloads and
results are not emitted in lifecycle events. Historical pre-upgrade event/log
data is not rewritten—apply your retention/incident policy separately. Avoid
logging secrets in custom handlers, and disable HTTP header/body debug capture
in proxies, tracing agents, and load tools.

## Verification

`make test-recovery` runs the complete suite with disposable, resource-limited
PostgreSQL/Redis, including real API-process authentication and recovery tests.
Security tests exercise two-tenant denial/list isolation, independently scoped
idempotency, verifier storage, rotation/revocation, HTTP 401/403/413/429, concurrent
admission and lease limits, retry quota enforcement, and event redaction.
No heavy benchmark is required.

Verified on 2026-10-22 (UTC): **139 top-level tests passed, none failed or
skipped**, against the isolated stack. The security integration case completed
in under one second; this is correctness evidence, not a throughput benchmark.
Raw [test output](../artifacts/recovery/forgequeue-recovery-1792649504-46971/tests.txt)
and [environment](../artifacts/recovery/forgequeue-recovery-1792649504-46971/environment.txt)
are retained. Focused race tests for API/coordinator/tenant/worker, `go vet ./...`,
Compose configuration validation, and `git diff --check` also passed. The macOS
linker emitted LC_DYSYMTAB warnings on race binaries; no race was reported.
