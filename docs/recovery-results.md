# Wave 16: reliability and recovery proof

PostgreSQL owns job state and lease generations. Redis holds disposable ready
and delayed hints. Recovery restores those hints from PostgreSQL; it does not
recover a lost PostgreSQL volume. Kafka is not needed for these guarantees.

## Changes

- Every lease acquisition increments `lease_generation`. Start, renewal,
  success, and failure compare the supplied generation with the worker ID,
  state, and expiry in the same PostgreSQL update. Reusing a worker ID cannot
  authorize a delayed request from an older lease.
- Workers carry the generation from their claim through gRPC and renewal.
  Zero and stale generations fail closed; the coordinator never looks up a
  fresh generation to authorize an old request.
- Reconciliation walks bounded pages by stable job ID, wrapping after the last
  page. Already-present high-priority jobs cannot hide missing hints beyond the
  first 1,000 rows. Redis still orders execution hints by priority and age.
- Migration 007 makes unlisted event transitions, including cancellation, a
  no-op for event capture instead of aborting the job transaction. Job event
  payloads also include the lease generation.

## Reproduce with isolated dependencies

```bash
make test-recovery
```

Requirements: Go, Docker Compose, and an available Docker daemon. The runner
uses a unique Compose project, a disposable `forgequeue_recovery` database,
and allocated loopback ports pinned across restarts. Container operations
verify project and service labels before stopping anything.

PostgreSQL is capped at 0.5 CPU / 256 MB and Redis at 0.5 CPU / 128 MB. Go uses
`GOMAXPROCS=2` by default, packages run sequentially, and the worker
test uses one job slot. The 1,001-row pagination fixture is the smallest fixture
exceeding the production page; those rows run no handlers. This is a correctness
suite, not a throughput benchmark.

The runner builds API/coordinator/worker binaries, runs all tests including
PostgreSQL/Redis integration tests, and writes evidence under
`artifacts/recovery/forgequeue-recovery-<timestamp>-<pid>/`. On exit it removes
its own containers, network, database volume, and temporary binaries. Evidence
is retained. The older integration fixtures truncate tables, so use this runner
instead of pointing `make test-integration` at valuable data.

## Assertions

| Scenario | Required outcome |
| --- | --- |
| Crash after claim, before start | New lease generation; old start/renewal rejected |
| Crash during execution, reused worker ID | New attempt completes; old success/failure/renewal rejected |
| Side effect commits before lost acknowledgment | Unguarded effect occurs twice; `ExecuteOnceTx` effect commits once across two attempts |
| Ready and delayed hints lost | All 1,002 ready hints restored, including a due retry; no duplicate sorted-set members |
| Real worker SIGKILL/restart | Natural lease expiry; replacement with same ID completes attempt 2 |
| Coordinator SIGKILL/restart | Existing durable lease can renew and complete |
| API SIGKILL/restart | Job remains readable; same submission key returns original job |
| Redis stop/start without persistence | Readiness fails during outage; admission fails closed; queued hint restored |
| PostgreSQL stop/start with volume retained | Reads/submissions fail during outage; pools reconnect; committed result survives |
| Startup with PostgreSQL unavailable | Exit during dependency initialization, before opening API listener |
| Events on/off | Cancellation succeeds in both configurations |

## Availability and data-loss boundary

API `/healthz` reports liveness; `/readyz` checks PostgreSQL and Redis with
configured timeouts. The API can accept durable work while the coordinator is
restarting; readiness does not promise worker capacity. Coordinator gRPC health
describes its server lifecycle, not an ongoing dependency probe. Claims and
mutations still fail if PostgreSQL is unavailable.

Redis is disposable storage but remains a runtime dependency: dispatch pauses
during an outage and admission fails closed when queue depth is unavailable.
A job committed before enqueue fails remains recoverable. Retry promotion may
persist `queued` before a failed enqueue; reconciliation repairs it later.

Reconciliation repairs one page per tick. For a fixed backlog a sweep takes
approximately `ceil(queued_jobs / 1000)` polling intervals plus operation time.
A missing hint behind the cursor waits for another sweep. This is a bounded-work
design, not a fixed recovery SLA. Database pools may return a transient error
while discarding sockets closed during restart; a healthy readiness request
does not guarantee every pooled connection is already refreshed. Retrying
submissions with the same idempotency key resolves ambiguous responses.

Fencing protects queue state. It cannot undo an external effect already
performed by an expired worker. External systems need stable idempotency keys
or their own fencing. PostgreSQL effects can use `ExecuteOnceTx` to commit the
effect and its completion marker atomically.

These tests retain the PostgreSQL volume. They do not prove recovery from disk
loss, corruption, multi-host partitions, or regional failure. Backup/restore
belongs to Wave 18. Reported failure retries honor `max_attempts`; the existing
expired-lease recovery path requeues crashed attempts independently of that
budget. Repeated crashes can keep a job active and require operator intervention.

## Upgrade notes

Migrations 006/007 are additive. Upgrade coordinator and workers together:
older workers omit the generation and their mutations are rejected. Drain work
before upgrade, or let old leases expire and recover with a new generation.
Keep API/coordinator migration startup ordered as in the supported Compose
stack. These tests do not establish mixed-version rolling-upgrade compatibility.

## Recorded verification

Run `forgequeue-recovery-1792646609-42739` passed the complete suite: **134
top-level tests**, with PostgreSQL and Redis integration tests enabled. The
new recovery package completed in 15.034 seconds on Darwin arm64, Go 1.25.0.
The resource limits above were applied. Raw [test output](../artifacts/recovery/forgequeue-recovery-1792646609-42739/tests.txt)
and [environment record](../artifacts/recovery/forgequeue-recovery-1792646609-42739/environment.txt)
are retained; the environment records the base commit and uncommitted changes
under test, since the implementation was verified before committing it.

| Observation | Elapsed time |
| --- | ---: |
| Coordinator restart through renewed lease and completion | 1.049 s |
| API restart through readable job and deduplicated submission | 64 ms |
| Redis start through restored queued hint | 1.273 s |
| PostgreSQL start through pool recovery and successful submission retry | 243 ms |
| Worker kill through replacement completion (5 s lease + 2 s handler) | 7.110 s |
| Repair 1,002 ready hints, including a due retry, with direct bounded passes | 501 ms |

These are single local observations, not p99 latency, repeated-run statistics,
or production availability guarantees. Restart measurements start after the
old process/container is stopped; the worker measurement starts after SIGKILL.
Direct reconciliation passes do not include the normal polling interval.
The side-effect test observed two committed effects without a guard and one
with `ExecuteOnceTx`, each across two handler attempts. Cleanup removed the
isolated dependencies and test database volume after the run.

Focused race checks for coordinator, worker, and queue packages passed, as did
`go vet ./...` and the patch whitespace check. The race run used unit tests;
the live outage evidence above comes from the ordinary integration run.
