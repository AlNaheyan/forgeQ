# ForgeQueue chaos experiments

This report records Wave 12 failure-injection runs. Experiments use the local
Docker Compose stack and retain PostgreSQL data. They restart or kill only the
named service/container; they never remove volumes.

## Commands

The chaos CLI chooses distinct running worker containers at random and writes
one JSON event per killed container, including UTC timestamps:

```bash
go run ./cmd/chaos worker-kill --count 2
go run ./cmd/chaos worker-kill --count 2 --dry-run
go run ./cmd/chaos restart redis
go run ./cmd/chaos restart coordinator
```

The default worker-crash experiment is intentionally small for a developer
machine: 16 jobs at 8 jobs/s, four workers, and 15-second handler duration. It
kills two workers two seconds after submission begins and retains the k6
summary, log, worker-health timeline, PostgreSQL terminal/attempt summary, and
kill event timestamps under `artifacts/chaos/<run-id>/`:

```bash
make chaos-worker-crash
```

Override `RUN_ID` to select an artifact directory and `JOBS`, `RATE`,
`WORKER_COUNT`, or `KILL_AFTER_SECONDS` to adjust the test. Before running,
confirm the API is ready, the intended worker count is healthy, the queue is
drained, and `MAX_QUEUE_DEPTH` exceeds the workload. Do not launch another
workload until the previous run is terminal. Large runs are opt-in; they are
not needed for the recovery checks in this report.

The Redis and coordinator experiments default to 500 jobs at 50 jobs/s,
restart the selected service two seconds after submission begins, and wait for
all accepted jobs to become terminal:

```bash
make chaos-redis-restart
make chaos-coordinator-restart
```

The chaos load script classifies expected 5xx responses during the Redis
outage separately from unexpected responses. A coordinator restart should not
cause API submission errors; worker registration/health snapshots show the
reconnect interval.

## Measurement definitions

- Failure injection time is the `started_at` timestamp in the CLI JSONL event.
- Worker detection time ends when the coordinator first reports a killed
  worker unhealthy in `GET /v1/workers`; keep a timestamped poll log.
- Recovery time ends when all jobs from the run have reached a terminal state
  and the queue and active-job counts return to zero.
- Jobs recovered are run jobs that had an open attempt at worker loss and
  later reached a terminal state after retry/reassignment.
- Jobs lost are run jobs not in a terminal state after the drain timeout.
- Duplicate execution is the number of run jobs with more than one attempt.
  This is expected under at-least-once delivery and should be reported rather
  than called data loss.
- Redis recovery is successful when API readiness returns and every accepted
  job reaches a terminal state after PostgreSQL reconciliation restores queue
  hints.
- Coordinator recovery is successful when workers re-register/heartbeat and
  queued or leased jobs resume without manual state changes.

## Worker crash / lease recovery

| Field | Result |
| --- | --- |
| Run ID / UTC interval | `wave12-worker-lease-recovery-20261016`; 05:59:37–06:00:28 UTC |
| Workload | 16 accepted jobs; 8 submissions/s; 4 workers; 15-second sleep handler |
| Killed workers | `deployments-worker-5` at 05:59:39.918Z; `deployments-worker-6` at 05:59:40.015Z |
| Detection time | 12.2s and 13.1s after respective kills, first unhealthy health samples |
| Recovery time | 47.7s from first kill until the final job finished |
| Jobs recovered | 1 job had a retry after worker loss; all 16 reached succeeded |
| Jobs lost | 0 |
| Jobs with duplicate attempts | 1 (at-least-once retry; handler has no external side effect) |
| HTTP errors / dropped iterations | 0 unexpected; 0 dropped |
| Evidence | [k6 summary](../artifacts/chaos/wave12-worker-lease-recovery-20261016/k6-summary.json), [kill events](../artifacts/chaos/wave12-worker-lease-recovery-20261016/worker-kills.jsonl), [health timeline](../artifacts/chaos/wave12-worker-lease-recovery-20261016/worker-health.jsonl), [PostgreSQL summary](../artifacts/chaos/wave12-worker-lease-recovery-20261016/postgres-summary.txt) |

An earlier exploratory run attempted 100,000 jobs at 1,000 submissions/s with
eight workers. It accepted 99,895 and reported 106 dropped generator iterations;
all 99,895 eventually succeeded, but the killed workers did not have an
in-flight job to recover. It is not counted as the recovery result above and
was not repeated. See [exploratory artifacts](../artifacts/chaos/wave12-worker-crash-20261016/).

## Redis restart

| Field | Result |
| --- | --- |
| Run ID / UTC interval | `wave12-redis-restart-verified-20261016`; 06:06:23–06:06:34 UTC |
| Workload | 500 planned; 50 submissions/s; 128 VUs |
| Redis outage duration | 558ms Compose restart; readiness returned within 0.77s of restart completion |
| Jobs accepted / terminal / lost | 496 accepted; all 496 succeeded; 0 lost |
| Requests during outage | 4 unavailable responses; 0 unexpected; 0 dropped iterations |
| Queue reconciliation observed | Accepted jobs completed after Redis returned; PostgreSQL summary reports no retry attempts. This verifies completion, not a direct queue-depth reconciliation trace. |
| Evidence | [k6 summary](../artifacts/chaos/wave12-redis-restart-verified-20261016/k6-summary.json), [restart event](../artifacts/chaos/wave12-redis-restart-verified-20261016/service-restart.json), [readiness timeline](../artifacts/chaos/wave12-redis-restart-verified-20261016/readiness.jsonl), [PostgreSQL summary](../artifacts/chaos/wave12-redis-restart-verified-20261016/postgres-summary.txt) |

## Coordinator restart

| Field | Result |
| --- | --- |
| Run ID / UTC interval | `wave12-coordinator-restart-light-20261016`; 06:05:26–06:05:34 UTC |
| Workload | 500 jobs; 100 submissions/s |
| Coordinator restart duration | 446ms |
| Worker re-registration | Not confirmed: `/v1/workers` returned an empty list in post-restart samples. This is an observability gap, despite successful job processing. |
| Jobs accepted / terminal / lost | 500 accepted; all 500 succeeded; 0 lost |
| Submission errors / dropped iterations | 0 unavailable; 0 unexpected; 0 dropped |
| Evidence | [k6 summary](../artifacts/chaos/wave12-coordinator-restart-light-20261016/k6-summary.json), [restart event](../artifacts/chaos/wave12-coordinator-restart-light-20261016/service-restart.json), [worker-health timeline](../artifacts/chaos/wave12-coordinator-restart-light-20261016/worker-health.jsonl), [PostgreSQL summary](../artifacts/chaos/wave12-coordinator-restart-light-20261016/postgres-summary.txt) |

An initial Redis run (2,000 planned, 100/s) and a 500-job low-VU retry are
retained for transparency; the latter had six dropped generator iterations.
The verified 500-job run above had none.

## Limitations

An attempt count above one measures repeated attempts, not proof that a
downstream side effect happened more than once. The built-in sleep handler has
no external side effect to deduplicate. Validate at-least-once side-effect
behavior separately with an idempotent test handler before making stronger
claims.
