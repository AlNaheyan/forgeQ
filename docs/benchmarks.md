# ForgeQueue benchmarks

This page contains the reproducible benchmark procedure and the results of the
2026-10-16 local Wave 11 run. The numbers describe one Docker Desktop host;
they are not production capacity claims.

## Workloads

`load/jobs.js` submits a fixed number of zero-duration `sleep` jobs at a fixed
arrival rate. It fails when k6 cannot sustain that rate, when the API rejects a
job, or when the API latency thresholds are exceeded. Its controls are:

| Variable | Default | Meaning |
| --- | ---: | --- |
| `BASE_URL` | `http://localhost:8080` | ForgeQueue API origin |
| `JOBS` | `10000` | Exactly 10k, 50k, 100k, or another positive count |
| `RATE` | `100` | Target submissions per second |
| `CONCURRENCY` | `32` | Preallocated and maximum k6 virtual users |
| `JOB_DURATION_MS` | `0` | Worker handler duration; keep at `0` for pipeline benchmarks |
| `RUN_ID` | generated | Unique prefix for idempotency keys and metric tags |

The constant-arrival scenario rounds its duration up to a whole second and
guards the extra iterations, so a successful run sends exactly `JOBS`
requests. A `dropped_iterations` threshold of zero makes insufficient k6
concurrency visible rather than silently reporting a lower offered load.

`load/burst.js` sends an unpaced burst while a separate scenario probes
`/readyz`. With `EXPECT_429=true`, it passes only if backpressure activates at
least once, all submission responses are either `201` or `429`, and P99
readiness latency remains below 500 ms.

## Reproducible setup

Record the commit and keep the machine otherwise idle. The baseline topology is
one API, one coordinator, and four workers with concurrency four each:

```bash
git rev-parse HEAD
docker compose up -d --build --scale worker=4
docker compose ps
curl --fail http://localhost:8080/readyz
mkdir -p artifacts/benchmarks
```

Wait for all four workers to appear in `/v1/workers` before starting. Use a
fresh database for each compared run, or wait until every submitted job reaches
a terminal state. Mixing old queued jobs into a run invalidates queue-depth,
wait-time, and completion-throughput comparisons.

Capture the environment with the results:

```bash
uname -a
docker version
docker compose version
k6 version
docker stats --no-stream
```

The generator is intentionally outside Compose so its CPU and network costs can
be identified separately. For strong results, run k6 from a second machine and
set `BASE_URL` to the Docker host.

## Baseline: 10k, 50k, and 100k jobs

Choose an arrival rate the baseline can sustain, then keep `RATE`, job payload,
and k6 host fixed for all job-count runs. These examples offer 500 jobs/second:

```bash
k6 run --summary-export artifacts/benchmarks/baseline-10k.json \
  -e JOBS=10000 -e RATE=500 -e CONCURRENCY=64 \
  -e RUN_ID="baseline-10k-$(date -u +%Y%m%dT%H%M%SZ)" load/jobs.js

k6 run --summary-export artifacts/benchmarks/baseline-50k.json \
  -e JOBS=50000 -e RATE=500 -e CONCURRENCY=64 \
  -e RUN_ID="baseline-50k-$(date -u +%Y%m%dT%H%M%SZ)" load/jobs.js

k6 run --summary-export artifacts/benchmarks/baseline-100k.json \
  -e JOBS=100000 -e RATE=500 -e CONCURRENCY=64 \
  -e RUN_ID="baseline-100k-$(date -u +%Y%m%dT%H%M%SZ)" load/jobs.js
```

## Scale-out: 2, 4, and 8 workers

Use the 100k workload and an offered rate high enough to expose worker-side
capacity. Change only the worker count between runs:

```bash
docker compose up -d --scale worker=2
k6 run --summary-export artifacts/benchmarks/scale-2-workers.json \
  -e JOBS=100000 -e RATE=1000 -e CONCURRENCY=256 \
  -e RUN_ID="scale-2-$(date -u +%Y%m%dT%H%M%SZ)" load/jobs.js

docker compose up -d --scale worker=4
k6 run --summary-export artifacts/benchmarks/scale-4-workers.json \
  -e JOBS=100000 -e RATE=1000 -e CONCURRENCY=256 \
  -e RUN_ID="scale-4-$(date -u +%Y%m%dT%H%M%SZ)" load/jobs.js

docker compose up -d --scale worker=8
k6 run --summary-export artifacts/benchmarks/scale-8-workers.json \
  -e JOBS=100000 -e RATE=1000 -e CONCURRENCY=256 \
  -e RUN_ID="scale-8-$(date -u +%Y%m%dT%H%M%SZ)" load/jobs.js
```

Do not begin the next run until the previous run is fully drained. Confirm the
registered and healthy worker counts in Grafana or `/v1/workers`, and capture a
Prometheus snapshot for the exact run interval. If an offered rate saturates the
API before the workers, report that as an API limit rather than worker scaling.

## Backpressure burst

Set `MAX_QUEUE_DEPTH` below `BURST_JOBS`, restart the API with that setting, and
run:

```bash
k6 run --summary-export artifacts/benchmarks/backpressure-burst.json \
  -e BURST_JOBS=10000 -e CONCURRENCY=200 -e EXPECT_429=true \
  -e RUN_ID="burst-$(date -u +%Y%m%dT%H%M%SZ)" load/burst.js
```

Record accepted requests, `429` responses, `Retry-After` presence, readiness
P99, maximum queue depth, and time for the queue to drain. A passing probe shows
that the API remained responsive; it does not by itself prove that every
accepted job completed.

## Measurement rules

- Submission throughput and API P50/P95/P99 come from the saved k6 summary.
- For an end-to-end completed-work report, completion throughput is the exact
  terminal job count divided by the interval from first creation to last
  attempt completion. Use `rate(forgequeue_jobs_completed_total[1m])` for a
  live rolling view, but do not substitute HTTP submission rate.
- Queue-wait percentiles may come from exact persisted attempt timestamps, as
  in this report, or from `forgequeue_job_queue_wait_seconds_bucket` with
  `histogram_quantile` for a live view. State which source was used.
- Maximum queue depth comes from `forgequeue_queue_depth` over the run window.
- Error rate includes unexpected HTTP responses, dropped k6 iterations, and
  terminal job failures. Report `429` separately when testing backpressure.
- A run is invalid if k6 reports dropped iterations, the intended worker count
  is not healthy, dependencies restart unexpectedly, or accepted jobs have not
  been accounted for as terminal or still queued at measurement end.
- Repeat each configuration at least three times after one warm-up. Report the
  median and retain every raw summary; do not select only the fastest run.

## Measured environment

| Field | Measured value |
| --- | --- |
| Date/time (UTC) | 2026-10-16 04:23–05:04 |
| Git revision | `3491a21da29e0ac18b25a39d77cec3b6e365faae`, plus the uncommitted Wave 9–11 working tree under test |
| Host CPU | Apple M3, 8 logical CPUs |
| Host memory | 16 GiB |
| OS/kernel | macOS arm64, Darwin 24.6.0 |
| Docker version/resources | Engine 28.3.0; Compose v2.38.1-desktop.1; 8 CPUs and 8,218,316,800 bytes (7.65 GiB) assigned to Docker Desktop |
| k6 version and host | k6 v2.3.0, official `grafana/k6:latest` Linux/arm64 container on the same host |
| PostgreSQL/Redis versions | PostgreSQL 16.15; Redis 7.4.11 |
| Scheduler strategy | `least_loaded` |
| API/coordinator replicas | 1 / 1 |
| Worker concurrency | 4 per worker |
| Payload / admission limit | zero-duration `sleep`; `MAX_QUEUE_DEPTH=100000` |

The baseline used 500 submissions/s and 64 k6 VUs. The scale comparison used
1,000 submissions/s and 256 k6 VUs for every topology. The initial two-worker
1,000-job warm-up at 128 VUs dropped one generator iteration, so it was
invalidated and retained as evidence. A 5,000-job warm-up at 256 VUs then
passed, and 256 was held constant for all measured scale runs. Each subsequent
four- and eight-worker topology also received a passing 5,000-job warm-up.

Every measured run used a unique idempotency-key prefix. The database was not
reset between runs; instead, the next run began only after all jobs from the
previous prefix were terminal and both Redis/Prometheus queue depth and worker
active-job counts returned to zero. Completion throughput is
`jobs / (last attempt finished_at - first job created_at)`. Queue-wait
percentiles are calculated from PostgreSQL as `attempt.started_at -
job.created_at`. Maximum queue depth is the largest coordinator gauge sample
in the exact run interval. API percentiles and error/drop counts come from the
linked k6 summary. The benchmark operator recorded the SQL/Prometheus-derived
completion, queue-wait, and queue-depth values in the tables, but did not retain
the query output or a Prometheus snapshot. Those values are reported as
measured results, but cannot be independently reconstructed from the committed
artifacts; rerun with query captures before using them for capacity planning.

Only one measured sample was collected for each configuration after warm-up,
not the three repetitions recommended above. These results complete the Wave
11 functional benchmark matrix, but should be repeated before using small
differences as capacity-planning evidence.

## Baseline results

| Jobs | Workers | Offered jobs/s | Completed jobs/s | API P50 | API P95 | API P99 | Max queue depth | Error rate | Evidence |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 10,000 | 4 | 500 | 453.76 | 1.04 ms | 2.98 ms | 18.22 ms | 1,689 | 0% | [k6 JSON](../artifacts/benchmarks/2026-10-16/baseline-4-workers-10k.json) |
| 50,000 | 4 | 500 | 498.81 | 0.94 ms | 1.99 ms | 9.81 ms | 2,310 | 0% | [k6 JSON](../artifacts/benchmarks/2026-10-16/baseline-4-workers-50k.json) |
| 100,000 | 4 | 500 | 498.37 | 0.93 ms | 2.52 ms | 11.31 ms | 3,761 | 0% | [k6 JSON](../artifacts/benchmarks/2026-10-16/baseline-4-workers-100k.json) |

All baseline rows submitted exactly the requested count, reported zero dropped
iterations and zero rejected or unexpected responses, and ended with every job
in `succeeded`. The 10k run's lower end-to-end rate reflects its proportionally
larger startup/drain tail; the 50k and 100k runs both converged near 499 jobs/s.

## Scale-out results

| Workers | Jobs | Worker concurrency | Completed jobs/s | Speedup vs. 2 | Queue-wait P50 | Queue-wait P95 | Queue-wait P99 | Max queue depth | API P99 | Error rate | Evidence |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2 | 100,000 | 4 | 707.92 | 1.00x | 38.40 s | 43.90 s | 44.17 s | 43,896 | 30.55 ms | 0% | [k6 JSON](../artifacts/benchmarks/2026-10-16/scale-2-workers-100k.json) |
| 4 | 100,000 | 4 | 800.66 | 1.13x | 21.08 s | 27.74 s | 28.35 s | 27,210 | 58.00 ms | 0% | [k6 JSON](../artifacts/benchmarks/2026-10-16/scale-4-workers-100k.json) |
| 8 | 100,000 | 4 | 468.96 | 0.66x | 70.32 s | 104.46 s | 110.34 s | 63,656 | 94.08 ms | 0% | [k6 JSON](../artifacts/benchmarks/2026-10-16/scale-8-workers-100k.json) |

Each scale row accepted and succeeded exactly 100,000 jobs with zero dropped
iterations, rejections, unexpected responses, or terminal failures.

## Backpressure result

A separate 500-request unpaced burst ran with a deliberately small admission
limit. The API accepted 14 jobs and rejected 486 with `429`; every rejection
included `Retry-After`, and k6 recorded no unexpected responses or dropped
iterations. All 51 concurrent `/readyz` probes passed. Readiness latency was
8.12 ms at P95 and 23.36 ms at maximum, and the configured P99-under-500-ms
threshold passed. See the [k6 JSON](../artifacts/benchmarks/2026-10-16/backpressure-bounded.json).

## Interpretation

Scaling from two to four workers improved end-to-end throughput by 13% and cut
median queue wait by 45%. It was not close to a 2x gain, which shows that worker
slots were not the only constrained resource on this single-host topology.

Eight workers regressed to 0.66x the two-worker throughput, while API P99 rose
to 94.08 ms, maximum queue depth reached 63,656, and queue-wait P99 reached
110.34 seconds. The likely bottleneck is contention on shared host and
PostgreSQL resources: adding workers also adds concurrent lease/attempt/status
transactions while the API inserts 1,000 jobs/s into the same database. That
is an inference from the shape of the results, not a database profile; a
follow-up should capture PostgreSQL wait events, transaction latency, CPU, and
I/O before attributing the regression to one specific query or lock.

The practical result for this environment is that four workers were the best
of the measured 2/4/8 configurations. More workers reduced capacity rather
than increasing it, so production scaling should be driven by queue-wait and
database telemetry instead of worker count alone.

## Raw evidence

The required-run summaries are linked from the tables. Warm-up evidence is
also retained:

- [four-worker 1k warm-up](../artifacts/benchmarks/2026-10-16/warmup-4-workers-1k.json)
- [invalid two-worker 1k warm-up at 128 VUs](../artifacts/benchmarks/2026-10-16/warmup-2-workers-1k.json)
- [passing two-worker 5k warm-up at 256 VUs](../artifacts/benchmarks/2026-10-16/warmup-2-workers-5k.json)
- [passing four-worker 5k scale warm-up](../artifacts/benchmarks/2026-10-16/warmup-scale-4-workers-5k.json)
- [passing eight-worker 5k scale warm-up](../artifacts/benchmarks/2026-10-16/warmup-scale-8-workers-5k.json)
