# ForgeQueue

ForgeQueue is a production-style distributed job execution platform written in
Go. It is being built to make coordination, failure recovery, at-least-once
delivery, idempotency, backpressure, and observability explicit rather than
hiding them behind a workflow framework.

Waves 0 through 11 are complete: ForgeQueue has a durable job API, Redis-backed
queues, PostgreSQL reconciliation, bounded-concurrency workers, and an explicit
gRPC coordinator that owns worker registration, heartbeats, health, durable job
leases, renewal, crash recovery, attempt tracking, retries, dead-lettering,
pluggable multi-worker scheduling, durable execution-idempotency helpers,
backpressure, Prometheus/Grafana observability, and reproducible k6 workloads.
The Wave 11 report includes measured 10k, 50k, and 100k baselines plus a
100k-job comparison across two, four, and eight workers.

## Planned architecture

```text
Client --HTTP--> API ----------> PostgreSQL (durable truth)
                  |                     ^
                  |                     |
                  +--gRPC--> Coordinator +--> Redis (queue hints)
                                ^
                                |
                         gRPC   |
                              Worker
```

ForgeQueue will provide at-least-once execution. A job may run more than once
when a worker completes its side effect but fails before acknowledging the
result, so side-effecting handlers must be idempotent.

For PostgreSQL-backed effects, `internal/idempotency.Executor` provides
`ExecuteOnce` and transaction-aware `ExecuteOnceTx` guards. See
[`docs/failure-semantics.md`](docs/failure-semantics.md) for the complete
delivery contract and guidance for external side effects.

## Requirements

- Go 1.23 or newer
- Docker with Docker Compose

## Local development

Copy the example environment if you want to override the development defaults:

```bash
cp .env.example .env
```

Start PostgreSQL and Redis:

```bash
docker compose up -d postgres redis
```

Or build and run the complete API/worker pipeline:

```bash
docker compose up --build
```

Scale the worker fleet while keeping one coordinator:

```bash
docker compose up -d --build --scale worker=3
```

Workers receive unique container-hostname IDs automatically. Select
`least_loaded`, `round_robin`, or `weighted_capacity` with the coordinator's
`SCHEDULER_STRATEGY` environment variable.

For long-lived deployments, set `WORKER_ID` from a stable, unique instance
identity (for example, a StatefulSet pod name). Automatically generated IDs
change on restart, so repeatedly restarting workers can create stale
`worker_id` label series in Prometheus. Do not give concurrent workers the
same explicit ID.

Open the built-in operator dashboard at
[http://localhost:8080/dashboard](http://localhost:8080/dashboard). It can
submit jobs, track their state, inspect payloads and results, cancel queued
work, and show live worker health, capacity, and assignment totals. It is
embedded in the Go API, so there is no separate frontend build or service.

Operational telemetry is available through
[Prometheus](http://localhost:9091) and the provisioned
[Grafana dashboard](http://localhost:3000/d/forgequeue-overview/forgequeue-overview).
The API also exposes its Prometheus data directly at
[http://localhost:8080/metrics](http://localhost:8080/metrics).

New submissions are rejected with HTTP `429` and `Retry-After` when the ready
queue reaches `MAX_QUEUE_DEPTH`. The in-flight reservation makes this bound
safe for concurrent requests within one API process; deployments with multiple
API replicas should use a shared atomic admission mechanism.

PostgreSQL is exposed on port `55432` to avoid colliding with a system database;
Redis is exposed on `6379`.

Start the API (migrations are applied automatically):

```bash
go run ./cmd/api
```

Submit and inspect a job:

```bash
curl -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-sleep-1' \
  -d '{"type":"sleep","payload":{"duration_ms":1000}}'

curl http://localhost:8080/v1/jobs?status=queued
```

The worker registers with the coordinator, reports load through heartbeats, and
requests work through gRPC. The coordinator atomically leases queued jobs in
PostgreSQL before they start. Long-running handlers renew their leases; if a
worker disappears, the coordinator requeues its job after the lease expires.
A successful `sleep` job moves from `queued` to `leased` to `running` to
`succeeded`. Retryable failures enter `retry_wait` with capped exponential
backoff and jitter. Exhausted jobs enter `dead_letter` and can be manually
requeued from the dashboard or API.

Available HTTP endpoints:

- `POST /v1/jobs`
- `GET /v1/jobs/{id}`
- `GET /v1/jobs?status=&type=&limit=&cursor=`
- `POST /v1/jobs/{id}/cancel`
- `POST /v1/jobs/{id}/retry`
- `GET /v1/workers`
- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

The sustained and overload workloads live in `load/`. See
[`docs/benchmarks.md`](docs/benchmarks.md) for k6 commands, measurement rules,
raw evidence links, measured results, and limitations.

Wave 12 failure-injection commands are available through `make chaos-worker-crash`,
`make chaos-redis-restart`, and `make chaos-coordinator-restart`. They write timestamped evidence under
`artifacts/chaos/`; see [`docs/chaos-results.md`](docs/chaos-results.md) before
running experiments against the local Compose stack.

Wave 13 runtime limits, connection pools, shutdown behavior, and payload-size
protection are described in [`docs/production-hardening.md`](docs/production-hardening.md).

Wave 14 adds an optional Kafka lifecycle-event stream backed by a transactional
PostgreSQL outbox. It stays disabled in the default stack; opt in with the
Compose overlay and see [`docs/event-stream.md`](docs/event-stream.md) for the
delivery contract and consumer guidance.

Run the test suite:

```bash
go test ./...
```

Run the PostgreSQL and Redis integration tests while the dependencies are up:

```bash
make test-integration
```

Build every command:

```bash
go build ./...
```

Stop the local dependencies:

```bash
docker compose down
```

## Repository map

- `cmd/`: service and tooling entry points
- `internal/config/`: typed environment configuration
- `internal/jobs/`: job model, state machine, and application service
- `internal/idempotency/`: durable handler execution guards
- `internal/repository/postgres/`: durable job storage
- `internal/queue/`: Redis queue and PostgreSQL-to-Redis reconciliation
- `internal/worker/`: worker runtime, concurrency control, and built-in handlers
- `internal/coordinator/`: worker registry, health state, and gRPC control plane
- `proto/`: coordinator protocol and generated Go stubs
- `internal/api/`: HTTP transport
- `internal/telemetry/`: structured logging, request IDs, and Prometheus metrics
- `load/`: k6 sustained-load and overload workloads
- `deployments/grafana/`: provisioned operations dashboard
- `deployments/prometheus/`: local scrape configuration
- `migrations/`: embedded, ordered PostgreSQL migrations
- `deployments/`: local deployment definitions
- `docs/`: build specification and architecture documentation

The implementation follows the ordered waves in
[`docs/forgequeue_build_spec.md`](docs/forgequeue_build_spec.md).
