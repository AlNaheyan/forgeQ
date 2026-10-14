# ForgeQueue

ForgeQueue is a production-style distributed job execution platform written in
Go. It is being built to make coordination, failure recovery, at-least-once
delivery, idempotency, backpressure, and observability explicit rather than
hiding them behind a workflow framework.

Waves 0 through 4 are complete: ForgeQueue has a durable job API, Redis-backed
queues, PostgreSQL reconciliation, bounded-concurrency workers, and an explicit
gRPC coordinator that owns worker registration, heartbeats, health, and job
claims.

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
requests work through gRPC. The coordinator atomically claims queued jobs in
PostgreSQL. A successful `sleep` job moves from `queued` to `running` to
`succeeded`; Wave 5 will replace the coordinator's in-memory assignment record
with durable leases and crash recovery.

Available Wave 1 endpoints:

- `POST /v1/jobs`
- `GET /v1/jobs/{id}`
- `GET /v1/jobs?status=&type=&limit=&cursor=`
- `POST /v1/jobs/{id}/cancel`
- `GET /v1/workers`
- `GET /healthz`
- `GET /readyz`

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
- `internal/repository/postgres/`: durable job storage
- `internal/queue/`: Redis queue and PostgreSQL-to-Redis reconciliation
- `internal/worker/`: worker runtime, concurrency control, and built-in handlers
- `internal/coordinator/`: worker registry, health state, and gRPC control plane
- `proto/`: coordinator protocol and generated Go stubs
- `internal/api/`: HTTP transport
- `internal/telemetry/`: shared structured logging and, later, metrics
- `migrations/`: embedded, ordered PostgreSQL migrations
- `deployments/`: local deployment definitions
- `docs/`: build specification and architecture documentation

The implementation follows the ordered waves in
[`docs/forgequeue_build_spec.md`](docs/forgequeue_build_spec.md).
