# Distributed Job Execution Platform — Build Specification

## 0. Executive Summary

Build a production-style distributed job execution platform in Go that accepts jobs through an HTTP API, persists job state, schedules work across multiple workers, survives worker failures, retries failed work, prevents unsafe duplicate side effects through idempotency, exposes detailed metrics, and can be stress-tested under failure.

This project is intentionally designed as a **backend/distributed-systems portfolio project**, not as a CRUD app.

The finished system should demonstrate:

- Distributed worker coordination
- Job queues
- Fault tolerance
- Worker registration and heartbeats
- Job leases
- Retries and exponential backoff
- Dead-letter queues
- At-least-once delivery semantics
- Idempotent execution
- Backpressure
- Load-aware scheduling
- Horizontal worker scaling
- gRPC communication
- Redis coordination
- PostgreSQL persistence
- Prometheus metrics
- Grafana dashboards
- Structured logging
- Chaos testing
- Load testing
- Dockerized local deployment
- Optional Kubernetes deployment
- Optional Kafka-based event log in a later wave

The project should be implemented in **waves**. Each wave contains independent tickets with clear acceptance criteria. The coding agent should complete tickets in order unless otherwise noted.

---

# 1. Project Name

Working name:

**ForgeQueue**

Alternative names:

- Relay
- Dispatch
- TaskMesh
- IronQueue
- WorkGrid
- PulseQueue

Use `forgequeue` as the repository/module name unless instructed otherwise.

---

# 2. Core Product Definition

ForgeQueue is a distributed task execution system.

Clients submit jobs such as:

```json
{
  "type": "sleep",
  "payload": {
    "duration_ms": 5000
  }
}
```

The system returns:

```json
{
  "job_id": "job_01J...",
  "status": "queued"
}
```

Workers claim jobs and execute them.

Job lifecycle:

```text
QUEUED
  |
  v
LEASED
  |
  v
RUNNING
  |
  +------> SUCCEEDED
  |
  +------> RETRY_WAIT
                |
                v
              QUEUED
  |
  +------> DEAD_LETTER
```

The system must tolerate worker crashes.

Example:

```text
Worker A claims Job X
Worker A starts Job X
Worker A crashes
Lease expires
Job X returns to queue
Worker B claims Job X
Worker B executes Job X
Job X succeeds
```

The system provides **at-least-once execution**, not exactly-once execution.

Duplicate execution is possible under failures.

For side-effecting jobs, idempotency must be supported.

---

# 3. Primary Learning Goals

The project should demonstrate understanding of:

1. Distributed failure
2. Coordination
3. Concurrency
4. Reliability
5. Durable state
6. Asynchronous processing
7. Backpressure
8. Scheduling
9. Observability
10. Performance engineering
11. Load testing
12. Graceful degradation

---

# 4. Technology Stack

## Required

### Backend

- Go 1.23+
- Chi or standard `net/http` for REST API
- gRPC for internal worker/coordinator communication
- Protobuf

### Persistence

- PostgreSQL 16+
- Redis 7+

### Infrastructure

- Docker
- Docker Compose

### Observability

- Prometheus
- Grafana
- Structured logs using Go `slog` or Zap

### Testing

- Go unit tests
- Go integration tests
- Testcontainers if practical
- k6 for load testing

---

# 5. Optional Later Technologies

These should **not** be introduced until the simpler version works.

- Kafka
- Kubernetes
- OpenTelemetry
- Jaeger or Tempo
- Loki
- Terraform

Do not introduce complexity before there is a clear reason for it.

---

# 6. High-Level Architecture

Initial architecture:

```text
                    +------------------+
                    |      Client      |
                    +---------+--------+
                              |
                              | HTTP
                              v
                    +------------------+
                    |    API Server    |
                    +---------+--------+
                              |
                              |
                    +---------+----------+
                    |                    |
                    v                    v
              +-----------+        +-----------+
              | PostgreSQL|        |   Redis   |
              +-----------+        +-----------+
                                       |
                                       |
                              +--------+--------+
                              |   Coordinator   |
                              +--------+--------+
                                       |
                                      gRPC
                     +-----------------+-----------------+
                     |                 |                 |
                     v                 v                 v
                +---------+       +---------+       +---------+
                | Worker 1|       | Worker 2|       | Worker 3|
                +---------+       +---------+       +---------+

                              +----------------+
                              |   Prometheus   |
                              +--------+-------+
                                       |
                              +--------v-------+
                              |    Grafana     |
                              +----------------+
```

---

# 7. Service Responsibilities

## API Server

Responsibilities:

- Accept new jobs
- Validate requests
- Persist jobs
- Query job state
- Cancel jobs
- Expose queue statistics
- Enforce backpressure
- Expose health endpoints
- Expose Prometheus metrics

The API server must not execute jobs directly.

---

## Coordinator

Responsibilities:

- Track worker registrations
- Track worker heartbeats
- Determine worker health
- Assign or broker work
- Manage leases
- Detect expired leases
- Requeue abandoned jobs
- Maintain scheduling logic
- Track worker capacity
- Expose coordinator health metrics

---

## Worker

Responsibilities:

- Register itself
- Send heartbeats
- Claim work
- Execute jobs
- Report success/failure
- Renew leases for long-running jobs
- Respect concurrency limits
- Shut down gracefully

---

## PostgreSQL

Source of truth for:

- Jobs
- Job attempts
- Job state
- Retry metadata
- Idempotency records
- Dead-letter jobs
- Worker history if desired

---

## Redis

Used for fast coordination:

- Ready queue
- Delayed retry queue
- Worker heartbeat cache
- Lease metadata
- Queue counters
- Optional pub/sub notifications

Postgres is the durable source of truth.

Redis should be treated as reconstructable infrastructure.

---

# 8. Repository Structure

Recommended layout:

```text
forgequeue/
├── cmd/
│   ├── api/
│   │   └── main.go
│   ├── coordinator/
│   │   └── main.go
│   ├── worker/
│   │   └── main.go
│   ├── chaos/
│   │   └── main.go
│   └── loadgen/
│       └── main.go
│
├── internal/
│   ├── api/
│   │   ├── handlers.go
│   │   ├── middleware.go
│   │   └── routes.go
│   │
│   ├── jobs/
│   │   ├── model.go
│   │   ├── service.go
│   │   ├── state_machine.go
│   │   └── errors.go
│   │
│   ├── scheduler/
│   │   ├── scheduler.go
│   │   ├── round_robin.go
│   │   ├── least_loaded.go
│   │   └── weighted.go
│   │
│   ├── coordinator/
│   │   ├── coordinator.go
│   │   ├── leases.go
│   │   ├── heartbeats.go
│   │   └── recovery.go
│   │
│   ├── worker/
│   │   ├── worker.go
│   │   ├── executor.go
│   │   ├── heartbeat.go
│   │   └── lease.go
│   │
│   ├── queue/
│   │   ├── queue.go
│   │   └── redis_queue.go
│   │
│   ├── repository/
│   │   ├── jobs.go
│   │   ├── attempts.go
│   │   ├── idempotency.go
│   │   └── postgres/
│   │
│   ├── telemetry/
│   │   ├── metrics.go
│   │   └── logging.go
│   │
│   ├── config/
│   │   └── config.go
│   │
│   └── platform/
│       ├── postgres.go
│       └── redis.go
│
├── proto/
│   └── worker.proto
│
├── migrations/
│   ├── 001_jobs.sql
│   ├── 002_attempts.sql
│   └── 003_idempotency.sql
│
├── dashboards/
│   └── forgequeue.json
│
├── load/
│   └── jobs.js
│
├── scripts/
│   ├── dev.sh
│   ├── migrate.sh
│   └── chaos.sh
│
├── deployments/
│   ├── docker-compose.yml
│   └── kubernetes/
│
├── docs/
│   ├── architecture.md
│   ├── failure-semantics.md
│   ├── benchmarks.md
│   └── adr/
│
├── Makefile
├── go.mod
├── go.sum
├── Dockerfile
├── .env.example
└── README.md
```

---

# 9. Domain Model

## Job

```go
type JobStatus string

const (
    JobQueued     JobStatus = "queued"
    JobLeased     JobStatus = "leased"
    JobRunning    JobStatus = "running"
    JobRetryWait  JobStatus = "retry_wait"
    JobSucceeded  JobStatus = "succeeded"
    JobFailed     JobStatus = "failed"
    JobDeadLetter JobStatus = "dead_letter"
    JobCanceled   JobStatus = "canceled"
)
```

Suggested fields:

```text
id
type
payload
status
priority
created_at
updated_at
scheduled_at
attempt_count
max_attempts
lease_owner
lease_expires_at
idempotency_key
result
last_error
```

---

# 10. PostgreSQL Schema

## jobs

```sql
CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    priority INT NOT NULL DEFAULT 0,

    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,

    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,

    idempotency_key TEXT,

    result JSONB,
    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Indexes:

```sql
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_scheduled ON jobs(scheduled_at);
CREATE INDEX idx_jobs_priority ON jobs(priority DESC);
CREATE INDEX idx_jobs_lease_expiry ON jobs(lease_expires_at);
```

---

## job_attempts

```sql
CREATE TABLE job_attempts (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),

    worker_id TEXT NOT NULL,
    attempt_number INT NOT NULL,

    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,

    status TEXT NOT NULL,
    error TEXT
);
```

---

## idempotency_keys

```sql
CREATE TABLE idempotency_keys (
    key TEXT PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

# 11. Redis Data Structures

Suggested keys:

```text
forgequeue:ready
forgequeue:delayed
forgequeue:workers
forgequeue:worker:{worker_id}
forgequeue:leases
```

Recommended structures:

### Ready queue

Sorted set.

Score can encode priority and creation time.

Example:

```text
ZADD forgequeue:ready <score> <job_id>
```

---

### Delayed retry queue

Sorted set:

```text
score = retry_at unix timestamp
```

---

### Worker heartbeat

Hash:

```text
forgequeue:worker:{id}
```

Fields:

```text
last_heartbeat
active_jobs
max_concurrency
status
```

Use TTL if appropriate.

---

# 12. Public REST API

## POST /v1/jobs

Request:

```json
{
  "type": "sleep",
  "payload": {
    "duration_ms": 1000
  },
  "priority": 10,
  "max_attempts": 5,
  "idempotency_key": "customer-action-123"
}
```

Response:

```json
{
  "job_id": "uuid",
  "status": "queued",
  "created_at": "..."
}
```

---

## GET /v1/jobs/{id}

Response:

```json
{
  "job_id": "...",
  "type": "sleep",
  "status": "running",
  "attempt_count": 1,
  "max_attempts": 5,
  "created_at": "...",
  "updated_at": "..."
}
```

---

## GET /v1/jobs

Filters:

```text
status
type
limit
cursor
```

---

## POST /v1/jobs/{id}/cancel

Rules:

- queued jobs can be canceled
- retry_wait jobs can be canceled
- running job cancellation may be best-effort
- succeeded jobs cannot be canceled

---

## GET /v1/stats

Example:

```json
{
  "queued": 124,
  "running": 8,
  "retry_wait": 14,
  "dead_letter": 2,
  "workers": {
    "healthy": 6,
    "unhealthy": 1
  }
}
```

---

## GET /healthz

Process health.

---

## GET /readyz

Dependency readiness.

---

## GET /metrics

Prometheus metrics.

---

# 13. Internal gRPC Protocol

File:

```text
proto/worker.proto
```

Suggested API:

```proto
syntax = "proto3";

package forgequeue.worker;

service WorkerCoordinator {
  rpc RegisterWorker(RegisterWorkerRequest)
      returns (RegisterWorkerResponse);

  rpc Heartbeat(HeartbeatRequest)
      returns (HeartbeatResponse);

  rpc ClaimJob(ClaimJobRequest)
      returns (ClaimJobResponse);

  rpc StartJob(StartJobRequest)
      returns (StartJobResponse);

  rpc CompleteJob(CompleteJobRequest)
      returns (CompleteJobResponse);

  rpc FailJob(FailJobRequest)
      returns (FailJobResponse);

  rpc RenewLease(RenewLeaseRequest)
      returns (RenewLeaseResponse);
}
```

---

# 14. Worker Registration

Worker generates or receives:

```text
worker_id
```

Example:

```text
worker-7f87b4
```

Worker reports:

```json
{
  "worker_id": "worker-7f87b4",
  "max_concurrency": 4,
  "supported_job_types": [
    "sleep",
    "http",
    "compute"
  ]
}
```

Coordinator responds with:

```text
heartbeat_interval
lease_duration
```

---

# 15. Heartbeats

Worker sends heartbeat every:

```text
5 seconds
```

Coordinator considers worker unhealthy after:

```text
15 seconds
```

These values should be configurable.

Heartbeat includes:

```text
worker_id
active_jobs
max_concurrency
timestamp
```

---

# 16. Job Leasing

Workers do not permanently own jobs.

A worker receives a lease.

Example:

```text
job_id: abc
worker: worker-3
lease_duration: 30 sec
lease_expires_at: 12:05:30
```

A running worker should renew long-running jobs.

If the lease expires:

1. Coordinator checks whether job is still incomplete
2. Clears lease owner
3. Increments recovery metric
4. Returns job to ready queue

---

# 17. Delivery Semantics

The project must explicitly document:

## Guarantee

```text
At-least-once execution
```

Meaning:

A job may execute more than once.

Why:

A worker can successfully perform work but crash before acknowledging completion.

Therefore:

Exactly-once side effects are not guaranteed.

---

# 18. Idempotency

The API should support:

```text
Idempotency-Key
```

or body field:

```text
idempotency_key
```

Behavior:

Same idempotency key should return the previously created job.

Example:

```text
POST job
key = payment-abc

network timeout

client retries

POST job
key = payment-abc

same job returned
```

Workers should also be designed so example side-effecting job handlers can safely deduplicate execution.

---

# 19. Retry Policy

Default:

```text
max_attempts = 3
```

Exponential backoff:

```text
base_delay * 2^(attempt-1)
```

Example:

```text
attempt 1
fail
wait 2 sec

attempt 2
fail
wait 4 sec

attempt 3
fail

dead-letter
```

Add jitter:

```text
delay = exponential_delay + random_jitter
```

Cap backoff.

Example:

```text
max_backoff = 60 sec
```

---

# 20. Dead-Letter Queue

Jobs enter dead-letter state after exhausting retries.

Endpoint:

```text
GET /v1/jobs?status=dead_letter
```

Optional endpoint:

```text
POST /v1/jobs/{id}/retry
```

This manually moves a dead-letter job back into the queue.

---

# 21. Scheduling Strategies

Use an interface:

```go
type Scheduler interface {
    SelectWorker(job Job, workers []WorkerState) (WorkerState, error)
}
```

Implement:

## Round Robin

Baseline.

---

## Least Loaded

Pick worker with lowest:

```text
active_jobs / max_concurrency
```

---

## Weighted Capacity

Prefer workers with more available capacity.

Optional later strategy:

- job affinity
- capability-aware scheduling

---

# 22. Backpressure

System must protect itself under overload.

Config:

```text
MAX_QUEUE_DEPTH=100000
```

If queue depth exceeds threshold:

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 5
```

Expose metric:

```text
forgequeue_backpressure_rejections_total
```

---

# 23. Example Built-In Job Types

Workers should support simple built-in jobs for testing.

## sleep

```json
{
  "duration_ms": 2500
}
```

---

## compute

CPU workload.

Example:

```json
{
  "iterations": 5000000
}
```

---

## http

Worker performs an outbound HTTP request.

Use for controlled testing only.

---

## fail

Fails intentionally.

```json
{
  "failures_before_success": 2
}
```

Useful for retry tests.

---

# 24. Concurrency

Worker must support:

```text
WORKER_CONCURRENCY=4
```

Use goroutines with semaphore control.

Example:

```go
sem := make(chan struct{}, maxConcurrency)
```

A worker must never execute more than its configured concurrency.

---

# 25. Graceful Shutdown

Worker shutdown process:

1. Stop claiming new jobs
2. Continue active jobs
3. Allow grace period
4. Finish or abandon remaining jobs
5. Close gRPC connection
6. Exit

API/coordinator shutdown:

1. Stop accepting new work
2. Drain requests
3. Close DB/Redis connections
4. Exit cleanly

---

# 26. Metrics

Prometheus metrics should include:

```text
forgequeue_jobs_submitted_total
forgequeue_jobs_started_total
forgequeue_jobs_completed_total
forgequeue_jobs_failed_total
forgequeue_jobs_retried_total
forgequeue_jobs_dead_letter_total

forgequeue_queue_depth

forgequeue_job_duration_seconds
forgequeue_job_queue_wait_seconds

forgequeue_workers_registered
forgequeue_workers_healthy
forgequeue_workers_unhealthy

forgequeue_active_jobs

forgequeue_lease_expirations_total

forgequeue_backpressure_rejections_total

forgequeue_job_attempts
```

Histograms should support P50/P95/P99 analysis.

---

# 27. Grafana Dashboard

Create at least one dashboard with:

```text
Job Throughput
Queue Depth
Success Rate
Failure Rate
Retry Rate
P50 Latency
P95 Latency
P99 Latency
Healthy Workers
Unhealthy Workers
Active Jobs
Lease Expirations
Backpressure Rejections
```

---

# 28. Structured Logging

Every log should include contextual fields when applicable:

```text
service
job_id
worker_id
attempt
status
duration
error
request_id
```

Example:

```json
{
  "level": "INFO",
  "service": "worker",
  "worker_id": "worker-3",
  "job_id": "abc",
  "attempt": 2,
  "msg": "job completed",
  "duration_ms": 842
}
```

---

# 29. Testing Strategy

## Unit tests

Test:

- state transitions
- retry calculation
- scheduler
- lease expiration logic
- idempotency
- validation

---

## Integration tests

Test with real Redis/Postgres.

Scenarios:

```text
submit -> process -> success
```

```text
submit -> fail -> retry -> success
```

```text
submit -> repeated failure -> dead letter
```

```text
worker crash -> lease expires -> job requeued
```

```text
duplicate idempotency key -> same job
```

```text
backpressure threshold -> 429
```

---

# 30. Load Testing

Use k6.

Tests:

## Throughput test

Submit:

```text
100 jobs/sec
500 jobs/sec
1000 jobs/sec
```

Track:

```text
API latency
queue growth
completion throughput
P95/P99
```

---

## Burst test

Submit:

```text
10,000 jobs rapidly
```

Observe queue recovery.

---

## Saturation test

Keep increasing load until:

```text
queue continuously grows
```

Record system limit.

---

# 31. Chaos Testing

Create CLI:

```bash
forgequeue-chaos
```

Capabilities:

```text
kill random worker
pause worker
add worker latency
force job failure
stop Redis
restart Redis
stop Postgres
```

Initial implementation can rely on Docker commands/scripts.

Example benchmark:

```text
Workers: 8
Jobs: 100,000

At T+20s:
workers 3 and 7 killed

Detected failures: 11 seconds
Jobs recovered: 18,420
Jobs lost: 0
Duplicate executions: 12
Final completion rate: 100%
```

Document results.

---

# 32. Configuration

All config through environment variables.

Example:

```env
HTTP_PORT=8080
GRPC_PORT=9090

DATABASE_URL=postgres://...
REDIS_ADDR=redis:6379

HEARTBEAT_INTERVAL=5s
WORKER_TIMEOUT=15s

LEASE_DURATION=30s
LEASE_RENEW_INTERVAL=10s

DEFAULT_MAX_ATTEMPTS=3

MAX_QUEUE_DEPTH=100000

LOG_LEVEL=info
```

---

# 33. Docker Compose

Compose should start:

```text
postgres
redis
api
coordinator
worker-1
worker-2
worker-3
prometheus
grafana
```

Command:

```bash
docker compose up --build
```

should bring entire stack online.

---

# 34. Documentation Requirements

README must include:

1. What ForgeQueue is
2. Architecture diagram
3. Why at-least-once semantics were chosen
4. Failure scenarios
5. Local setup
6. API examples
7. How workers coordinate
8. How leases work
9. How retries work
10. How backpressure works
11. Observability screenshots
12. Benchmark results
13. Chaos test results
14. Tradeoffs
15. Future work

---

# 35. Architectural Decision Records

Create ADRs.

Examples:

```text
ADR-001: PostgreSQL as durable source of truth
ADR-002: Redis for transient coordination
ADR-003: At-least-once execution
ADR-004: Lease-based work ownership
ADR-005: gRPC for worker communication
```

Each ADR should contain:

```text
Context
Decision
Alternatives
Consequences
```

---

# 36. Development Waves

---

# WAVE 0 — Foundation

Goal:

Create repository, tooling, infrastructure, and development conventions.

## Ticket 0.1 — Initialize Go project

Tasks:

- create repository structure
- initialize Go module
- add Makefile
- add `.gitignore`
- add `.env.example`
- create base README

Acceptance criteria:

```text
go test ./...
```

runs successfully.

---

## Ticket 0.2 — Docker development dependencies

Create Compose services for:

- Postgres
- Redis

Acceptance criteria:

```bash
docker compose up postgres redis
```

starts both successfully.

---

## Ticket 0.3 — Configuration package

Implement typed environment configuration.

Acceptance criteria:

- missing required settings produce useful errors
- config has defaults for local development
- unit tests exist

---

## Ticket 0.4 — Logging

Implement structured logger.

Acceptance criteria:

Every service log contains:

```text
timestamp
level
service
message
```

---

# WAVE 1 — Durable Job API

Goal:

Allow jobs to be created and queried.

## Ticket 1.1 — Database migrations

Create:

```text
jobs
job_attempts
idempotency_keys
```

Acceptance criteria:

Migrations can run from empty database.

---

## Ticket 1.2 — Job domain model

Implement:

- statuses
- validation
- legal transitions

Acceptance criteria:

Invalid state transitions are rejected.

Examples:

```text
queued -> running
```

valid

```text
succeeded -> running
```

invalid.

---

## Ticket 1.3 — Job repository

Implement:

```text
CreateJob
GetJob
ListJobs
UpdateStatus
CancelJob
```

Acceptance criteria:

Repository integration tests pass against Postgres.

---

## Ticket 1.4 — POST /v1/jobs

Implement submission endpoint.

Acceptance criteria:

- validates job type
- stores payload
- returns job id
- default status is queued

---

## Ticket 1.5 — GET job endpoints

Implement:

```text
GET /v1/jobs/{id}
GET /v1/jobs
```

Acceptance criteria:

pagination/filtering works.

---

## Ticket 1.6 — Idempotent submission

Implement idempotency.

Acceptance criteria:

Two requests with the same idempotency key return the same job.

---

# WAVE 2 — Redis Queue

Goal:

Move queued jobs into a real work queue.

## Ticket 2.1 — Queue interface

Create queue abstraction.

Methods:

```go
Enqueue
Dequeue
Remove
Depth
ScheduleRetry
PopDueRetries
```

---

## Ticket 2.2 — Redis implementation

Implement Redis sorted-set-backed queue.

Acceptance criteria:

Queue survives API restart.

---

## Ticket 2.3 — API integration

After successful Postgres insert:

```text
enqueue job
```

Need failure handling if DB succeeds but queue write fails.

Preferred behavior:

- job remains persisted
- reconciliation loop can recover missing queue entries

---

## Ticket 2.4 — Queue reconciliation

Create background process that finds:

```text
status=queued
```

jobs missing from Redis and re-enqueues them.

This demonstrates recovery from Redis inconsistency.

---

# WAVE 3 — Worker MVP

Goal:

Execute queued jobs.

## Ticket 3.1 — Worker process

Create worker executable.

Config:

```text
worker id
concurrency
supported job types
```

---

## Ticket 3.2 — Basic job claiming

Worker can claim one queued job.

Initially use simple Redis atomic operation.

Acceptance criteria:

Two workers cannot normally claim the same queue entry simultaneously.

---

## Ticket 3.3 — Job execution framework

Create handler interface:

```go
type Handler interface {
    Execute(ctx context.Context, payload json.RawMessage) (any, error)
}
```

---

## Ticket 3.4 — Sleep job

Implement sleep handler.

---

## Ticket 3.5 — Fail job

Implement deterministic failure test job.

---

## Ticket 3.6 — Worker concurrency

Worker executes up to configured concurrency.

Acceptance criteria:

Concurrency never exceeds configured limit.

---

# WAVE 4 — Coordinator + gRPC

Goal:

Introduce explicit coordinator service.

## Ticket 4.1 — Protobuf definitions

Implement worker/coordinator protocol.

Generate Go code.

---

## Ticket 4.2 — Worker registration

Worker registers with coordinator.

Coordinator stores worker state.

---

## Ticket 4.3 — Heartbeats

Worker sends periodic heartbeat.

Coordinator tracks:

```text
healthy
suspect
unhealthy
```

---

## Ticket 4.4 — Worker health API

Expose internal or admin endpoint:

```text
GET /v1/workers
```

---

## Ticket 4.5 — Move job claiming behind coordinator

Workers no longer directly own scheduling logic.

Coordinator controls claims.

---

# WAVE 5 — Leasing and Crash Recovery

Goal:

Make worker crashes recoverable.

## Ticket 5.1 — Lease model

Add:

```text
lease_owner
lease_expires_at
```

---

## Ticket 5.2 — Atomic job lease

Coordinator should atomically lease eligible job.

Avoid race between workers.

Acceptance criteria:

Only one active lease exists at a time.

---

## Ticket 5.3 — Lease renewal

Long jobs renew leases.

---

## Ticket 5.4 — Lease expiration scanner

Coordinator periodically finds expired jobs.

Behavior:

```text
running/leased
+
expired lease
=
requeue
```

---

## Ticket 5.5 — Worker crash integration test

Test:

1. submit long job
2. worker claims
3. kill worker
4. wait for lease expiration
5. second worker claims job
6. job completes

Acceptance criteria:

No job remains permanently stuck.

---

# WAVE 6 — Retries and Dead-Letter Queue

Goal:

Handle failures correctly.

## Ticket 6.1 — Attempt tracking

Every attempt creates a `job_attempts` record.

---

## Ticket 6.2 — Retry engine

Implement exponential backoff + jitter.

---

## Ticket 6.3 — Delayed retry queue

Failed job enters:

```text
retry_wait
```

Redis delayed queue stores due timestamp.

---

## Ticket 6.4 — Retry promotion

Background loop promotes due retries to ready queue.

---

## Ticket 6.5 — Dead-letter state

If:

```text
attempt_count >= max_attempts
```

set:

```text
dead_letter
```

---

## Ticket 6.6 — Manual retry endpoint

Implement:

```text
POST /v1/jobs/{id}/retry
```

for dead-letter jobs.

---

# WAVE 7 — Scheduling

Goal:

Support multiple worker selection strategies.

## Ticket 7.1 — Scheduler interface

Create pluggable scheduler abstraction.

---

## Ticket 7.2 — Round robin

Implement baseline.

---

## Ticket 7.3 — Least loaded

Pick worker with greatest free capacity.

---

## Ticket 7.4 — Weighted scheduling

Support workers with different capacities.

---

## Ticket 7.5 — Scheduler metrics

Expose:

```text
jobs_assigned_total{worker_id}
```

---

# WAVE 8 — Idempotent Execution Semantics

Goal:

Explicitly address duplicate execution.

## Ticket 8.1 — Document execution guarantee

Write:

```text
docs/failure-semantics.md
```

Explain:

- at-most-once
- at-least-once
- exactly-once
- why ForgeQueue uses at-least-once

---

## Ticket 8.2 — Execution idempotency helper

Provide helper library for job handlers.

Example:

```go
ExecuteOnce(key, func() error)
```

---

## Ticket 8.3 — Duplicate execution test

Simulate:

```text
job executes
ACK lost
lease expires
job executes again
```

Verify idempotent handler only performs side effect once.

---

# WAVE 9 — Backpressure

Goal:

Keep platform stable during overload.

## Ticket 9.1 — Queue depth limit

Config:

```text
MAX_QUEUE_DEPTH
```

---

## Ticket 9.2 — HTTP rejection

When queue exceeds limit:

```text
429
Retry-After
```

---

## Ticket 9.3 — Backpressure metrics

Expose rejection counter.

---

## Ticket 9.4 — Burst load test

Send enough requests to activate backpressure.

Acceptance criteria:

API remains responsive.

---

# WAVE 10 — Observability

Goal:

Make distributed behavior visible.

## Ticket 10.1 — Prometheus integration

Expose `/metrics`.

---

## Ticket 10.2 — Core counters/gauges

Implement metrics listed earlier.

---

## Ticket 10.3 — Histograms

Track:

```text
queue wait
job duration
API duration
```

---

## Ticket 10.4 — Grafana

Add datasource and dashboard provisioning.

---

## Ticket 10.5 — Request IDs

Every API request receives request ID.

Propagate ID into logs where useful.

---

# WAVE 11 — Load and Benchmarking

Goal:

Produce measurable system results.

## Ticket 11.1 — k6 script

Create job submission load generator.

---

## Ticket 11.2 — Baseline benchmark

Benchmark:

```text
1 API
1 coordinator
4 workers
4 concurrency each
```

Test:

```text
10k jobs
50k jobs
100k jobs
```

---

## Ticket 11.3 — Scale-out benchmark

Compare:

```text
2 workers
4 workers
8 workers
```

Measure throughput improvement.

---

## Ticket 11.4 — Benchmark report

Create:

```text
docs/benchmarks.md
```

Include:

```text
hardware
configuration
throughput
P50
P95
P99
queue depth
error rate
```

---

# WAVE 12 — Chaos Engineering

Goal:

Demonstrate fault tolerance.

## Ticket 12.1 — Worker kill command

Implement chaos command to kill random worker.

---

## Ticket 12.2 — Automated crash experiment

Scenario:

```text
100k jobs
8 workers
kill 2 workers
```

Record:

```text
detection time
recovery time
jobs recovered
jobs lost
duplicate count
```

---

## Ticket 12.3 — Redis restart test

Restart Redis during workload.

Verify system recovers using Postgres/reconciliation.

---

## Ticket 12.4 — Coordinator restart test

Restart coordinator.

Workers reconnect/re-register.

---

## Ticket 12.5 — Chaos report

Create:

```text
docs/chaos-results.md
```

---

# WAVE 13 — Production Hardening

Goal:

Improve reliability.

## Ticket 13.1 — Graceful shutdown

All services support SIGTERM.

---

## Ticket 13.2 — Connection pooling

Tune PostgreSQL pool.

---

## Ticket 13.3 — Redis reconnect

Handle transient Redis disconnects.

---

## Ticket 13.4 — Timeouts

Set:

```text
HTTP timeout
DB timeout
Redis timeout
gRPC timeout
job timeout
```

---

## Ticket 13.5 — Input limits

Protect API against massive payloads.

---

# WAVE 14 — Optional Kafka Evolution

Do not start until earlier waves work.

Goal:

Introduce durable event stream.

Events:

```text
JobCreated
JobLeased
JobStarted
JobSucceeded
JobFailed
JobRetried
JobDeadLettered
WorkerRegistered
WorkerLost
```

Kafka can become:

- audit stream
- analytics stream
- event-driven integration point

Do not replace Postgres as source of truth initially.

Architecture guardrails for this wave and later product work:

- PostgreSQL remains authoritative for job state, attempts, leases, retries,
  idempotency, and tenant ownership.
- Redis is a disposable queue accelerator/hint. Correctness and recovery must
  not depend on a Redis entry surviving.
- Kafka is an optional durable event integration stream, not a second job
  authority or a replacement for the transactional job state.
- Replacing PostgreSQL as the source of truth requires a separate architecture
  proposal, migration plan, and recovery proof; do not do it as part of routine
  productization.

---

# WAVE 15 — Optional Kubernetes

This is an optional deployment target, not a prerequisite for a usable product.
Defer it until Waves 16–18 establish recovery behavior, tenant boundaries, and
operational practices. The first supported product deployment may remain a
single-host Docker Compose deployment if its limits and recovery procedures are
documented.

Goal:

Run system as scalable cluster.

Deploy:

```text
API Deployment
Coordinator Deployment
Worker Deployment
Prometheus
Grafana
```

Features:

```text
readiness probes
liveness probes
resource requests
resource limits
horizontal worker scaling
```

Do not use Kubernetes as a substitute for application-level recovery tests,
tenant isolation, or operational runbooks.

---

# WAVE 16 — Reliability and Recovery Proof

Status: implemented and verified. See [recovery results](recovery-results.md)
for the isolated test command, measured evidence, guarantees, and limitations.
Wave 17 adds tenant safety; Kubernetes remains deferred.

Goal:

Make the PostgreSQL-authoritative, Redis-accelerated design demonstrably
recoverable under component failure and ambiguous delivery.

Tickets:

```text
16.1 Verify Redis is only a hint: lose/restart Redis and prove queued and
     retry-due PostgreSQL jobs are rediscovered without loss or duplication
     beyond the documented at-least-once execution contract.
16.2 Exercise PostgreSQL, API, and coordinator outages/restarts; define and
     verify startup readiness, dependency recovery, and job visibility.
16.3 Inject worker failure at claim, handler side-effect, and acknowledgment
     boundaries; prove lease recovery and document duplicate-execution cases.
16.4 Add fencing or equivalent stale-worker protection wherever a worker whose
     lease expired could otherwise overwrite a newer attempt's state.
16.5 Add bounded, repeatable recovery integration tests and record observed
     recovery time, duplicate behavior, and any accepted data-loss boundary.
```

Acceptance criteria:

```text
PostgreSQL remains the source of truth
Redis can be restarted or emptied without permanently stranding durable jobs
stale workers cannot complete or mutate a newer lease/attempt
recovery assertions are automated and run against isolated test dependencies
delivery and side-effect guarantees are documented without claiming exactly once
```

Do not run destructive failure experiments against a user's or production
database. Tests must use isolated, disposable dependencies.

---

# WAVE 17 — Tenant Safety and API Security

Status: implemented and verified. See [tenant security](tenant-security.md)
for key lifecycle, isolation boundaries, atomic quotas, setup, and limitations.
Wave 18 (production operations and lifecycle) is next.

Goal:

Support multiple users safely, with authentication and enforceable tenant
boundaries across job, worker, and operational APIs.

Tickets:

```text
17.1 Define tenant and credential lifecycle, including key creation, rotation,
     revocation, and secure storage of credential verifiers.
17.2 Authenticate API requests and associate each request with one tenant.
17.3 Scope job reads, cancellation, retry, idempotency keys, and dashboard
     actions to the authenticated tenant; add cross-tenant denial tests.
17.4 Add configurable per-tenant submission/rate, queued-job, payload-size, and
     concurrency limits with clear 401/403/413/429 responses as applicable.
17.5 Ensure secrets and sensitive payloads are not emitted to logs, metrics,
     traces, or event records; avoid tenant IDs as unbounded metric labels.
17.6 Document key handling, tenant isolation, quotas, and API error semantics.
```

Acceptance criteria:

```text
unauthenticated requests cannot access protected operations
tenant A cannot read or mutate tenant B's jobs, idempotency records, or results
limits are checked atomically enough to hold under concurrent submissions
security and isolation behavior has automated tests
the local development workflow has an explicit safe way to create test tenants
```

Choose the simplest credential model that fits the deployment and document its
limits. Do not introduce a large identity platform unless a concrete product
requirement calls for it.

---

# WAVE 18 — Production Operations and Lifecycle

Status: implemented; isolated backup/restore, upgrade, retention, and recovery
tests pass. See [operator runbook](operations.md) for procedures and verification
scope. Wave 19 (architecture evidence and product demo) is next.

Goal:

Make installation, upgrade, recovery, and ongoing operation understandable
and repeatable for an operator.

Tickets:

```text
18.1 Define supported deployment shape and resource assumptions; provide a
     versioned, reproducible deployment path (Compose first, if appropriate).
18.2 Document and test PostgreSQL backup and restore, including a restore drill
     into an isolated environment and verification of recovered jobs.
18.3 Define retention and cleanup policies for completed jobs, results,
     idempotency records, event outbox rows, and audit events; implement bounded
     cleanup with metrics and safe defaults.
18.4 Establish backward-compatible database migration and application upgrade
     procedures, including rollback/forward-fix guidance.
18.5 Add actionable alerts and runbooks for queue age/depth, worker health,
     retry/dead-letter growth, dependency errors, and event-publisher lag.
18.6 Verify readiness, shutdown, secret configuration, and resource limits for
     the supported deployment; document recovery steps for common incidents.
```

Acceptance criteria:

```text
a new operator can deploy from documented instructions
a backup can be restored and validated using a repeatable procedure
retention is explicit, bounded, observable, and does not delete active work
alerts identify symptoms and link to practical runbooks
upgrade steps are exercised against a disposable copy of representative data
```

Kubernetes may be used as an optional deployment exercise, but Wave 18 must not
assume it is required for the first real users.

---

# WAVE 19 — Architecture Evidence and Product Demo

Status: complete. See [architecture](architecture.md), [decisions](decisions.md),
[evidence](evidence.md), and the [verified live demo](demo.md). All planned waves
are complete; documented production limitations remain explicit follow-up work.

Goal:

Present an accurate, reproducible account of the system and its measured
behavior for users, reviewers, and recruiters.

Tickets:

```text
19.1 Publish an architecture overview with component boundaries, data
     ownership, key request/job flows, failure semantics, and tradeoffs.
19.2 Maintain concise decision records for PostgreSQL authority, Redis hints,
     Kafka's optional event role, delivery semantics, and deployment scope.
19.3 Make benchmark commands reproducible and record hardware, OS, versions,
     worker/API configuration, workload, raw results, throughput, and latency
     percentiles; never invent or extrapolate results.
19.4 Link recovery-test evidence and explain what each experiment proves and
     does not prove.
19.5 Refresh the README, dashboard screenshots, and a short demo walkthrough
     from a clean local environment; clearly label optional services/features.
```

Acceptance criteria:

```text
architecture and claims match the current implementation
benchmark results can be rerun with documented commands and modest-machine
profiles, without requiring an oversized local workload
screenshots/demo are captured from a working build and contain no secrets
known limitations and next steps are explicit
```

---

# 37. Ticket Execution Rules for Coding Agent

For each ticket:

1. Read ticket requirements
2. Inspect existing code
3. Implement smallest coherent change
4. Add tests
5. Run:

```bash
go test ./...
```

6. Run relevant integration tests
7. Update docs when behavior changes
8. Commit only logically related changes
9. Do not begin next wave until current wave tests pass

---

# 38. Coding Standards

## Go

Prefer:

- small interfaces
- explicit dependencies
- contexts
- error wrapping
- typed errors where useful

Avoid:

- global mutable state
- giant interfaces
- excessive abstraction
- premature generic frameworks

---

# 39. Error Handling

Errors should be classified.

Examples:

```text
ErrJobNotFound
ErrInvalidTransition
ErrLeaseConflict
ErrWorkerUnavailable
ErrQueueFull
ErrDuplicateIdempotencyKey
```

REST should map appropriately:

```text
400 invalid input
404 missing job
409 invalid state
429 queue overload
500 unexpected error
503 dependency unavailable
```

---

# 40. Concurrency Safety

Potential races must be explicitly considered.

Examples:

```text
two workers try to lease same job
```

```text
lease expires while completion ACK arrives
```

```text
job canceled while worker starts execution
```

```text
retry scanner and lease scanner touch same job
```

Critical state transitions should use database transactions or atomic Redis operations.

---

# 41. Recommended Atomic Lease Pattern

Prefer Postgres transaction.

Conceptually:

```sql
SELECT id
FROM jobs
WHERE status = 'queued'
AND scheduled_at <= NOW()
ORDER BY priority DESC, created_at
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

Then update:

```text
status = leased
lease_owner = worker
lease_expires_at = now + duration
```

This pattern should be considered carefully.

Redis can still provide queue hints, but durable lease state should remain in Postgres.

---

# 42. Failure Scenarios to Support

The system should eventually behave reasonably when:

```text
worker crashes
worker freezes
API crashes
coordinator crashes
Redis restarts
Postgres temporarily unavailable
ACK is lost
job handler panics
job takes too long
duplicate request arrives
queue overloads
network latency increases
```

---

# 43. Important Tradeoffs to Document

The README/docs should explain decisions such as:

## Redis + Postgres

Why two systems?

```text
Postgres = durable truth
Redis = fast coordination
```

---

## At-least-once execution

Why not exactly once?

Because distributed failures make exactly-once side effects impractical without cooperation from the downstream system.

---

## Leasing

Why leases instead of permanent ownership?

Because ownership must expire when workers disappear.

---

## gRPC

Why use gRPC internally?

- typed contract
- efficient internal RPC
- streaming support later
- realistic service architecture

---

# 44. Portfolio Demonstration

Final demo should show:

## Demo 1

Normal processing.

```text
submit 100 jobs
watch workers process
```

---

## Demo 2

Worker crash.

```text
submit 10k jobs
kill worker
show jobs being recovered
```

---

## Demo 3

Retries.

```text
intentional failure job
show exponential retry
```

---

## Demo 4

Dead-letter.

```text
permanent failure
show dead-letter queue
```

---

## Demo 5

Backpressure.

```text
flood API
show 429 protection
```

---

## Demo 6

Horizontal scaling.

```text
2 workers
measure throughput

8 workers
measure throughput
```

---

# 45. Final README Numbers

Do not invent performance claims.

After benchmarking, fill in measured results.

Ideal presentation:

```text
Processed: 100,000 jobs

Workers: 8
Worker concurrency: 8

Throughput:
12,430 jobs/sec

Queue wait:
P50 18 ms
P95 64 ms
P99 103 ms

Execution failures:
0.2%

Jobs lost during two-worker crash:
0

Recovered leases:
18,420
```

Only publish real measured values.

---

# 46. Resume Goal

Final project should support a bullet similar to:

> Built a distributed job execution engine in Go using gRPC, Redis, and PostgreSQL, implementing lease-based work ownership, worker heartbeats, automatic failover, exponential retries, idempotent processing, dead-letter queues, backpressure, and configurable scheduling; benchmarked throughput and P99 latency across horizontally scaled workers using Prometheus, Grafana, and k6.

Replace performance claims with measured values.

---

# 47. MVP Definition

The system is considered MVP-complete when:

```text
jobs can be submitted
multiple workers process jobs
worker concurrency works
worker heartbeats work
leases work
crashed workers recover jobs
retries work
dead-letter works
idempotent job submission works
Prometheus metrics exist
Grafana dashboard works
Docker Compose launches everything
integration tests cover crash recovery
```

This should roughly correspond to Waves 0–10.

---

# 48. Portfolio-Ready Definition

Portfolio-ready means:

```text
MVP complete
+
load testing
+
chaos experiments
+
benchmark report
+
architecture docs
+
clean README
+
screenshots
+
demo video
```

Waves 16–19 extend the portfolio into product-readiness work. Kubernetes and
Kafka remain optional; do not imply either is required for the core guarantees.

Target Waves:

```text
0 through 13 for the core portfolio baseline
Waves 16–19 for product-readiness claims
```

Kafka and Kubernetes are optional extensions.

---

# 49. What NOT to Do

Do not:

- turn this into a frontend-heavy project
- build a large React UI before the backend works
- introduce Kubernetes in Wave 1
- introduce Kafka before basic queue semantics work
- claim exactly-once execution
- hide distributed failures behind a library
- use Celery or Temporal to implement the core system
- skip failure testing
- skip metrics
- skip benchmarks
- add unnecessary microservices
- overengineer authentication before core execution works
- make Kafka a second source of truth without an explicit architecture review
- treat Redis queue entries as the only durable record of ready work
- treat Kubernetes deployment as proof of application-level reliability
- publish unmeasured performance, availability, or recovery claims

---

# 50. Recommended Build Order Summary

```text
Wave 0
Foundation

Wave 1
Durable Job API

Wave 2
Redis Queue

Wave 3
Worker MVP

Wave 4
Coordinator + gRPC

Wave 5
Leases + Crash Recovery

Wave 6
Retries + DLQ

Wave 7
Scheduling

Wave 8
Idempotency Semantics

Wave 9
Backpressure

Wave 10
Observability

Wave 11
Load Testing

Wave 12
Chaos Engineering

Wave 13
Production Hardening

Wave 14
Kafka [optional]

Wave 15
Kubernetes [optional; defer until Waves 16–18]

Wave 16
Reliability and Recovery Proof

Wave 17
Tenant Safety and API Security

Wave 18
Production Operations and Lifecycle

Wave 19
Architecture Evidence and Product Demo
```

---

# 51. First Milestone

The coding agent should initially work only through:

```text
Wave 0
Wave 1
Wave 2
Wave 3
```

At that milestone, this command:

```bash
docker compose up --build
```

should launch:

```text
Postgres
Redis
API
Worker
```

and this request:

```bash
curl -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "sleep",
    "payload": {
      "duration_ms": 1000
    }
  }'
```

should create a job that a worker successfully processes.

Do not proceed to distributed failure handling until this basic pipeline is stable.

---

# 52. Second Milestone

Complete through Wave 6.

Demonstrate:

```text
3 workers
worker crash
lease expiry
automatic reassignment
retry
dead-letter
```

This is the point where the project becomes meaningfully distributed.

---

# 53. Third Milestone

Complete through Wave 10.

Demonstrate:

```text
multiple scheduling strategies
idempotent semantics
backpressure
Prometheus
Grafana
```

At this point the system should be interview-demo ready.

---

# 54. Final Milestone

Complete through Wave 13.

Demonstrate:

```text
100k+ job benchmark
horizontal scaling
worker chaos test
Redis restart
coordinator restart
graceful shutdown
documented performance
```

At this stage the project should be strong enough to use for backend/distributed-systems internship and new-grad interviews.

---

# 55. Guiding Principle

Whenever there is a design choice, prefer the implementation that makes distributed-systems behavior visible and explainable.

The purpose of ForgeQueue is not merely to process jobs.

The purpose is to demonstrate engineering understanding of:

```text
what happens when machines fail
what happens when messages are duplicated
what happens when systems overload
what happens when ownership becomes uncertain
what happens when work must be coordinated across nodes
```

Every important subsystem should therefore have:

```text
implementation
tests
metrics
failure scenario
documentation
```

That is what should make this project stand out.
