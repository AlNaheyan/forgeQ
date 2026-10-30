# ForgeQueue architecture

ForgeQueue is a Go distributed job runner with a durable PostgreSQL control
plane, Redis dispatch hints, and a trusted worker pool. It demonstrates leases,
fencing, reconciliation, retries, tenant admission, and operational recovery.
It is not a general workflow engine or a sandbox for arbitrary customer code.
The built-in runnable handlers are `sleep` and `fail`; application handlers need
code integration. There is no stable public SDK/package compatibility promise.

## Components and ownership

```text
Tenant client / embedded dashboard
       | HTTP + tenant API key
       v
      API ------transactions------> PostgreSQL
       |                               ^
       +----ready hints----> Redis     | leases/attempts/recovery
       |                       ^       |
       +--fleet RPC------> Coordinator -+
                                ^
                                | authenticated gRPC pull + heartbeat
                             Workers ---> application effects

PostgreSQL outbox --> API publisher --> Kafka (optional observers only)
API + coordinator --> Prometheus --> Grafana / alert evaluation (optional)
```

| Component | Owns | Does not guarantee |
| --- | --- | --- |
| API | Authentication, tenant-scoped jobs, admission, reconciliation, retention | Worker capacity merely because readiness is green |
| PostgreSQL | Jobs, attempts, lease generations, tenants/keys, quotas, idempotency, outbox | External side-effect atomicity without application cooperation |
| Redis | Priority/age ready hints and delayed hints | Durable truth; lost hints are rebuilt |
| Coordinator | In-memory registration, heartbeat health, placement, durable lease transitions, retry/recovery scanning | Highly available multi-coordinator scheduling |
| Worker | Bounded handler concurrency, start/renew/complete/fail RPCs | Isolation of untrusted handler code or global exactly-once effects |
| Kafka | Optional committed lifecycle-event delivery from outbox | Job dispatch or end-to-end exactly-once publication |

Implementation: [API](../internal/api/handlers.go),
[job service](../internal/jobs/service.go),
[repository](../internal/repository/postgres/jobs.go),
[coordinator](../internal/coordinator/grpc_server.go),
[worker](../internal/worker/worker.go).

## Request and execution flow

1. API resolves a bearer key to a tenant. In a database transaction, admission
   locks that tenant, checks durable limits, and creates the job plus optional
   submission-idempotency mapping. Replay returns the same tenant's original job.
2. After commit, API enqueues the job ID in Redis. A failed enqueue cannot erase
   the durable job; paginated reconciliation repairs the hint later.
3. Workers register supported types and capacity, then pull through the
   coordinator. Placement considers worker health/capacity and the selected
   scheduling strategy. PostgreSQL serializes tenant concurrency admission and
   atomically leases an eligible queued job, incrementing its generation.
4. Worker starts an attempt and renews its lease during execution. Start,
   renewal, and terminal writes compare worker ID, generation, state, and expiry.
   Tenant identity travels with the job into the handler context.
5. Success records the result. Retryable failures wait with capped exponential
   backoff; exhaustion or nonretryable failure dead-letters the job. Manual retry
   preserves history and consumes admission quota. Expired leases are requeued;
   crash recovery does not enforce the same attempt budget as reported failure.

The lease generation fences **database mutations**, not a payment/email already
sent by a stale worker. `ExecuteOnceTx` can atomically commit PostgreSQL effects
with a completion marker. Remote effects need downstream idempotency/fencing.
See [failure semantics](failure-semantics.md) and [decision records](decisions.md).

## Security, resources, and operations

Tenant keys scope job reads, mutation, pagination, and both idempotency domains.
Operator keys add fleet/metrics visibility, not cross-tenant job access. Service
RPCs use a separate credential. Database administrators and worker code remain
trusted across tenants; this is not row-level security or per-tenant sandboxing.
The private service transport is plaintext; remote production access needs TLS
and network controls. See [security](tenant-security.md).

The supported deployment is single-host Compose with one coordinator. Redis
loss pauses dispatch/admission but does not permanently strand committed jobs;
PostgreSQL disk loss needs backup restore. Opt-in cleanup preserves active work,
keyed jobs, and execution markers. See [operations](operations.md) for resources,
backups, migrations, alert runbooks, and rollout limitations.

## Evidence and limits

[Evidence index](evidence.md) separates automated assertions, historical load
measurements, and the current eight-job visual demo. No old benchmark is a
measurement of today's authentication/quota path. Remaining product work includes
HA coordination, stronger identity/transport integration, a stable SDK, richer
handlers, fair tenant scheduling, and target-host deployment rehearsal. Kubernetes
is an optional deployment exercise, not proof of these guarantees.
