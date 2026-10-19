# Production hardening

Wave 13 adds bounded shutdown, dependency pools and deadlines, transient Redis
reconnect settings, and request/job limits. Values are configurable through the
environment; the Compose stack sets explicit local defaults.

## Runtime settings

| Variable | Default | Purpose |
| --- | ---: | --- |
| `SHUTDOWN_TIMEOUT` | `10s` | Grace period for HTTP/gRPC shutdown and worker drain |
| `POSTGRES_MAX_OPEN_CONNS` | `25` | Maximum open connections per process |
| `POSTGRES_MAX_IDLE_CONNS` | `5` | Maximum idle PostgreSQL connections; cannot exceed max open |
| `POSTGRES_CONN_MAX_IDLE` | `5m` | Maximum idle lifetime for pooled PostgreSQL connections |
| `DATABASE_TIMEOUT` | `5s` | Startup ping and readiness timeout for PostgreSQL |
| `REDIS_POOL_SIZE` | `10` | Redis client pool size |
| `REDIS_TIMEOUT` | `3s` | Redis dial, read, write, and readiness timeout |
| `GRPC_TIMEOUT` | `5s` | Deadline for worker/coordinator RPC calls |
| `JOB_TIMEOUT` | `5m` | Maximum execution time for a worker handler |
| `HTTP_READ_HEADER_TIMEOUT` | `5s` | Maximum time to read request headers |
| `HTTP_READ_TIMEOUT` | `15s` | Maximum time to read an HTTP request |
| `HTTP_WRITE_TIMEOUT` | `30s` | Maximum time to write an HTTP response |
| `HTTP_IDLE_TIMEOUT` | `60s` | Keep-alive idle connection timeout |
| `HTTP_REQUEST_TIMEOUT` | `15s` | Deadline propagated through API handlers to database/Redis calls |

The API and coordinator use bounded PostgreSQL pools. Redis clients have
bounded pools, socket timeouts, and command retries with exponential backoff;
the Redis client reconnects when subsequent commands need a connection after a
transient disconnect. A command active during an outage may still fail and
should be retried by its caller where safe. Wave 12 includes a Redis restart
run in which all 496 accepted jobs completed after Redis recovered.

## Shutdown behavior

All three application services handle SIGINT/SIGTERM. The API stops accepting
new HTTP work and waits for in-flight requests. The coordinator marks gRPC
health not-serving and gracefully drains RPCs before forcing a stop at the
configured deadline. Workers stop claiming jobs, let active handlers finish,
and exit within `SHUTDOWN_TIMEOUT`; if the grace expires, the process exits and
the normal lease-recovery path can reassign unfinished jobs. Compose allows a
small margin beyond the configured eight-second shutdown deadline before it
sends SIGKILL.

## Payload and execution limits

The complete JSON request body is capped at 1 MiB. Larger submissions return
HTTP `413 payload_too_large`; malformed JSON remains `400 invalid_request`.
Worker handlers receive a deadline from `JOB_TIMEOUT`. Handlers should honor
context cancellation and make external side effects idempotent, because a
timed-out or interrupted attempt can be retried under the queue's at-least-once
delivery semantics.

## Validation

Unit coverage verifies configuration defaults and validation, pool-setting
validation, the 413 response, RPC deadlines, and worker draining of an active
job during shutdown. Redis restart recovery is covered by the recorded
Wave 12 chaos experiment. Database/Redis restart smoke tests require the local
Compose dependencies to be running; do not run integration tests against a
database containing data you need, because those suites truncate test tables.
