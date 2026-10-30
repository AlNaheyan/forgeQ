# Architecture decision records

These records describe the implemented design, not hypothetical features.

## ADR-001: PostgreSQL is authoritative — accepted

Jobs require conditional state transitions, attempt history, tenant admission,
and durable deduplication. Keep those facts in PostgreSQL transactions rather
than treating Redis membership as the source of truth. Conditional writes and
lease generations reject stale ownership. Tradeoff: database availability and
write throughput bound the control plane; backups are essential. Redis recovery
cannot repair a missing PostgreSQL volume.

## ADR-002: Redis is a rebuildable dispatch index — accepted

Use Redis sorted-set hints for ready priority/age ordering and delays. Commit
job state before enqueue, then reconcile PostgreSQL pages to repair gaps.
This avoids pretending PostgreSQL and Redis share an atomic commit. Tradeoff:
eventual repair latency and stale/duplicate hints are normal; claims must still
validate database state. Redis remains a runtime dependency even though its
contents are disposable.

## ADR-003: Kafka observes lifecycle events — accepted

Write an outbox in the same database transaction as lifecycle changes, then
publish asynchronously. Kafka is optional and does not replace dispatch.
This preserves a small core deployment while supporting independent observers.
Tradeoff: publication can repeat after an ambiguous acknowledgment; consumers
must deduplicate. Capture defaults off without Kafka. A full Kafka dispatch
redesign is not part of this implementation.

## ADR-004: At-least-once execution with explicit effect guards — accepted

Recover work after lease expiry instead of silently losing it. Fence stale
database writes using monotonic lease generations and live-owner checks.
Submission idempotency prevents duplicate job creation; execution idempotency
guards handler effects, and these are different contracts. Tradeoff: external
effects can repeat unless the application coordinates with the downstream system.
Never advertise universal exactly-once execution.

## ADR-005: Compose-first, single coordinator — accepted

Support a small private host with documented resource limits, backups, alerts,
and upgrades before claiming multi-host HA. This keeps the demonstration
reproducible on a modest machine. Tradeoff: host/coordinator availability is a
known limitation; worker scale-out is not proof of control-plane HA. The saved
image archive and source revision identify a release; mutable tags alone do not.

## ADR-006: Conservative retention and shared trusted workers — accepted

Use opaque tenant API keys and atomic per-tenant database quotas. Keep workers
trusted and authenticate service RPCs separately. Retention is disabled by
default; keyed jobs and execution markers have no automatic expiry. Tradeoff:
metadata growth and limited tenant isolation at the execution layer. A future
archival policy or code sandbox requires an explicit contract change.
