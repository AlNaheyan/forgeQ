# Failure and Execution Semantics

ForgeQueue provides **at-least-once execution**. It makes durable job state and
lease ownership safe under concurrency, but it does not promise that a handler
or its external side effects run only once.

Wave 16 adds a monotonically increasing `lease_generation` to every lease.
Start, renewal, and terminal updates must present that generation as well as
the worker ID; PostgreSQL checks both atomically. A delayed request from an old
lease cannot affect a newer attempt even when the same worker ID is reused.
See [the recovery report](recovery-results.md) for failure tests, availability
boundaries, and upgrade requirements.

## The three common guarantees

### At-most-once

An at-most-once system makes no more than one delivery or execution attempt.
If a worker crashes or a message is lost, the system does not retry it.

This avoids duplicate execution, but work can be lost. It is appropriate only
when dropping a job is preferable to performing it twice.

### At-least-once

An at-least-once system retries work whose successful completion has not been
durably observed. A job eventually gets another attempt after a worker crash,
an expired lease, or a retryable failure.

This favors eventual completion over duplicate avoidance. The handler may run
more than once, and two attempts can briefly overlap when an old worker keeps
running after its lease expires.

### Exactly-once

Exactly-once means that one logical job produces one externally visible
effect. A queue cannot generally provide that guarantee by itself. Atomically
committing both ForgeQueue's PostgreSQL state and an arbitrary effect such as a
payment, email, or third-party API call would require cooperation from that
downstream system.

Applications can achieve *effectively once* behavior for a particular effect
with transactions, uniqueness constraints, downstream idempotency keys, or a
durable deduplication record. That is an application-level protocol, not a
universal delivery guarantee.

## Why ForgeQueue chooses at-least-once

Workers and networks can fail at any instruction boundary. After a worker
becomes unreachable, the coordinator cannot distinguish between these cases:

- the worker stopped before performing the side effect;
- the worker performed the side effect and stopped before reporting success;
- the completion request or response was lost in transit; or
- the worker is still running but is partitioned from the coordinator.

Not retrying would silently lose work in the first case. ForgeQueue therefore
uses expiring leases and retries: PostgreSQL is the durable authority for job
state, a worker may update a job only while it owns a live lease, and recovery
requeues an attempt whose lease expires. This keeps the platform available and
makes abandoned work recoverable, at the cost of possible duplicate handler
execution.

Leases prevent two workers from both recording a successful completion for the
same active attempt. They do not make side effects transactional with the job
record and cannot stop a partitioned process at the instant its lease expires.

## Concrete lost-completion sequence

Consider a job that charges an account:

1. Worker A leases the job and starts attempt 1.
2. Worker A sends the charge to the payment provider, which accepts it.
3. Worker A calls `CompleteJob`, but the request is lost before the coordinator
   records success. From ForgeQueue's perspective the job is still running.
4. Worker A crashes or can no longer renew its lease.
5. The lease expires, recovery closes attempt 1 as expired, and the job returns
   to the queue.
6. Worker B leases the job as attempt 2 and sends the charge again.

Without an idempotency key understood by the payment provider, the account can
be charged twice.

There is a second possible lost-ACK outcome: PostgreSQL may commit the success,
but the `CompleteJob` response may be lost on its way back to Worker A. The
worker cannot know whether the commit happened. In this case the durable job is
already `succeeded`, so ForgeQueue will not lease it again; nevertheless, the
ambiguity is why blindly retrying arbitrary side effects is unsafe.

## Writing safe handlers

Treat every handler as replayable and assume a prior attempt may have completed
some or all of its effects.

- Derive a stable idempotency key from the logical operation, commonly the
  ForgeQueue job ID or a domain key supplied in the payload. Do not use the
  attempt number: every retry must use the same key.
- Pass that key to downstream APIs that support idempotent requests.
- For database effects, enforce uniqueness with a unique constraint and use an
  atomic insert/upsert. If the effect and deduplication record share a database,
  write both in the same transaction.
- For publishing follow-up messages, prefer a transactional outbox. For
  consuming messages, an inbox/deduplication table with a unique operation key
  provides the corresponding pattern.
- Persist the side effect before reporting job success. Reporting success first
  can permanently lose work if the worker fails before the effect is durable.
- Make multi-step handlers resumable. Record completed steps durably, and make
  each step safe to repeat rather than relying only on an in-memory flag.
- Keep idempotency records at least as long as a job can be retried, manually
  replayed, or redelivered. Expiring them too early reintroduces duplicates.

Avoid a separate "check whether processed" query followed by the side effect.
Two attempts can both pass the check before either writes the result. The check
and claim must be one atomic operation, normally enforced by the datastore.

### Using the execution guard

`internal/idempotency.Executor` stores durable completion keys in PostgreSQL.
For a database effect, use `ExecuteOnceTx` and perform the effect through the
provided transaction so the effect and completion marker commit together:

```go
guard := idempotency.NewExecutor(db)
executed, err := guard.ExecuteOnceTx(ctx, "credit-order:"+orderID,
	func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			"UPDATE accounts SET credit = credit + $1 WHERE id = $2",
			amount, accountID,
		)
		return err
	})
```

`executed` is `false` when that key already completed. Concurrent calls using
the same key serialize in PostgreSQL. Callback errors and panics roll back the
claim, allowing a later attempt to retry.

`ExecuteOnce` provides the same replay guard for a plain callback. It is useful
when the callback calls a downstream service that independently honors the same
idempotency key. It cannot close the crash window between an unrelated external
effect and committing ForgeQueue's PostgreSQL marker.

## Boundaries and limitations

- Lease renewal reduces accidental overlap; it does not eliminate duplicates.
- A stale worker's terminal update is rejected after its lease expires, but an
  external action it already performed cannot be revoked by ForgeQueue.
- Retry counts and attempt history describe ForgeQueue attempts, not the exact
  number of downstream effects.
- Cancellation is cooperative. A handler or dependency that ignores
  cancellation may continue after ownership has moved to another worker.
- Some effects are not naturally idempotent. Sending email, invoking legacy
  APIs, and interacting with physical systems may require a domain-specific
  ledger, reconciliation process, or acceptance that duplicates are possible.
- An idempotency helper can coordinate duplicate calls only within the scope and
  durability of its backing store. It cannot make an unrelated external system
  transactional.

The practical contract is: ForgeQueue keeps retrying until it can durably
record an outcome; application handlers are responsible for making repeated
execution safe.
