# Privileged bootstrap and runtime roles

Run the `migrate` binary once using the separately supplied migration/admin DSN.
It applies embedded forward-only migrations and sets EVENTS_ENABLED. Runtime
API/coordinator production startup only verifies the exact schema and event
configuration, then rejects administrative database rights. A worker needs no
control-plane PostgreSQL or Redis credentials.

After migrations, an authorized database administrator applies
`bootstrap-roles.sql` in this installation's database. It creates NOLOGIN group
roles `forgeq_api`, `forgeq_coordinator`, and `forgeq_backup`. Provision dedicated
LOGIN roles with secret passwords outside this repository; grant exactly the
matching group and no other memberships. Do not use the database/schema/table
owner as a runtime login. PostgreSQL must require TLS and password authentication
in pg_hba.conf; clients require sslmode=verify-full and a mounted sslrootcert.

The bootstrap intentionally fails if the named roles already exist; review the
existing grants as an administrator instead of silently resetting privileges.
Reapply reviewed grants when later migrations add objects. Backup SELECT grants
cover the current public schema and sequences; future tables need explicit review.
Restore uses separate administrative credentials for a new isolated database.

Runtime API can mutate job data, admission counters, publish outbox events and
perform approved retention. Coordinator can update jobs/leases and attempts and
record events. Neither may issue/revoke API keys, edit tenant limits, toggle event
capture, or run DDL. This trusts the API with maintenance and tenant verifier reads;
it does not sandbox a compromised API process. Functions remain security invoker,
so no security-definer owner escalation is introduced.

Rollback to development auto-migration with runtime credentials is unsupported.
Restore the previous compatible binary only if its schema contract matches, or
forward-fix migrations with the privileged command. Never grant runtime DDL to
make a failed deployment start. No migration010 is necessary for these grants.
