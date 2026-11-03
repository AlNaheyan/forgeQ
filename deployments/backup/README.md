# Encrypted backup operations

Local implementation only. Off-host storage, key custody, owner RPO/RTO targets,
representative staging recovery and real notification delivery remain pending.
The local adapter cannot survive loss of its own host; do not call it off-host.

Build `go build -o backup ./cmd/backup`. PostgreSQL 16 `pg_dump` and `pg_restore`
must be available on PATH. age v1 encryption uses pinned `filippo.io/age v1.2.1`.
Create keys separately using maintained age tooling; never commit identities.

Configure `BACKUP_STORAGE_KIND=local`, `BACKUP_LOCAL_ROOT` (private directory),
`BACKUP_OBJECT_PREFIX` (installation-owned name without slashes or dots),
`BACKUP_ENCRYPTION_RECIPIENTS_FILE`, `BACKUP_DATABASE_URL_FILE`, `BACKUP_KEY_ID`
(non-secret operator identifier), `BACKUP_REVISION`, `BACKUP_SCHEMA`, and
`BACKUP_DATABASE_MAJOR`. Both files must be regular, private (0600), and <=64 KiB;
mounted-secret symlinks are accepted after validating their regular-file target.
The upload runtime receives public recipients only, never the restore identity.
The backup database role needs CONNECT, schema USAGE and SELECT on all queue
application tables and sequences; it must not have DDL, mutation or role-admin
rights. Production database connections require verified TLS and root CA.

`backup once` creates a custom-format logical dump streamed into encrypted local
spooling, uploads immutable ciphertext, reads it back to verify size and SHA256,
then publishes a version 1 completed manifest. Only successful manifests count
as snapshots. A failed run exits nonzero; orphan/incomplete objects remain for
inspection. Manifests contain revision/schema/DB major, snapshot start, verified
upload time, ciphertext checksum and public key ID; never credentials.

`BACKUP_INTERVAL=24h backup schedule` runs immediately then waits the configured
interval after each success. Each operation has a 30-minute deadline, configurable
with `BACKUP_TIMEOUT` (1s–24h). The scheduler and one-shot/retention commands share
an exclusive root-directory lock. Never use separate roots for the same storage
namespace. Failure stops the scheduler with nonzero exit: supervisor/monitoring
must alert and restart according to operator policy. A crash leaves a stale lock;
verify no process remains before manually removing `backup.lock`. No timer or
external service is enabled automatically. Restore does not take the scheduler
lock; stop backup/retention before restoring a selected object.

Retention is opt-in: set `BACKUP_RETENTION_DAILY=7` and
`BACKUP_RETENTION_WEEKLY=4`, then `backup retention` to print a dry-run plan.
Only `BACKUP_RETENTION_APPLY=true` applies it, at most 100 exact objects per run.
The newest checksum-verified complete snapshot always survives. Keep newest
snapshots in UTC daily and ISO-week buckets; invalid/incomplete objects remain
for investigation. Manifest deletion precedes archive deletion so interruption
cannot leave a completed manifest pointing at an intentionally removed archive.
Retention manages backup objects only, never tenant data or deduplication expiry.
A checksum is not a periodic decryption drill; schedule restore validation too.

Restore with `BACKUP_MANIFEST_KEY`, `BACKUP_DECRYPTION_IDENTITY_FILE` and
`BACKUP_RESTORE_DATABASE_URL_FILE`, then `backup restore`. The current automation
only accepts loopback PostgreSQL URLs and database names starting
`forgeq_restore_`; create a fresh isolated database first with a separate restore
admin credential. It refuses nonempty destinations and connection-host overrides.
There is no overwrite switch. Stop other clients and ensure the isolated restore
role is the only writer until validation finishes. Ciphertext is verified and
fully authenticated into a 0600 temporary plaintext file before transactional
`pg_restore`; the file is removed afterward. Ensure encrypted/private temporary
storage and enough disk for ciphertext plus plaintext. A process/host crash can
leave private temporary files; review/remove only owned leftovers.

`bash scripts/backup-test-restore.sh` provisions a unique disposable PostgreSQL
stack, exercises encrypted backup, deletes its source DB, restores separately,
and verifies tenant credentials, results, attempts, submission markers, execution
deduplication and queued work via real repository lease/start/finish operations.
It also refuses restoring over the populated destination. This fixture proves
correctness, not representative production RPO/RTO or off-host durability.

Failure drills: `GOMAXPROCS=2 go test -p 1 ./internal/backup` covers interrupted
dumps, denied/truncated uploads, missing/wrong keys, corrupt checksums, missing
manifest completion, scope violations, collisions and non-destructive retention.
For an upload failure, preserve the previous usable snapshot, inspect storage
permissions/capacity and restart only after remediation. For a corrupt archive,
quarantine it from restore selection, test the preceding complete copy, and
investigate storage integrity; do not erase the remaining known-good copy.
Failure observer callbacks can feed the reserved backup metrics; failed runs
must not advance freshness. Absent metrics are unknown, never healthy zero.

Recover keys and configuration separately from the lost source host. Keep an
approved offline/independent copy of restore identities and their custody/access
procedure, public-recipient/key-ID history, exact release/config inventory,
verified CA roots and restore tooling. Exercise recovery without copying private
keys into evidence or the backup object's metadata. Losing the private identity
makes the ciphertext unrecoverable. Key rotation retains old identities until
all archives encrypted to them expire and a new-key restore has passed.

Before reopening admission after recovery, reconcile revoked keys and suspended
or deleted tenants against authoritative post-snapshot policy. Rebuild Redis
hints from PostgreSQL; do not restore Redis as source of truth. Recover expired
leases under the normal fencing path. Restoring older execution markers can
replay external effects committed after the snapshot: reconcile downstream
idempotency state before enabling workers. Logical backup loss includes the
snapshot interval plus failed-run/retry delays. WAL/PITR is an owner-reviewed
next step only if measured logical-backup RPO misses agreed targets.

Rollback: disable the scheduler and keep old encrypted objects/identities; no
schema migration is introduced. Restores are isolated and never replace the
source automatically. Unsupported manifests fail closed. Forward fixes preserve
the age v1 format and manifest version or introduce explicit version readers.

Wave24 telemetry handoff: completed manifests are the durable source for
`forgequeue_backup_last_success_timestamp_seconds` (`uploaded`),
`forgequeue_backup_last_snapshot_timestamp_seconds` (`snapshot`) and
`forgequeue_backup_archive_bytes` (`size`). A future exporter must scan the owned
prefix, reject invalid/incomplete manifests, select the latest verified snapshot,
and preserve unknown/absent as unknown. `Runner.Observe(Manifest,duration,error)`
is the in-process hook for runs_total outcome and duration histogram; a failed
attempt never advances successful timestamps. Exporter/notification wiring is
reserved for Wave24; this CLI does not pretend its short-lived memory can provide
persistent scrape metrics. Monitor scheduler exit status until that is wired.
