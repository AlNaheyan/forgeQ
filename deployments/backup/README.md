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
Scope future storage credentials to this installation prefix: uploader put/get,
retention list/get/delete, and restore get plus put for restore receipts. Keep
retention deletion credentials separately controlled; no bucket-wide delete.
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
The newest checksum-verified complete snapshot and newest successfully restored
snapshot always survive. No deletion occurs until a matching successful restore
receipt exists. Restore publishes an immutable `.restored.json` receipt after
decryption and transactional pg_restore; publish failure is reported even though
the isolated destination may already contain the restored data. Receipts are
trusted operational records, not signatures or application readiness proof. Keep newest
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

Wave24 metrics use a durable status file because the backup CLI is short-lived.
Set `BACKUP_STATUS_FILE` for `backup once` or `backup schedule` to a path on a
shared status volume. The coordinator mounts that volume read-only at
`/var/lib/forgeq-backup` and reads `/var/lib/forgeq-backup/status.json` when
scraped. The file contains only timestamps, byte count, duration, and success or
failure totals; it contains no object names, tenant data, secrets, or raw errors.
Provision the shared directory with write access for the backup process and read
and traverse access for the coordinator. Status files are written with mode 0640;
run both processes under the same user or arrange a shared group (including group
inheritance on the directory). Existing directory ownership and permissions are
not changed automatically. If it is absent, malformed, or unreadable, the coordinator publishes
`forgequeue_backup_status_available=0` so monitoring alerts rather than treating
missing state as healthy. `Runner.Observe` updates the file atomically after each
attempt. A failure increments the failure count and preserves the last successful
timestamps. If status publication fails, the backup command exits with an error;
the encrypted backup may still have completed, so inspect storage before retrying.
