# Recovery measurement gate (23.6)

Status: tooling ready; representative staging measurements and agreed targets
are pending. No RPO/RTO commitment or measured production result is recorded.

The owner must first supply the authorized staging target, representative data
shape/volume, off-host destination, key-recovery custody and RPO/RTO targets.
Record image digest/revision, schema, PostgreSQL major, host resources, dataset
counts/bytes, snapshot schedule, restore-key recovery source, and incident owner.
Use only approved synthetic data until data-sensitivity policy is resolved.

After an actual declared loss of the source, record `failure_at` in UTC. Restore
from an independently accessible completed encrypted object into an isolated
fresh destination. The restoration command must include all readiness gates:
manifest/checksum validation, tenant/result/attempt/submission/execution-marker
assertions, revoked-credential reconciliation, queue rebuild/lease recovery,
and a bounded queued-work canary. A pg_restore success alone is insufficient.
Do not reopen admission before downstream-effect reconciliation is complete.

Run the measurement wrapper around that reviewed restore-and-validation script:

```sh
python3 scripts/backup-measure.py \
  --manifest-file /private/evidence/completed-manifest.json \
  --failure-at '<actual UTC failure timestamp>' \
  --rpo-target-seconds '<owner value>' --rto-target-seconds '<owner value>' \
  --representative-staging -- /private/rehearsal/restore-and-validate.sh \
  > /private/evidence/recovery-measurement.json
```

The wrapper does not select storage, issue keys, create a destination, or invoke
a deployment by itself. The explicit command performs those separately approved
steps. It suppresses subprocess output to prevent secret leakage; retain reviewed
sanitized evidence independently. Its exit status fails on restoration failure or
missed targets. Without representative staging and both owner targets, the gate
stays pending even if the command succeeds. Do not set representative-staging
for the tiny local fixture. Measurement tests use synthetic timestamps only.

Recovery point is conservatively `failure_at - snapshot_start`; recovery time is
`validated_at - failure_at`, including detection/operator/key-recovery delays.
The monotonic restore-command duration is also recorded separately. Include
clock synchronization evidence: negative/inconsistent chronology is refused.
Retain raw timestamps and explain missing/failed backups and dataset coverage.

Compare results with agreed targets and repeat after significant data growth or
release/schema/key changes. If logical snapshot frequency plus retry delays miss
RPO, evaluate PostgreSQL WAL archiving/PITR, continuous retention integrity and
independent key custody in a separate reviewed design. Do not claim WAL/PITR
merely because logical restore tooling exists. A missed RTO requires measured
bottleneck investigation and another approved rehearsal, not changing the target
silently. The owner records launch go/no-go after evidence review.
