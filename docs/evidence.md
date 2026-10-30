# Evidence index

## What has been demonstrated

| Evidence | Demonstrates | Does not demonstrate |
| --- | --- | --- |
| [Recovery assertions](recovery-results.md#assertions) | Stale generation rejection, process restarts, queue-hint reconstruction, ambiguous completion behavior | Regional failure, disk corruption, universal exactly once |
| [Tenant tests](tenant-security.md#verification) | Cross-tenant denial, key lifecycle, quota/concurrency bounds, error redaction | Untrusted-code isolation, independent penetration testing |
| [Operations drill](operations.md#verification-evidence) | Real custom-format dump/restore, schema 008→009 upgrade, recovered credential/job/result, bounded cleanup | Production RPO/RTO, full target-host rollout |
| [Historical benchmark summaries](benchmarks.md) | Recorded HTTP percentiles, submission counts/errors for the Wave 11 build | Performance of the current security/operations build |
| [Live dashboard demo](demo.md) | Authenticated dashboard, one real worker, queued/running/succeeded jobs | Capacity benchmark or HA proof |

The Wave 16 and Wave 18 reports link raw test output and environment records.
The October 16 k6 JSON summaries are retained. SQL-derived completion throughput
and queue-wait figures in the historical report lack retained query output;
they cannot be independently reconstructed from those JSON files. Keep that
qualification when presenting them. One sample per historical configuration is
not a statistical performance comparison.

## Recruiter/demo claims that match the implementation

- Built a PostgreSQL-authoritative job system with Redis dispatch hints,
  generation-fenced leases, retries, dead-lettering, and reconciliation.
- Implemented tenant-scoped API keys and idempotency with transactional admission
  and lease-concurrency limits across API instances.
- Automated process/dependency recovery, isolated backup/restore and upgrade
  drills, and a browser demo; instrumented the system with Prometheus/Grafana.

Always name the tested revision/topology next to a performance number. Do not
replace submission throughput with completed-work throughput, and do not turn a
small-fixture restore duration into a production recovery objective.
