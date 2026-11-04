# Security verification and remaining gates

Run from the repository root with no production secrets in the environment:

```sh
GOMAXPROCS=2 go test -p 1 ./internal/security ./internal/config ./internal/tenant ./internal/platform ./internal/api ./internal/coordinator ./internal/worker ./internal/telemetry
GOMAXPROCS=2 go test -race -p 1 ./internal/security ./internal/tenant ./internal/config
bash scripts/security-test-roles.sh
```

The last command owns one fresh PostgreSQL container with a generated password,
random loopback port and automatic cleanup. It refuses a nonempty fixture DB.
Do not point SECURITY_TEST_DATABASE_URL at an existing/shared database. Without
that explicit fixture DSN, its Go test skips; a skip is not database evidence.
Run only one Docker/build/integration fixture at a time.

Local evidence includes direct HTTPS with spoofed forwarding headers; production
plaintext and partial configuration rejection; verified service TLS with wrong
CA/DNS, missing/expired certificates and ambiguous identities; unary/streaming
method permissions; worker identity binding; CA-overlap rotation and retired trust;
secret-source conflicts, mounted symlinks, size/permissions and redacted errors;
protected metrics; and real Redis-client authentication against a local TLS/RESP
fixture (correct/wrong/missing password and wrong CA).

The database fixture runs actual migration/bootstrap grants. API submission uses
its runtime role and exercises tenant admission locking/counters and event
triggers. The API fleet adapter and worker broker call the real coordinator over
mTLS, backed by the coordinator runtime DB role: registration, listing, claim,
start and completion. Unauthorized worker impersonation and revoked tenant keys
are denied. Runtime DDL (including temporary tables), credential mutation, tenant
limits and global configuration are denied; column-level credential grants and
administrative startup identities are rejected.

The database fixture uses disposable loopback PostgreSQL plaintext; production
PostgreSQL verified-TLS validation is covered separately by configuration tests.
Redis transport uses a fake RESP endpoint; queue hint storage in the DB/RPC
fixture is in-memory. Neither is evidence of a production dependency deployment.
Production PostgreSQL/Redis TLS, certificate issuer/provider, real firewall and
host exposure, TLS-aware readiness probes, monitoring delivery and coordinated
rotation under real workload still require the authorized target rehearsal.
No external domain, paid resource, certificate issuance or production deployment
was performed. No uninterrupted rotation, production SLO/RPO/RTO or HA claim is
made. Keep the prior valid credential/image snapshot; use privileged forward
migrations rather than granting runtime administration to recover a rollout.
