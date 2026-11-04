# Production exposure checklist

The operations overlay explicitly selects production. It requires separate
read-only secret directories per service; never mount migration, backup decrypt,
API credential or database administration secrets into a worker. Base Compose
host publications are loopback-only. Keep coordinator, PostgreSQL, Redis,
Prometheus and Grafana behind the host firewall/private network. Only explicitly
approved API ingress may be published publicly; the application terminates HTTPS
itself and never trusts forwarded headers or opens an HTTP redirect port.

Before launch, inspect the resolved Compose configuration (without printing
secret values), host firewall and listening ports. Containers share a trusted
private network in this baseline; transport authentication still applies.

Supply PostgreSQL pg_hba.conf denying unencrypted host access and requiring
scram-sha-256 for scoped users over hostssl. Supply Redis redis.conf with `port 0`,
`tls-port 6379`, mounted certificate/key/CA paths, appendonly yes, noeviction,
maxmemory 128mb and an ACL/requirepass secret. Configure tls-auth-clients according
to the password-authenticated client contract (the queue clients do not send a
Redis client certificate). Never copy actual passwords into this repository.
PostgreSQL key ownership and mode must satisfy the server's checks.

API/coordinator DB URL files contain the scoped login and
`sslmode=verify-full&sslrootcert=/run/forgeq/database-ca.crt`. Each service's mounted
root/cert/key filenames are listed in the overlay. Coordinator certificates need
DNS SAN coordinator; API HTTPS certificates need the ingress DNS and API scrape
DNS api. Metrics certificate needs coordinator. Service client certificates use
exactly one recognized URI SAN as defined by the identity contract.

Prometheus has only an API operator read credential and independent coordinator
metrics token. The supplied HTTPS scrape fragment verifies CA and DNS names.
API /metrics retains operator authorization; coordinator /metrics rejects missing
or incorrect credentials. Coarse /healthz has no secret-bearing data. Production
Compose removes old plaintext wget/redis-cli healthchecks; TLS-aware readiness
and canaries must be installed and rehearsed on the selected target. Container
start order is not readiness evidence.

Run privileged migration and role bootstrap before application startup. The admin
profile is intentionally not launched by normal `up`. A separate release migrator
image must match the application revision. No external domain, issuer, provider,
network exception or production secret delivery mechanism is selected here.
