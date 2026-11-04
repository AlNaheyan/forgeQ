#!/usr/bin/env bash
# Fresh disposable cluster only; no existing DB/volume or real credentials.
set -euo pipefail
fixture_name="forgeq-security-$$"
fixture_password=$(openssl rand -hex 24)
cleanup() { docker rm -f "$fixture_name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
# PostgreSQL uses plaintext only on this disposable loopback test connection.
# Production DSN verification and service TLS are tested separately.
docker run --detach --name "$fixture_name" \
  -e POSTGRES_PASSWORD="$fixture_password" -e POSTGRES_DB=forgeq_security_fixture \
  -p 127.0.0.1::5432 postgres:16-alpine >/dev/null
for attempt in $(seq 1 30); do
  if docker exec "$fixture_name" pg_isready -U postgres -d forgeq_security_fixture >/dev/null 2>&1; then break; fi
  sleep 1
done
fixture_port=$(docker port "$fixture_name" 5432/tcp | cut -d: -f2)
export SECURITY_TEST_DATABASE_URL="postgres://postgres:$fixture_password@127.0.0.1:$fixture_port/forgeq_security_fixture?sslmode=disable"
GOMAXPROCS=2 go test -count=1 -p 1 ./internal/security -run TestRuntimeRolesIntegration -v
