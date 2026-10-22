#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
project="forgequeue-recovery-$(date +%s)-$$"
read -r FORGEQUEUE_RECOVERY_POSTGRES_PORT FORGEQUEUE_RECOVERY_REDIS_PORT < <(go run scripts/recovery-ports.go)
export FORGEQUEUE_RECOVERY_POSTGRES_PORT FORGEQUEUE_RECOVERY_REDIS_PORT
compose=(docker compose -p "$project" -f deployments/docker-compose.recovery.yml)
evidence="$(pwd)/artifacts/recovery/$project"
mkdir -p "$evidence"
recovery_bin_dir="$(mktemp -d "${TMPDIR:-/tmp}/forgequeue-recovery-bin.XXXXXX")"
cleanup() {
  "${compose[@]}" logs --no-color > "$evidence/dependencies.log" 2>&1 || true
  "${compose[@]}" down --volumes --remove-orphans > "$evidence/cleanup.log" 2>&1 || true
  rm -f "$recovery_bin_dir/api" "$recovery_bin_dir/coordinator" "$recovery_bin_dir/worker"
  rmdir "$recovery_bin_dir" || true
}
trap cleanup EXIT

export GOMAXPROCS="${GOMAXPROCS:-2}"
go version > "$evidence/environment.txt"
uname -sm >> "$evidence/environment.txt"
git rev-parse HEAD >> "$evidence/environment.txt"
git diff --stat >> "$evidence/environment.txt"
printf 'Explicit loopback ports: PostgreSQL=%s Redis=%s\n' "$FORGEQUEUE_RECOVERY_POSTGRES_PORT" "$FORGEQUEUE_RECOVERY_REDIS_PORT" >> "$evidence/environment.txt"
"${compose[@]}" up -d --wait --wait-timeout 60
pg_addr="$("${compose[@]}" port postgres 5432)"
redis_addr="$("${compose[@]}" port redis 6379)"
export FORGEQUEUE_TEST_DATABASE_URL="postgres://forgequeue:recovery-test-only@$pg_addr/forgequeue_recovery?sslmode=disable"
export FORGEQUEUE_TEST_REDIS_ADDR="$redis_addr"
export FORGEQUEUE_RECOVERY_ISOLATED=1
export FORGEQUEUE_RECOVERY_POSTGRES_CONTAINER="$("${compose[@]}" ps -q postgres)"
export FORGEQUEUE_RECOVERY_REDIS_CONTAINER="$("${compose[@]}" ps -q redis)"
export FORGEQUEUE_RECOVERY_PROJECT="$project"
export FORGEQUEUE_RECOVERY_BIN_DIR="$recovery_bin_dir"
export FORGEQUEUE_RECOVERY_EVIDENCE_DIR="$evidence"
go build -p 2 -o "$recovery_bin_dir/api" ./cmd/api
go build -p 2 -o "$recovery_bin_dir/coordinator" ./cmd/coordinator
go build -p 2 -o "$recovery_bin_dir/worker" ./cmd/worker
go test -p 1 -count=1 -timeout=180s -v ./... 2>&1 | tee "$evidence/tests.txt"
printf 'Recovery evidence: %s\n' "$evidence"
