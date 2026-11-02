#!/usr/bin/env bash
# One disposable stack; coordinate the integration slot before invoking.
set -euo pipefail
cd "$(dirname "$0")/.."
project="forgequeue-backup-test-$(date +%s)-$$"
compose=(docker compose -p "$project" -f deployments/backup/compose.test.yml)
cleanup() { "${compose[@]}" down --volumes --remove-orphans >/dev/null; }
trap cleanup EXIT
"${compose[@]}" up -d --wait --wait-timeout 60
address="$("${compose[@]}" port postgres 5432)"
export FORGEQUEUE_BACKUP_TEST_DATABASE_URL="postgres://backup_fixture:synthetic-local-only@$address/forgequeue_backup_test?sslmode=disable"
GOMAXPROCS=2 go test -p 1 -count=1 -timeout=120s -v ./internal/backup
