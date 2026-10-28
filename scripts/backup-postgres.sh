#!/usr/bin/env bash
# A logical database backup. The destination must not exist; failed partials
# remain for inspection. Secrets inside the archive require encrypted storage.
set -euo pipefail
set -o noclobber
umask 077
cd "$(dirname "$0")/.."
output="${1:?usage: bash scripts/backup-postgres.sh /absolute/path/backup.dump}"
[[ "$output" = /* && ! -e "$output" && ! -e "$output.partial" ]] || { echo 'Use a new absolute destination path' >&2; exit 1; }
docker compose -p forgequeue -f deployments/docker-compose.yml exec -T postgres pg_dump -U forgequeue -d forgequeue --format=custom --no-owner --no-acl > "$output.partial"
mv -n "$output.partial" "$output"
[[ ! -e "$output.partial" ]] || { echo 'Destination appeared during backup; partial retained' >&2; exit 1; }
shasum -a 256 "$output"
