#!/usr/bin/env bash
# Explicit encrypted lifecycle command; configure private files per backup README.
set -euo pipefail
umask 077
cd "$(dirname "$0")/.."
exec go run ./cmd/backup "${1:-once}"
