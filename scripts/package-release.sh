#!/usr/bin/env bash
# Compatibility entry point: new releases use a verified, scan-gated exact archive.
set -euo pipefail
exec bash "$(dirname "$0")/ci-release-build.sh" "$@"
