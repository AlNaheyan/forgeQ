#!/usr/bin/env bash
# Tool downloads are versioned; Go module downloads are verified by sum.golang.org.
set -euo pipefail
cd "$(dirname "$0")/.."
export GOMAXPROCS=2
export GOFLAGS=-p=1
case "${1:?tool group required}" in
  proto)
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
    # Release archive checksum must be supplied from the reviewed lock file.
    python3 scripts/ci-download.py protoc
    ;;
  scan|source-scan)
    go install github.com/zricethezav/gitleaks/v8@v8.24.3
    go install golang.org/x/vuln/cmd/govulncheck@v1.9.0
    go install github.com/google/go-licenses/v2@v2.0.1
    if [[ "$1" == scan ]]; then
      go install github.com/aquasecurity/trivy/cmd/trivy@v0.61.1
    fi
    ;;
  *) exit 2 ;;
esac
