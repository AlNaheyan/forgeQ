#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export GOMAXPROCS=2
case "${1:-unit}" in
  unit)
    files=$(git ls-files '*.go')
    test -z "$(gofmt -l $files)" || { echo 'Go formatting drift' >&2; exit 1; }
    go vet -p 1 ./...
    go test -p 1 -count=1 ./...
    (cd examples/invoice-app && go vet -p 1 ./... && go test -p 1 -count=1 ./...)
    ;;
  race) go test -p 1 -race -count=1 ./internal/worker ./internal/coordinator ./internal/queue ./internal/tenant ;;
  integration) bash scripts/test-recovery.sh ;;
  proto)
    test "$(protoc --version)" = 'libprotoc 33.4'
    test "$(protoc-gen-go --version)" = 'protoc-gen-go v1.36.6'
    test "$(protoc-gen-go-grpc --version)" = 'protoc-gen-go-grpc 1.5.1'
    sh proto/generate.sh
    git diff --exit-code -- proto/workerpb
    test -z "$(git ls-files --others --exclude-standard proto/workerpb)"
    ;;
  *) echo 'Expected unit, race, integration, or proto' >&2; exit 2 ;;
esac
