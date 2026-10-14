#!/usr/bin/env sh
set -eu

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

cd "$root_dir"
protoc \
  --go_out=. \
  --go_opt=module=forgequeue \
  --go-grpc_out=. \
  --go-grpc_opt=module=forgequeue \
  proto/worker.proto
