#!/usr/bin/env bash
set -euo pipefail
umask 077
cd "$(dirname "$0")/.."
output="${1:?new absolute release directory required}"
[[ "$output" = /* && ! -e "$output" ]] || exit 2
GO_IMAGE="${GO_IMAGE:-$(python3 -c 'import json; print(json.load(open("scripts/ci-images.lock.json"))["go_image"])')}"
RUNTIME_IMAGE="${RUNTIME_IMAGE:-$(python3 -c 'import json; print(json.load(open("scripts/ci-images.lock.json"))["runtime_image"])')}"
for image in "$GO_IMAGE" "$RUNTIME_IMAGE"; do
  [[ "$image" =~ ^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$ ]] || { echo 'Base images must use sha256 digests' >&2; exit 2; }
done
platform="${RELEASE_PLATFORM:-linux/amd64}"
[[ "$platform" = linux/amd64 || "$platform" = linux/arm64 ]] || exit 2
git diff --quiet HEAD || { echo 'Tracked checkout is dirty' >&2; exit 1; }
revision=$(git rev-parse HEAD)
mkdir -m 700 "$output"
python3 scripts/ci-scan.py secrets --output "$output/dependencies"
python3 scripts/ci-scan.py dependencies --output "$output/dependencies"
mv "$output/dependencies/"* "$output/"
rmdir "$output/dependencies"
git archive HEAD > "$output/source.tar"
# Build only the tracked commit, excluding untracked files and local secrets.
context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
tar -xf "$output/source.tar" -C "$context"
images=()
for service in api coordinator worker migrate; do
  tag="forgequeue-$service:$revision"
  docker build --platform "$platform" --build-arg "SERVICE=$service" --build-arg "REVISION=$revision" --build-arg "GO_IMAGE=$GO_IMAGE" --build-arg "RUNTIME_IMAGE=$RUNTIME_IMAGE" --tag "$tag" "$context"
  python3 scripts/ci-image-smoke.py "$tag" "$service"
  python3 scripts/ci-scan.py image --image "$tag" --output "$output/$service-scan"
  mv "$output/$service-scan/sbom.cdx.json" "$output/$service.sbom.cdx.json"
  mv "$output/$service-scan/image-scan.json" "$output/$service.scan.json"
  rmdir "$output/$service-scan"
  images+=("$tag")
done
docker image save --output "$output/images.tar" "${images[@]}"
python3 scripts/release-manifest.py create "$output" --revision "$revision" --platform "$platform" --go-image "$GO_IMAGE" --runtime-image "$RUNTIME_IMAGE"
