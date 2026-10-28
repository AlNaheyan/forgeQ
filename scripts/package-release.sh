#!/usr/bin/env bash
# Package exact built images and tracked configuration; never include .env or
# deployment secrets. Run once per reviewed clean commit, store off-host.
set -euo pipefail
umask 077
cd "$(dirname "$0")/.."
output="${1:?usage: bash scripts/package-release.sh /absolute/new/release-directory}"
[[ "$output" = /* && ! -e "$output" ]] || { echo 'Use a new absolute directory' >&2; exit 1; }
git diff --quiet HEAD || { echo 'Commit tracked changes before packaging' >&2; exit 1; }
export FORGEQUEUE_VERSION="$(git rev-parse HEAD)"
compose=(docker compose -p forgequeue -f deployments/docker-compose.yml -f deployments/docker-compose.operations.yml)
"${compose[@]}" config --quiet
"${compose[@]}" build api coordinator worker
"${compose[@]}" pull postgres redis prometheus grafana
mkdir -m 700 "$output"
git archive --format=tar HEAD > "$output/source.tar"
images=()
while IFS= read -r image; do images+=("$image"); done < <("${compose[@]}" config --images)
docker image inspect "${images[@]}" > "$output/images.json"
docker image save -o "$output/images.tar" "${images[@]}"
printf '%s\n' "$FORGEQUEUE_VERSION" > "$output/revision.txt"
(cd "$output" && shasum -a 256 source.tar images.tar images.json revision.txt > SHA256SUMS)
printf 'Release archived in %s; encrypt and copy off-host.\n' "$output"
