#!/usr/bin/env bash
set -euo pipefail

service="${1:-}"
if [[ "$service" != "redis" && "$service" != "coordinator" ]]; then
  printf 'usage: bash scripts/chaos-restart-service.sh redis|coordinator\n' >&2
  exit 2
fi
run_id="${RUN_ID:-chaos-${service}-restart-$(date -u +%Y%m%dT%H%M%SZ)}"
artifact_dir="artifacts/chaos/${run_id}"
restart_after_seconds="${RESTART_AFTER_SECONDS:-2}"
job_count="${JOBS:-500}"
arrival_rate="${RATE:-50}"
concurrency="${CONCURRENCY:-128}"
if [[ ! "$run_id" =~ ^[A-Za-z0-9_-]+$ ]]; then
  printf 'RUN_ID may contain only letters, numbers, underscores, and hyphens\n' >&2
  exit 2
fi
mkdir -p "$artifact_dir"

python3 scripts/poll-worker-health.py \
  --output "$artifact_dir/worker-health.jsonl" &
worker_poll_pid=$!
python3 scripts/poll-worker-health.py \
  --url http://localhost:8080/readyz \
  --output "$artifact_dir/readiness.jsonl" &
ready_poll_pid=$!
cleanup() {
  kill "$worker_poll_pid" "$ready_poll_pid" 2>/dev/null || true
  wait "$worker_poll_pid" "$ready_poll_pid" 2>/dev/null || true
}
trap cleanup EXIT

docker run --rm \
  -v "$PWD/load:/scripts:ro" \
  -v "$PWD/$artifact_dir:/results" \
  grafana/k6:latest run \
  --summary-export "/results/k6-summary.json" \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e JOBS="$job_count" \
  -e RATE="$arrival_rate" \
  -e CONCURRENCY="$concurrency" \
  -e RUN_ID="$run_id" \
  /scripts/chaos.js >"$artifact_dir/k6.log" 2>&1 &
k6_pid=$!

sleep "$restart_after_seconds"
go run ./cmd/chaos restart "$service" >"$artifact_dir/service-restart.json"
k6_exit=0
if wait "$k6_pid"; then
	:
else
	k6_exit=$?
fi
kill "$worker_poll_pid" "$ready_poll_pid" 2>/dev/null || true
wait "$worker_poll_pid" "$ready_poll_pid" 2>/dev/null || true

run_prefix="${run_id}-"
sql="SELECT count(*) FROM jobs WHERE left(idempotency_key, length('${run_prefix}')) = '${run_prefix}' AND status NOT IN ('succeeded','failed','dead_letter','canceled');"
for attempt in $(seq 1 300); do
  nonterminal="$(docker compose -f deployments/docker-compose.yml exec -T postgres psql -U forgequeue -d forgequeue -At -c "$sql")"
  if [[ "$nonterminal" == "0" ]]; then
    break
  fi
  if [[ "$attempt" == "300" ]]; then
    printf 'timed out waiting for %s jobs to reach a terminal state\n' "$nonterminal" >&2
    exit 1
  fi
  sleep 2
done

summary_sql="SELECT 'job_status', status, count(*) FROM jobs WHERE left(idempotency_key, length('${run_prefix}')) = '${run_prefix}' GROUP BY status ORDER BY status; SELECT 'attempt_summary', count(DISTINCT j.id), count(DISTINCT j.id) FILTER (WHERE j.attempt_count > 1), min(j.created_at), max(a.finished_at) FROM jobs j LEFT JOIN job_attempts a ON a.job_id = j.id WHERE left(j.idempotency_key, length('${run_prefix}')) = '${run_prefix}';"
docker compose -f deployments/docker-compose.yml exec -T postgres psql -U forgequeue -d forgequeue -At -F '|' -c "$summary_sql" >"$artifact_dir/postgres-summary.txt"

printf 'k6 summary: %s/k6-summary.json\n' "$artifact_dir"
printf 'service restart event: %s/service-restart.json\n' "$artifact_dir"
printf 'worker health timeline: %s/worker-health.jsonl\n' "$artifact_dir"
printf 'API readiness timeline: %s/readiness.jsonl\n' "$artifact_dir"
printf 'PostgreSQL run summary: %s/postgres-summary.txt\n' "$artifact_dir"
if [[ "$k6_exit" != "0" ]]; then
  printf 'k6 exited with status %s; see the saved log and summary\n' "$k6_exit" >&2
  exit "$k6_exit"
fi
