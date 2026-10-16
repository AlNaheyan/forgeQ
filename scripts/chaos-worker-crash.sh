#!/usr/bin/env bash
set -euo pipefail

run_id="${RUN_ID:-chaos-worker-crash-$(date -u +%Y%m%dT%H%M%SZ)}"
artifact_dir="artifacts/chaos/${run_id}"
kill_after_seconds="${KILL_AFTER_SECONDS:-2}"
worker_count="${WORKER_COUNT:-4}"
job_count="${JOBS:-16}"
arrival_rate="${RATE:-8}"
concurrency="${CONCURRENCY:-64}"
job_duration_ms="${JOB_DURATION_MS:-15000}"
if [[ ! "$run_id" =~ ^[A-Za-z0-9_-]+$ ]]; then
  printf 'RUN_ID may contain only letters, numbers, underscores, and hyphens\n' >&2
  exit 2
fi
mkdir -p "$artifact_dir"

docker compose -f deployments/docker-compose.yml up -d --scale "worker=${worker_count}"
docker compose -f deployments/docker-compose.yml ps worker

python3 scripts/poll-worker-health.py \
  --output "$artifact_dir/worker-health.jsonl" &
poll_pid=$!
cleanup() {
  kill "$poll_pid" 2>/dev/null || true
  wait "$poll_pid" 2>/dev/null || true
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
  -e JOB_DURATION_MS="$job_duration_ms" \
  -e RUN_ID="$run_id" \
  /scripts/jobs.js >"$artifact_dir/k6.log" 2>&1 &
k6_pid=$!

sleep "$kill_after_seconds"
go run ./cmd/chaos worker-kill --count 2 >"$artifact_dir/worker-kills.jsonl"
k6_exit=0
if wait "$k6_pid"; then
	:
else
  k6_exit=$?
fi

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

kill "$poll_pid" 2>/dev/null || true
wait "$poll_pid" 2>/dev/null || true

summary_sql="SELECT 'job_status', status, count(*) FROM jobs WHERE left(idempotency_key, length('${run_prefix}')) = '${run_prefix}' GROUP BY status ORDER BY status; SELECT 'attempt_summary', count(DISTINCT j.id), count(DISTINCT j.id) FILTER (WHERE j.attempt_count > 1), min(j.created_at), max(a.finished_at), percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (a.started_at - j.created_at))), percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (a.started_at - j.created_at))), percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (a.started_at - j.created_at))) FROM jobs j LEFT JOIN job_attempts a ON a.job_id = j.id WHERE left(j.idempotency_key, length('${run_prefix}')) = '${run_prefix}';"
docker compose -f deployments/docker-compose.yml exec -T postgres psql -U forgequeue -d forgequeue -At -F '|' -c "$summary_sql" >"$artifact_dir/postgres-summary.txt"

printf 'k6 summary: %s/k6-summary.json\n' "$artifact_dir"
printf 'worker kill events: %s/worker-kills.jsonl\n' "$artifact_dir"
printf 'worker health timeline: %s/worker-health.jsonl\n' "$artifact_dir"
printf 'PostgreSQL run summary: %s/postgres-summary.txt\n' "$artifact_dir"
if [[ "$k6_exit" != "0" ]]; then
  printf 'k6 exited with status %s; see the saved log and summary\n' "$k6_exit" >&2
  exit "$k6_exit"
fi
