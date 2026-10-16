import http from "k6/http";
import exec from "k6/execution";
import { Counter } from "k6/metrics";

function positiveInteger(name, fallback) {
  const raw = __ENV[name];
  if (raw === undefined || raw === "") return fallback;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer; received ${raw}`);
  }
  return parsed;
}

const baseURL = (__ENV.BASE_URL || "http://localhost:8080").replace(/\/$/, "");
const jobCount = positiveInteger("JOBS", 10000);
const rate = positiveInteger("RATE", 100);
const concurrency = positiveInteger("CONCURRENCY", 64);
const runID = __ENV.RUN_ID || `chaos-${Date.now()}`;
const accepted = new Counter("forgequeue_chaos_accepted");
const rejected = new Counter("forgequeue_chaos_rejected");
const unavailable = new Counter("forgequeue_chaos_unavailable");
const unexpected = new Counter("forgequeue_chaos_unexpected");

export const options = {
  discardResponseBodies: true,
  scenarios: {
    submit_jobs: {
      executor: "constant-arrival-rate",
      exec: "submitJob",
      rate,
      timeUnit: "1s",
      duration: `${Math.ceil(jobCount / rate)}s`,
      preAllocatedVUs: concurrency,
      maxVUs: concurrency,
      gracefulStop: "30s",
      tags: { workload: "chaos_submission" },
    },
  },
  thresholds: {
    dropped_iterations: ["count==0"],
    forgequeue_chaos_unexpected: ["count==0"],
  },
  tags: { run_id: runID, suite: "forgequeue_chaos" },
};

http.setResponseCallback(http.expectedStatuses(200, 201, 429, 500, 502, 503, 504));

export function setup() {
  accepted.add(0);
  rejected.add(0);
  unavailable.add(0);
  unexpected.add(0);
  const response = http.get(`${baseURL}/readyz`);
  if (response.status !== 200) {
    throw new Error(`ForgeQueue is not ready: GET /readyz returned ${response.status}`);
  }
}

export function submitJob() {
  const sequence = exec.scenario.iterationInTest;
  if (sequence >= jobCount) return;
  const response = http.post(
    `${baseURL}/v1/jobs`,
    JSON.stringify({ type: "sleep", payload: { duration_ms: 0 }, max_attempts: 3 }),
    {
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": `${runID}-${sequence}`,
      },
      tags: { endpoint: "create_job", workload: "chaos_submission" },
    },
  );

  if (response.status === 201 || response.status === 200) accepted.add(1);
  else if (response.status === 429) rejected.add(1);
  else if (response.status >= 500) unavailable.add(1);
  else unexpected.add(1);
}
