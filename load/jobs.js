import http from "k6/http";
import exec from "k6/execution";
import { check } from "k6";
import { Counter } from "k6/metrics";

function positiveInteger(name, fallback) {
  const raw = __ENV[name];
  if (raw === undefined || raw === "") {
    return fallback;
  }

  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer; received ${raw}`);
  }
  return parsed;
}

function nonnegativeInteger(name, fallback) {
  const raw = __ENV[name];
  if (raw === undefined || raw === "") {
    return fallback;
  }

  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer; received ${raw}`);
  }
  return parsed;
}

const baseURL = (__ENV.BASE_URL || "http://localhost:8080").replace(/\/$/, "");
const jobCount = positiveInteger("JOBS", 10000);
const rate = positiveInteger("RATE", 100);
const concurrency = positiveInteger("CONCURRENCY", 32);
const sleepDurationMS = nonnegativeInteger("JOB_DURATION_MS", 0);
const runID = __ENV.RUN_ID || `k6-${Date.now()}`;

const submitted = new Counter("forgequeue_load_submitted");
const rejected = new Counter("forgequeue_load_rejected");
const unexpected = new Counter("forgequeue_load_unexpected");

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
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
      tags: { workload: "job_submission" },
    },
  },
  thresholds: {
    checks: ["rate>0.99"],
    dropped_iterations: ["count==0"],
    forgequeue_load_submitted: [`count==${jobCount}`],
    forgequeue_load_rejected: ["count==0"],
    forgequeue_load_unexpected: ["count==0"],
    "http_req_duration{endpoint:create_job}": ["p(95)<1000", "p(99)<2000"],
  },
  tags: {
    run_id: runID,
    suite: "forgequeue",
  },
};

http.setResponseCallback(http.expectedStatuses(200, 201));

export function setup() {
  // Emit zero-valued samples so zero-count thresholds remain explicit in a
  // completely successful run.
  submitted.add(0);
  rejected.add(0);
  unexpected.add(0);

  const response = http.get(`${baseURL}/readyz`, {
    tags: { endpoint: "readyz", workload: "setup" },
  });
  if (response.status !== 200) {
    throw new Error(`ForgeQueue is not ready: GET /readyz returned ${response.status}`);
  }
}

export function submitJob() {
  // A constant-arrival-rate scenario works in whole seconds. Guard its rounded
  // final second so every successful run sends exactly JOBS requests.
  const sequence = exec.scenario.iterationInTest;
  if (sequence >= jobCount) {
    return;
  }

  const idempotencyKey = `${runID}-${sequence}`;
  const response = http.post(
    `${baseURL}/v1/jobs`,
    JSON.stringify({
      type: "sleep",
      payload: { duration_ms: sleepDurationMS },
      max_attempts: 3,
    }),
    {
      headers: {
        Authorization: `Bearer ${__ENV.FORGEQUEUE_API_KEY || ""}`,
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey,
      },
      tags: {
        endpoint: "create_job",
        job_type: "sleep",
        workload: "job_submission",
      },
    },
  );

  if (response.status === 201) {
    submitted.add(1);
  } else if (response.status === 429) {
    rejected.add(1);
  } else {
    unexpected.add(1);
  }

  check(response, {
    "job was created": (value) => value.status === 201,
  });
}

