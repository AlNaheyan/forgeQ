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

function booleanEnv(name, fallback) {
  const raw = __ENV[name];
  if (raw === undefined || raw === "") {
    return fallback;
  }
  if (raw === "true") {
    return true;
  }
  if (raw === "false") {
    return false;
  }
  throw new Error(`${name} must be true or false; received ${raw}`);
}

const baseURL = (__ENV.BASE_URL || "http://localhost:8080").replace(/\/$/, "");
const burstJobs = positiveInteger("BURST_JOBS", 10000);
const concurrency = positiveInteger("CONCURRENCY", 100);
const probeRate = positiveInteger("PROBE_RATE", 2);
const expectRejections = booleanEnv("EXPECT_429", true);
const runID = __ENV.RUN_ID || `burst-${Date.now()}`;

const accepted = new Counter("forgequeue_burst_accepted");
const rejected = new Counter("forgequeue_burst_rejected");
const unexpected = new Counter("forgequeue_burst_unexpected");
const missingRetryAfter = new Counter("forgequeue_burst_missing_retry_after");

const thresholds = {
  "checks{probe:readyz}": ["rate>0.99"],
  "dropped_iterations{scenario:readiness_probe}": ["count==0"],
  forgequeue_burst_missing_retry_after: ["count==0"],
  forgequeue_burst_unexpected: ["count==0"],
  "http_req_duration{endpoint:readyz}": ["p(99)<500"],
};
if (expectRejections) {
  thresholds.forgequeue_burst_rejected = ["count>0"];
}

export const options = {
  discardResponseBodies: true,
  scenarios: {
    burst: {
      executor: "shared-iterations",
      exec: "submitBurstJob",
      vus: concurrency,
      iterations: burstJobs,
      maxDuration: __ENV.BURST_MAX_DURATION || "2m",
      gracefulStop: "15s",
      tags: { workload: "backpressure_burst" },
    },
    readiness_probe: {
      executor: "constant-arrival-rate",
      exec: "probeReadiness",
      rate: probeRate,
      timeUnit: "1s",
      duration: __ENV.PROBE_DURATION || "1m",
      preAllocatedVUs: 1,
      maxVUs: 2,
      tags: { workload: "readiness_probe" },
    },
  },
  thresholds,
  tags: {
    run_id: runID,
    suite: "forgequeue",
  },
};

http.setResponseCallback(http.expectedStatuses(200, 201, 429));

export function setup() {
  accepted.add(0);
  rejected.add(0);
  unexpected.add(0);
  missingRetryAfter.add(0);

  const response = http.get(`${baseURL}/readyz`, {
    tags: { endpoint: "readyz", workload: "setup" },
  });
  if (response.status !== 200) {
    throw new Error(`ForgeQueue is not ready: GET /readyz returned ${response.status}`);
  }
}

export function submitBurstJob() {
  const sequence = exec.scenario.iterationInTest;
  const response = http.post(
    `${baseURL}/v1/jobs`,
    JSON.stringify({
      type: "sleep",
      payload: { duration_ms: 25 },
      max_attempts: 3,
    }),
    {
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": `${runID}-${sequence}`,
      },
      tags: {
        endpoint: "create_job",
        job_type: "sleep",
        workload: "backpressure_burst",
      },
    },
  );

  if (response.status === 201) {
    accepted.add(1);
  } else if (response.status === 429) {
    rejected.add(1);
    if (!response.headers["Retry-After"]) {
      missingRetryAfter.add(1);
    }
  } else {
    unexpected.add(1);
  }

  check(
    response,
    { "burst request was accepted or rejected by backpressure": (value) => value.status === 201 || value.status === 429 },
    { probe: "submission" },
  );
}

export function probeReadiness() {
  const response = http.get(`${baseURL}/readyz`, {
    tags: { endpoint: "readyz", workload: "readiness_probe" },
  });
  check(response, { "readyz remains responsive": (value) => value.status === 200 }, { probe: "readyz" });
}

