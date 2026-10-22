#!/usr/bin/env python3
"""Write timestamped worker health snapshots until interrupted."""

import argparse
import datetime as dt
import json
import os
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description="Capture timestamped API or worker-health snapshots")
    parser.add_argument("--url", default="http://localhost:8080/v1/workers")
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be positive")

    with open(args.output, "a", encoding="utf-8") as output:
        while True:
            observed_at = dt.datetime.now(dt.timezone.utc).isoformat()
            try:
                request = urllib.request.Request(args.url, headers={"Authorization": "Bearer " + os.environ.get("FORGEQUEUE_API_KEY", "")})
                with urllib.request.urlopen(request, timeout=3) as response:
                    body = response.read().decode("utf-8", errors="replace")
                    try:
                        snapshot = json.loads(body)
                    except json.JSONDecodeError:
                        snapshot = body.strip()
                    record = {"observed_at": observed_at, "http_status": response.status}
                    if isinstance(snapshot, dict) and "workers" in snapshot:
                        record["workers"] = snapshot["workers"]
                    else:
                        record["body"] = snapshot
            except urllib.error.HTTPError as error:
                record = {
                    "observed_at": observed_at,
                    "http_status": error.code,
                    "body": error.read().decode("utf-8", errors="replace").strip(),
                }
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
                record = {"observed_at": observed_at, "http_status": None, "error": str(error)}
            output.write(json.dumps(record, separators=(",", ":")) + "\n")
            output.flush()
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
