#!/usr/bin/env python3
"""Measure an operator-supplied restore+validation command; never choose a target."""
import argparse
import datetime as dt
import json
import math
import subprocess
import time

def timestamp(value):
    parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('timestamp requires timezone')
    return parsed

def report(manifest, failure, started, ended, elapsed, targets, representative, success):
    snapshot = timestamp(manifest['snapshot'])
    lost = (failure - snapshot).total_seconds()
    if lost < 0 or started < failure or ended < started:
        raise ValueError('invalid recovery chronology')
    measured = representative and success and all(v is not None for v in targets)
    return {'schema_version': 1, 'snapshot': manifest['snapshot'],
            'failure_at': failure.isoformat(), 'restore_started_at': started.isoformat(),
            'validated_at': ended.isoformat(), 'restore_elapsed_seconds': elapsed,
            'recovery_point_seconds': lost,
            'recovery_time_seconds': (ended - failure).total_seconds(),
            'rpo_target_seconds': targets[0], 'rto_target_seconds': targets[1],
            'representative_staging': representative, 'validation_passed': success,
            'target_gate': ('passed' if lost <= targets[0] and (ended-failure).total_seconds() <= targets[1]
                            else 'failed') if measured else 'pending',
            'revision': manifest['revision'], 'archive_bytes': manifest['size']}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest-file', required=True)
    p.add_argument('--failure-at', required=True)
    p.add_argument('--rpo-target-seconds', type=float)
    p.add_argument('--rto-target-seconds', type=float)
    p.add_argument('--representative-staging', action='store_true')
    p.add_argument('command', nargs=argparse.REMAINDER)
    args = p.parse_args()
    command = args.command
    if command and command[0] == '--': command = command[1:]
    if not command: p.error('explicit restore and readiness-validation command required')
    for value in (args.rpo_target_seconds, args.rto_target_seconds):
        if value is not None and (not math.isfinite(value) or value < 0): p.error('targets must be nonnegative')
    with open(args.manifest_file, encoding='utf8') as f: manifest = json.load(f)
    failure = timestamp(args.failure_at)
    started = dt.datetime.now(dt.timezone.utc)
    if (manifest.get('version') != 1 or not manifest.get('complete') or
            manifest.get('encryption') != 'age-v1' or timestamp(manifest['snapshot']) > failure or failure > started):
        p.error('completed manifest and valid recovery chronology required')
    before = time.monotonic()
    # Suppress command output: it might contain credentials. Keep sanitized
    # evidence independently; command exit status must include readiness checks.
    result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    ended = dt.datetime.now(dt.timezone.utc)
    evidence = report(manifest, failure, started, ended, time.monotonic()-before,
                      (args.rpo_target_seconds, args.rto_target_seconds),
                      args.representative_staging, result.returncode == 0)
    print(json.dumps(evidence, indent=2))
    return 0 if result.returncode == 0 and evidence['target_gate'] != 'failed' else 1
if __name__ == '__main__':
    raise SystemExit(main())
