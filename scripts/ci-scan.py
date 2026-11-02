#!/usr/bin/env python3
"""Fail closed and never forward scanner stdout/stderr (including secret matches)."""
import argparse
import csv
import io
import json
from pathlib import Path
import shutil
import subprocess
import tempfile


def run(command, output=None):
    if not shutil.which(command[0]):
        raise RuntimeError('required scanner unavailable')
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError('scanner reported findings or failed')
    if output is not None:
        if not result.stdout.strip():
            raise RuntimeError('scanner produced no report')
        output.write_bytes(result.stdout)


def scan(kind, directory, image=None):
    directory.mkdir(parents=True, exist_ok=True)
    if kind == 'secrets':
        with tempfile.TemporaryDirectory() as temp:
            for mode in ('git', 'dir'):
                report = Path(temp, mode + '.json')
                run(['gitleaks', mode, '.', '--redact=100', '--no-banner',
                     '--report-format=json', '--report-path=' + str(report), *(['--log-opts=--all'] if mode == 'git' else [])])
                if not report.is_file() or json.loads(report.read_text()) != []:
                    raise RuntimeError('missing secret report or findings')
        (directory / 'secrets.json').write_text('{"history": "passed", "worktree": "passed"}\n')
    elif kind == 'dependencies':
        run(['govulncheck', '-json', './...'], directory / 'govulncheck.json')
        run(['go-licenses', 'report', './...'], directory / 'licenses.csv')
        messages = []
        data = (directory / 'govulncheck.json').read_text().strip()
        decoder = json.JSONDecoder()
        while data:
            message, end = decoder.raw_decode(data)
            messages.append(message)
            data = data[end:].lstrip()
        # govulncheck emits pretty-printed JSON objects, not necessarily JSONL.
        if any('finding' in message for message in messages):
            raise RuntimeError('dependency vulnerability findings')
        if not any('config' in message for message in messages):
            raise RuntimeError('missing vulnerability report')
        rows = list(csv.reader(io.StringIO((directory / 'licenses.csv').read_text())))
        if not rows or any(len(row) != 3 for row in rows):
            raise RuntimeError('invalid license inventory')
    elif kind == 'image':
        if not image:
            raise RuntimeError('exact image required')
        run(['trivy', 'image', '--scanners', 'vuln', '--severity', 'HIGH,CRITICAL',
             '--exit-code', '1', '--format', 'json', image], directory / 'image-scan.json')
        report = json.loads((directory / 'image-scan.json').read_text())
        if report.get('SchemaVersion') != 2 or 'Results' not in report:
            raise RuntimeError('invalid image scan report')
        run(['trivy', 'image', '--format', 'cyclonedx', image], directory / 'sbom.cdx.json')
        if json.loads((directory / 'sbom.cdx.json').read_text()).get('bomFormat') != 'CycloneDX':
            raise RuntimeError('invalid SBOM')
    else:
        raise RuntimeError('unknown scan')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('kind', choices=['secrets', 'dependencies', 'image'])
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--image')
    args = parser.parse_args()
    try:
        scan(args.kind, args.output, args.image)
    except (RuntimeError, OSError, ValueError):
        raise SystemExit('Scan failed: findings, unavailable tool, or invalid report; raw output suppressed.')
