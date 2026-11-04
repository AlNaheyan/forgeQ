#!/usr/bin/env python3
"""Fail closed and never forward scanner stdout/stderr (including secret matches)."""
import argparse
import csv
import io
import json
import re
from pathlib import Path
import shutil
import subprocess
import tempfile


def run(command, output=None):
    if not shutil.which(command[0]):
        raise RuntimeError(f'{Path(command[0]).name}: required scanner unavailable')
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if output is not None and result.stdout.strip():
        output.write_bytes(result.stdout)
    if result.returncode:
        reason = ''
        if b'panic:' in result.stderr:
            reason = ' (scanner panic)'
        elif b'fatal error:' in result.stderr:
            reason = ' (runtime fatal error)'
        raise RuntimeError(f'{Path(command[0]).name}: scanner exited {result.returncode}{reason}')
    if output is not None:
        if not result.stdout.strip():
            raise RuntimeError(f'{Path(command[0]).name}: scanner produced no report')


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
        report = directory / 'govulncheck.json'
        try:
            run(['govulncheck', '-json', './...'], report)
        except RuntimeError as error:
            # Only print validated public advisory identifiers, never scanner
            # output or source paths. A non-zero exit still blocks the job.
            if report.is_file():
                ids = sorted(set(re.findall(r'"osv"\s*:\s*"(GO-\d{4}-\d+)"', report.read_text())))
                if ids:
                    raise RuntimeError('govulncheck reported advisories: ' + ', '.join(ids)) from error
            raise
        module = re.search(r'^module\s+(\S+)', Path('go.mod').read_text(), re.MULTILINE)
        if not module:
            raise RuntimeError('missing root module identity')
        run(['go-licenses', 'report', '--ignore=' + module.group(1), './...'], directory / 'licenses.csv')
        messages = []
        data = (directory / 'govulncheck.json').read_text().strip()
        decoder = json.JSONDecoder()
        while data:
            message, end = decoder.raw_decode(data)
            messages.append(message)
            data = data[end:].lstrip()
        # govulncheck emits pretty-printed JSON objects, not necessarily JSONL.
        if any('finding' in message for message in messages):
            ids = sorted({m['finding'].get('osv', '') for m in messages if 'finding' in m and re.fullmatch(r'GO-\d{4}-\d+', m['finding'].get('osv', ''))})
            raise RuntimeError('dependency vulnerability findings' + (': ' + ', '.join(ids) if ids else ''))
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
    except RuntimeError as error:
        raise SystemExit(f'Scan failed: {error}; raw output suppressed.')
    except (OSError, ValueError):
        raise SystemExit('Scan failed: scanner output unavailable or invalid; raw output suppressed.')
