#!/usr/bin/env python3
"""Load an exact release while retaining a separately verified rollback archive."""
import argparse
import importlib.util
import json
from pathlib import Path
import subprocess

spec = importlib.util.spec_from_file_location('manifest', Path(__file__).with_name('release-manifest.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def install(current, checksum, previous, previous_checksum):
    if current.resolve() == previous.resolve():
        raise ValueError('previous release must be retained separately')
    if m.sha(previous / 'manifest.json') != previous_checksum:
        raise ValueError('previous manifest checksum mismatch')
    old = m.validate(previous)
    new = m.validate(current)
    if old['revision'] == new['revision']:
        raise ValueError('previous release must be a different revision')
    if old['platform'] != new['platform']:
        raise ValueError('rollback platform mismatch')
    m.load(current, checksum)
    (current / 'installation.json').write_text(json.dumps({
        'revision': new['revision'], 'manifest_sha256': checksum,
        'previous_revision': old['revision'], 'previous_manifest_sha256': previous_checksum,
        'schema_rollback': 'operator compatibility review required; migrations are forward-only',
    }, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--previous', type=Path, required=True)
    parser.add_argument('--previous-manifest-sha256', required=True)
    args = parser.parse_args()
    try:
        install(args.directory, args.manifest_sha256, args.previous, args.previous_manifest_sha256)
    except (ValueError, KeyError, OSError, TypeError, subprocess.SubprocessError):
        raise SystemExit('Installation failed: current/previous archive verification failed; no deployment started.')
