#!/usr/bin/env python3
"""Exact offline release inventory. Image IDs are OCI config digests, not registry digests."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

SERVICES = ('api', 'coordinator', 'worker', 'migrate')
DIGEST = re.compile(r'sha256:[0-9a-f]{64}\Z')

def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()

def docker(*args):
    result = subprocess.run(['docker', *args], capture_output=True, check=True)
    return result.stdout.decode()

def validate(root):
    manifest = json.loads((root / 'manifest.json').read_text())
    if manifest.get('schema_version') != 1 or not re.fullmatch('[0-9a-f]{40}', manifest.get('revision', '')):
        raise ValueError('invalid manifest revision/schema')
    if set(manifest['images']) != set(SERVICES):
        raise ValueError('invalid service inventory')
    for service, item in manifest['images'].items():
        if not DIGEST.fullmatch(item['id']) or item['tag'] != 'forgequeue-' + service + ':' + manifest['revision']:
            raise ValueError('invalid image identity')
    required = {'images.tar', 'source.tar', 'provenance.json', 'secrets.json', 'govulncheck.json', 'licenses.csv'} | {s + '.sbom.cdx.json' for s in SERVICES} | {s + '.scan.json' for s in SERVICES}
    if set(manifest['files']) != required:
        raise ValueError('incomplete release inventory')
    for name, digest in manifest['files'].items():
        path = root / name
        if path.is_symlink() or not path.is_file() or sha(path) != digest:
            raise ValueError('release checksum mismatch')
    return manifest

def create(root, revision, platform, go_image, runtime_image):
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('revision must be full commit SHA')
    images = {}
    for service in SERVICES:
        tag = 'forgequeue-' + service + ':' + revision
        info = json.loads(docker('image', 'inspect', tag))[0]
        labels = info['Config'].get('Labels') or {}
        if labels.get('org.opencontainers.image.revision') != revision or labels.get('io.forgeq.service') != service:
            raise ValueError('image build identity mismatch')
        if info['Os'] + '/' + info['Architecture'] != platform:
            raise ValueError('image platform mismatch')
        images[service] = {'tag': tag, 'id': info['Id']}
    provenance = {'schema_version': 1, 'revision': revision, 'platform': platform,
                  'builder': 'scripts/ci-release-build.sh', 'inputs': {'go_image': go_image, 'runtime_image': runtime_image},
                  'subjects': images, 'attestation': 'unsigned local build record'}
    (root / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    names = ['source.tar', 'images.tar', 'provenance.json', 'secrets.json', 'govulncheck.json', 'licenses.csv'] + [s + suffix for s in SERVICES for suffix in ('.sbom.cdx.json', '.scan.json')]
    manifest = {'schema_version': 1, 'revision': revision, 'platform': platform, 'images': images,
                'files': {name: sha(root / name) for name in names}}
    (root / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    validate(root)
    (root / 'SHA256SUMS').write_text(''.join(sha(root / n) + '  ' + n + '\n' for n in ['manifest.json', *names]))

def load(root, expected_manifest_sha256):
    if not re.fullmatch('[0-9a-f]{64}', expected_manifest_sha256 or '') or sha(root / 'manifest.json') != expected_manifest_sha256:
        raise ValueError('trusted manifest checksum required')
    manifest = validate(root)
    docker('image', 'load', '--input', str(root / 'images.tar'))
    for service, item in manifest['images'].items():
        info = json.loads(docker('image', 'inspect', item['tag']))[0]
        if info['Id'] != item['id']:
            raise ValueError('loaded image digest mismatch')
    # The override uses config digest IDs; mutable tags cannot affect installation.
    (root / 'compose.images.json').write_text(json.dumps({'services': {s: {'image': i['id'], 'pull_policy': 'never'} for s, i in manifest['images'].items()}}, indent=2) + '\n')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('command', choices=['create', 'verify', 'load'])
    p.add_argument('directory', type=Path)
    p.add_argument('--revision')
    p.add_argument('--platform', default='linux/amd64')
    p.add_argument('--go-image')
    p.add_argument('--runtime-image')
    p.add_argument('--manifest-sha256')
    a = p.parse_args()
    try:
        if a.command == 'create':
            create(a.directory, a.revision, a.platform, a.go_image, a.runtime_image)
        elif a.command == 'load':
            load(a.directory, a.manifest_sha256)
        else:
            validate(a.directory)
    except (ValueError, KeyError, OSError, TypeError, subprocess.SubprocessError):
        raise SystemExit('Release verification failed; archive or image identity is invalid.')
