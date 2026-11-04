#!/usr/bin/env python3
"""Called only by protected manual publishing job; never reads runtime credentials."""
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys

spec = importlib.util.spec_from_file_location('manifest', Path(__file__).with_name('release-manifest.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

if __name__ == '__main__':
    try:
        root = Path(sys.argv[1])
        manifest = m.validate(root)
        registry = os.environ['RELEASE_REGISTRY']
        namespace = os.environ['RELEASE_NAMESPACE']
        if not re.fullmatch(r'[a-z0-9][a-z0-9.-]+(?::[0-9]+)?', registry) or not re.fullmatch(r'[a-z0-9][a-z0-9_/-]*', namespace):
            raise ValueError()
        subprocess.run(['docker', 'login', registry, '--username', os.environ['RELEASE_USERNAME'], '--password-stdin'],
                       input=os.environ['RELEASE_PASSWORD'].encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        try:
            published = {}
            for service, item in manifest['images'].items():
                ref = registry + '/' + namespace + '/' + service
                tag = ref + ':' + manifest['revision']
                if json.loads(m.docker('image', 'inspect', item['tag']))[0]['Id'] != item['id']:
                    raise ValueError()
                m.docker('tag', item['id'], tag)
                m.docker('push', tag)
                info = json.loads(m.docker('image', 'inspect', tag))[0]
                digest = next(d for d in info['RepoDigests'] if d.startswith(ref + '@sha256:'))
                if not m.DIGEST.fullmatch(digest.split('@')[1]):
                    raise ValueError()
                published[service] = {'reference': digest, 'config_digest': item['id']}
            (root / 'registry-manifest.json').write_text(json.dumps({'schema_version': 1, 'revision': manifest['revision'], 'images': published}, indent=2) + '\n')
        finally:
            m.docker('logout', registry)
    except Exception:
        raise SystemExit('Publishing failed; registry output and credentials suppressed.')
