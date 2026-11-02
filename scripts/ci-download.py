#!/usr/bin/env python3
"""Install the exact protoc release, verifying its published SHA256 digest."""
import hashlib
import io
import json
import os
from pathlib import Path
import urllib.request
import zipfile

lock = json.loads(Path('scripts/ci-tools.lock.json').read_text())
item = lock['protoc-linux-amd64']
data = urllib.request.urlopen(item['url'], timeout=60).read()
if hashlib.sha256(data).hexdigest() != item['sha256']:
    raise SystemExit('protoc checksum mismatch')
target = Path(os.environ.get('RUNNER_TEMP', '/tmp')) / 'forgeq-protoc-33.4'
with zipfile.ZipFile(io.BytesIO(data)) as archive:
    archive.extractall(target)
(target / 'bin/protoc').chmod(0o755)
with open(os.environ['GITHUB_PATH'], 'a') as path:
    path.write(str(target / 'bin') + '\n')
