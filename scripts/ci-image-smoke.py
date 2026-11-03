#!/usr/bin/env python3
"""Prove the image contains the intended Go command and starts it fail-closed."""
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path


def run(*args, timeout=30):
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=True).stdout.decode()

if __name__ == '__main__':
    container = None
    try:
        image, service = sys.argv[1:]
        if service not in ('api', 'coordinator', 'worker', 'migrate'):
            raise ValueError()
        info = json.loads(run('docker', 'image', 'inspect', image))[0]
        if info['Config']['Entrypoint'] != ['/usr/local/bin/forgequeue'] or info['Config']['User'] in ('', '0', 'root'):
            raise ValueError()
        container = run('docker', 'create', '--network', 'none', '--read-only', '-e', 'APP_ENV=production', image).strip()
        with tempfile.TemporaryDirectory() as temp:
            binary = str(Path(temp, 'forgequeue'))
            run('docker', 'cp', container + ':/usr/local/bin/forgequeue', binary)
            metadata = run('go', 'version', '-m', binary)
            if not re.search(r'\s+path\s+\S+/cmd/' + service + r'\s', metadata):
                raise ValueError()
        result = subprocess.run(['docker', 'start', '--attach', container], capture_output=True, timeout=20)
        state = json.loads(run('docker', 'inspect', container))[0]['State']
        if state['Running'] or state['ExitCode'] not in (1, 2) or state['Error']:
            raise ValueError()
    except Exception:
        raise SystemExit('Image startup/command identity smoke check failed; output suppressed.')
    finally:
        if container:
            subprocess.run(['docker', 'rm', '-f', container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
