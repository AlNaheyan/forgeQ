#!/usr/bin/env python3
"""Exercise the real pinned detector on a removed historical synthetic credential."""
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tempfile

script = Path(__file__).with_name('ci-scan.py').resolve()
if not shutil.which('gitleaks'):
    raise SystemExit('Required Gitleaks scanner unavailable')
with tempfile.TemporaryDirectory(prefix='forgeq-secret-fixture-') as temp:
    root = Path(temp)
    def git(*args):
        subprocess.run(['git', '-c', 'user.name=CI fixture', '-c', 'user.email=fixture@example.invalid', *args],
                       cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    def scan():
        return subprocess.run([sys.executable, str(script), 'secrets', '--output', str(root/'reports')],
                              cwd=root, capture_output=True)
    git('init')
    (root/'README').write_text('Synthetic history test; no real credentials.\n')
    git('add', 'README'); git('commit', '-m', 'clean fixture')
    clean = scan()
    if clean.returncode:
        raise SystemExit('Clean synthetic repository did not pass secret scan')
    token = 'gh' + 'p_' + ''.join(secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789') for _ in range(36))
    (root/'credential.txt').write_text('token=' + token + '\n')
    git('add', 'credential.txt'); git('commit', '-m', 'synthetic credential fixture')
    (root/'credential.txt').unlink()
    git('add', 'credential.txt'); git('commit', '-m', 'remove fixture')
    result = scan()
    if result.returncode == 0 or token.encode() in result.stdout + result.stderr:
        raise SystemExit('Historical secret gate failed or leaked its synthetic value')
print('PASS: removed synthetic credential detected in history; output redacted')
