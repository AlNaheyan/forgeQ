#!/usr/bin/env python3
"""Negative gate tests use fake tools; no Docker or dependency services."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('ci-check.sh').resolve()

class Gates(unittest.TestCase):
    def run_gate(self, check, fail=''):
        with tempfile.TemporaryDirectory() as directory:
            tools = {
                'git': 'case "$*" in *diff*) exit ${FAIL_DIFF:-0};; *ls-files*) exit 0;; esac',
                'gofmt': 'exit 0',
                'go': 'exit ${FAIL_GO:-0}',
                'protoc': 'if [ "$1" = --version ]; then echo "libprotoc 33.4"; fi',
                'protoc-gen-go': 'echo "protoc-gen-go v1.36.6"',
                'protoc-gen-go-grpc': 'echo "protoc-gen-go-grpc 1.5.1"',
            }
            for name, body in tools.items():
                p = Path(directory, name)
                p.write_text('#!/bin/sh\n' + body + '\n')
                p.chmod(0o755)
            env = dict(os.environ, PATH=directory + ':' + os.environ['PATH'])
            if fail:
                env[fail] = '1'
            return subprocess.run(['bash', str(SCRIPT), check], env=env, capture_output=True).returncode

    def test_unit_failure_blocks(self):
        self.assertEqual(self.run_gate('unit'), 0)
        self.assertNotEqual(self.run_gate('unit', 'FAIL_GO'), 0)

    def test_drift_blocks(self):
        self.assertEqual(self.run_gate('proto'), 0)
        self.assertNotEqual(self.run_gate('proto', 'FAIL_DIFF'), 0)

if __name__ == '__main__':
    unittest.main()
