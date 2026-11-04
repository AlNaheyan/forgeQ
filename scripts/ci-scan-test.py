#!/usr/bin/env python3
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('scan', Path(__file__).with_name('ci-scan.py'))
scan = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scan)

class ScanTests(unittest.TestCase):
    def test_missing_scanner_fails(self):
        with patch.object(scan.shutil, 'which', return_value=None):
            with self.assertRaises(RuntimeError):
                scan.run(['missing'])

    def test_findings_never_escape(self):
        secret = 'synthetic-secret-do-not-emit'
        with patch.object(scan.shutil, 'which', return_value='/tool'), patch.object(
            scan.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, secret.encode(), secret.encode())):
            with self.assertRaises(RuntimeError) as error:
                scan.run(['gitleaks'])
            self.assertNotIn(secret, str(error.exception))

    def test_json_findings_block_even_with_zero_exit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            def report(command, output=None):
                if command[0] == 'govulncheck':
                    output.write_text('{"config": {}}\n{"finding": {"osv": "GO-test"}}')
                else:
                    output.write_text('example.invalid/module,https://example.invalid/license,MIT')
            with patch.object(scan, 'run', side_effect=report):
                with self.assertRaises(RuntimeError):
                    scan.scan('dependencies', root)

    def test_empty_report_fails(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(scan.shutil, 'which', return_value='/tool'), patch.object(
            scan.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, b'', b'')):
            with self.assertRaises(RuntimeError):
                scan.run(['scanner'], Path(temp, 'report'))

if __name__ == '__main__':
    unittest.main()
