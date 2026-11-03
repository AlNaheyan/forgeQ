#!/usr/bin/env python3
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('manifest', Path(__file__).with_name('release-manifest.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class ManifestTests(unittest.TestCase):
    def fixture(self, root):
        for name in ['source.tar', 'images.tar', 'secrets.json', 'govulncheck.json', 'licenses.csv'] + [s + suffix for s in m.SERVICES for suffix in ('.sbom.cdx.json', '.scan.json')]:
            (root / name).write_text('{}')
        def inspect(*args):
            service = args[-1].split(':')[0].removeprefix('forgequeue-')
            return json.dumps([{'Id': 'sha256:' + '1'*64, 'Os': 'linux', 'Architecture': 'amd64',
                                'Config': {'Labels': {'org.opencontainers.image.revision': 'a'*40, 'io.forgeq.service': service}}}])
        with patch.object(m, 'docker', side_effect=inspect):
            m.create(root, 'a'*40, 'linux/amd64', 'go@sha256:' + '2'*64, 'runtime@sha256:' + '3'*64)

    def test_exact_manifest_and_corruption(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.fixture(root)
            self.assertEqual(m.validate(root)['revision'], 'a'*40)
            (root / 'images.tar').write_text('corrupted')
            with self.assertRaises(ValueError):
                m.validate(root)

    def test_missing_inventory_and_invalid_digest(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.fixture(root)
            manifest = json.loads((root / 'manifest.json').read_text())
            manifest['images']['api']['id'] = 'latest'
            (root / 'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaises(ValueError):
                m.validate(root)
            self.fixture(root)
            (root / 'api.sbom.cdx.json').unlink()
            with self.assertRaises(ValueError):
                m.validate(root)

    def test_wrong_build_identity(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(m, 'docker', return_value='[{"Config":{"Labels":{}}}]'):
            with self.assertRaises(ValueError):
                m.create(Path(temp), 'a'*40, 'linux/amd64', '', '')

if __name__ == '__main__':
    unittest.main()
