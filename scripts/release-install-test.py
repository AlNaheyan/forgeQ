#!/usr/bin/env python3
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result

fixtures = module('fixtures', 'release-manifest-test.py')
i = module('installer', 'release-install.py')
m = i.m

class InstallationTests(unittest.TestCase):
    def test_archive_load_and_digest_mismatch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixtures.ManifestTests().fixture(root)
            checksum = m.sha(root / 'manifest.json')
            def fake(*args):
                return 'loaded' if args[1] == 'load' else json.dumps([{'Id': 'sha256:' + '1'*64}])
            with patch.object(m, 'docker', side_effect=fake):
                m.load(root, checksum)
            override = json.loads((root / 'compose.images.json').read_text())
            self.assertEqual(override['services']['api']['image'], 'sha256:' + '1'*64)
            with patch.object(m, 'docker', return_value='[{"Id":"sha256:bad"}]'):
                with self.assertRaises(ValueError):
                    m.load(root, checksum)
            with patch.object(m, 'docker') as docker:
                with self.assertRaises(ValueError):
                    m.load(root, '0'*64)
                docker.assert_not_called()

    def test_previous_archive_retained(self):
        with tempfile.TemporaryDirectory() as temp:
            current, previous = Path(temp, 'new'), Path(temp, 'old')
            current.mkdir(); previous.mkdir()
            fixtures.ManifestTests().fixture(current)
            fixtures.ManifestTests().fixture(previous)
            manifest = json.loads((previous / 'manifest.json').read_text())
            manifest['revision'] = 'b'*40
            for service, item in manifest['images'].items():
                item['tag'] = 'forgequeue-' + service + ':' + 'b'*40
            (previous / 'manifest.json').write_text(json.dumps(manifest))
            before = {p.name: p.read_bytes() for p in previous.iterdir()}
            with patch.object(m, 'load') as load:
                i.install(current, m.sha(current/'manifest.json'), previous, m.sha(previous/'manifest.json'))
                load.assert_called_once()
            self.assertEqual(before, {p.name: p.read_bytes() for p in previous.iterdir()})
            with self.assertRaises(ValueError):
                i.install(current, '', current, '')

if __name__ == '__main__':
    unittest.main()
