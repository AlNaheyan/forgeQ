#!/usr/bin/env python3
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('guard', Path(__file__).with_name('ci-publish-guard.py'))
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)

class GuardTests(unittest.TestCase):
    def test_required_gates(self):
        env = {'protection_rules': [{'type': 'required_reviewers', 'reviewers': [{'id': 1}], 'prevent_self_review': True}]}
        run = {'head_sha': 'a'*40, 'event': 'push', 'head_branch': 'main', 'status': 'completed', 'conclusion': 'success'}
        self.assertTrue(g.approved(env, {'workflow_runs': [run]}, 'a'*40))
        self.assertFalse(g.approved({}, {'workflow_runs': [run]}, 'a'*40))
        self.assertFalse(g.approved(env, {'workflow_runs': [run]}, 'b'*40))
        for key, value in [('event', 'pull_request'), ('conclusion', 'failure'), ('head_branch', 'untrusted')]:
            self.assertFalse(g.approved(env, {'workflow_runs': [dict(run, **{key: value})]}, 'a'*40))

if __name__ == '__main__':
    unittest.main()
