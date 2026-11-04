#!/usr/bin/env python3
"""Check server-side approval configuration and CI for this exact trusted commit."""
import json
import os
import re
import urllib.request


def approved(environment, runs, revision):
    reviewers = [r for r in environment.get('protection_rules', []) if r.get('type') == 'required_reviewers']
    if not reviewers or not reviewers[0].get('reviewers') or not reviewers[0].get('prevent_self_review'):
        return False
    return any(r.get('head_sha') == revision and r.get('event') == 'push' and
               r.get('head_branch') == 'main' and r.get('status') == 'completed' and
               r.get('conclusion') == 'success' for r in runs.get('workflow_runs', []))


def get(path):
    request = urllib.request.Request('https://api.github.com/repos/' + os.environ['GITHUB_REPOSITORY'] + path,
                                    headers={'Authorization': 'Bearer ' + os.environ['GH_TOKEN'],
                                             'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)

if __name__ == '__main__':
    try:
        sha = os.environ['GITHUB_SHA']
        if os.environ.get('GITHUB_EVENT_NAME') != 'workflow_dispatch' or os.environ.get('GITHUB_REF') != 'refs/heads/main' or not re.fullmatch('[0-9a-f]{40}', sha):
            raise ValueError()
        if not approved(get('/environments/release'), get('/actions/workflows/ci.yml/runs?head_sha=' + sha + '&event=push'), sha):
            raise ValueError()
    except Exception:
        raise SystemExit('Release blocked: trusted main CI and protected release environment with independent reviewers are required.')
