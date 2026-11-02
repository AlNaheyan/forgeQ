# Publication checks

Checked source revision: `0ea7aa4ea71b3be70e8de425e6eb798ea58e8e2c`.
The verification was performed from a fresh `git clone --no-local`, not from
the original working directory. No remote is configured; nothing was published.

## Secrets and history

- Gitleaks v8.24.2 scanned all 18 commits reachable through local Git refs:
  **zero findings**. Secret-value redaction was enabled.
- Gitleaks also scanned the fresh checkout contents: **zero findings**.
- A supplementary scan checked all 424 historical blobs for full ForgeQueue
  bearer-key patterns and private-key headers: **zero findings**.
- No `.env`, private key, or populated deployment-secret file was found among
  the tracked secret-file paths inspected in history. `.env.example` and the
  empty `deployments/secrets/.gitkeep` are intentional.
- Local-development database passwords and deterministic test-only service
  tokens exist in fixtures/configuration. Do not reuse those values in production.
- `.env.*` variants are now excluded from Git and Docker build context, while
  `.env.example` remains publishable.

Scanner image digest:
`ghcr.io/gitleaks/gitleaks@sha256:b5918eb91b8d2473cec722f066abb4352e4ffdc4ec9f4283ec143aba9ec9ebc4`.

These checks reduce risk; they do not certify that every possible secret is
absent. Scope is local reachable history and checkout, not remote repositories,
deleted/unreachable objects, external storage, or secrets embedded in arbitrary
images. Existing evidence contains local filesystem paths, host/version details,
and timestamps; review that metadata if personal-path privacy matters.
No history rewrite or credential rotation was needed based on these findings.

## Fresh-clone verification

The documented `scripts/test-recovery.sh` command built the actual API,
coordinator, and worker binaries and exercised disposable PostgreSQL/Redis:
**142 top-level tests passed; one optional browser-demo test skipped**.
The test containers, volume, network, and temporary service binaries were cleaned
up. The fresh clone remained clean afterward. `go vet ./...` and base Compose
configuration validation also passed.

The clone used the host's existing Go module/build cache and Docker installation.
This proves a clean source checkout, not an empty/offline host installation.
It does not repeat full application-image packaging, a production deployment,
or the previously recorded browser demo. See [operations](operations.md) and
[demo evidence](demo.md) for those procedures and their validation scope.

## License

An [MIT license](../LICENSE) has been added with copyright holder **ForgeQ**.
Existing code/package identifiers are unchanged. Nothing has been pushed or
published by these checks.
