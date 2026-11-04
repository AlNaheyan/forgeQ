# Exact releases

The supported release artifact is a single-platform archive containing the API,
coordinator, worker and migration images; supported targets are linux/amd64 and
linux/arm64. Dependency services and runtime configuration remain separately
reviewed deployment inputs. No runtime secrets enter build arguments or archives.

CI uses read-only tokens, immutable action revisions, a fixed Go patch version,
and SHA256-verified protoc. PR code has no publishing credentials. Unit/vet/race
checks precede the isolated recovery job. Generated protobuf files must match the
pinned generators. Missing scan tools or failed reports fail closed; see
[scan policy](SECURITY-POLICY.md).

## Build and verify

Install the exact scanner versions with `bash scripts/ci-tools.sh scan`. From a
clean, committed checkout run:

```sh
bash scripts/package-release.sh /absolute/new/release-directory
python3 scripts/release-manifest.py verify /absolute/new/release-directory
```

The build uses tracked source only and the digest-pinned official base images in
`scripts/ci-images.lock.json`. Reviewed `GO_IMAGE` and `RUNTIME_IMAGE` overrides
must also contain SHA256 digests. `RELEASE_PLATFORM` defaults to linux/amd64; set
linux/arm64 explicitly for that target and use a native Docker host for the selected
architecture; cross-architecture emulation is not configured here. Scan databases
must be reachable. Image
smoke checks inspect the embedded Go command path and run each image in isolated,
read-only production mode with no credentials, requiring startup to fail closed.
This is startup identity evidence, not an application readiness test.

`manifest.json` records full Git revision, platform, OCI **config digest** (Docker
image ID), revision tag, and checksums of the archive, source, scan reports,
license inventory, SBOMs and provenance. These config digests are valid for local
Docker installation; they are not registry manifest digests. `provenance.json` is
an unsigned build record, not a signed SLSA attestation. `SHA256SUMS` includes the
manifest itself. Transfer the expected manifest SHA256 through a trusted channel;
checksums copied alongside a malicious replacement do not authenticate it.

## Approved registry publishing

`release.yml` is manual-only on main and disabled unless the repository variable
`RELEASE_PUBLISH_ENABLED` is exactly `true`. An administrator must first create
the `release` environment with required reviewers and prevent-self-review enabled,
restrict it to main, and prohibit protection bypass. The workflow reads the
server-side environment rules and exact-commit CI result before building; lack
of API access or protections blocks publishing. No automation here configures
these settings. Branch protection and a live GitHub run still require owner setup.

Only that environment receives `RELEASE_USERNAME` and `RELEASE_PASSWORD`. Scope
the registry account to push/pull only the four ForgeQ repositories in the
selected namespace, with no delete/admin rights; use short-lived credentials
where supported. Set `RELEASE_REGISTRY` and `RELEASE_NAMESPACE` variables after
choosing the registry. Optional digest-pinned base overrides are
`RELEASE_GO_IMAGE` / `RELEASE_RUNTIME_IMAGE`. Credentials are passed on stdin and
registry stdout/stderr are suppressed. The job logs out after publishing.

The resulting `registry-manifest.json` records actual repository manifest digests
returned after push plus the expected config digests. Publish no floating `latest`
tag. A failed push may leave a partial set of revision tags: do not deploy until
the complete registry manifest exists and all four digest references verify.
Retain the archived release and evidence independently of Actions' 90-day
artifact retention. A real registry push/pull, approval run, credential rotation,
and supported architecture build remain external acceptance checks until run.

## Archive installation and rollback

For the first release, independently verify the expected manifest checksum, then:

```sh
python3 scripts/release-manifest.py load /srv/releases/REVISION \
  --manifest-sha256 TRUSTED_MANIFEST_SHA256
```

For an upgrade retain the previous complete archive in a separate directory:

```sh
python3 scripts/release-install.py /srv/releases/NEW \
  --manifest-sha256 TRUSTED_NEW_MANIFEST_SHA256 \
  --previous /srv/releases/PREVIOUS \
  --previous-manifest-sha256 TRUSTED_PREVIOUS_MANIFEST_SHA256
```

These commands verify every artifact before Docker load, verify loaded image IDs,
and write `compose.images.json` using config digests and `pull_policy: never`.
They do not start services, migrate databases, prune images, or remove the previous
archive. Compose it last, after the reviewed deployment/security/config overlays,
and use `--no-build` when starting services. Check the resolved configuration's
image IDs before startup. The migration image is a separately privileged command,
not a permanently running runtime service.

Keep at least the active and one known-compatible previous release, configuration
revision, and independently restorable encrypted backup. Before admitting work,
review schema/handler/protocol compatibility and test readiness plus a tenant job
on an isolated target. Database migrations are forward-only: changing image IDs
does not undo schema or data changes. Roll back binaries only if the previous
release is compatible with the current schema/config; otherwise forward-fix or
restore to a fresh destination with an explicitly accepted recovery window.
External side effects and accepted jobs after a backup can require reconciliation.
Never restore an old database over an active destination automatically.

The local Python fixtures exercise corruption, missing inventory, untrusted PR
release denial, loaded-digest mismatch and preservation of the previous archive.
They use fake Docker/scanner calls and do not prove live archive load, production
readiness, off-host recovery, or a successful container vulnerability scan.
