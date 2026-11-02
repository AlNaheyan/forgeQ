# Release scan policy

CI scans the full fetched Git history and checkout with Gitleaks, Go reachability
with govulncheck, every service image with Trivy, and inventories module licenses
with go-licenses. All findings from Gitleaks/govulncheck block; HIGH/CRITICAL image
vulnerabilities block even without a fix. Missing scanners, database downloads,
invalid reports and empty reports are failures. Raw scanner error output is
suppressed because it may contain credentials; do not upload raw secret reports.

There are currently **no vulnerability exceptions**. An exception requires a
reviewed policy change specifying CVE/GHSA, exact package/version/image digest,
impact/reachability, justification, compensating control, owner, approval and an
ISO UTC expiry no later than 30 days. Expired or wildcard exemptions are invalid.
Do not add ignore-unfixed or blanket scanner ignores to make a release green.
License inventory is evidence for review, not automatic legal approval.

Tool versions are explicit in ci-tools.sh; protoc archives additionally use the
SHA256 in ci-tools.lock.json. Go installs use the Go checksum database. Review
version updates and rescan before publication. Cached vulnerability databases are
not accepted as evidence that a new scan ran. A local fake-tool test proves the
gate behavior, not a clean dependency/container/history scan.
