# Coordinated credential rotation

Production uses direct API HTTPS, verified service mTLS, verified PostgreSQL
TLS and authenticated verified Redis TLS. Development/test token mode remains
available only when APP_ENV explicitly selects those modes (development is the
local default). A tenant API key never substitutes for a service identity.

Mount secrets read-only. Secret value files may be symlinks to regular mounted
files; targets must be at most 64 KiB with no world permissions (0600 or 0640).
A final newline is removed; other whitespace is preserved. Never set both a
secret environment value and its `_FILE` partner. Provider agents can deliver
these files; no production provider is selected by this implementation.

Rotation uses validated coordinated restart; there is no live reload or SIGHUP.
Keep the previous deployment and credential snapshot until replacements verify:

1. Generate replacement credentials through the approved issuer/provider. Install
   an old+new CA trust bundle on each endpoint, atomically replacing mounted files.
2. Restart coordinator with overlap trust, then API/workers with replacement
   certificates and explicit worker IDs matching URI SANs. A coordinator restart
   closes channels; workers may need restart/re-registration. Existing leased
   work follows normal fencing/recovery, so schedule a controlled drain window.
3. Restart coordinator with its replacement serving certificate; verify API fleet
   read, worker registration/claim/completion, HTTPS tenant auth and metrics.
4. Replace dependency passwords/tenant keys through their privileged management
   path and restart dependent processes. Revoke old tenant keys after validating
   replacements; each new HTTP request rechecks the database verifier.
5. Remove old CA trust only after every client has moved; restart all endpoints
   to close previously authenticated channels. A changed file does not revoke an
   existing gRPC channel. Removing trust may intentionally interrupt in-flight RPCs.

Startup rejects expired certificates; the certificate-expiry gauge reports the
loaded snapshot, not files later changed on disk. Monitor expiry and restart well
before expiry. A failed replacement startup never falls back to plaintext.
Rollback is a coordinated restart using a still-valid previous snapshot; never
restore revoked secrets to work around verification failures. Lost acknowledgments
continue to require existing job/lease/idempotency handling.

Public issuer/provider, target host, real rotation interruption measurements and
production recovery/alert delivery remain external rehearsal gates.
