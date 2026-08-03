# Local Operator Demo Server

## Scope

This guide runs the current local/dev Lock Server over real HTTP for operator/reviewer inspection.

Creator publishing is authenticated. The removed unauthenticated local/dev creator publishing route shape is no longer available. For a complete local Pubky testnet creator-publishing flow, use `scripts/dev-legacy-connect-testnet.sh locked-content`; it obtains a frontend session through legacy-connect, then calls the authenticated creator routes.

## Relationship to other docs

- [`docs/RUNTIME.md`](RUNTIME.md): startup/config/worker/health-readiness behavior.
- [`docs/API.md`](API.md): route contract and fixture-backed request/response shapes.
- [`docs/LOCAL_DEMO.md`](LOCAL_DEMO.md): E2E/test-support client demo using Axum `Router::oneshot`.

## Dev legacy-connect testnet automation

For local Pubky-Core testnet work without pubky.app, use:

```bash
scripts/dev-legacy-connect-testnet.sh auth
```

This assumes both the Pubky-Core testnet and Lock Server are already running. It creates/reuses a local dev Pubky user under `.local/pubky-lock-dev/`, starts the hosted Lock-Server `/connect` shell, approves the rendered `pubkyauth://` URL with the Pubky SDK, completes the shell flow, and exchanges the redirected one-time code for a Locks-local frontend session token.

To continue into creator publishing with the acquired frontend session, run:

```bash
scripts/dev-legacy-connect-testnet.sh locked-content
```

`locked-content` creates a Lock Service Pointer, uploads guarded bytes via `PUT /creator/priv-resources/content/<path>`, creates a content lock, submits a viewer proof bundle, manually completes verification through the dev route, issues an access credential, and proxy-reads the guarded bytes. It expects a dev integration runtime shape: `runtime.environment = "development"` and `creator_authority_acquisition.enabled = true` with `method = "legacy-connect"`.

Canonical config for the script:

```toml
[pubky]
network = "testnet"

[runtime]
environment = "development"

[creator_authority_acquisition]
enabled = true
method = "legacy-connect"

```

If `/connect` returns `404`, the running server has not mounted hosted legacy-connect routes. For this testnet integration flow, enable `[creator_authority_acquisition].enabled = true` with `method = "legacy-connect"`.

If `/creator/lock-service-config` or `/creator/priv-resources/content/<path>` returns `401 frontend_session_unavailable`, the authenticated Pubky-backed creator routes are mounted but the frontend session token was missing/invalid. If it returns `503 creator_authority_unavailable`, creator authority could not be restored or revalidated for Pubky homeserver I/O.

## Prerequisites

- A Postgres database reachable through `PUBKY_LOCK_DATABASE_URL`.
- A 32-byte base64url creator-authority encryption key in `PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY`.
- `curl`, `jq`, and `python3` available in your shell.
- A generated/default Lock Server config and secret under `~/.pubky-lock/`.

The database URL below is a local development example. Real credentials must come from the operator environment and must not be committed.

```bash
export PUBKY_LOCK_DATABASE_URL='postgres://locks:locks@localhost:55433/locks_test'
```

Generate a local creator-authority encryption key for this shell before starting the server:

```bash
export PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY="$(python3 - <<'PY'
import base64, os
print(base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip('='))
PY
)"
```

Treat this key as secret local demo material and do not commit or paste it into logs.

## Prepare local runtime config

If `~/.pubky-lock/config.toml` does not exist yet, run the server once without `--config` so it generates a real secret and matching public key:

```bash
cargo run -p locks-server
```

Stop it with `Ctrl-C` after it logs that it is starting. Then edit the generated config to the dev integration shape above and disable the worker if you plan to use manual verification completion; otherwise the worker can complete the task before the manual completion step.

## Start the dev Postgres server

From the repository root, run:

```bash
cargo run -p locks-server
```

The generated default config binds to:

```toml
bind_addr = "127.0.0.1:3000"
```

To change log verbosity without `RUST_LOG`, edit the generated config:

```toml
[logging]
level = "debug"
```

`RUST_LOG` still overrides `logging.level` when it is set.

## Smoke check health and readiness

In another terminal:

```bash
curl -sS http://127.0.0.1:3000/healthz | jq .
curl -sS http://127.0.0.1:3000/readyz | jq .
```

Expected health response:

```json
{
  "status": "ok"
}
```

Expected readiness response for this manual-completion dev Postgres runtime:

```json
{
  "status": "ready",
  "runtime_storage": "persisted",
  "worker_enabled": false
}
```

Readiness responses are intentionally secret-free. They do not expose database URLs, secret paths, Lock Server public keys, worker IDs, task counts, credentials, raw errors, or submitted proof material.

## Authenticated route smoke examples

The supported creator publishing route uses a frontend session bearer token and raw bytes:

```bash
BASE_URL='http://127.0.0.1:3000'
FRONTEND_SESSION_TOKEN='<from scripts/dev-legacy-connect-testnet.sh auth or locked-content>'

printf 'guarded bytes' | curl -sS -X PUT "$BASE_URL/creator/priv-resources/content/example.txt" \
  -H "Authorization: Bearer $FRONTEND_SESSION_TOKEN" \
  -H 'Content-Type: text/plain' \
  --data-binary @- | jq .
```

For a complete creator-to-viewer sequence, prefer the maintained automation:

```bash
scripts/dev-legacy-connect-testnet.sh locked-content
```
