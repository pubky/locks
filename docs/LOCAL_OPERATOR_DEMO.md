# Local Operator Demo Server

## Scope

This guide runs the current local/dev Lock Server over real HTTP for operator/reviewer inspection.

Creator publishing is authenticated. The removed unauthenticated local/dev creator publishing route shape is no longer available. For a complete local Pubky testnet creator-publishing flow, use `scripts/dev-legacy-connect-testnet.sh locked-content`; it obtains a frontend session through legacy-connect, then calls the authenticated creator routes.

## Relationship to other docs

- [`docs/RUNTIME.md`](RUNTIME.md): startup/config/worker/health-readiness behavior.
- [`docs/API.md`](API.md): route contract and fixture-backed request/response shapes.
- [`docs/LOCAL_DEMO.md`](LOCAL_DEMO.md): E2E/test-support client demo using Axum `Router::oneshot`.
- [`examples/js-sdk/README.md`](../examples/js-sdk/README.md): browser-facing Paykit local demo setup and operator commands.

## Paykit Compose local demonstration

The repository's browser-facing Paykit demonstration is a separate operator path from the manual single-server walkthrough below. Its local-only definition is `compose.paykit-local-demo.yaml`. It composes PostgreSQL, Bitcoin regtest, Fulcrum, Pubky testnet v0.11, Locks, Paykit Server, and the creator and reader browser demos. External source builds use anonymous public Git contexts, so no sibling repository checkout is required. Paykit Server is pinned to `v0.1.0-rc2`, its compatible Locks build context is pinned to `v0.1.0-rc1`, and Paykit library contexts use `v0.1.0-rc48`. The local worktree override remains available through the exact absolute `PAYKIT_SERVER_CONTEXT` flow documented in the example README.

The Paykit iframe remains Paykit-owned and presents the production Bitkit QR/deep-link path without local helper instructions. The host wrapper is only a controlled local fallback: its helper comes from the Paykit local-demo image/runtime stage and is not part of the normal production package/runtime. Operators must manually obtain the local-only bearer URL from the labeled Paykit Server log event and follow the secret-handling guidance in [`examples/js-sdk/README.md`](../examples/js-sdk/README.md).

The startup dependency flow is:

```mermaid
flowchart TD
    composeBootstrap[compose-bootstrap] --> postgres
    composeBootstrap --> paykitPostgres[paykit-postgres]
    composeBootstrap --> bitcoin

    postgres --> pubkyTestnet[pubky-testnet]
    postgres --> locksServer[locks-server]
    pubkyTestnet --> locksServer[locks-server]

    bitcoin --> bitcoinBootstrap[bitcoin-bootstrap]
    bitcoinBootstrap --> fulcrum
    fulcrum --> electrumReadiness[electrum-readiness]

    locksServer --> paykitConfig[paykit-config]
    locksServer --> demoConfig[demo-config]

    paykitPostgres --> paykitServer[paykit-server]
    pubkyTestnet --> paykitServer
    paykitConfig --> paykitServer
    electrumReadiness --> paykitServer

    locksServer --> creatorDemo[creator-demo]
    paykitServer --> creatorDemo
    demoConfig --> creatorDemo
    pubkyTestnet --> creatorDemo

    locksServer --> readerDemo[reader-demo]
    paykitServer --> readerDemo
    demoConfig --> readerDemo
    pubkyTestnet --> readerDemo
```

`creator-demo` and `reader-demo` wait for healthy `locks-server` and `paykit-server` services plus successful `demo-config` completion.

The bootstrap and configuration services are one-shot startup jobs. Exiting successfully is their healthy terminal state; they do not remain as long-running processes:

| Service | Responsibility | Downstream gate |
| --- | --- | --- |
| `compose-bootstrap` | Creates or validates ignored local credentials, service environment files, Pubky homeserver configuration, state directories, ownership, and permissions. | Both PostgreSQL services and Bitcoin start only after successful completion. |
| `bitcoin-bootstrap` | Waits for regtest RPC, creates or loads the `miner` wallet, and mines to height 101 so coinbase funds are mature and spendable. | Fulcrum starts only after successful completion. |
| `electrum-readiness` | Sends an Electrum `server.version` request and validates the response; a started container or open TCP port alone is insufficient. | Paykit Server starts only after protocol readiness succeeds. |
| `paykit-config` | Waits for Locks Server to publish its runtime public key, then generates Paykit Server configuration that trusts that exact identity. | Paykit Server starts only after successful generation. |
| `demo-config` | Generates the shared browser configuration from the runtime Locks Server identity and local testnet endpoints. | Creator and reader demos start only after successful generation. |

The complete startup and reset commands, browser URLs, local state boundaries, and manual payment workflow are maintained in [`examples/js-sdk/README.md`](../examples/js-sdk/README.md).

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
