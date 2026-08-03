# JS SDK local testnet creator/reader demos

These examples are a script-driven local Pubky testnet workflow plus browser UIs for the Locks JS/WASM SDK creator and unauthenticated reader paths.

The creator demo publishes locked content and displays a **Viewer content lock resource**. It has separate controls for selecting the primary file and optional secondary files. The primary file becomes the default resource readers usually open first; each secondary file is uploaded as an additional resource in the same content lock. Copy the viewer resource into the separate reader demo to exercise the unauthenticated reader flow.

## Files

```text
examples/js-sdk/package.json
examples/js-sdk/index.html
examples/js-sdk/app.js
examples/js-sdk/creator-complete-flow.js
examples/js-sdk/reader.html
examples/js-sdk/reader-app.js
examples/js-sdk/reader-flow.js
examples/js-sdk/scripts/init-config.mjs
examples/js-sdk/scripts/create-user.mjs
examples/js-sdk/scripts/authenticate.mjs
examples/js-sdk/scripts/start-demo-server.mjs
examples/js-sdk/scripts/start-reader-demo-server.mjs
```

Generated local state lives under:

```text
./.local/js-sdk-demo/config.json
./.local/js-sdk-demo/content-creator-session.json
./.local/lock-server/passphrase
./.local/lock-server/recovery_file
./.local/lock-server/profile.json
./.local/content-creator/passphrase
./.local/content-creator/recovery_file
./.local/content-creator/profile.json
```

## Prerequisites

Required tools/services:

- Rust/Cargo and Node/npm.
- `wasm-pack` for rebuilding `locks-sdk/bindings/js/pkg`.
- Postgres reachable by the Lock Server.
- A local Pubky testnet exposing:
  ```text
  PKARR relay      http://localhost:15411
  HTTP/auth relay  http://localhost:15412
  Auth inbox       http://localhost:15412/inbox/
  DHT bootstrap    localhost:6881
  ```

Build the local WASM SDK package first:

```bash
npm --prefix locks-sdk/bindings/js run build
```

Install the dedicated examples package dependencies:

```bash
npm --prefix examples/js-sdk install
```

The examples package uses `@synonymdev/pubky` for Node-side Pubky testnet auth/key operations. Browser code imports the generated local SDK package from:

```js
../../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js
```

## Local environment setup

The demos need four processes/services alive at the same time:

1. local Pubky testnet
2. Postgres
3. Lock Server on `127.0.0.1:3000`
4. creator demo server on `localhost:8080` and/or reader demo server on `localhost:8081`

### Docker Compose local stack

For a containerized local stack from the repository root, set a creator-authority encryption key and start compose:

```bash
export PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY="$(
  python3 - <<'PY'
import base64, os
print(base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip('='))
PY
)"

docker compose up --build
```

The compose stack starts:

- Postgres on host port `55433`
- Pubky testnet on `15411`, `15412`, `6881`, homeserver HTTP on `6286`, and homeserver admin on `6288`
- Lock Server on `http://localhost:3000`
- creator demo on `http://localhost:8080/examples/js-sdk/`
- reader demo on `http://localhost:8081/reader/`

The Pubky testnet image is built from the public `pubky/pubky-core` repository at
the revision pinned in `docker-compose.yml`; no sibling checkout is required.

Compose keeps Lock Server identity/config in the `lock-home` Docker volume and Postgres data in `postgres-data`. To reset everything:

```bash
docker compose down -v
```

If writer authentication fails after a local testnet restart with a malformed or missing `_pubky.<writer>/session` PKARR record, regenerate the demo writer keypair and retry with a fresh `pubkyauth://` URL:

```bash
docker compose exec creator-demo npm --prefix examples/js-sdk run create-user -- --role content-creator --force
```

The browser-facing demo config still uses `localhost`; container-internal health checks/auth use Docker service names through `LOCKS_INTERNAL_*` environment overrides.

### 1. Start local Pubky testnet

Start `pubky-core/pubky-testnet` using its local static development defaults. From the Locks examples' point of view, these endpoints must respond:

```bash
curl -i http://localhost:15411
curl -i http://localhost:15412
```

`404` from the relay root is fine. Connection refused means the testnet is not running or is using different ports.

The examples assume the homeserver Pubky is:

```text
pubky8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo
```

If your local testnet homeserver differs, edit `./.local/js-sdk-demo/config.json` after `init-config` and change `testnet.homeserver`.

### 2. Start Postgres

Use whatever local Postgres you normally use. The Lock Server reads its URL from `PUBKY_LOCK_DATABASE_URL`; include the database name explicitly:

```bash
export PUBKY_LOCK_DATABASE_URL='postgres://locks:locks@localhost:55433/locks_test'
```

A quick readiness check:

```bash
psql "$PUBKY_LOCK_DATABASE_URL" -c 'select 1;'
```

If you use a different local database/user/port, keep the same environment variable name and update only the URL value.

### 3. Configure Lock Server for the JS demos

The examples do **not** generate or mutate Lock Server TOML. They read the Lock Server Pubky from:

```text
~/.pubky-lock/config.toml
```

Generate a local creator-authority encryption key for the same shell that starts the Lock Server:

```bash
export PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY="$(
  python3 - <<'PY'
import base64, os
print(base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip('='))
PY
)"
```

To generate the default config and Lock Server secret, start the server once after setting `PUBKY_LOCK_DATABASE_URL` and `PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY`:

```bash
cargo run -p locks-server
```

Stop it after it writes `~/.pubky-lock/config.toml` and `~/.pubky-lock/secret.sess`. The generated config contains the real `credentials.lock_server_public_key`; the JS demo config initializer refuses placeholder values.

For the browser creator/reader demos, edit `~/.pubky-lock/config.toml` and make sure it has these local-testnet values:

```toml
bind_addr = "127.0.0.1:3000"

[worker]
enabled = true

[runtime]
environment = "development"

[creator_authority_acquisition]
enabled = true
method = "legacy-connect"
frontend_session_ttl_seconds = 86400
frontend_session_code_ttl_seconds = 120

[creator_authority_acquisition.legacy_connect]
allowed_return_origins = ["http://localhost:8080"]

[pubky]
network = "testnet"

[pkdns]
icann_domain = "localhost"
public_icann_http_port = 3000
pkarr_relays = ["http://localhost:15411"]
```

Keep the generated or derived `credentials.lock_server_public_key` value in that file. If it is still:

```toml
lock_server_public_key = "<derived-on-first-run>"
```

start the Lock Server once so it can initialize `~/.pubky-lock/secret.sess` and rewrite/derive the real public key before running `npm --prefix examples/js-sdk run init-config`.

### 4. Start Lock Server

```bash
cargo run -p locks-server
```

Wait for these checks to pass:

```bash
curl -fsS http://127.0.0.1:3000/healthz
curl -fsS http://127.0.0.1:3000/readyz
curl -fsS http://127.0.0.1:3000/.well-known/locks-server
```

The Lock Server also needs its PKARR record published to the local relay. With the config above, startup/republishing should publish through `http://localhost:15411`. The browser SDK depends on that record when resolving `_pubky.<lock-server>`.

## Local Pubky testnet defaults

`pubky-core/pubky-testnet` local static development uses:

```text
PKARR relay     = http://localhost:15411
HTTP/auth relay = http://localhost:15412
Pubky Auth inbox = http://localhost:15412/inbox/
DHT bootstrap   = localhost:6881
```

These values are written to:

```text
./.local/js-sdk-demo/config.json
```

## Setup

Initialize JS demo config:

```bash
npm --prefix examples/js-sdk run init-config
```

Create the content creator signing keypair:

```bash
npm --prefix examples/js-sdk run create-user -- --role content-creator
```

The unauthenticated reader demo does not need a `content-viewer` keypair.

Existing keypairs are reused. To regenerate one role:

```bash
npm --prefix examples/js-sdk run create-user -- --role content-creator --force
```

## Run the demo server

```bash
npm --prefix examples/js-sdk run start-server
```

Default behavior is fail-hard preflight. For iterative development:

```bash
npm --prefix examples/js-sdk run start-server -- --allow-unhealthy
```

Open:

```text
http://localhost:8080/examples/js-sdk/
```

## Run the reader demo server

The reader demo is a separate static/debug server on port `8081` inside the same package:

```bash
npm --prefix examples/js-sdk run start-reader-server
```

For iterative development:

```bash
npm --prefix examples/js-sdk run start-reader-server -- --allow-unhealthy
```

Open:

```text
http://localhost:8081/reader/
```

Reader-server responsibilities are intentionally narrow:

- serve static reader files and generated WASM package files
- expose `/config.json`, `/api/preflight`, `/api/debug/config`, and `/api/client-log`
- never proxy Lock Server viewer APIs

The browser JS/WASM SDK calls the Lock Server directly through browser PKARR transport.

## Browser flow

### 1. Authenticate demo server to homeserver

The page calls:

```http
POST /api/demo-auth/start
GET /api/demo-auth/status
```

It displays a `pubkyauth://...` string and command like:

```bash
npm --prefix examples/js-sdk run authenticate -- \
  --role content-creator \
  --auth "pubkyauth://..."
```

`authenticate` also supports prompt fallback:

```bash
npm --prefix examples/js-sdk run authenticate -- --role content-creator
```

It signs up/registers the `content-creator` with the configured homeserver, approves the auth string, and the demo server persists its session to:

```text
./.local/js-sdk-demo/content-creator-session.json
```

### 2. Authenticate creator to Lock Server

Click **Authenticate to Lock Server**.

The browser uses the Locks JS/WASM SDK to redirect to the Lock-Server-hosted `/connect` shell. The raw legacy-connect authorization URL stays on the Lock Server origin.

The callback URL is:

```text
http://localhost:8080/auth/lock-server/callback
```

Approve the Lock Server auth string with the same role:

```bash
npm --prefix examples/js-sdk run authenticate -- \
  --role content-creator \
  --auth "pubkyauth://..."
```

After callback, the browser stores the Locks frontend session in `localStorage`.

### 3. Configure pointer and create locked content

Click **Configure Lock Service Pointer**. This idempotently sets the content creator's pointer to the configured Lock Server.

Then upload a file and click **Create locked content**.

Rules:

- file upload only
- guarded path prefix is fixed:
  ```text
  /priv/locks.app/content/
  ```
- only the filename segment is editable
- `/` in filename is rejected
- verifier dropdown has one option:
  ```text
  dev-static
  ```

The browser uses the Locks JS/WASM SDK for publishing:

```text
browser → Locks JS/WASM SDK → Lock Server → creator homeserver
```

The Node demo server does not write Locks resources directly to the homeserver.

After success, the page displays the **Viewer content lock resource**:

```text
<creator_pubky>/pub/locks.app/<lock_id>.json
```

## Reader browser flow

The reader demo is unauthenticated for now. It does not create or use a Pubky reader identity.

1. Copy the creator demo's **Viewer content lock resource** and paste it into the reader demo.
2. Click **Load lock**. The browser SDK validates the content lock and resolves the Lock Server.
3. Choose `dev-static` proof control:
   ```text
   satisfied = true | false
   ```
4. Click **Submit proof bundle**.
5. Click **Complete dev verification**. This uses the Lock Server's dev-only viewer completion route directly from the browser SDK.
6. Click **Issue access credential**.
7. Click **Read guarded content**.

The reader page persists local progress in browser `localStorage` under `pubky-locks-reader-demo.*` and has a visible **Reset reader state** button. Bundle IDs and access credentials are bearer-like local-dev secrets; the demo displays them for debugging only.

## Static drift check

Run:

```bash
npm --prefix locks-sdk/bindings/js run smoke:examples
```

This does not run live browser flows. It verifies that the examples keep the agreed scripts, config shape, local-testnet defaults, and documented SDK calls.

## Boundaries

- Authenticated reader UI is deferred.
- The reader demo uses manual paste only; it does not auto-read creator demo state.
- Lock Server TOML generation is out of scope.
- The second auth flow must use Lock Server `/connect`, not a demo-origin rendering of the raw `authorization_url`.
- The examples do not use a gateway/base URL fallback. SDK calls resolve through browser PKARR/domain paths using the configured local PKARR relay.
