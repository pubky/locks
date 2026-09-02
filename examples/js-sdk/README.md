# JS SDK local testnet creator/reader demos

These examples are a script-driven local Pubky testnet workflow plus browser UIs for the Locks JS/WASM SDK creator and reader paths. The reader browser remains unauthenticated; `paykit-payment` additionally uses a fixed `content-viewer` identity only inside the native Paykit reader helper.

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
examples/js-sdk/scripts/homegate-bridge.mjs
examples/js-sdk/scripts/create-user.mjs
examples/js-sdk/scripts/authenticate.mjs
examples/js-sdk/scripts/prepare-paykit-reader.mjs
examples/js-sdk/scripts/receive-paykit-request.mjs
examples/js-sdk/scripts/register-paykit-reader.mjs
examples/js-sdk/scripts/lib/paykit-reader-worker.mjs
examples/js-sdk/scripts/test-paykit-reader-worker.mjs
examples/js-sdk/scripts/start-demo-server.mjs
examples/js-sdk/scripts/start-reader-demo-server.mjs
```

Generated local state lives under:

```text
./.local/demo-config/config.json
./.local/js-sdk-demo/content-creator-session.json
./.local/lock-server/passphrase
./.local/lock-server/recovery_file
./.local/lock-server/profile.json
./.local/content-creator/passphrase
./.local/content-creator/recovery_file
./.local/content-creator/profile.json
./.local/content-viewer/passphrase
./.local/content-viewer/recovery_file
./.local/content-viewer/profile.json
./.local/paykit-reader/state.v1
./.local/paykit-reader/prepared.v1.json
./.local/paykit-reader/worker.v1.json
./.local/paykit-reader/owner.lock
```

## Prerequisites

Required tools/services:

- Rust/Cargo and Node/npm.
- `wasm-pack` for rebuilding `locks-sdk/bindings/js/pkg`.
- Postgres reachable by the Lock Server.
- A local Pubky testnet exposing:
  ```text
  PKARR relay      http://127.0.0.1:15411
  HTTP/auth relay  http://127.0.0.1:15412
  Auth inbox       http://127.0.0.1:15412/inbox/
  DHT bootstrap    127.0.0.1:6881
  ```

For direct npm development, build the local WASM SDK package first:

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

The supported end-to-end path is the complete local Compose stack documented below. It generates ignored owner-only credentials, starts both databases and both application servers, bootstraps Bitcoin regtest, waits for Fulcrum using `server.version`, and starts the creator and reader demos.

The Compose image builds the JS/WASM package itself. A fresh checkout does not need a host-generated `locks-sdk/bindings/js/pkg` directory.

For direct npm development without Compose, provide a running local Pubky testnet, PostgreSQL, Lock Server, and Paykit Server first. `init-config` reads the Lock Server public key from `~/.pubky-lock/config.toml` by default; it does not read the Lock Server signing secret.

### Basic Locks stack

For a containerized local stack from the repository root:

```bash
docker compose up --build
```

On first startup, the Lock Server entrypoint generates a random creator-authority
encryption key and persists it in the private `lock-home` volume. Later starts reuse
that key. To supply your own 32-byte base64url key instead, export
`PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY` before running Compose.

The compose stack starts:

- Postgres on host port `55433`
- Pubky testnet on `15411`, `15412`, `6881`, homeserver HTTP on `6286`, and homeserver admin on `6288`
- Lock Server on `http://127.0.0.1:3000`
- creator demo on `http://127.0.0.1:8080/examples/js-sdk/`
- reader demo on `http://127.0.0.1:8088/reader/`

The Pubky testnet image is built from the public `pubky/pubky-core` repository at
the revision pinned in `docker-compose.yml`; no sibling checkout is required.

Compose keeps the Lock Server identity, config, and generated creator-authority
encryption key in the `lock-home` Docker volume and Postgres data in `postgres-data`.
To reset everything:

```bash
docker compose down -v
```

If writer authentication fails after a local testnet restart with a malformed or missing `_pubky.<writer>/session` PKARR record, regenerate the demo writer keypair and retry with a fresh `pubkyauth://` URL:

```bash
docker compose exec creator-demo npm --prefix examples/js-sdk run create-user -- --role content-creator --force
```

The browser-facing demo config uses `127.0.0.1`; container-internal health checks/auth use explicit loopback or Docker service names through `LOCKS_INTERNAL_*` environment overrides.

## Local Pubky testnet defaults

`pubky-core/pubky-testnet` local static development uses:

```text
PKARR relay     = http://127.0.0.1:15411
HTTP/auth relay = http://127.0.0.1:15412
Pubky Auth inbox = http://127.0.0.1:15412/inbox/
DHT bootstrap   = 127.0.0.1:6881
Paykit browser  = http://127.0.0.1:3001
```

These values are written to:

```text
./.local/demo-config/config.json
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

The dev-static reader flow does not need a `content-viewer` keypair. The Paykit reader flow does:

```bash
npm --prefix examples/js-sdk run create-user -- --role content-viewer
```

The embedded reader worker registers that identity with the configured local homeserver before invoking the native helper. Reader recovery material is loaded from the existing encrypted role files and sent only on helper stdin.

Existing keypairs are reused. To regenerate one role:

```bash
npm --prefix examples/js-sdk run create-user -- --role content-creator --force
```

Deleting `.local` deletes the local recovery-file identities as well as disposable demo state.
Authentication never silently regenerates a missing identity because that would approve as a
different Pubky. Recreate the fallback identity explicitly before authenticating:

```bash
npm --prefix examples/js-sdk run create-user -- --role content-creator
```

Replacing the content-creator identity clears any persisted demo-auth session for the old key before and after rotation. The demo server also validates persisted and newly approved sessions against the current role profile, so an approval that completes during rotation cannot restore the old identity. Authenticate the demo again before continuing.

## Run the demo server

### Complete local Compose stack: quickstart

1. From the repository root, build and start the complete stack in the background:

```bash
docker compose --file compose.paykit-local-demo.yaml up -d --build
```

2. Open the content-creator demo:

```text
http://127.0.0.1:8080/examples/js-sdk/
```

3. Approve browser requests with the external wallet under test. When using the local
   recovery-file fallback, run authentication commands from the repository host:

```bash
npm --prefix examples/js-sdk run authenticate -- --role content-creator
npm --prefix examples/js-sdk run authenticate-paykit -- --role content-creator
```

Do not wrap these commands in `docker compose exec`. The host wrappers load private role
state locally and bridge only bounded helper input into the relevant container.

The Paykit Server build context uses the immutable `v0.1.0-rc2` tag, its compatible
Locks context uses `v0.1.0-rc1`, Paykit Rust uses `v0.1.0-rc48`, and Pubky Core uses
`v0.11.0`. The active Locks checkout is used only for the Locks and browser-demo
images being developed. No sibling repository checkout is required.

For coordinated pre-merge Paykit Server work, select an explicit absolute local worktree
without changing the committed public default:

```bash
PAYKIT_SERVER_CONTEXT=/absolute/path/to/paykit-server \
  docker compose --file compose.paykit-local-demo.yaml up -d --build
```

Compose validation requires the rendered context to match that environment value exactly.

`compose.paykit-local-demo.yaml` is intentionally limited to local development and demonstration. When `.local` is absent, the one-shot `compose-bootstrap` service creates the ignored owner-only credentials and non-state configuration before dependent services start. Existing generated credentials are validated and reused. For a quiet configuration check without printing generated environment values, run `npm --prefix examples/js-sdk run validate:paykit-compose`; the wrapper inspects a captured `docker compose --file compose.paykit-local-demo.yaml config --no-env-resolution` model.

This starts separate Locks and Paykit PostgreSQL services, Bitcoin Core regtest, a 101-block wallet bootstrap, Fulcrum readiness through `server.version`, Pubky testnet, a local Homegate-compatible signup bridge, Locks, Paykit Server, and both browser demos. All published ports bind to host loopback. Paykit is browser-visible at `http://127.0.0.1:3001`, the Homegate bridge at `http://127.0.0.1:6288`, and Fulcrum at `tcp://127.0.0.1:60001`. Locks reaches Paykit at `http://127.0.0.1:3001` inside the shared Pubky network namespace. The unprivileged creator and reader images contain the reviewed native helpers and a package built in the image; they receive only their explicit runtime directories, never the repository root or Lock Server identity volume.

Open:

```text
Creator: http://127.0.0.1:8080/examples/js-sdk/
Reader:  http://127.0.0.1:8088/reader/
Paykit:  http://127.0.0.1:3001/setup
```

The reader uses port `8088` for both direct npm and Compose; there is no host/container remap.

The reader displays directly runnable Bitcoin commands without JSON-style escaped quotes.
The generated send command has this shape, with the current request address and amount
substituted for `BCRT_ADDRESS` and `BTC_AMOUNT`:

```bash
docker compose --file compose.paykit-local-demo.yaml exec -T bitcoin sh -ec 'bitcoin-cli -conf=/home/bitcoin/.bitcoin/bitcoin.conf -regtest -rpcwallet=miner sendtoaddress BCRT_ADDRESS BTC_AMOUNT'
```

The Compose reader process listens on port `8088`. To remove the four explicit disposable database/Bitcoin/Fulcrum volumes, empty bootstrap scratch directory, and encrypted reader-helper state while preserving generated credentials/config, role identities, and Lock Server identity:

```bash
npm --prefix examples/js-sdk run reset-paykit-demo
```

Do not use `docker compose --file compose.paykit-local-demo.yaml down -v` unless you intentionally want to delete the persistent Lock Server identity volume.

### Helper-free staging services demo

To run only the Creator and reader browser apps against fixed deployed staging services:

```bash
docker compose --file compose.paykit-staging-demo.yaml up -d --build
```

This path uses two distinct external Bitkit staging identities, the standard public Pubky network, and no native Paykit helpers or local backend services. Creator and reader remain on ports 8080 and 8088. See [`docs/PAYKIT_STAGING_DEMO.md`](../../docs/PAYKIT_STAGING_DEMO.md) for the exact role split, pasted-reader-Pubky gate, local-client reset, known external setup blocker, and full acceptance milestones.

### Direct npm server

```bash
npm --prefix examples/js-sdk run start-server
```

Default behavior is fail-hard preflight. For iterative development:

```bash
npm --prefix examples/js-sdk run start-server -- --allow-unhealthy
```

Open:

```text
http://127.0.0.1:8080/examples/js-sdk/
```

## Run the reader demo server directly with npm

Outside the Paykit Compose flow, the reader demo is a separate static/debug server on the
same port `8088` inside the same package:

```bash
npm --prefix examples/js-sdk run start-reader-server
```

For iterative development:

```bash
npm --prefix examples/js-sdk run start-reader-server -- --allow-unhealthy
```

Open:

```text
http://127.0.0.1:8088/reader/
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

It displays a `pubkyauth://...` string. In the Compose external-wallet flow, scan or paste that request into the wallet under test. The approved wallet identity becomes the canonical creator identity and is published for the reader and Paykit services; the demo never imports the wallet private key.

For direct npm development outside the Compose external-wallet mode, the recovery-file command remains available:

```bash
npm --prefix examples/js-sdk run authenticate -- \
  --role content-creator \
  --auth "pubkyauth://..."
```

`authenticate` also supports prompt fallback:

```bash
npm --prefix examples/js-sdk run authenticate -- --role content-creator
```

That command signs up/registers the local `content-creator`, approves the auth string, and the demo server persists its session to:

```text
./.local/js-sdk-demo/content-creator-session.json
```

### 2. Authenticate creator to Lock Server

Click **Authenticate to Lock Server**.

Both creator pages open the Lock Server `/connect` shell in an iframe modal. The raw legacy-connect authorization URL stays on the Lock Server origin.

The shell returns `{ state, code }` directly to the parent with `postMessage`. The parent accepts the result only from the exact Lock Server origin and iframe window, then validates the state before exchanging the one-time code. The configured callback URL supplies the parent target origin; the browser does not navigate to it:

```text
http://127.0.0.1:8080/auth/lock-server/callback
```

Approve the Lock Server auth string with the same identity. In Compose, scan or paste it into the same external wallet. For direct npm development, use:

```bash
npm --prefix examples/js-sdk run authenticate -- \
  --role content-creator \
  --auth "pubkyauth://..."
```

The demo homeserver flow and Lock Server flow must both be approved by that same content-creator identity. The browser verifies the creator returned by the Lock Server against the live demo-auth creator. If the demo creator later changes or signs out, the browser revokes and clears the old Locks frontend session, closes any pending Locks auth flow, clears creator-scoped pointer state, and requires matching reauthentication before publishing. After a successful code exchange, the Locks frontend session is kept in memory only and is cleared on reload.

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
- lock type defaults to `dev-static`; `paykit-payment` is the alternate mode
- `paykit-payment` amount is a positive decimal integer string in sats
- payment asset is fixed to `BTC`
- payment recipient is the authenticated content creator; it is not user-editable
- payment is the content lock's sole criterion and the lock logic references exactly that criterion
- payment publishing is rejected until Paykit setup succeeds for the current authenticated creator
- selecting `paykit-payment` queries setup readiness through the current authenticated Locks frontend session
- `ready` skips authorization, `setup_required` opens `GET http://127.0.0.1:3001/setup` in a Paykit-origin iframe, and `unavailable` shows a retry state without opening authorization
- the parent accepts completion only from that exact iframe window and origin with the pending state
- the success callback is only `{ type: "paykit-setup-callback", state }`; failures add only `error: "setup-failed"`, and account data stays inside Paykit

The Paykit iframe is owned by Paykit and displays only the production Bitkit QR/deep-link path; it contains no local-helper instructions. In production, scan that QR with Bitkit. There is no production handle/helper surface.

For the local Compose fallback, first create or load the dedicated Bitcoin Core descriptor wallet and print its external BIP84 account `tpub` and account index:

```bash
npm --prefix examples/js-sdk run generate-paykit-account-tpub
```

This command uses the running Compose regtest node, requests public descriptors only, selects `m/84'/1'/0'`, and intentionally prints only the account-level `tpub` and index at this explicit setup boundary. It never prints or exports the account private key.

Next, inspect the latest Paykit Server logs manually:

```bash
docker compose --file compose.paykit-local-demo.yaml logs --tail=100 paykit-server
```

Find the event labeled `paykit_setup_authorization_url` and copy its `authorization_url` value. Do not automate log parsing. The URL is a local-only bearer secret: the operator owns Paykit log access and retention. Do not publish, reuse, or retain it beyond this local setup operation.

Then run the host helper wrapper:

```bash
npm --prefix examples/js-sdk run authenticate-paykit -- --role content-creator
```

Run it from the repository host, where the encrypted content-creator recovery file is stored. The wrapper loads that local identity and streams one bounded JSON request over stdin to `/usr/local/bin/paykit-companion-auth` in the running `creator-demo` container; private role files are not mounted into the container. `PAYKIT_COMPANION_AUTH_BIN` may override the helper executable path for local testing.

Interactive input is prompted in this exact order: Paykit pubkyauth URL, account xpub/tpub, account index. Non-TTY stdin is exactly those three ordered lines, with one optional final newline:

```text
pubkyauth://...
tpub...
0
```

The wrapper sends the helper this closed schema over stdin only. Its fields are `version`, `auth_url`, `creator_secret`, `account_xpub`, and `account_index`:

```json
{"version":1,"auth_url":"pubkyauth://...","creator_secret":"<base64url-32>","account_xpub":"tpub...","account_index":0}
```

The example trusts the operator-supplied URL. A modified URL can substitute the requester key (`cpk`), relay, or encryption secret and redirect the grant or encrypted xpub claim; that risk is accepted only for the controlled local demo. The auth URL, Creator secret, and xpub are never placed in process arguments, wrapper output, or `postMessage`.

The helper comes only from the Paykit local-demo image/runtime stage consumed by this Compose demo. It is not part of the normal production Paykit package or runtime.

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

The browser remains unauthenticated. A `paykit-payment` proof carries the public key prepared by the native helper; the browser never receives the reader secret or encrypted Paykit state.

1. Copy the creator demo's **Viewer content lock resource** and paste it into the reader demo.
2. Click **Load lock**. The browser SDK validates the content lock and resolves the Lock Server.
3. The loaded lock selects its verifier mode. For `dev-static`, choose:
   ```text
   satisfied = true | false
   ```
4. Click **Submit proof bundle**.
5. Click **Complete dev verification**. This uses the Lock Server's dev-only viewer completion route directly from the browser SDK.
6. Click **Issue access credential**.
7. Click **Read guarded content**.

For `paykit-payment`:

1. The in-process Paykit reader worker starts with `reader-demo`, creates or restores the durable encrypted reader state, publishes and reads back its Receiver Marker, and waits for private Paykit messages. The page polls its closed status automatically; proof submission remains disabled until the worker is prepared and its Reader Pubky matches the current `content-viewer` identity.
2. Click **Submit proof bundle**. The browser submits one `paykit-payment` proof with the confirmed top-level `reader_public_key` and an empty `{}` criterion payload. It never calls the dev completion route.
3. The worker advances the Paykit/Noise link and receives the real Payment Request without a foreground command. The page displays only its validated request ID, regtest address, amount in sats, canonical manual `bitcoin-cli` payment command, and optional mining command.
4. Run the displayed payment command in a terminal. Mining is optional because local Locks uses `minimum_confirmations = 0`.
5. The page polls `pending` and `in_progress` lifecycle states. On `completed`, it issues an access credential and reads the primary guarded resource. `failed`, `expired`, and unknown states fail closed. Use **Resume payment verification polling** after a reload.

The worker is the sole mutable owner of `./.local/paykit-reader/state.v1`. A direct child holds a kernel advisory lock on the owner-only `./.local/paykit-reader/owner.lock` file for the worker lifetime; the child exits when the parent's stdin closes, so the kernel releases ownership after normal exit or a crash without stale-lock takeover. The legacy `prepare-paykit-reader` and `receive-paykit-request` wrappers reject execution while the embedded worker is enabled and acquire the same lock when run standalone.

`GET /api/health` reports HTTP-process liveness only. `GET /api/paykit-reader/status` reports the separate worker readiness/projection contract; Compose uses that second endpoint for health so a serving but unprepared or failed reader is not considered ready.

The native helper is `/usr/local/bin/paykit-reader-demo`; `PAYKIT_READER_DEMO_BIN` is a test-only executable override. Its state path and local Pubky endpoints come from the `PAYKIT_READER_*` Compose environment. Reader homeserver registration runs in a separate direct-spawned Node subprocess with bounded output, timeout, and TERM→KILL cancellation because the Pubky JS API does not expose request cancellation; cancellation waits for child settlement before ownership is released. The worker derives the Paykit peer from the public `content-creator` profile, then passes only the closed native helper environment. The state path must end in `.local/paykit-reader/state.v1`. The helper owns encrypted versioned state, owner-only file permissions, fresh-nonce rewrites, and invariant validation. The worker fences status publication and state checkpoints on current kernel-lock ownership, atomically writes its separate owner-only `worker.v1.json` projection, and clears in-memory readiness immediately if ownership is lost. The HTTP server validates the projection again and requires current in-memory ownership before returning a ready browser status. Terminal worker failure closes PID 1 after a coarse error so Compose restart policy applies.

The reader page persists local progress in browser `localStorage` under `pubky-locks-reader-demo.*` and has a visible **Reset reader state** button. Retrieved guarded bytes are never persisted: text and JSON render as text, images use a temporary object URL, and other binary content exposes metadata and a temporary download link. Bundle IDs and access credentials are bearer-like local-dev secrets; the demo displays them for debugging only.

## Static drift check

Run:

```bash
npm --prefix locks-sdk/bindings/js run smoke:examples
```

This does not run live browser flows. It verifies that the examples keep the agreed scripts, config shape, local-testnet defaults, and documented SDK calls.

## Boundaries

- Browser-side authenticated reader sessions are not used; the Paykit reader identity stays in the one-shot native helper workflow.
- The reader demo manually pastes only the creator's content-lock resource; the Paykit Reader Pubky comes exclusively from the local prepared-status handshake.
- Compose generates closed Locks and Paykit TOML from actual local identities; trusted-key placeholders are never runnable configuration.
- The second auth flow must use Lock Server `/connect`, not a demo-origin rendering of the raw `authorization_url`.
- The examples do not use a gateway/base URL fallback. SDK calls resolve through browser PKARR/domain paths using the configured local PKARR relay.
