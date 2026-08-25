# Browser SDK

## Status

The browser SDK foundation exists in two layers:

- `locks-sdk`: Rust SDK core and canonical request planner for deterministic request construction, `.well-known` validation, viewer/access request helpers, session export/restore, and browser transport rewrite helpers.
- `locks-sdk/bindings/js`: `wasm-bindgen` JS/WASM wrapper crate exposing browser-facing `Locks`, `Viewer`, `Session`, and `Creator` APIs by wrapping the Rust SDK request planners rather than duplicating route/method/body construction.

The JS/WASM binding now resolves the Lock Server PKARR record at runtime for browser connect URLs, frontend-session exchange, viewer/access calls, signout, and authenticated creator calls. Requests are rewritten to the browser-usable domain endpoint while preserving path/query and carrying the original Lock Server key as `pubky-host`. The binding crate has a private npm package scaffold for local `wasm-pack` builds, but generated `pkg/` artifacts are ignored for now; publishing policy remains follow-up work.

JSON-like JS/WASM API return values are plain JavaScript objects/arrays, not nested `Map` trees. This applies to content-lock request builder output, public content-lock reads, and viewer lifecycle/access-credential JSON responses, so browser callers can use normal property access and `JSON.stringify` without adapter code.

## Local JS/WASM package commands

From the repository root:

```bash
npm --prefix locks-sdk/bindings/js run build
npm --prefix locks-sdk/bindings/js run smoke:pkg
npm --prefix locks-sdk/bindings/js run smoke:demo
npm --prefix locks-sdk/bindings/js run smoke:examples
npm --prefix locks-sdk/bindings/js run release:audit
npm --prefix locks-sdk/bindings/js run live:smoke:check
npm --prefix locks-sdk/bindings/js test
```

`build` runs `wasm-pack build --target web --out-dir pkg`. `smoke:pkg` rebuilds the generated package and verifies that `pkg/package.json` and `pkg/locks_sdk_wasm.d.ts` expose the documented browser SDK API. `smoke:demo` verifies the static demo stays aligned with the generated package import and documented auth/session/creator flow. `smoke:examples` verifies the copyable root `examples/js-sdk/` flows stay aligned with the documented SDK API. `release:audit` reports package publishing blockers; see [`SDK_RELEASE.md`](SDK_RELEASE.md). `live:smoke:check` reports the live Pubky/testnet prerequisites; see [`SDK_LIVE_SMOKE.md`](SDK_LIVE_SMOKE.md). `test` runs the native binding tests, `wasm32-unknown-unknown` compile check, package smoke check, demo smoke check, and examples smoke check. The generated `locks-sdk/bindings/js/pkg/` directory is intentionally ignored until package publishing policy is finalized.

The private scaffold package is named `@pubky/locks-sdk`; the current wasm-pack generated package under `pkg/` is named `locks-sdk-wasm` because it follows the Rust crate name. Treat the generated name as local build output until publishing policy chooses the final public npm package name.

## Local browser demo

A minimal static demo lives at:

```text
locks-sdk/bindings/js/demo/
```

Build the generated package first, then serve the binding directory with any static file server:

```bash
npm --prefix locks-sdk/bindings/js run build
python3 -m http.server 8080 --directory locks-sdk/bindings/js
```

Open:

```text
http://127.0.0.1:8080/demo/
```

The demo imports `../pkg/locks_sdk_wasm.js`, so it only exercises locally generated wasm-pack output. It still requires a real configured Lock Server Pubky with a browser-usable PKARR domain endpoint and working `legacy-connect` creator acquisition.

`smoke:demo` is a static alignment check, not a live browser integration test:

```bash
npm --prefix locks-sdk/bindings/js run smoke:demo
```

## Copyable JS SDK examples

Complete browser-facing SDK flows live at:

```text
examples/js-sdk/
```

They are application examples, not operator scripts. Use them when integrating the SDK into a browser app. They cover creator connect/publishing and viewer discovery/access flows, while keeping verifier-specific proof construction caller-owned. For `paykit-payment`, callers construct a single proof with `verifier_type = "paykit-payment"`, `payload = {}`, and top-level `reader_public_key` on the submitted proof bundle; payment details come from the creator's content lock params, not the submitted proof payload.

Run the static drift check:

```bash
npm --prefix locks-sdk/bindings/js run smoke:examples
```

## Lock Server prerequisites

The browser SDK assumes the Lock Server is configured for authenticated creator publishing:

```toml
[runtime]
environment = "production"

[creator_authority_acquisition]
enabled = true
method = "legacy-connect"

[creator_authority_acquisition.legacy_connect]
allowed_return_origins = ["https://pubky.app"]

[pubky]
network = "mainnet"
```

The Lock Server must also publish a PKARR record with a browser-usable domain endpoint. Runtime publication is configured under `[pkdns]`; see [`docs/RUNTIME.md`](RUNTIME.md#pkarr-and-browser-sdk-reachability).

The SDK verifies the public service identity endpoint:

```http
GET /.well-known/locks-server
```

Expected shape:

```json
{
  "service": "pubky-locks-server",
  "api_version": "0.1",
  "lock_server": "pubky..."
}
```

## SDK initialization

Applications can initialize the SDK in two ways.

### Explicit Lock Server

Use this when the application already knows the Lock Server Pubky:

```ts
const lockServer = "pubky...";
const locks = Locks.forServer(lockServer);
```

For local Pubky testnet browser development, configure the local PKARR relay explicitly:

```ts
import { Locks, LocksOptions } from "locks-sdk-wasm";

const options = new LocksOptions();
options.addPkarrRelay("http://127.0.0.1:15411");

const locks = Locks.forServerWithOptions("pubky...", options);
```

Local `pubky-core/pubky-testnet` defaults are:

```text
PKARR relay     = http://127.0.0.1:15411
HTTP/auth relay = http://127.0.0.1:15412
DHT bootstrap   = 127.0.0.1:6881
```

This remains the stable browser path.

### Creator Lock Service Pointer

Creators publish their current default Lock Server at:

```text
/pub/locks.app/config.json
```

Expected pointer shape:

```json
{
  "version": 1,
  "default_lock_server": "pubky...",
  "created_at": "2026-06-03T00:00:00Z"
}
```

The Rust SDK can parse an already-fetched pointer and construct a client from its `default_lock_server`:

```rust
use locks_sdk::{CreatorLockServicePointer, LocksClient};

let pointer = CreatorLockServicePointer::validate_value(pointer_json)?;
let client = LocksClient::for_creator_pointer(pointer);
```

The JS/WASM binding exposes both the already-fetched pointer path and a live browser discovery convenience:

```ts
const pointer = await fetchCreatorPointerSomehow(); // caller-owned Pubky/browser read
const locksFromPointer = Locks.fromCreatorLockServicePointer(pointer);

const locksFromCreator = await Locks.forCreator("pubky...");
```

`Locks.forCreator` performs a browser PKARR/domain lookup for the creator homeserver, fetches `/pub/locks.app/config.json`, validates the pointer JSON, then uses the discovered `default_lock_server`. It does not use a gateway/base URL fallback.

## Connect flow

The first browser auth surface is the Lock-Server-hosted `legacy-connect` shell. pubky.app or another creator UI never receives the secret-bearing legacy Pubky `authorization_url`; it only receives a short-lived `code` and caller-managed `state` on the configured callback URL.

```ts
import { Locks, ConnectUrlOptions, ExchangeFrontendSessionCodeOptions } from "locks-sdk-wasm";

const lockServer = "pubky...";
const locks = Locks.forServer(lockServer);

const state = crypto.randomUUID();
const returnTo = `${window.location.origin}/locks/callback`;

// Caller owns state persistence and validation.
sessionStorage.setItem("locks-connect-state", state);

const connectUrl = await locks.createConnectUrl(new ConnectUrlOptions(returnTo, state));
window.location.href = connectUrl;
```

Callback page:

```ts
import { Locks, ExchangeFrontendSessionCodeOptions } from "locks-sdk-wasm";

const locks = Locks.forServer("pubky...");
const callback = Locks.parseConnectCallback(window.location.href);

const expectedState = sessionStorage.getItem("locks-connect-state");
if (callback.state !== expectedState) {
  throw new Error("invalid Locks connect state");
}

const session = await locks.exchangeFrontendSessionCode(
  new ExchangeFrontendSessionCodeOptions(callback.code, callback.state),
);
```

`exchangeFrontendSessionCode` posts only:

```json
{
  "code": "...",
  "state": "..."
}
```

The raw `session_token` is returned by the Lock Server once and becomes the SDK `Session` secret.

## Session handling

The SDK does not use browser storage. Applications decide whether and where to store the session secret.

```ts
const secret = session.exportSecret();
const restored = locks.restoreSession(secret);

console.log(restored.lockServer());
```

Sign out revokes the current frontend session on the Lock Server:

```ts
await session.signout();
```

This sends:

```http
DELETE /frontend-sessions/current
Authorization: Bearer <frontend_session_token>
```

## Creator APIs

Authenticated creator calls derive the creator identity from the frontend session. Request bodies intentionally omit `creator`.

### Register guarded resource

```ts
import { RegisterGuardedResourceOptions } from "locks-sdk-wasm";

const bytes = new TextEncoder().encode("guarded bytes");
const registered = await session.creator.registerGuardedResource(
  new RegisterGuardedResourceOptions(
    "example.txt", // relative path under /priv/locks.app/content/
    "text/plain",
    bytes,
  ),
);
```

The SDK sends:

```http
PUT /creator/priv-resources/content/example.txt
Authorization: Bearer <frontend_session_token>
Content-Type: text/plain

guarded bytes
```

Callers supply only the relative content path, not the full `/priv/locks.app/content/` path. The request body is raw bytes (`Uint8Array` in JS/WASM), not JSON/base64.

### Create content lock

`createContentLock` accepts the content-lock request object matching the HTTP API and returns the Lock Server JSON response. For JS/WASM callers, use `CreateContentLockRequestBuilder` to build the multi-resource body without hand-writing the wire shape.

```ts
import { CreateContentLockRequestBuilder } from "locks-sdk-wasm";

const contentLockRequest = new CreateContentLockRequestBuilder()
  .primaryResource(registered.guarded_resource)
  .secondaryResource(attachmentRegistered.guarded_resource)
  .criteria([
    {
      criterion_id: "criterion-1",
      verifier_type: "dev-static",
      params: { satisfied: true },
    },
  ])
  .lockLogic({ type: "all", criteria: ["criterion-1"] })
  .accessPolicy({ requested_credential_ttl_seconds: 3600 })
  .lockServer({ override: lockServer })
  .build();

const contentLock = await session.creator.createContentLock(contentLockRequest);
```

The builder accepts an optional primary resource and any number of secondary resources. Secondary resources are passed as full uploaded `GuardedResource` descriptors; the builder keys them by full private path and strips them to `{ hash, content_type, size }` values. It rejects empty resource sets and duplicate primary/secondary paths. The returned request is a plain JS object backed by the Rust SDK/core request contract.

### Set Lock Service Pointer

```ts
import { SetLockServicePointerOptions } from "locks-sdk-wasm";

await session.creator.setLockServicePointer(
  new SetLockServicePointerOptions(lockServer),
);
```

Request body:

```json
{
  "default_lock_server": "pubky..."
}
```

## Viewer/access APIs

Viewer calls do not require a Pubky identity in v0. Callers choose and durably store a `BundleId`; treat it as bearer-like recovery state.

### Read public content lock

Viewer apps can read public lock policy JSON from a canonical Pubky lock resource:

```ts
const contentLock = await Locks.readContentLock(
  "pubky.../pub/locks.app/<lock_id>.json",
);

const locksForContent = await Locks.forContentLock(
  "pubky.../pub/locks.app/<lock_id>.json",
);
```

`readContentLock` performs a browser PKARR/domain lookup for the creator homeserver, fetches the public content lock JSON without auth, validates that the returned content lock matches the requested resource, and returns the validated JSON. It does not solve lock criteria or submit proofs.

`forContentLock` performs the same content lock read, then selects the Lock Server: it prefers the content lock's per-lock `lock_server.override`; if absent, it discovers the creator's default Lock Server through `/pub/locks.app/config.json`.

### Submit proof bundle

```ts
import { BundleId, VerificationTaskHandleOptions } from "locks-sdk-wasm";

const viewer = locks.viewer;
const bundleId = BundleId.generate().toString(); // caller must store durably; bearer-like recovery handle
const creator = "pubky...";

const lifecycle = await viewer.submitProofBundle({
  version: 1,
  bundle_id: bundleId,
  pubky_lock_resource: `${creator}/pub/locks.app/<lock_id>.json`,
  proofs: [
    {
      criterion_id: "criterion-1",
      verifier_type: "dev-static",
      payload: { satisfied: true },
    },
  ],
});
```

`submitProofBundle` parses the submitted object through the Rust `SubmittedProofBundle` contract before sending it, so identifiers such as `bundle_id` are validated and serialized in canonical form. It does not generate verifier proofs for callers.

```ts
const handle = new VerificationTaskHandleOptions(creator, bundleId);
const current = await viewer.lookupVerificationTask(handle);
const issued = await viewer.issueAccessCredential(handle);
const bytes = await viewer.proxyReadGuardedResource(issued.credential, "example.txt");
```

`VerificationTaskHandleOptions` validates `creator` as a Pubky identity and validates/canonicalizes `bundleId` as a `BundleId` when constructed.

The corresponding HTTP shapes are:

```http
POST /proof-bundles
POST /verification-task-lookups
POST /access-credentials
GET /priv-resources/content/example.txt
Authorization: Bearer <access_credential>
```

Only `proxyReadGuardedResource` sends an `Authorization` header. It also requires the relative guarded resource path to read from the authorized content lock resource set. Polling and credential issuance use `{ creator, bundle_id }` JSON bodies; they do not use internal task IDs.

The Rust SDK also exposes typed response parsers for non-browser callers:

```rust
use locks_sdk::{ViewerLocks, VerificationTaskLifecycleResponse, AccessCredentialResponse};

let lifecycle: VerificationTaskLifecycleResponse =
    ViewerLocks::parse_lifecycle_response(response_json)?;
let issued: AccessCredentialResponse =
    ViewerLocks::parse_access_credential_response(response_json)?;
```

Those parsers reject unknown fields so internal `task_id`, raw proof material, or entitlement evidence cannot silently become part of the public SDK response surface. The JS/WASM viewer methods reuse the same Rust parsers internally before returning lifecycle or access-credential JSON to browser callers.

## Current verification commands

```bash
cargo test -p locks-sdk
cargo test -p locks-sdk-wasm
cargo check -p locks-sdk-wasm --target wasm32-unknown-unknown
```

Full workspace verification additionally requires the local Postgres test database:

```bash
TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5433/locks_test' cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
git diff --check
```
