# Locks HTTP API Reference

## Scope

This document describes the current Lock Server HTTP API contract for local development, tests, and implemented production creator-route authorization.

Creator publishing is authenticated. Creator routes require `Authorization: Bearer <frontend_session_token>`, derive creator identity from the Locks-local frontend session, reject request-body `creator`, and always use Pubky homeserver-backed creator storage.

This document is the living HTTP contract reference. ADRs explain why the boundaries exist; this file describes what callers can send and receive.

Executable creator-publishing examples live under `locks-server/tests/fixtures/creator_publishing/` and are exercised by route contract tests named `creator_publishing_contract_fixtures_*`. The inline JSON below mirrors those fixtures; when changing route shapes, update the fixture and this document together.

For an executable creator-to-viewer local flow using the test-support client, see [`LOCAL_DEMO.md`](LOCAL_DEMO.md).

## Runtime gates

The Lock Server has one non-production route family and one authenticated creator route family:

- `POST /verification-task-completions`
  - Requires `runtime.environment = "development"`.
  - `staging` and `production` never mount it.
- Authenticated creator publishing routes: `PUT /creator/priv-resources/content/<path>`, `DELETE /creator/priv-resources/content/<path>`, `POST /creator/content-locks`, `POST /creator/lock-service-config`
  - Always Pubky homeserver-backed.
  - Can run in `development`, `staging`, or `production`.
  - Require `Authorization: Bearer <frontend_session_token>`.
  - Derive creator identity from the frontend session. Request-body `creator` is rejected for authenticated routes.
  - Missing/unknown/expired frontend sessions use the JSON error envelope (`401 frontend_session_unavailable` or `401 frontend_session_expired`).
  - Missing/revoked creator-granted homeserver authority remains a separate operational error (`503 creator_authority_unavailable`).
- Creator authority status route: `GET /creator/authority-status`
  - Requires `Authorization: Bearer <frontend_session_token>`.
  - Derives creator from the Locks-local frontend session.
  - Returns secret-free missing/present creator authority status.
- Hosted legacy creator connect/session routes: `GET /connect`, `POST /connect/{flow_id}/complete`, `POST /frontend-sessions`, `DELETE /frontend-sessions/current`
  - Mount when explicit `[creator_authority_acquisition].enabled = true`.
  - `DELETE /frontend-sessions/current` requires `Authorization: Bearer <frontend_session_token>` and revokes the current frontend session.
  - `GET /connect` validates `return_to` against `[creator_authority_acquisition].allowed_return_origins` before starting a legacy flow.
  - `POST /connect/{flow_id}/complete` completes the Lock-Server-owned flow, revalidates the stored `return_to`, and redirects with only `state` and one-time `code`.

When gated routes are disabled, they are not mounted and return `404 Not Found`.

## Route status summary

Gated-off routes are plain Axum `404 Not Found` responses because the route is intentionally not mounted; they may not use the JSON error envelope.

| Route | Success | Auth / mount | Secret handoff | Representative errors |
| --- | --- | --- | --- | --- |
| `PUT /creator/priv-resources/content/<path>` | `200` JSON guarded-resource descriptor | Requires `Authorization: Bearer <frontend_session_token>`. Raw bytes body; MIME from `Content-Type`. | No bearer secrets or raw bytes in response. | `400 invalid_request`, `401 frontend_session_unavailable`, `401 frontend_session_expired`, `413 payload_too_large`, `503 creator_authority_unavailable` |
| `DELETE /creator/priv-resources/content/<path>` | `204` empty response | Requires `Authorization: Bearer <frontend_session_token>`. | No bearer secrets or raw bytes in response. | `401 frontend_session_unavailable`, `401 frontend_session_expired`, `404 guarded_resource_not_found`, `503 creator_authority_unavailable` |
| `POST /creator/content-locks` | `200` JSON content lock | Requires `Authorization: Bearer <frontend_session_token>`. | No bearer secrets in response. | `400 invalid_request`, `404 guarded_resource_not_found`, `401 frontend_session_unavailable`, `401 frontend_session_expired`, `503 creator_authority_unavailable` |
| `POST /creator/lock-service-config` | `200` JSON lock-service pointer | Requires `Authorization: Bearer <frontend_session_token>`. | No bearer secrets in response. | `400 invalid_request`, `401 frontend_session_unavailable`, `401 frontend_session_expired`, `503 creator_authority_unavailable` |
| `GET /connect` | `200` HTML Lock-Server-hosted connect shell | No bearer auth. Mounted when `[creator_authority_acquisition].enabled = true`; `return_to` must match `allowed_return_origins` or explicit wildcard policy. | HTML intentionally contains the secret-bearing Pubky authorization URL on Lock Server origin; response must not contain frontend session token, one-time code, or creator authority secret. | `400 invalid_request`, `503 creator_authority_unavailable`, `404` when route gated off |
| `POST /connect/{flow_id}/complete` | `303` redirect to stored `return_to` | No bearer auth. Mounted when `[creator_authority_acquisition].enabled = true`; stored `return_to` is revalidated before redirect. | `Location` contains only callback `state` and one-time `code`; no authorization URL, frontend session token, or creator authority secret. | `400 invalid_request`, `404 creator_connect_flow_unavailable`, `410 creator_connect_flow_expired`, `503 creator_authority_unavailable`, `404` when route gated off |
| `POST /frontend-sessions` | `200` JSON frontend session token handoff | No bearer auth. Requires one-time `code` plus matching `state`. | Response intentionally contains raw `session_token` exactly once; no creator authority secret, authorization URL, or one-time code reuse. | `400 invalid_request`, `400 frontend_session_state_mismatch`, `404 frontend_session_code_unavailable`, `410 frontend_session_code_expired`, `410 frontend_session_code_already_consumed`, `404` when route gated off |
| `DELETE /frontend-sessions/current` | `204` empty response | Requires `Authorization: Bearer <frontend_session_token>`. Mounted with creator authority acquisition. | Token is request-only and is deleted from the frontend session store. | `401 frontend_session_unavailable`, `401 frontend_session_expired`, `404` when route gated off |
| `GET /.well-known/locks-server` | `200` JSON service identity | Public. Always mounted. CORS-enabled. | No secrets. Used by browser SDK to verify service, API version, and Lock Server Pubky identity. | n/a |
| `GET /creator/authority-status` | `200` JSON secret-free authority status | Requires `Authorization: Bearer <frontend_session_token>`. Creator is derived from the frontend session. | Response contains only creator, boolean status, auth kind, scopes, and optional expiry; no tokens, codes, authorization URLs, secrets, or DB/config values. | `401 frontend_session_unavailable`, `401 frontend_session_expired`, `404` only if route absent in older deployments |
| `POST /proof-bundles` | `200` JSON lifecycle | Public viewer route. A new `paykit-payment` lifecycle identity requires `[paykit]` runtime config; an exact persisted replay does not. | No bearer secrets, invoice data, or raw proof material in response. | `400 invalid_request`, `409 task_state_conflict`, `422 unsupported_verifier_type`, `422 paykit_not_configured`, `422 reader_pubky_unresolvable`, `429 rate_limited`, `502 paykit_invoice_creation_failed` |
| `POST /verification-task-lookups` | `200` JSON lifecycle | Public viewer route. | No bearer secrets in response. | `400 invalid_request`, `404 verification_task_not_found` |
| `POST /verification-task-completions` | `200` JSON lifecycle | Dev-only completion gate. | No bearer secrets in response. | `400 invalid_request`, `404 verification_task_not_found`, `409 task_state_conflict`, `404` when route gated off |
| `POST /access-credentials` | `200` JSON credential | Public viewer route after entitlement. | Response intentionally contains raw viewer access credential exactly once. | `400 invalid_request`, `403 entitlement_not_authorized`, `404 verification_task_not_found` |
| `GET /priv-resources/content/<path>` | `200` raw bytes | Requires viewer `Authorization: Bearer <access_credential>`. | No JSON response; credential is request-only. | `401 invalid_access_credential`, `401 expired_access_credential`, `403 entitlement_not_authorized`, `404 guarded_resource_not_found` |

## Error envelope

Non-2xx JSON errors use the stable envelope:

```json
{
  "error": {
    "code": "invalid_request",
    "message": "human-readable explanation"
  }
}
```

Error `code` is the stable programmatic field. Error `message` is diagnostic and may change.

Representative error examples live under `locks-server/tests/fixtures/errors/` and are checked by `locks-server/src/api/errors.rs` serialization tests.

Stable error codes and statuses mirror `locks-server/src/api/errors.rs` tests:

| Code | Status | Meaning |
| --- | ---: | --- |
| `invalid_request` | 400 | JSON shape, validation, or malformed domain object failed. |
| `invalid_identifier` | 400 | Identifier syntax is invalid. |
| `invalid_access_credential` | 401 | Bearer credential is missing, malformed, unknown, or supplied outside the Authorization header. |
| `expired_access_credential` | 401 | Bearer credential existed but expired. |
| `entitlement_not_authorized` | 403 | Credential exists but entitlement/content-lock checks no longer authorize access. |
| `verification_task_not_found` | 404 | No verification task exists for the public handle. |
| `guarded_resource_not_found` | 404 | Referenced guarded resource bytes are unavailable. |
| `content_lock_not_found` | 404 | Referenced content lock is unavailable. |
| `creator_connect_flow_unavailable` | 404 | Pending creator connect flow is missing. |
| `creator_connect_flow_expired` | 410 | Pending creator connect flow expired. |
| `frontend_session_code_unavailable` | 404 | One-time frontend session code is missing. |
| `frontend_session_code_expired` | 410 | One-time frontend session code expired. |
| `frontend_session_code_already_consumed` | 410 | One-time frontend session code was already used. |
| `frontend_session_unavailable` | 401 | Frontend session token is missing or unknown. |
| `frontend_session_expired` | 401 | Frontend session token existed but expired. |
| `frontend_session_state_mismatch` | 400 | One-time code exchange state did not match. |
| `creator_authority_unavailable` | 503 | Creator-granted homeserver authority is unavailable or could not be revalidated. |
| `task_state_conflict` | 409 | Submission or completion conflicts with existing task state. |
| `unsupported_verifier_type` | 422 | Proof references a verifier unavailable in the current runtime. |
| `paykit_not_configured` | 422 | A `paykit-payment` proof was submitted to a Lock Server without a `[paykit]` runtime section. |
| `reader_pubky_unresolvable` | 422 | A `paykit-payment` proof had a syntactically valid `reader_public_key` that could not be resolved to a Pubky homeserver/PKARR record before invoice creation. |
| `rate_limited` | 429 | Submission exceeded configured rate limits. |
| `payload_too_large` | 413 | Raw guarded-resource upload exceeded `[content_locks].max_resource_bytes`. |
| `paykit_invoice_creation_failed` | 502 | Lock Server could not create the Paykit invoice; no verification task is created. |
| `internal_error` | 500 | Unexpected server-side failure. |

## Service discovery

### `GET /.well-known/locks-server`

Returns minimal public service identity for browser SDK bootstrap and compatibility checks.

```json
{
  "service": "pubky-locks-server",
  "api_version": "0.1",
  "lock_server": "pubkyserver123"
}
```

The SDK should verify all three fields:

- `service == "pubky-locks-server"`
- `api_version == "0.1"` for milestone 1
- `lock_server` matches the explicit Lock Server Pubky resolved through PKARR

The endpoint is public, always mounted, CORS-enabled, and must not return secrets or runtime storage details.

## Legacy creator connect/session routes

These routes are the current legacy-connect implementation of the Creator Authority Acquisition protocol.

The hosted browser routes are mounted when `[creator_authority_acquisition].enabled = true` and `method = "legacy-connect"`:

- `GET /connect`
- `POST /connect/{flow_id}/complete`
- `POST /frontend-sessions`
- `DELETE /frontend-sessions/current`

The protocol proves the backend flow: the Lock Server owns the legacy Pubky auth flow, stores creator-granted homeserver authority as private runtime state, returns a one-time frontend code through the hosted shell callback, and exchanges that code for a Locks-local frontend session token. Authenticated Pubky-backed creator publishing routes consume those Locks-local frontend session tokens. ADR 0019 chooses a Lock-Server-hosted redirect/popup page as the human-facing legacy-connect shell. pubky.app-native QR/deeplink rendering is not allowed for legacy-connect because the legacy Pubky authorization URL is secret-bearing and must stay on the Lock Server origin.

### `GET /connect`

Starts the human-facing Lock-Server-hosted legacy-connect shell.

```http
GET /connect?return_to=https%3A%2F%2Fpubky.app%2Flocks%2Fcallback&state=opaque-state
```

`return_to` must be a full `http`/`https` URL whose origin matches `[creator_authority_acquisition].allowed_return_origins`, unless the operator explicitly configured wildcard mode with `allowed_return_origins = ["*"]`. The Lock Server validates this before starting the Pubky auth flow.

Success returns `200 text/html; charset=utf-8`. The HTML intentionally contains the secret-bearing Pubky authorization QR SVG, deeplink, and raw fallback text, but only on the Lock Server origin.

The HTML form posts back to:

```http
POST /connect/{flow_id}/complete
```

### `POST /connect/{flow_id}/complete`

Completes the pending Lock-Server-owned legacy auth flow, stores creator authority, issues a one-time frontend-session code, revalidates the stored `return_to`, and redirects back to pubky.app:

```http
303 See Other
Location: https://pubky.app/locks/callback?state=opaque-state&code=<one-time-code>
```

The callback URL carries only `state` and `code`. It must not contain `authorization_url`, `pubkyauth`, creator-authority session material, or frontend session tokens. pubky.app then calls `POST /frontend-sessions` with the code and state.

### `POST /frontend-sessions`

Exchanges a one-time code plus original state for a Locks-local frontend session token.

#### Request

```json
{
  "code": "<one-time-code>",
  "state": "opaque-state"
}
```

#### Success response

```json
{
  "session_token": "<locks-local-session-token>",
  "creator": "pubkycreator123",
  "expires_at": "2026-06-17T13:00:00Z"
}
```

The raw `session_token` is returned exactly once. Frontend session tokens authenticate only future `pubky.app/browser -> Lock Server` APIs; they are not Pubky homeserver sessions and must not be confused with Locks-issued reader access credentials.

### `DELETE /frontend-sessions/current`

Revokes the current Locks-local frontend session token.

```http
DELETE /frontend-sessions/current
Authorization: Bearer <frontend_session_token>
```

Success returns `204 No Content`. The request token is deleted from the frontend session store; subsequent authenticated creator requests with that token return `401 frontend_session_unavailable`. The response body is empty and must not echo the raw token.

## Creator authority status route

### `GET /creator/authority-status`

Returns secret-free status for the Lock Server's creator-granted homeserver authority, derived from the authenticated Locks-local frontend session.

Request:

```http
GET /creator/authority-status
Authorization: Bearer <frontend_session_token>
```

No request body, no query-string creator, and no query-string token are accepted. The creator identity is derived from the frontend session token.

Missing authority response:

```json
{
  "creator": "pubkycreator123",
  "authorized": false,
  "auth_kind": null,
  "granted_scopes": [],
  "session_expires_at": null
}
```

Authorized response:

```json
{
  "creator": "pubkycreator123",
  "authorized": true,
  "auth_kind": "legacy_cookie",
  "granted_scopes": ["/pub/locks.app/:rw", "/priv/locks.app/:rw"],
  "session_expires_at": null
}
```

The response must not include creator authority secrets, frontend session tokens, one-time codes, authorization URLs, or database configuration. This route is a read model only; it does not revalidate the underlying Pubky session. Pubky I/O paths still revalidate before writes/reads.

Error cases:

- Missing/malformed frontend session bearer: `401 frontend_session_unavailable`.
- Expired frontend session: `401 frontend_session_expired`.

## Creator publishing routes

Creator publishing routes always use Pubky homeserver-backed repositories. Callers must pass `Authorization: Bearer <frontend_session_token>`. Handlers derive creator from the Locks-local frontend session and reject request-body `creator`; frontend session tokens must not be supplied in query strings, request bodies, or cookies.

### `PUT /creator/priv-resources/content/<path>`

Response-shape fixture: `locks-server/tests/fixtures/creator_publishing/register_guarded_resource_response_shape.json`

Registers or replaces the current guarded resource bytes for the authenticated creator and decoded relative content path. The server reconstructs the canonical private guarded resource path as `/priv/locks.app/content/<path>`. With Pubky-backed repositories, it writes bytes to the creator homeserver under that path.

#### Request

```http
PUT /creator/priv-resources/content/example.txt
Authorization: Bearer <frontend_session_token>
Content-Type: text/plain

guarded bytes
```

The request body is raw resource bytes, not JSON. `Content-Type` is required and becomes the guarded resource MIME type returned during proxy-read.

Path rules:

- MUST be non-empty after percent-decoding.
- MUST be relative to `/priv/locks.app/content/`; callers supply only the relative path, not the full private path.
- MAY contain nested slash-separated segments.
- MUST NOT start with `/`.
- MUST NOT contain `..` traversal segments, including encoded traversal.
- MUST NOT contain `//` ambiguity.
- MUST NOT be URL-like content.

Upload size is limited by `[content_locks].max_resource_bytes` and defaults to 10,000,000 bytes.

#### Success response

```json
{
  "creator": "pubkycreator123",
  "guarded_resource": {
    "path": "/priv/locks.app/content/example.txt",
    "hash": "<guarded_resource_hash>",
    "content_type": "text/plain",
    "size": 13
  }
}
```

The response is descriptor-only. It does not return raw bytes.

#### Error cases

- Missing, unknown, or expired frontend session: `401 frontend_session_unavailable` or `401 frontend_session_expired`.
- Missing or invalid `Content-Type`: `400 invalid_request`.
- Invalid relative path: `400 invalid_request`.
- Empty body: `400 invalid_request`.
- Body exceeds configured upload limit: `413 payload_too_large`.
- Missing/revoked creator-granted homeserver authority: `503 creator_authority_unavailable`.
- Old `POST /creator/priv-resources` JSON/base64 route: `404 Not Found`.

### `DELETE /creator/priv-resources/content/<path>`

Deletes the current guarded resource bytes for the authenticated creator and decoded relative path. Deleting or replacing a guarded resource path makes existing content locks that reference the previous descriptor unreadable for that path.

```http
DELETE /creator/priv-resources/content/example.txt
Authorization: Bearer <frontend_session_token>
```

Success returns `204 No Content`. Missing resources return `404 guarded_resource_not_found`.

### `POST /creator/content-locks`

Fixtures:

- Request: `locks-server/tests/fixtures/creator_publishing/create_content_lock_request.json`
- Response shape: `locks-server/tests/fixtures/creator_publishing/create_content_lock_response_shape.json`

Creates or replaces a content lock from a resource set. A content lock may contain:

- `primary_resource`: optional full [`GuardedResource`](#success-response) descriptor.
- `secondary_resources`: optional map keyed by full canonical private path. Values contain `hash`, `content_type`, and `size` only.

At least one resource is required. If a primary resource is present, its path must not also appear in `secondary_resources`. `secondary_resources` keys are full canonical private paths such as `/priv/locks.app/content/attachments/a.txt`.

With Pubky-backed repositories, this writes the public content lock JSON to the creator homeserver under its derived `content_lock_path`. Test-support composition may use in-memory repositories behind the same authenticated route contract.

Every referenced guarded resource must currently exist for the same creator/path and must match hash, content type, and size. If the creator has overwritten or deleted a guarded resource path, content lock creation rejects the stale descriptor.

`paykit-payment` is the only active payment verifier wire value. Its v1 criterion params are exactly:

```json
{
  "recipient_pubky": "pubky<recipient>",
  "amount": "50000",
  "asset": "BTC",
  "payment_in": 24
}
```

`recipient_pubky` must be a valid Pubky public key string equal to the content-lock creator, `amount` must be a positive base-unit integer encoded as a string, `asset` must be a non-empty string, and `payment_in` must be a positive whole-hour JSON `u64`. The lock params do not include Paykit server URLs, account IDs, memos, expiry, payment references, or reader identity. A v1 content lock that uses `paykit-payment` must contain exactly that one criterion, and its `all` or `any` lock logic must reference that criterion exactly once. Mixed criteria, multiple payment criteria, recipient/creator mismatch, and duplicate or mismatched logic references return `400 invalid_request`.

#### Request

```json
{
  "primary_resource": {
    "path": "/priv/locks.app/content/post.json",
    "hash": "<primary_hash>",
    "content_type": "application/json",
    "size": 123
  },
  "secondary_resources": {
    "/priv/locks.app/content/attachments/a.txt": {
      "hash": "<attachment_hash>",
      "content_type": "text/plain",
      "size": 13
    }
  },
  "criteria": [
    {
      "criterion_id": "criterion-1",
      "verifier_type": "dev-static",
      "params": { "satisfied": true }
    }
  ],
  "lock_logic": {
    "type": "all",
    "criteria": ["criterion-1"]
  },
  "access_policy": {
    "requested_credential_ttl_seconds": 900
  },
  "lock_server": {
    "override": "pubkyserver123"
  }
}
```

#### Success response

```json
{
  "lock_id": "<lock_id>",
  "content_lock_path": "/pub/locks.app/<lock_id>.json",
  "content_lock": {
    "version": 1,
    "creator": "pubkycreator123",
    "primary_resource": {
      "path": "/priv/locks.app/content/post.json",
      "hash": "<primary_hash>",
      "content_type": "application/json",
      "size": 123
    },
    "secondary_resources": {
      "/priv/locks.app/content/attachments/a.txt": {
        "hash": "<attachment_hash>",
        "content_type": "text/plain",
        "size": 13
      }
    },
    "criteria": [
      {
        "criterion_id": "criterion-1",
        "verifier_type": "dev-static",
        "params": { "satisfied": true }
      }
    ],
    "lock_logic": {
      "type": "all",
      "criteria": ["criterion-1"]
    },
    "access_policy": {
      "requested_credential_ttl_seconds": 900
    },
    "lock_server": {
      "override": "pubkyserver123"
    },
    "created_at": "2026-06-03T12:00:00Z"
  }
}
```

The response does not include raw guarded resource bytes.

#### Error cases

- Invalid JSON shape: `400 invalid_request`.
- Authenticated request includes body `creator`: `400 invalid_request`.
- Empty resource set: `400 invalid_request`.
- Too many resources or aggregate descriptor size over `[content_locks]` limits: `400 invalid_request`.
- Guarded resource descriptor is malformed: `400 invalid_request`.
- Guarded resource descriptor exists but is stale/mismatched: `400 invalid_request`.
- Guarded resource is missing for creator/path: `404 guarded_resource_not_found`.

### `POST /creator/lock-service-config`

Fixtures:

- Request: `locks-server/tests/fixtures/creator_publishing/set_lock_service_config_request.json`
- Response shape: `locks-server/tests/fixtures/creator_publishing/set_lock_service_config_response_shape.json`

Stores or replaces the creator's default Lock Service Pointer for the canonical Pubky path `/pub/locks.app/config.json`.

With Pubky-backed repositories, this writes the pointer JSON to the creator homeserver at `/pub/locks.app/config.json`. Test-support composition may use in-memory repositories behind the same authenticated route contract.

Content lock creation does not require a Lock Service Pointer. A content lock may carry `lock_server.override`; future viewer discovery can use the pointer when no override exists.

#### Request

```json
{
  "default_lock_server": "pubkyserver123"
}
```

#### Success response

```json
{
  "creator": "pubkycreator123",
  "path": "/pub/locks.app/config.json",
  "lock_service_pointer": {
    "version": 1,
    "default_lock_server": "pubkyserver123",
    "created_at": "2026-06-03T00:00:00Z"
  }
}
```

#### Error cases

- Invalid JSON shape: `400 invalid_request`.
- Authenticated request includes body `creator`: `400 invalid_request`.
- Invalid `default_lock_server`: `400 invalid_request`.

## Viewer/access routes

### `POST /proof-bundles`

Submits a viewer proof bundle for asynchronous verification.

Fixtures:

- Request: `locks-server/tests/fixtures/viewer_access/submit_proof_bundle_request.json`
- Response shape: `locks-server/tests/fixtures/viewer_access/submit_proof_bundle_response_shape.json`

Request envelope:

```json
{
  "submitted_proof_bundle": {
    "version": 1,
    "bundle_id": "<bundle_id>",
    "pubky_lock_resource": "pubky<creator>/pub/locks.app/<lock_id>.json",
    "reader_public_key": "pubky<reader>",
    "proofs": [
      {
        "criterion_id": "criterion-1",
        "verifier_type": "paykit-payment",
        "payload": {}
      }
    ]
  }
}
```

Success response returns lifecycle metadata only. It does not return internal `task_id`, raw proof material, entitlement evidence, or access credentials.

For non-payment verifier types, `reader_public_key` may be omitted. For `paykit-payment`, `reader_public_key` is required as a top-level field on `submitted_proof_bundle`; the payment proof payload itself must be `{}`. Payment submissions are v1 single-proof only: a bundle with more than one `paykit-payment` proof, or a mix of `paykit-payment` and any other proof type, is rejected with `400 invalid_request`.

Submission processing applies rate limiting, validates proof shape, loads the current canonical content lock referenced by `pubky_lock_resource`, verifies its lock identity and payment policy (including recipient/creator equality), and resolves `reader_public_key` through Pubky/PKARR/homeserver discovery. It then checks the permanent lifecycle identity `{ creator, bundle_id }`. An exact persisted replay returns the existing lifecycle; changed submitted proof material returns `409 task_state_conflict`. Neither case calls Paykit again. Only a new identity requires `[paykit]` configuration and calls `POST /invoices`. Task insertion retains race reconciliation after invoice creation. The signed Paykit invoice body is exactly:

```json
{
  "bundle_id": "<bundle_id>",
  "lock_resource": "pubky<creator>/pub/locks.app/<lock_id>.json",
  "reader": "pubky<reader>"
}
```

Any 2xx Paykit invoice response is accepted and its body is ignored. Paykit invoice `409 Conflict` maps to `409 task_state_conflict`. Other invoice failures return `502 paykit_invoice_creation_failed`; no verification task is created unless invoice creation was accepted.

Paykit status verification is worker-owned. The Lock Server sends canonical JSON `{ "creator": "pubky...", "bundle_id": "..." }` to `POST /transactions/status` with `X-Paykit-Signature` over those exact canonical body bytes. Valid response statuses are `undetected`, `detected`, and `confirmed`. Transport failures, timeouts, every non-2xx response (including `404` and authentication/authorization failures), and malformed success bodies are durably rescheduled as pending and are not retried again before the worker poll interval elapses. V1 has no terminal Paykit payment-failure status.

Rate limiting, when enabled, returns `429 rate_limited` with the stable error envelope.

### `POST /verification-task-lookups`

Looks up lifecycle metadata by public handle `{ creator, bundle_id }` using a JSON body. Bundle ID is bearer-secret-like, so it is not placed in URL paths or query strings.

Fixture: `locks-server/tests/fixtures/viewer_access/verification_task_handle_request.json`

### `POST /verification-task-completions`

Dev/test-only route that completes a verification task by public handle. It is not mounted in production mode and is separate from normal worker-owned completion.

Request fixture: `locks-server/tests/fixtures/viewer_access/verification_task_handle_request.json`

### `POST /access-credentials`

Issues a raw opaque bearer access credential after a verified entitlement exists.

Fixtures:

- Request: `locks-server/tests/fixtures/viewer_access/verification_task_handle_request.json`
- Response shape: `locks-server/tests/fixtures/viewer_access/access_credential_response_shape.json`

Request:

```json
{
  "creator": "pubkycreator123",
  "bundle_id": "<bundle_id>"
}
```

Response includes the raw credential exactly once. Polling routes never return credentials.

### `GET /priv-resources/content/<path>`

Proxy-reads one guarded resource from the content lock authorized by a bearer credential. The `<path>` segment is the same relative path used for upload; the server reconstructs `/priv/locks.app/content/<path>` and verifies that path is in the credential's content lock resource set before reading bytes.

Credentials are accepted only through:

```http
Authorization: Bearer <credential>
```

Successful response:

- status `200 OK`
- `Content-Type` is the locked resource descriptor `content_type`
- `Content-Length` is the returned byte length
- `ETag` is the verified resource hash quoted as an HTTP entity tag
- body is raw guarded resource bytes
- no JSON/base64 wrapping

Missing or malformed bearer credentials return `401 invalid_access_credential`. Unknown paths, deleted resources, or stale/reuploaded resources return opaque `404 guarded_resource_not_found`.

The old no-path `GET /priv-resources` route is removed and returns plain `404 Not Found` because it is not mounted.
