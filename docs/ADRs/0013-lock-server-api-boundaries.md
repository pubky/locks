# ADR 0013: Lock Server API Boundary Keeps Credential Issuance Explicit

## Status

Accepted

## Context

The first retrieval/access vertical slice now proves the use-case flow in memory: submit proof, poll a verification task, complete verification through `dev-static`, persist entitlement evidence, issue and validate an access credential, and proxy-read guarded content.

The next likely implementation phase is a runnable Lock Server skeleton around those use cases. Route shape will quickly become client-facing product surface, even if the first skeleton remains dev-only and in-memory.

The HTTP/runtime surface should not turn `locks-service` into a catch-all crate. `locks-service` is currently the application/service layer: use cases, ports, application models, in-memory adapters, and verifier adapters. HTTP routing, request/response DTOs, status-code mapping, process config, tracing, and bind/serve lifecycle are runtime concerns. Runtime config for `locks-server` should use a config file from the start, aligned with other Pubky stack services such as homeserver and nexus. The default service home is `~/.pubky-lock`, with config at `~/.pubky-lock/config.toml` and Lock Server secret at `~/.pubky-lock/secret.sess`. If no `--config` path is provided and the default config is missing, initialize the service home, write a complete valid config, and create a secret whose derived public key is stored as `credentials.lock_server_public_key`. If `--config <path>` is provided, the file must already exist and must not be auto-generated. If any config already exists, the application must not rewrite or repair it: all values must be valid, the configured secret must exist, and `credentials.lock_server_public_key` must match the secret-derived public key. Placeholder public-key values are only for checked-in examples. Config paths support only leading `~/` expansion to the current user's home directory; relative paths resolve relative to the config file directory; do not support `~user`, environment-variable expansion, or shell interpolation.

The current in-memory skeleton uses a local filesystem identity provider that writes a `.sess`-shaped secret and validates the public-key/secret relationship without performing Pubky signup/signin. Strict `pubky::PubkySession::write_secret_file` integration remains a production Pubky adapter concern because the SDK method is available on an established `PubkySession`; obtaining that session requires homeserver/signup/signin configuration that is still outside this skeleton.

Access credentials are raw bearer secrets. Task polling is a read-oriented client workflow and may be retried, cached, logged, or observed more broadly than credential issuance. Worker completion is an internal mutation that persists entitlement evidence. Mixing these concerns would make it unclear whether a completed task means verified evidence, a minted credential, or a credential safely delivered to the viewer.

## Decision

The Lock Server API boundary mirrors the application use-case separation:

HTTP/runtime composition belongs in a separate `locks-server` crate:

```text
locks-core     = protocol/domain payloads and pure rules
locks-service  = application use cases, ports, in-memory adapters, verifier adapters
locks-server   = HTTP API, route DTOs, error mapping, runtime config, process entrypoint
locks-e2e      = cross-crate/system E2E tests for product flows
```

`locks-server` depends on `locks-service` and `locks-core`; `locks-service` must not depend on `axum`, `tower`, HTTP status codes, route DTOs, or server process lifecycle.

Root-level/system E2E tests belong in a dedicated workspace crate such as `locks-e2e`, not in a bare root `tests/` directory. The workspace root is a virtual Cargo workspace, so root `tests/` would not be a normal integration-test target unless the root also became a package. Keep ordinary use-case composition tests in `locks-service`, route/API tests in `locks-server`, and reserve `locks-e2e` for product flows that cross crate or process boundaries. Do not create `locks-e2e` as an empty placeholder; add it when the first concrete E2E test is introduced.

The API flow remains:

```text
Submit proof -> task
Complete verification -> entitlement
Issue credential -> temporary bearer access
Proxy-read -> consume bearer access
```

Credential issuance remains an explicit mutation. Neither read-only polling nor worker-owned completion returns a raw access credential.

Recommended initial HTTP/resource shape for the in-memory skeleton uses JSON error envelopes for all non-2xx responses. `docs/API.md` is the living HTTP route contract reference; this ADR records the API boundary decisions and rationale:

```json
{
  "error": {
    "code": "missing_record",
    "message": "verification task not found"
  }
}
```

`error.code` is stable and machine-readable. `error.message` is human-readable and not a compatibility contract. Error responses must not expose stack traces, internal Rust enum formatting, or bearer credential values.

Initial status/code mapping:

| Condition | Status | Error code |
|---|---:|---|
| malformed JSON/body | `400` | `invalid_request` |
| invalid path/body ID format | `400` | `invalid_identifier` |
| missing verification task | `404` | `verification_task_not_found` |
| missing guarded resource | `404` | `guarded_resource_not_found` |
| missing content lock during verification/validation | `404` | `content_lock_not_found` |
| invalid access credential | `401` | `invalid_access_credential` |
| expired access credential | `401` | `expired_access_credential` |
| entitlement missing/revoked/unsatisfied | `403` | `entitlement_not_authorized` |
| unsupported verifier type | `422` | `unsupported_verifier_type` |
| task transition conflict | `409` | `task_state_conflict` |
| internal storage/verifier/server error | `500` | `internal_error` |

Recommended initial HTTP/resource shape:

```text
POST /proof-bundles
  -> SubmitProofBundleUseCase
  -> body { submitted_proof_bundle }
  -> creates or finds the task for { creator, bundle_id }
  -> returns public lifecycle metadata with creator and Bundle ID, but no task_id or submitted proof material

POST /verification-task-lookups
  -> GetVerificationTaskByHandleUseCase
  -> body identifies public verification attempt handle: { creator, bundle_id }
  -> read-only status/timestamp/failure polling
  -> returns the same public lifecycle metadata as submission

POST /verification-task-completions
  -> CompleteVerificationTaskUseCase
  -> dev/internal worker trigger for the skeleton
  -> mounted only in dev mode with expose_dev_completion_route = true
  -> body identifies public verification attempt handle: { creator, bundle_id }

POST /access-credentials
  -> IssueAccessCredentialUseCase
  -> body identifies durable entitlement identity: { creator, bundle_id }
  -> returns { credential, expires_at }

GET /priv-resources
  -> ProxyReadGuardedResourceUseCase
  -> Authorization: Bearer <credential>
  -> returns raw guarded resource bytes with stored guarded resource Content-Type

POST /creator/priv-resources
  -> RegisterGuardedResourceUseCase
  -> dev/test-gated local creator publishing route
  -> body includes creator, guarded resource path, MIME content type, and base64 bytes
  -> computes hash and positive size; stores/replaces current local guarded resource for creator/path

POST /creator/content-locks
  -> CreateContentLockUseCase
  -> dev/test-gated local creator publishing route
  -> body includes creator, guarded resource descriptor, criteria, lock logic, access policy, and lock server config
  -> verifies current local guarded resource descriptor before storing derived content lock

POST /creator/lock-service-config
  -> SetLockServicePointerUseCase
  -> dev/test-gated local creator publishing route
  -> body includes creator and default Lock Server identity
  -> stores/replaces local creator config whose canonical future Pubky path is /pub/locks.app/config.json

GET /healthz
  -> runtime liveness only
  -> returns { "status": "ok" }

GET /readyz
  -> runtime dependency readiness only
  -> returns { "status": "ready"|"not_ready", "runtime_storage": "ephemeral"|"persisted" }
```

`POST /proof-bundles` returns lifecycle metadata for the public verification attempt handle: `creator`, `bundle_id`, `status`, `submitted_at`, `started_at`, `completed_at`, and `failure_message`. It does not return `task_id`, `pubky_lock_resource`, or the submitted proof bundle. Rationale: `{ creator, bundle_id }` is the durable viewer-held handle for the verification attempt, while Task ID is only internal runtime coordination state.

HTTP DTOs use a mixed approach. `locks-core` protocol/domain payloads that are already explicit versioned wire contracts, such as `SubmittedProofBundle`, may be reused inside HTTP envelopes. Route envelopes, status responses, credential responses, and errors get separate `locks-server` DTOs so HTTP shape can evolve without exposing application model internals just because they currently serialize conveniently.

`POST /verification-task-lookups` returns lifecycle metadata only. It does not include raw credentials, submitted proof material, entitlement evidence, or worker claim metadata. Rationale: polling is task-state inspection, not credential issuance. It uses a JSON body instead of a path/query handle so the bearer-secret-like Bundle ID is not repeatedly placed in URLs.

Credential issuance via `POST /access-credentials` accepts only `{ creator, bundle_id }` for now. It does not accept `{ task_id }`, and the skeleton should not support both shapes. Rationale: creator plus Bundle ID is the durable entitlement identity and survives task-state cleanup, credible exit, and future Lock Server migration. Task ID is internal runtime/correlation state, not access recovery state.

The proxy-read response returns raw bytes, not JSON/base64. Successful proxy-read uses the guarded resource metadata's stored MIME `content_type` as the HTTP `Content-Type` response header and the guarded resource bytes as the response body. Do not add `Content-Disposition`, filename, or rich metadata yet. Future metadata should be expressed as HTTP headers where possible; do not wrap successful byte responses in JSON just to carry metadata.

Creator publishing routes are local dev/test product surface, not production Pubky publishing. `POST /creator/priv-resources`, `POST /creator/content-locks`, and `POST /creator/lock-service-config` are unauthenticated while local because real creator authorization depends on Pubky capability/session work. They must be mounted only when an explicit creator-publishing config flag is enabled in dev/test runtime, and production runtime must reject configs that expose them. Do not add fake shared-secret/admin-token auth.

`POST /creator/priv-resources` overwrites the current local guarded resource by creator/path. `POST /creator/content-locks` verifies that the referenced guarded resource currently exists and matches path, hash, content type, and size before creating a content lock. `POST /creator/lock-service-config` stores the creator's default Lock Service Pointer for `/pub/locks.app/config.json`, but content lock creation does not require it. These semantics exercise local creator-to-viewer flow without adding fake Pubky writes.

Proxy-read accepts credentials only through `Authorization: Bearer <credential>`. The `Bearer` scheme is case-insensitive, but parsing is otherwise strict: exactly one `Authorization` header, exactly one non-empty token after the scheme, no query-string credential, and no body credential. Missing or malformed bearer credentials map to `401 invalid_access_credential`.

Health/readiness endpoints are operational routes, not product/domain use cases. `GET /healthz` only reports process liveness. `GET /readyz` reports runtime dependency readiness: `ephemeral` readiness currently means the in-process in-memory runtime is composed, while `persisted` readiness currently means the Postgres-backed runtime can ping its configured pool. These endpoints must remain secret-free and must not include database URLs, secret paths, Lock Server identities, worker IDs, task counts, Task IDs, claim metadata, submitted proof material, credentials, or rate-limit counters.

Route and E2E tests may still use test-support helpers for setup that is not under test. Creator publishing is now explicit local product surface through dev/test-gated HTTP routes; fake seed endpoints remain forbidden. The normal `locks-server` binary starts with empty in-memory repositories by default; do not hide demo content locks or guarded resources in local startup. `locks-server` exposes test helpers through a gated test-support module:

```rust
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
```

`locks-e2e` depends on `locks-server` with `features = ["test-support"]` and uses `locks_server::testing::TestServerApp`. This avoids duplicating app wiring in E2E tests without making runtime internals broadly public.

The completion endpoint is a skeleton/dev convenience, not the long-term production scheduling model. It is allowed only for tests/local dev routing so tests and local demos can trigger worker-owned completion when explicitly configured. Implementation comments and route documentation must mark it as dev/internal.

The current productionization path is the worker model: production routing does not expose `POST /verification-task-completions`; pending tasks are completed by the in-process worker loop. The route is mounted only in `mode = "dev"` when `expose_dev_completion_route = true`. Future production alternatives may still include an authenticated internal control-plane route, but not an unguarded public route.

Do not add fake production auth such as hardcoded bearer tokens or `X-Internal-Secret: dev`. That creates accidental production surface without solving the scheduling/security model.

No task-id-based credential convenience endpoint is planned for the public skeleton. If a future internal/admin control plane needs task correlation, it should remain authenticated/internal and delegate to the durable `{ creator, bundle_id }` entitlement path rather than introducing a second viewer-facing handle.

## Rationale

Keeping credential issuance separate preserves clear semantics:

- `GetVerificationTaskByHandleUseCase` answers “what happened to my verification attempt?”
- `CompleteVerificationTaskUseCase` answers “has entitlement evidence been persisted?”
- `IssueAccessCredentialUseCase` answers “should this viewer receive a short-lived bearer credential now?”
- `ProxyReadGuardedResourceUseCase` answers “does this bearer credential currently authorize reading guarded bytes?”

This avoids:

- task polling with hidden mutations
- repeatedly exposing bearer credentials through polling responses
- accidental credential generation by internal workers
- lost-credential states where a raw bearer token is generated and stored by lookup key but the response never reaches the viewer
- ambiguity around `VerificationTaskStatus::Completed`

## Consequences

Positive:

- Polling remains safe, read-only, and secret-free.
- Credential minting is auditable and explicit.
- Viewers can request a fresh credential later from the same durable entitlement without re-running verification.
- Production worker orchestration can evolve without changing the client credential boundary.
- The API surface matches existing DDD/use-case boundaries.
- HTTP/server dependencies remain out of `locks-service`, preserving it as a reusable application/service crate.
- `locks-server` can evolve runtime config, observability, and route DTOs without polluting application use cases.
- `locks-e2e` can own slower product/system tests without bloating route modules or turning the virtual workspace root into a package.

Negative:

- Clients perform one extra request after seeing task completion.
- Clients must retain or recover creator and Bundle ID for credential issuance.
- A task-based credential convenience endpoint may still be useful later for ergonomics.
- The workspace gets one more crate now, but this is cheaper than splitting HTTP/runtime concerns out after routes and tests accumulate in `locks-service`.
- Adding `locks-e2e` introduces another test crate when cross-crate/system tests appear, but keeps crate-local tests fast and localized.

## Out of Scope

- Final production route stability.
- Real Pubky I/O and homeserver adapter semantics.
- Authentication/authorization of dev/internal completion trigger.
- Streaming/range semantics for guarded resource responses.
- SDK/admin/UI route wrappers.
