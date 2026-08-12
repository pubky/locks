# Locks Domain Model

This document describes the current domain model for Pubky Locks. It focuses on high-level information flow, Pubky-owned resource semantics, and runtime/application boundaries.

## Product Intent

Locks is a content-gating application for the Pubky ecosystem. A content creator publishes public content locks describing access criteria for guarded resources. A content viewer submits proof material to a Lock Server. The Lock Server verifies the proof asynchronously, stores a creator-owned entitlement record after success, issues a reusable-until-expiry opaque access credential, and proxy-reads guarded content for the viewer.

Locks is application-layer logic. Homeservers should not need to understand payment logic, subscription logic, passwords, follower checks, or other lock-type-specific verification.

## Architectural Stance

- Stay lock-type agnostic for now.
- Keep Pubky-Core integration behind ports. Pubky-backed repository adapters exist. Plan 0018 wires real SDK-backed server-binary runtime composition through infrastructure-only storage/session seams while keeping domain and application ports unchanged.
- Treat the Lock Server as a trusted proxy with explicit creator-granted authority; see [ADR 0017](ADRs/0017-creator-granted-auth-boundary.md).
- Distinguish durable entitlement records from short-lived access credentials.
- Preserve credible exit by storing successful entitlement records on the creator's homeserver, not only in Lock Server private storage.

## Repository and Crate Boundaries

Locks should be implemented as a Cargo workspace with top-level member folders.

Implemented workspace members:

- `locks-core`: shared protocol/domain payloads, value objects, validation, canonical JSON, hash derivation, and content lock path rules.
- `locks-service`: Lock Server application orchestration, ports, in-memory adapters, Pubky homeserver adapters, and dev-only verifiers. It uses a layer-first hexagonal structure, split by use case inside `application/` and by adapter type inside `infrastructure/`.
- `locks-server`: HTTP API, route DTOs, error mapping, runtime configuration, observability, process entrypoint, and test-support helpers. It depends on `locks-service` and `locks-core`; `locks-service` must not depend on HTTP/server framework concerns.
- `locks-sdk`: Rust SDK core and canonical request planner for client/browser-facing flows. JS/WASM bindings wrap this crate rather than duplicating request shapes.
- `locks-e2e`: cross-crate/system E2E tests for product flows. Use this instead of a bare root `tests/` directory while the workspace root is virtual.

Future candidate workspace members:

- `locks-admin`: operator CLI for Lock Server administration and diagnostics.
- `creator-ui`: slim web-based creator UI that uses the SDK and/or Lock Service API.

Dependency direction:

```text
locks-service -> locks-core
locks-server  -> locks-service, locks-core
locks-e2e     -> locks-server, locks-service, locks-core
locks-sdk     -> locks-core
locks-admin   -> locks-core and/or service API client, TBD
creator-ui    -> locks-sdk and/or Lock Service API, TBD
```

`locks-core` must not contain service runtime state, repositories, application ports, verifier traits, fake/dev verifier implementations, in-memory stores, HTTP routes, Pubky client code, database dependencies, HTTP clients, or async orchestration.

## Runtime Persistence Boundary

Postgres is the finalized persistence and worker-coordination substrate for Lock Server private runtime state. It stores verification tasks, verification task claims/leases, attempt metadata when needed, access credential lookup records, and creator-granted session material. SQLite is intentionally rejected for this role because its single-writer behavior is a poor fit for API/worker IPC and future concurrent worker execution.

Creator-granted session material is secret-bearing runtime state. It must be stored encrypted at rest in private Postgres tables using a server-side key supplied through environment/config secret. The implemented Postgres creator authority adapter supports encrypted persistence with a 32-byte key and stores only an AEAD envelope in the `creator_authorities.secret` column. Session material must never be stored in Pubky-owned resources, committed config examples, logs, readiness responses, debug formatting, error envelopes, or viewer-facing DTOs.

Postgres must not become the canonical store for Pubky-owned domain resources in the next runtime phase. Content locks, guarded resources, Lock Service Pointers, and entitlement records / verified proof bundles remain behind their existing ports. Local/dev runtime composition uses in-memory adapters; Pubky-backed adapters are implemented behind the same ports for public Locks resources and private `/priv/locks.app/` resources. Do not add `dev_*` Postgres tables for those resources.

Migrations are part of the runtime boundary from the first Postgres table. Schema creation must be managed by migrations rather than ad hoc test/startup SQL.

Current runtime composition uses Postgres-backed adapters for `VerificationTaskRepository`, `VerificationTaskClaimer`, `AccessCredentialStore`, creator-granted authority, creator connect flows, one-time frontend session codes, and frontend sessions. `ContentLockRepository`, `GuardedResourceRepository`, `LockServicePointerRepository`, and `EntitlementRepository` are Pubky homeserver-backed in server runtime. Server-binary composition restores creator-scoped SDK sessions from encrypted Postgres creator-authority records, validates restored-session identity against the requested `CreatorPubky`, and uses session-scoped Pubky storage behind the existing repository ports. Runtime startup resolves a configured Postgres URL, optionally runs migrations, and composes the process from those adapters.

Completion is worker-owned in production-shaped runtime. The server runs an in-process verification worker; the Postgres claim/lease boundary prepares for a later long-lived worker process once operational needs justify splitting it out. Avoid per-request child-process verification by default; use subprocess isolation only for verifier-specific sandboxing or resource isolation if that requirement emerges.

The dev HTTP completion route, `POST /verification-task-completions`, is not a production route. It accepts `{ creator, bundle_id }`, resolves the internal task, and is mounted only when `runtime.environment = "development"`; `staging` and `production` never mount it.

Creator publishing and deletion routes are authenticated and Pubky homeserver-backed. `PUT /creator/priv-resources/content/<path>`, `DELETE /creator/priv-resources/content/<path>`, `POST /creator/content-locks`, `DELETE /creator/content-locks/{lock_id}`, `GET /creator/content-locks/{lock_id}/deletion`, and `POST /creator/lock-service-config` require `Authorization: Bearer <frontend_session_token>` and derive creator from the Locks-local frontend session. The raw guarded-resource upload body is bytes, not JSON/base64. Deletion validates that any fetched Content Lock hashes to the requested Lock ID and belongs to that authenticated creator before freezing or deleting its manifest. Graceful-job creation/resume and permanent force-receipt creation share one canonical per-lock PostgreSQL fence, so active graceful work and a force receipt cannot coexist. Failed graceful replay requeues the same frozen job; synchronous force replaces terminal operational job state with the permanent receipt. Force against an active graceful job remains a durable worker escalation.

Runtime health and readiness are Lock Server operator concerns. `GET /healthz` reports process liveness once the HTTP router is serving. `GET /readyz` reports whether runtime dependencies are usable: `ephemeral` readiness currently means in-memory process composition, while `persisted` readiness currently means the Postgres-backed runtime can ping its configured pool. Health/readiness responses must be small and secret-free; they must not expose database URLs, secret paths, Lock Server identities, worker IDs, task counts, Task IDs, claim metadata, submitted proof material, access credentials, or rate-limit counters.

## Test Placement

- `locks-core`: protocol/domain unit tests and serialization/JSON-shape tests.
- `locks-service`: application use-case tests, in-memory adapter tests, and direct use-case composition flows such as the current retrieval/access slice.
- `locks-server`: HTTP route, DTO, status-code mapping, and server composition tests.
- `locks-e2e`: product/system flows that cross crate or process boundaries. Later Pubky-backed E2E tests should be gated explicitly when they require external services.

Do not use a bare root `tests/` directory for Rust E2E tests while the repository root remains a virtual Cargo workspace.

## Protocol Payload Rules

`locks-core` owns JSON serialization for shared protocol/domain payloads. These payloads are the v0 JSON contract source of truth.

Rules:

- Use explicit serde naming policy for protocol/domain payloads, with snake_case field names.
- Reject unknown JSON fields in v0.
- Include `version` on top-level persisted/protocol payloads.
- Do not include `version` on nested objects.
- Require JSON-shape tests so Rust field renames do not accidentally change the wire format.
- Keep `locks-core` dependencies minimal: protocol/domain dependencies such as `serde`, `serde_json`, `serde_json_canonicalizer`, `thiserror`, `blake3`, `base32`, `uuid`, and `time` are acceptable; runtime dependencies such as `tokio`, web frameworks, Pubky clients, database crates, and HTTP clients are not.

## Implementation Contracts

### LockId

- Derived from the full 32-byte BLAKE3 hash of the canonical content lock payload.
- Encoded with the Rust `base32` crate using Crockford base32.
- Fixed length: 52 characters.
- No prefix.
- No Crockford checksum.
- Canonical form is the uppercase string produced by `base32::encode` with the Crockford alphabet.
- Parsing uses `base32::decode` with the Crockford alphabet; lowercase and ambiguous Crockford characters are accepted/normalized according to that crate's decoder rather than custom logic in Locks.
- Hyphens/readability separators are rejected.
- Serialization and display emit canonical uppercase form.

### BundleId

- 128-bit cryptographically random viewer-generated bearer secret.
- Encoded with the Rust `base32` crate using Crockford base32.
- Fixed length: 26 characters.
- No prefix.
- No Crockford checksum.
- Canonical form is the uppercase string produced by `base32::encode` with the Crockford alphabet.
- Parsing uses `base32::decode` with the Crockford alphabet; lowercase and ambiguous Crockford characters are accepted/normalized according to that crate's decoder rather than custom logic in Locks.
- Hyphens/readability separators are rejected.
- Serialization and display emit canonical uppercase form.
- Must be safe as a path segment after validation.

### TaskId

- Server-generated UUID v4.
- Canonical lowercase hyphenated UUID string.
- Operational identifier only.
- Not a bearer secret.
- Not a durable recovery handle.
- Not used for credible exit.
- Identifies short-lived verification task state retained for a few hours.

### CreatorPubky and LockServerPubky

- Domain role wrappers around Pubky public-key identities.
- Parsing delegates validation to `pubky::PublicKey`.
- Display and serialization store the canonical `pubky<z32>` rendering.
- Bare z32 public keys may be accepted by the underlying Pubky parser, but Locks emits the `pubky`-prefixed form.

### ContentLockPath

- Relative creator-homeserver path only.
- Must be exactly `/pub/locks.app/<lock_id>.json`.
- Not a full Pubky URL.
- Not a homeserver URL.
- Not any other `/pub/...` path.
- Embedded `<lock_id>` must parse as a valid `LockId`.
- Display and serialization normalize the embedded Lock ID to canonical uppercase form.

### Canonical JSON and timestamps

- Content lock hashing uses RFC 8785/JCS-compatible canonical JSON.
- Rust implementation uses `serde_json_canonicalizer`.
- Hash input is the canonical JSON bytes of the serialized content lock payload.
- Protocol timestamps use `time::OffsetDateTime`.
- Timestamp JSON shape is RFC3339.
- Do not use `chrono` unless revisited by ADR.

## Bounded Context Map

### Lock Policy Context

Owns public lock definitions.

Responsibilities:

- Represent content locks.
- Represent lock criteria without embedding lock-type-specific verification behavior.
- Represent lock logic over criteria.
- Identify a guarded resource set: optional primary resource plus secondary resources keyed by full private path, each described by path/hash or hash/MIME/positive byte size as appropriate.
- Derive the Lock ID from the lock's canonical hash.
- Reference the default Lock Server through creator-owned configuration, with optional per-lock override as a v0 escape hatch.

### Creator Publishing Context

Owns creator-side lock authoring. It is first-class for the current product because Locks needs an operable authoring path, but it must remain extractable: Verification, Entitlement, and Access must not depend on Creator Publishing internals. The HTTP implementation derives the creator from a Locks-local frontend session and writes through Pubky-backed repositories using creator-granted homeserver authority. Local-memory repositories remain internal/test-support adapters, not an unauthenticated HTTP publishing shape.

Responsibilities:

- Register guarded resources through authenticated raw HTTP upload: `PUT /creator/priv-resources/content/<path>`; delete current guarded resource bytes with `DELETE /creator/priv-resources/content/<path>`.
- Store or replace current guarded resource bytes and metadata by creator/path in the selected creator repository backend: Pubky homeserver storage for production/dev integration, with local-memory only in internal/test-support composition. Server-binary Pubky storage is composed from encrypted creator authority and a restored SDK session, not from request-body authority or local-memory fallback.
- Compute guarded resource hash and exact positive byte size from uploaded bytes.
- Validate guarded resource MIME content type through Locks-native spec objects.
- Create content locks only for currently registered guarded resource descriptors; all resources are validated all-or-nothing, at least one resource is required, and primary/secondary paths must not duplicate each other.
- Author the Lock Service Pointer for the creator default Lock Server at canonical path `/pub/locks.app/config.json`.
- Require a Locks-local frontend session and derive creator identity from that session rather than trusting request-body creator fields.
- Refuse creator publishing operations when the Lock Server has no valid creator-granted homeserver authority for the authenticated creator.
- Keep future external-authoring mode possible, where another creator app pre-publishes guarded content and content locks while Locks only verifies, writes proof bundles, and proxy-reads through its creator-granted session.
- Avoid pretending local in-memory registration is a production Pubky homeserver write.

### Verification Context

Owns asynchronous proof processing.

Responsibilities:

- Accept viewer-submitted proof bundles.
- Require viewer-generated Bundle IDs.
- Create and track asynchronous verification tasks.
- Verify proof material through lock-type-specific verifier ports.
- Produce minimal criterion-level verification result evidence with per-criterion verification timestamps.
- Avoid storing failed proof submissions as entitlements.

### Entitlement Context

Owns successful access eligibility records.

Responsibilities:

- Store a verified proof bundle under the creator's guarded Locks path after successful verification.
- Store minimal criterion-level verification result evidence.
- Reference the content lock file using `pubky_lock_resource`; the resource embeds creator and the content lock path, whose Lock ID identifies the content lock version and leads to the guarded resource set when the content lock is read.
- Resolve existing entitlements by Bundle ID for credible exit or later access; the verified proof bundle / entitlement is the durable portable artifact across Lock Servers.
- Honor existing entitlements unless manually revoked by the content creator, expired by entitlement rules, invalidated by content lock removal/change, or blocked by guarded resource integrity failure. Revoking the Lock Server's creator-granted session does not by itself delete or invalidate durable entitlements.

### Access Context

Owns viewer-facing access after eligibility is established.

Responsibilities:

- Issue reusable-until-expiry opaque bearer access credentials.
- Require only the Locks-issued access credential for default viewer proxy-read access.
- Resolve access credentials to creator and Bundle ID, then resolve entitlement-backed access decisions.
- Support future criterion-specific viewer Pubky identity binding without making viewer Pubky identity a global access-route requirement.
- Keep anonymous-compatible bearer access as the default global model; identity-bound criteria are optional per-criterion extensions.
- Proxy-read a requested guarded resource path from the creator homeserver using Lock Server authority, after confirming that path belongs to the authorized content lock resource set.
- If creator-granted authority is missing, expired, revoked, or lacks capability during proxy-read, treat it as creator authority unavailable rather than viewer authorization failure.
- Creator-granted session revocation stops the Lock Server from serving or writing until reauthorized, but it does not itself mean the viewer's entitlement is invalid.
- Existing Lock-Server-local access credential records may remain until their own TTL expires; while creator authority is unavailable they cannot be used to serve bytes, and after reauthorization they may resume working if still unexpired and entitlement/integrity checks pass.
- Before returning proxy-read bytes, read the public content lock, hash the guarded bytes, and verify the guarded resource hash, exact byte size, and MIME content type against the lock's guarded resource descriptor.
- Keep access credentials local to the issuing Lock Server; migrated/new Lock Servers should issue fresh access credentials after resolving the durable creator-owned entitlement.
- Avoid treating access credentials as durable entitlements or credible-exit artifacts.

### Pubky Integration Context

Owns interaction with Pubky infrastructure.

Responsibilities:

- Resolve Pubky resources and Lock Server addresses.
- Use one creator-granted Locks app session per creator per Lock Server, reused across that creator's locks.
- Require creator-granted capability scope for `/pub/locks.app/:rw` and `/priv/locks.app/:rw` in the production Pubky-backed flow.
- Persist native session secrets for the Lock Server; current expected session lifetime is 6 months.
- Own Creator Authority Acquisition: the process by which the Lock Server obtains or refreshes creator-granted homeserver authority for Locks public and private namespaces.
- Treat redirect, popup, iframe, and native pubky.app rendering as presentation shells over the same acquisition state machine, not as domain/application concepts. ADR 0019 chooses Lock-Server-hosted redirect/popup for legacy-connect because the legacy Pubky authorization URL is secret-bearing; iframe and pubky.app-native rendering are deferred shells.
- Implement the existing Pubky QR/deeplink auth flow as the legacy/cookie creator authorization path first, then migrate to the SDK grant flow (`PubkyGrantAuthFlow` / `GrantCredential`) as the durable production auth primitive.
- Reject manual operator provisioning and direct raw-session submission as production paths for creator-granted session acquisition.
- Treat creator authority status-check UX/API semantics as part of the authenticated `pubky.app/browser -> Lock Server` relationship; if exposed, derive creator from that authenticated context rather than accepting an arbitrary public key query parameter.
- Read and write public Locks app resources under `/pub/locks.app/`.
- Read and write private Locks app resources under `/priv/locks.app/`, including guarded content bytes and verified proof bundles.
- Treat private `/priv/locks.app/...` writes as non-public: they emit no public events, private paths are not visible to clients, and Locks discovery relies on public `/pub/locks.app/...` resources.
- Revalidate creator sessions lazily before Pubky writes and proxy reads; if the SDK can refresh or revalidate an expired/refreshable session, attempt one refresh/revalidation before returning `creator_authority_unavailable`.
- Provide the production replacement for today's dev/test creator publishing routes by requiring both a Locks-local frontend session for `pubky.app/browser -> Lock Server` authorization and creator-granted homeserver authority for Pubky reads/writes.
- Hide unstable Pubky-Core details behind ports.

## Actors

### Content Creator

Publishes guarded resources and content locks. Grants the Lock Server one reusable Locks app session scoped to `/pub/locks.app/:rw` and `/priv/locks.app/:rw` so the Lock Server can author public Locks resources, manage guarded Locks resources, write verified proof bundles, and proxy-read guarded content across that creator's locks.

### Content Viewer

Discovers a content lock, gathers proof material, generates and stores a Bundle ID, submits a proof bundle, receives a Locks-issued access credential, and requests content through the Lock Server. A viewer does not need a Pubky identity by default; future identity-bound criteria may require viewer Pubky identity for those specific locks.

### Lock Server

Verifies viewer proof, writes successful entitlement records to creator-owned guarded storage, issues reusable-until-expiry Lock-Server-local access credentials, and proxy-reads guarded content. The Lock Server is Pubky-addressed and uses the same `_pubky.<raw_z32>` transport mapping as homeserver resources.

### Homeserver

In the production design, stores public lock policies, public Locks configuration, private guarded content bytes, and private verified proof bundles. Enforces Pubky sessions and path-scoped capabilities. The confirmed Locks private data namespace is `/priv/locks.app/`; Locks should depend on that homeserver capability rather than implementing a parallel guarded storage/auth namespace. The current local creator publishing implementation stores these resources in in-memory repositories instead of writing to a homeserver.

## Aggregates, Entities, and Value Objects

### ContentLock Aggregate

Represents a public lock policy.

Likely fields:

- `version`
- `creator: CreatorPubky`
- `guarded_resource: GuardedResource`
- `criteria: Vec<Criterion>`
- `lock_logic: LockLogic`
- `access_policy: AccessPolicy`
- `lock_server.override: Option<LockServerPubky>`
- `created_at`

Invariants:

- Lock hash is `BLAKE3` over the canonical JSON representation of all serialized content lock payload fields.
- Lock ID is the Crockford-base32 encoding of the lock hash, without a readability prefix.
- Lock ID and lock hash are derived values, not serialized fields inside the content lock payload, to avoid circular hashing.
- Public content lock path includes a `.json` extension: `/pub/locks.app/<lock_id>.json`.
- Criteria referenced by lock logic must exist in the lock.
- Guarded resource must include enough information to identify the content version: creator-relative path, guarded resource hash, MIME content type, and positive byte size.
- Guarded resource `content_type` must parse as MIME.
- Guarded resource `size` must be the exact byte length and greater than zero.
- Content lock must not contain bearer secrets.
- Requested access credential TTL is expressed by the access policy. The Lock Server rejects unsupported TTLs rather than silently clamping them.
- Per-lock Lock Server override is nested as `lock_server.override`; absence means use creator-owned default config.

### Criterion Entity or Value Object

Represents one lock-type-agnostic access requirement.

Invariants:

- Criterion ID is unique within a content lock.
- Criterion type is opaque to the core model except for dispatching to a verifier.
- Criterion parameters are lock-type-specific and should be validated by the corresponding verifier or schema.

### SubmittedProofBundle Aggregate

Represents viewer proof material submitted for verification.

Likely fields:

- `version`
- `bundle_id: BundleId`
- `pubky_lock_resource: PubkyLockResource`
- `proofs: Vec<Proof>`

Invariants:

- Bundle ID is generated by the content viewer.
- Bundle ID is a canonical 128-bit cryptographically random value encoded with Crockford base32.
- Bundle ID must be safe to use as a filename after validation.
- Submitted proof bundle is not an entitlement.

### VerificationTask Aggregate

Represents asynchronous verification work.

Fields:

- `task_id: TaskId`
- `creator: CreatorPubky`
- `submitted_proof_bundle: SubmittedProofBundle`
- `status`
- `submitted_at`
- `started_at: Option<OffsetDateTime>`
- `completed_at: Option<OffsetDateTime>`
- `failure_message: Option<String>`

Invariants:

- Verification is asynchronous even if a specific verifier completes quickly.
- Verification task statuses are `pending`, `in_progress`, `completed`, `failed`, and `expired`.
- Task IDs are UUID v4 operational identifiers distinct from Bundle IDs in both meaning and format.
- Task IDs are internal runtime/correlation identifiers and are not exposed through the public HTTP API.
- Public verification task status is addressed by `{ creator, bundle_id }`, which identifies one logical verification attempt lifecycle.
- Task state is stored in Lock Server private storage, but relevant task status is exposed to the content viewer and content creator.
- Task state is retained for a few hours.
- Failed verification does not create an entitlement record.
- Completed successful verification must attempt entitlement persistence before access credential issuance.
- Task records store operational lifecycle state only; they do not store `VerificationResult` because successful verification evidence lives in `VerifiedProofBundle`.
- Public lifecycle responses replace internal `task_id` with `creator` and `bundle_id`, keep status/timestamp/failure fields, and must not expose submitted proof material, raw credentials, entitlement evidence, or worker claim metadata.
- `{ creator, bundle_id }` is a permanent one-attempt lifecycle identity. After current canonical preflight, re-submitting the exact same submitted proof bundle returns the existing lifecycle state without creating new work or another Paykit invoice; different proof material for the same identity is a conflict.
- Paykit status lookup uses a signed `{ creator, bundle_id }` request. Any status-call transport, HTTP, authentication/authorization, protocol, or decoding failure returns the task to pending for durable retry; v1 has no terminal Paykit payment-failure status.
- Retrying after `failed` or `expired` requires a new Bundle ID.
- Allowed transitions are `pending -> in_progress`, `pending -> expired`, `in_progress -> completed`, `in_progress -> failed`, and `in_progress -> expired`.
- `completed`, `failed`, and `expired` are terminal states; retention cleanup deletes task records rather than transitioning terminal tasks.
- `failed` tasks require a non-empty failure message; other statuses must not carry failure messages.
- Transition methods validate current-state timestamp/failure-message invariants before applying a transition.

### VerifiedProofBundle Aggregate

Represents a stored entitlement record.

Likely fields:

- `version`
- `bundle_id: BundleId`
- `pubky_lock_resource: PubkyLockResource`
- `verification_result: VerificationResult`
- `entitlement_lifetime: EntitlementLifetime`

Invariants:

- Stored only after successful verification.
- Stores minimal criterion-level verification result evidence, not raw proof material.
- Stored under creator-owned guarded Locks storage.
- Resolvable by Bundle ID.
- References the content lock using `pubky_lock_resource`, exactly `pubky<creator_pubky>/pub/locks.app/<lock_id>.json`.
- To honor the entitlement, the Lock Server derives creator, content lock path, and Lock ID from `pubky_lock_resource`, reads the content lock, and verifies that the content lock file hashes to the embedded Lock ID.
- Remains intact when a changed lock creates a new Lock ID.
- Revoked by deleting the verified proof bundle.
- Stops being honored if the corresponding content lock file is missing.
- Stops being honored if the content lock file hash does not match the Lock ID embedded in `pubky_lock_resource`.
- Historical lock archives are not used for entitlement resolution.

### AccessCredential Value Object

Represents reusable-until-expiry opaque bearer access.

Invariants:

- Non-guessable.
- Reusable until expiry.
- Default requested TTL is 15 minutes.
- Longer requested TTLs are allowed by the model, potentially measured in days.
- The requested TTL lives in the content lock's access policy.
- The Lock Server rejects a requested TTL above its configured maximum.
- Server-side credential state resolves to at least `{ creator, bundle_id }`.
- Bundle ID anchors the entitlement; access credentials do not duplicate-bind to Lock ID or guarded resource hash.
- Not equivalent to an entitlement record.
- May be shareable for anonymous-compatible flows.

### VerificationResult Value Object

Represents minimal entitlement evidence for v0.

Likely fields:

- `criteria: Vec<CriterionVerificationResult>`

Invariants:

- Criterion-level, not only lock-level.
- Contains only criterion results necessary to satisfy the lock logic.
- Does not include failed criterion attempts in the entitlement record.
- Does not include arbitrary lock-type-specific metadata in v0.
- Does not store raw proof.

### CriterionVerificationResult Value Object

Represents successful verification evidence for one criterion.

Likely fields:

- `criterion_id`
- `satisfied`
- `verified_at`
- `verified_by`
- `verifier_type`

Invariants:

- `verified_by` records the Lock Server identity that produced the result.
- `verifier_type` records the verifier kind used for the criterion.
- `verifier_type` is represented in code as the `VerifierType` enum, serialized as the protocol string such as `dev-static`.
- Unknown verifier strings are rejected while parsing protocol payloads; known-but-unregistered verifier types return `ApplicationError::UnsupportedVerifierType` during service dispatch.

## Domain Services

This section names conceptual services. Protocol payloads and pure hashing/path derivation stay in `locks-core`; Lock Server authorization decisions that feed credential issuance/validation live in `locks-service` until another product needs to share them.

### LockHasher

Computes `BLAKE3(canonical_json(content_lock))` and derives Lock ID as Crockford-base32 encoded hash.

### LockLogicEvaluator

Evaluates whether stored verification results satisfy a content lock's logic for Lock Server credential issuance/validation. It currently lives in `locks-service/src/application/entitlement_evaluator.rs`, rejects malformed evidence (empty criteria, duplicate criteria/results, unknown result criteria), and returns `Ok(false)` for valid but insufficient evidence.

### VerificationOrchestrator

Creates verification tasks, dispatches proof checks to verifier ports, collects results, and coordinates entitlement persistence.

### AccessCredentialIssuer

Issues reusable-until-expiry opaque access credentials after successful verification or entitlement resolution.

### AccessDecision

Determines whether a presented access credential or resolved entitlement should be honored.

### EntitlementResolver

Looks up existing verified proof bundles by Bundle ID and determines whether they can be used to issue a new access credential.

## Ports and Repositories

Ports live in `locks-service/src/application/ports.rs`, not in `locks-core`. Application-layer operational models live in `locks-service/src/application/models.rs`. The shared core owns protocol/domain payloads and pure rules; the Lock Server application layer owns orchestration boundaries.

Service ports and use cases are async from v0. `locks-service` may use `async-trait` and Tokio for application/runtime concerns, while `locks-core` remains runtime-free. Port methods take `&self`; adapters own any interior mutability needed for in-memory or future Pubky-backed implementations.

Repository/store read methods return `Result<Option<T>, ApplicationError>`. A missing record is represented as `Ok(None)` because repositories expose persistence access, not workflow interpretation. Use cases convert `None` into product meaning such as invalid credential, revoked entitlement, unavailable content lock, expired task, or user-facing not found. Infrastructure failures remain `Err(ApplicationError)`.

Write method names are explicit:

- `insert_*`: create only; duplicate records are `ApplicationError::DuplicateRecord`.
- `upsert_*`: create or replace; replacement is allowed.
- `update_*`: update existing only; missing records are `ApplicationError::MissingRecord`.
- `delete_*`: means “ensure absent”; deleting a missing record is `Ok(())`. Use cases that need must-exist semantics must read first and interpret `None`.

`locks-service` should start with a layer-first hexagonal organization:
```text
locks-service/src/
  lib.rs
  application/
    mod.rs
    entitlement_evaluator.rs
    models.rs
    ports.rs
    errors.rs
    use_cases/
      mod.rs
      submit_proof_bundle.rs
      get_verification_task.rs
      complete_verification_task.rs
      issue_access_credential.rs
      validate_access_credential.rs
      proxy_read_guarded_resource.rs
  infrastructure/
    mod.rs
    memory/
      mod.rs
      content_locks.rs
      entitlements.rs
      guarded_resources.rs
      verification_tasks.rs
      access_credentials.rs
    verifiers/
      mod.rs
      dev_static.rs

locks-server/src/
  lib.rs
  main.rs
  app_state.rs
  config.rs
  runtime.rs
  testing.rs
  api/
    mod.rs
    dtos.rs
    errors.rs
    routes.rs
locks-server/config/
  example.dev.postgres.toml

locks-e2e/tests/
  retrieval_access_http.rs
```

HTTP/API delivery should be added in a separate `locks-server` crate so `locks-service` remains the reusable application/service layer. Pubky-backed adapters should be added later under clear adapter folders, for example `infrastructure/pubky/`, when their responsibilities are concrete.

### ContentLockRepository

Creates or replaces public content locks by creator and canonical `content_lock_path`. The path embeds the Lock ID, and entitlement validation re-hashes the fetched content lock against that embedded ID.

### LockConfigurationRepository

Deferred until Pubky-Core questions about creator-owned Locks config and Lock Server discovery are resolved. Reads creator-owned Locks config such as the default Lock Server pointer. The default pointer lives in creator-owned public config, not in a dedicated PKDNS record.

### EntitlementRepository

Inserts, reads, and ensures absence of verified proof bundles under creator-owned guarded storage. Insert is duplicate-sensitive; delete means ensure absent.

### GuardedResourceRepository

Creates or replaces current local guarded resource bytes and metadata by creator/path. Reads for proxy access require the expected guarded resource hash from the content lock; if the current stored hash differs, the read misses. Local creator publishing also reads the current descriptor by creator/path before creating a content lock.

### VerificationTaskRepository

Inserts, updates, reads, and ensures absence of local asynchronous verification task state. This may be Lock Server private state and does not need credible-exit portability. Status transitions are validated by `VerificationTaskRecord` in the application model layer.

### AccessCredentialStore

Inserts, resolves, and ensures absence of reusable-until-expiry opaque access credential lookup keys. This is Lock Server private state. Insert is duplicate-sensitive because credential collisions should be impossible. The raw bearer credential is never used as the store key; callers derive `AccessCredentialLookupKey = BLAKE3(access_credential.as_bytes())`.

### AccessCredentialGenerator

Generates short-lived opaque bearer access credentials using service infrastructure. The v0 credential format is 32 bytes of cryptographic randomness encoded as base64url without padding. Tests should inject deterministic generators.

### VerificationTaskIdGenerator

Generates server-owned operational `TaskId` values for verification tasks. Submit-proof use cases accept viewer-submitted proof material and create the task ID internally so callers cannot choose workflow polling identifiers. Tests should inject deterministic generators.

### Clock

Provides deterministic current time for task lifecycle and credential expiry use cases. This port is synchronous because reading time is not I/O.

### Pubky homeserver storage seam

`locks-service` exposes an object-safe Pubky homeserver storage seam for creator-scoped JSON and byte operations. Pubky-backed repositories use this seam for Lock Service Pointers, content locks, private guarded bytes, and verified proof bundles. The seam has an authorizing wrapper and a provider-backed SDK composition that restores creator-scoped sessions from encrypted creator-authority records, verifies restored identity against the requested creator, and uses session-scoped Pubky storage. Deterministic tests use fake homeserver storage clients; live Pubky smoke testing is deferred until testnet/setup credentials are available.

### LockServerIdentityProvider

`locks-server` owns runtime identity/secret initialization behind a provider boundary. The in-memory skeleton uses `FilesystemLockServerIdentityProvider` to create a local `.sess`-shaped secret, derive `LockServerPubky` from that secret, validate public-key mismatch on existing config, and avoid pulling Pubky signup/signin into the skeleton. Production Pubky-backed secret/session persistence remains deferred until homeserver/signup/signin configuration is clarified.

### TestServerApp

`locks-server::testing::TestServerApp` is exposed only under `#[cfg(any(test, feature = "test-support"))]`. Route and `locks-e2e` tests use it to build an in-memory router and seed content locks/priv resources without adding HTTP seed endpoints or demo runtime state.

### CriterionVerifier Port

Lock-type-specific verifier interface. Verifier dispatch uses the protocol-facing `VerifierType` enum, not raw strings or Rust module names. The `CriterionVerifierRegistry` maps a supported `VerifierType` to a concrete verifier adapter; if the type is known to the protocol but not registered on this Lock Server, completion returns `ApplicationError::UnsupportedVerifierType`. The first slice uses `StaticCriterionVerifierRegistry` to explicitly wire `VerifierType::DevStatic` to the non-production `DevStaticVerifier` only in dev/test runtime mode; production-mode workers leave it unregistered.

The domain model should allow verifier implementations without depending on payment, password, follower, or subscription details now.

## Commands

### RegisterGuardedResource

Local dev/test creator publishing use case that accepts creator, guarded resource path, MIME content type, and bytes; computes hash and positive size; validates the guarded resource descriptor; and stores/replaces the current guarded resource by creator/path. The local HTTP contract and Pubky-backed repository adapters both use `/priv/locks.app/content/` for private guarded bytes; the difference is the configured repository backend, not the path.

### CreateContentLock

Local dev/test creator publishing use case that verifies a registered guarded resource descriptor, builds a content lock, derives its Lock ID/content lock path, and stores the content lock through the configured repository port. Default local runtime uses in-memory storage; Pubky-backed repository tests prove the same use case writes public content locks under `/pub/locks.app/<lock_id>.json` when composed with Pubky homeserver adapters.

### SetLockServicePointer

Local dev/test creator publishing use case that stores or replaces the creator's default Lock Service Pointer for `/pub/locks.app/config.json` through the configured repository port. Content lock creation does not require this pointer, but viewer discovery uses it when a content lock has no `lock_server.override`.

### CreatorAuthorityAcquisition

Lock-Server-owned protocol that starts a legacy-connect Pubky auth flow, completes it after creator approval, stores creator-granted homeserver authority as encrypted private runtime state, returns a short-lived one-time frontend session code, and exchanges that code plus state for a Locks-local frontend session token. Redirect, popup, iframe, and native pubky.app rendering are presentation shells over this protocol, not separate domain concepts. For legacy-connect, ADR 0019 requires a Lock-Server-hosted redirect/popup shell because the Pubky authorization URL is secret-bearing. The implemented production gate is `[creator_authority_acquisition].enabled = true` with `method = "legacy-connect"`; live Pubky/testnet smoke remains deferred.

### GetCreatorAuthorityStatus

Read-only use case for `GET /creator/authority-status`. It validates a Locks-local frontend session, derives the creator from that session, reads the stored creator-authority record, and returns a secret-free missing/present status. Missing stored authority is represented as `authorized = false`, not as an operational error. The read model does not revalidate Pubky I/O; Pubky-backed repository operations revalidate authority before actual homeserver reads/writes.

### FrontendSessionExchange

Consumes a short-lived one-time frontend session code plus original state and returns a Locks-local frontend session token exactly once. Frontend sessions authenticate `pubky.app/browser -> Lock Server` creator APIs. They are not Pubky homeserver sessions and are separate from viewer access credentials.

### SubmitProofBundle

Accepts viewer-submitted proof material and creates an asynchronous verification task.

### GetVerificationTask

Internal read-only use case. Loads task state by Task ID and returns lifecycle metadata for runtime/worker coordination. It must not dispatch verifiers, mutate task state, load content locks, or write entitlements.

### GetVerificationTaskByHandle

Public read-only polling/API use case. Loads task state by `{ creator, bundle_id }` and returns lifecycle metadata without exposing internal Task ID, submitted proof material, credentials, entitlement evidence, or worker claim metadata.

### CompleteVerificationTask

Worker-owned use case. Dispatches/runs verifier work from the stored proof bundle, persists `InProgress` after dispatch, and records successful or failed verification result. Verifier requests use the Lock Server identity injected into the use case for `verified_by`. On success, it persists a verified proof bundle before marking the task `Completed`; on failure, it persists a non-empty failed-task message and stores no entitlement unless entitlement persistence had already succeeded.

### IssueAccessCredential

Explicit mutation use case that issues a reusable-until-expiry opaque access credential after successful verification or valid entitlement resolution. It is intentionally separate from task polling and worker completion: `Completed` means entitlement evidence has been persisted and is ready for credential issuance, not that a bearer credential has been minted or returned. Credential issuance returns the raw bearer credential once and should not happen as a side effect of `GetVerificationTask`.

### ResolveEntitlement

Uses a viewer-presented Bundle ID to find a verified proof bundle and decide whether to issue a fresh access credential.

### ProxyReadGuardedResource

Reads the guarded resource using Lock Server authority and returns it to a viewer with a valid access credential. The use case validates the credential, re-checks the entitlement and current content lock hash/path identity through `pubky_lock_resource`, then reads the guarded resource by creator, guarded resource path, and guarded resource hash. Missing guarded bytes or hash mismatch return `ApplicationError::GuardedResourceUnavailable`. Successful HTTP proxy-read returns raw bytes with the stored guarded resource MIME content type.

### RevokeEntitlement

Deletes the verified proof bundle so the Lock Server should no longer honor the entitlement.

## Domain Events

These are conceptual events for internal organization. They do not imply Pubky `/events` publication.

- `ContentPublished`
- `ContentLockPublished`
- `ProofBundleSubmitted`
- `VerificationTaskStarted`
- `ProofBundleVerified`
- `ProofBundleRejected`
- `EntitlementRecorded`
- `AccessCredentialIssued`
- `GuardedResourceProxyRead`
- `EntitlementRevoked`

## Main Flow: Content Creation

1. Content creator authorizes a Lock Server or creator app with required capabilities.
2. Content creator uploads guarded resource to their homeserver.
3. Content creator computes guarded resource hash.
4. Content creator defines lock criteria and lock logic.
5. System canonicalizes the content lock.
6. System derives Lock ID from the lock hash using Crockford base32.
7. Content creator publishes public content lock under `/pub/locks.app/<lock_id>.json`.
8. Content creator optionally publishes a preview post pointing to the content lock.

## Main Flow: Content Retrieval

1. Content viewer discovers a content lock through a preview post, public path, or events.
2. Content viewer reads the public content lock.
3. Content viewer resolves the Lock Server from `lock_server.override` if present; otherwise it uses creator-owned Locks config.
4. Content viewer generates a Bundle ID and stores it durably.
5. Content viewer submits a proof bundle to the Lock Server.
6. Lock Server creates an asynchronous verification task.
7. Lock Server verifies proof material through criterion verifier ports.
8. If verification fails, Lock Server reports failure and stores no entitlement.
9. If verification succeeds, Lock Server stores a verified proof bundle under creator-owned guarded storage.
10. Content viewer explicitly requests an access credential using creator and Bundle ID after observing successful verification or after resolving an existing entitlement.
11. Lock Server re-checks entitlement state and issues a reusable-until-expiry opaque access credential that resolves server-side to creator and Bundle ID.
12. Content viewer requests content using the access credential.
13. Lock Server validates the access credential and resolves the verified proof bundle by Bundle ID.
14. Lock Server derives creator, content lock path, and Lock ID from `pubky_lock_resource`, reads the content lock, and verifies that the content lock file hashes to the embedded Lock ID.
15. Lock Server confirms the entitlement is still honored.
16. Lock Server proxy-reads the guarded resource using creator-granted authority.
17. Lock Server returns the guarded resource response to the viewer.

## Main Flow: Credible Exit / Lock Server Migration

1. Content creator revokes old Lock Server authority.
2. Content creator grants new Lock Server authority to read guarded resources and verified proof bundles.
3. Content creator updates the Lock Service Pointer.
4. Content viewer presents stored Bundle ID to the new Lock Server.
5. New Lock Server reads the verified proof bundle from creator-owned guarded storage.
6. New Lock Server checks whether the corresponding content lock file still exists and the verified proof bundle has not been deleted. Homeserver error codes distinguish missing-lock revocation from temporary read failure.
7. New Lock Server issues a fresh reusable-until-expiry opaque access credential if the entitlement should still be honored.

## Security-Sensitive Invariants

- Bundle ID is a bearer secret.
- Bundle ID is generated by the content viewer.
- Bundle ID must be validated before use as a path segment or filename.
- Public content locks must not contain bearer secrets.
- Access credentials are non-guessable, reusable until expiry, and short-lived by default.
- Access credentials resolve server-side to creator and Bundle ID.
- Anonymous-compatible bearer access remains the default global access model; viewer Pubky identity is not required unless a future criterion specifically requires it.
- Access credentials are not durable entitlement records.
- Access credentials are minted only by an explicit issuance mutation, never as a side effect of read-only task polling.
- Completed verification tasks indicate entitlement readiness, not access credential issuance.
- Access credential validation must re-check the underlying entitlement state.
- Requested access credential TTL is controlled by the content lock's access policy and rejected if it exceeds the Lock Server's configured maximum.
- The Lock Server is a trusted proxy and can read guarded resources it is authorized to read.
- v1 keeps the trusted plaintext proxy model; encrypted guarded payloads and key-release/key-unwrapping designs are deferred research.
- Verified proof bundles are creator-owned guarded records.
- Verified proof bundles store minimal criterion-level entitlement evidence and no raw proof by default.
- Failed proof submissions do not create entitlement records.
- Failed criterion attempts are not stored in successful entitlement records.
- Failed verification attempts are kept only in local logs, with no required retention period.
- Verification submission through `POST /proof-bundles` is protected by a configurable process-local in-memory fixed-window rate limit keyed by client address and creator for the current HTTP skeleton.
- Verification submission rate limiting is an abuse guard only; it does not replace idempotency/conflict checks, entitlement lifetime, credential TTL, or lock-type-specific policy.
- Lock changes produce new content lock files because Lock ID is derived from the BLAKE3 hash of the canonical JSON content lock.
- Existing entitlements remain intact unless the verified proof bundle is deleted or the corresponding content lock file is removed.
- Entitlements are not honored if the content lock file content does not hash to the Lock ID embedded in `pubky_lock_resource`.

## Implementation Guidance Before Pubky-Core Clarification

The completed first implementation organized core logic around the workspace split and retrieval/access vertical slice. While Pubky-Core questions remain open, the next product slice may implement local Creator Publishing / Lock Authoring without real Pubky I/O:

- Keep placeholder `locks-sdk`, `locks-admin`, or `creator-ui` members documented but not created until their responsibilities are concrete.
- Do not hard-code final Pubky path or session-storage behavior into `locks-core`.
- Do not implement lock-type-specific payment/password/follower semantics yet.
- Use an asynchronous verification task abstraction from the start.
- Use opaque access credentials for the first access model.
- Persist minimal criterion-level verification result evidence in verified proof bundles.
- Implement local creator publishing only through dev/test-gated routes and in-memory repositories.
- Keep production Pubky-backed creator publishing behind Pubky-Core answers.
- Keep credential issuance as an explicit mutation route; task polling remains read-only and secret-free.
- Use a non-production `dev-static` verifier in dev/test runtime mode only; production-mode workers must leave it unregistered.
- Put in-memory repositories and `dev-static` under `locks-service`, not `locks-core`.
- Add JSON-shape tests for `locks-core` protocol payloads.
