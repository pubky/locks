# Locks Thesaurus

Canonical domain vocabulary for Pubky Locks. This file is the source of truth for domain-bearing names in code, docs, APIs, paths, events, and tests.

## Bounded Contexts

### Lock Policy Context
Public creator-owned data that describes what is guarded and what must be satisfied before access can be granted.

### Verification Context
The asynchronous process of accepting viewer proof material, checking it, and producing a verification result.

### Entitlement Context
Creator-owned guarded records proving that a viewer previously satisfied a content lock.

### Access Context
Short-lived Lock-Server-issued credentials and proxy access to guarded content.

### Pubky Integration Context
Interaction with Pubky homeservers, sessions, capabilities, guarded paths, public paths, and Pubky resource addressing.

### Implementation Context
Repository/workspace structure, protocol payload ownership, and code-boundary language used to keep shared contracts separate from service runtime details.

---

## Actors

### Content Creator
- **Definition**: A Pubky user who owns guarded content and publishes content locks controlling access to that content.
- **NOT**: The Lock Server; the Lock Server acts only with creator-granted authority.
- **Synonyms to AVOID**: creator user, publisher, author, owner
- **Related terms**: Guarded Content, Content Lock, Capability Grant

### Content Viewer
- **Definition**: A person or client attempting to access guarded content by submitting proof material that satisfies a content lock.
- **NOT**: The content creator or Lock Server operator.
- **Synonyms to AVOID**: user, consumer, reader
- **Related terms**: Submitted Proof Bundle, Access Credential, Proxy Read

### Creator Publishing
- **Definition**: First-class but extractable creator-side Lock Server workflow for registering guarded resources, creating content locks, and authoring Lock Service Pointers through authenticated Pubky-backed creator routes.
- **NOT**: Viewer proof submission, test fixture seeding, unauthenticated local-memory HTTP publishing, or a dependency that Verification/Entitlement/Access internals should require.
- **Synonyms to AVOID**: locked resource upload, seed content, creator admin API
- **Related terms**: Content Creator, Guarded Resource Registration, Content Lock Creation, Guarded Resource

### Lock Server
- **Definition**: A Pubky-addressed application service and process composition root that verifies submitted proof bundles, stores entitlement records, issues access credentials, proxy-reads guarded content using creator-granted authority, and mounts authenticated creator publishing routes when configured for Pubky homeserver repositories.
- **NOT**: A homeserver feature, payment processor, ultimate authority over creator-owned data, or a substitute for production creator authorization.
- **Synonyms to AVOID**: Locks Server, lock service, gatekeeper server, proxy server
- **Related terms**: Capability Grant, Access Credential, Verified Proof Bundle, Creator Publishing

---

## Lock Policy Context

### Content Lock
- **Definition**: Public structured data published by a content creator that defines the criteria and logic required to access a guarded resource.
- **NOT**: The guarded content itself or a stored entitlement record.
- **Synonyms to AVOID**: lock file, unlock conditions, paywall
- **Related terms**: Lock ID, Criterion, Lock Logic, Guarded Resource, Access Policy, Content Lock Path

### Content Lock Creation
- **Definition**: Local creator publishing action exposed in dev/test as `POST /creator/content-locks`; it verifies a registered guarded resource descriptor, builds a content lock, derives its Lock ID and content lock path, and stores it in the content lock repository.
- **NOT**: Viewer proof verification, raw guarded content upload, or production Pubky public-path write.
- **Synonyms to AVOID**: publish lock endpoint, create locked resource, policy upload
- **Related terms**: Content Lock, Creator Publishing, Guarded Resource Registration, Lock ID

### Lock ID
- **Definition**: Identifier for a content lock, encoded by the Rust `base32` crate as fixed-length 52-character Crockford base32 of the full 32-byte BLAKE3 lock hash with no prefix or checksum; canonical form is uppercase and used in `/pub/locks.app/<lock_id>.json` so changing the lock creates a new content lock file.
- **NOT**: A mutable arbitrary label or bearer secret.
- **Synonyms to AVOID**: lock name, policy id, file id
- **Related terms**: Content Lock, Lock Hash

### Lock Hash
- **Definition**: BLAKE3 hash of all serialized fields in a content lock's canonical JSON representation, used to derive the Lock ID and identify the exact lock version an entitlement belongs to; Lock ID and lock hash are derived values, not serialized content lock fields.
- **NOT**: The guarded resource hash.
- **Synonyms to AVOID**: policy checksum, lock checksum
- **Related terms**: Lock ID, Guarded Resource Hash, Entitlement Record

### Pubky Lock Resource

- **Definition**: Protocol-facing addressed Pubky resource for a public content lock, exactly `pubky<creator_pubky>/pub/locks.app/<lock_id>.json`, matching the preferred `PubkyResource` form from the `pubky` crate. It derives creator, content lock path, and Lock ID.
- **NOT**: `pubky://...`, an HTTP(S) transport URL, a creator-relative path alone, or a guarded resource path.
- **Synonyms to AVOID**: content lock URL, homeserver URL, lock URL
- **Related terms**: Content Lock Path, Lock ID, Creator Pubky

### Content Lock Path
- **Definition**: Canonical creator-homeserver-relative public path to a content lock file, exactly `/pub/locks.app/<lock_id>.json`; the embedded Lock ID must parse and match the content lock file hash before an entitlement is honored.
- **NOT**: The content lock payload itself, a guarded path, full Pubky URL, homeserver URL, or another `/pub/...` path.
- **Synonyms to AVOID**: lock path, lock file path, policy path
- **Related terms**: Content Lock, Lock ID, Public Lock Path, Verified Proof Bundle

### Criterion
- **Definition**: One lock-type-agnostic requirement that may be satisfied by viewer proof material.
- **NOT**: The verification algorithm itself.
- **Synonyms to AVOID**: condition, rule, requirement item
- **Related terms**: Lock Logic, Submitted Proof Bundle, Verification Result

### Lock Logic
- **Definition**: The boolean structure that determines which criteria must be satisfied for a content lock.
- **NOT**: Lock-type-specific verification logic.
- **Synonyms to AVOID**: condition logic, criteria expression, access logic
- **Related terms**: Criterion, Content Lock

### Access Policy
- **Definition**: Content lock section that describes access-credential behavior requested by the creator, including requested credential TTL.
- **NOT**: An entitlement lifetime, verifier rule, or Lock Server operator configuration.
- **Synonyms to AVOID**: token policy, access settings, credential config
- **Related terms**: Content Lock, Access Credential, Entitlement Lifetime

### Guarded Resource
- **Definition**: The creator-owned resource protected by a content lock, described by creator-relative path, payload hash, MIME content type, and positive byte size.
- **NOT**: The public lock policy, preview post, or raw guarded bytes themselves.
- **Synonyms to AVOID**: protected content, locked content, resource data
- **Related terms**: Guarded Content, Guarded Resource Hash, Guarded Resource Registration, Proxy Read

### Guarded Resource Registration
- **Definition**: Authenticated creator publishing action exposed as `PUT /creator/priv-resources/content/<path>`; it stores or replaces current guarded bytes and metadata for the session-derived creator and canonical guarded path under `/priv/locks.app/content/`, then returns a guarded resource descriptor.
- **NOT**: A creator authorization proof, public content lock, or viewer proxy-read.
- **Synonyms to AVOID**: seed endpoint, locked resource upload
- **Related terms**: Guarded Resource, Content Creator, Creator Publishing, Guarded Path

### Guarded Content
- **Definition**: Creator-owned content payload stored under a guarded path; when referenced by a content lock, it is modeled as a Guarded Resource.
- **NOT**: The public content lock, preview post, or verified proof bundle.
- **Synonyms to AVOID**: protected content, locked content, gated content
- **Related terms**: Guarded Resource, Guarded Path, Content Lock

### Guarded Resource Hash
- **Definition**: Hash of the guarded resource payload used to identify the exact content version an entitlement was verified against.
- **NOT**: The content lock hash.
- **Synonyms to AVOID**: content hash, resource checksum
- **Related terms**: Guarded Resource, Lock Hash, Entitlement Record

### Lock Service Pointer
- **Definition**: Creator-owned public configuration at `/pub/locks.app/config.json` that tells viewers which Lock Server to use by default when a content lock does not specify `lock_server.override`; authored through authenticated `POST /creator/lock-service-config`.
- **NOT**: A PKDNS/Pkarr record, the per-lock Lock Server override itself, production creator authorization, or a content lock.
- **Synonyms to AVOID**: service url, server config, migration pointer, _locks record
- **Related terms**: Lock Server, Credible Exit, Creator Publishing

### Lock Server Override
- **Definition**: Optional nested content lock field `lock_server.override` that points viewers to a specific Pubky-addressed Lock Server for that content lock.
- **NOT**: The default migration mechanism or creator-owned default Lock Service Pointer.
- **Synonyms to AVOID**: service override, per-lock service url, lock service override
- **Related terms**: Lock Server, Lock Service Pointer, Content Lock

---

## Verification Context

### Submitted Proof Bundle
- **Definition**: Viewer-submitted structured proof material for a content lock, identified by a viewer-generated Bundle ID before verification.
- **NOT**: An entitlement record; it has no access meaning until verification succeeds.
- **Synonyms to AVOID**: proof request, proof payload, submitted bundle
- **Related terms**: Content Viewer, Bundle ID, Verification Task

### Bundle ID
- **Definition**: Canonical 128-bit cryptographically random viewer-generated bearer secret encoded by the Rust `base32` crate as fixed-length 26-character Crockford base32, used as the durable recovery handle and final filename for a verified proof bundle.
- **NOT**: Public metadata, sequential ID, or server-generated database key.
- **Synonyms to AVOID**: proof id, entitlement id, bundle key
- **Related terms**: Submitted Proof Bundle, Verified Proof Bundle, Credible Exit

### Verification Task
- **Definition**: Asynchronous Lock Server work item created when a viewer submits a proof bundle; statuses are `pending`, `in_progress`, `completed`, `failed`, and `expired`; retained for a few hours; publicly addressed by `{ creator, bundle_id }`.
- **NOT**: A persisted entitlement record or a public Task ID resource.
- **Synonyms to AVOID**: job, async request, background check
- **Related terms**: Submitted Proof Bundle, Public Verification Attempt Handle, Task ID, Verification Result, Verification Task Claim

### Public Verification Attempt Handle
- **Definition**: The public client-facing verification task handle made from `creator` and `bundle_id`; it identifies one logical verification attempt lifecycle for submission idempotency, polling, dev completion, and credential issuance.
- **NOT**: The internal Task ID, raw proof material, credential, or entitlement evidence.
- **Synonyms to AVOID**: task id, creator id, bundle task id, proof lookup
- **Related terms**: Verification Task, Bundle ID, Content Creator, Content Viewer, Access Credential

### Verification Task Lookup
- **Definition**: Public lifecycle-status query by Public Verification Attempt Handle using `POST /verification-task-lookups`.
- **NOT**: A Task ID lookup, credential issuance, entitlement lookup, or raw proof retrieval.
- **Synonyms to AVOID**: bundle task id lookup, proof lookup, task search
- **Related terms**: Public Verification Attempt Handle, Verification Task, Bundle ID, Task ID, Content Viewer

### Verification Submission Rate Limit
- **Definition**: Configurable process-local Lock Server HTTP admission-control rule that limits public proof bundle submissions to `POST /proof-bundles` by client address and creator before a verification task is created. The current implementation is an in-memory fixed-window limiter and returns `429 rate_limited` with `Retry-After` when exceeded.
- **NOT**: Entitlement lifetime, credential TTL, worker claim limit, duplicate-submission conflict handling, distributed quota, or a Bundle-ID-keyed limit.
- **Synonyms to AVOID**: throttle, request quota, anti-spam setting
- **Related terms**: Submitted Proof Bundle, Verification Task, Lock Server Runtime State

### Verification Task Claim
- **Definition**: Runtime coordination state proving a worker has leased a verification task for completion until a claim-expiry time.
- **NOT**: Entitlement evidence, viewer-facing task status, or a Pubky record.
- **Synonyms to AVOID**: job lock, worker reservation, processing token
- **Related terms**: Verification Task, Lock Server Runtime State

### Lock Server Runtime State
- **Definition**: Lock-Server-private operational state stored in Postgres for verification tasks, task claims/leases, attempt metadata when needed, and access credential lookup records.
- **NOT**: Creator-owned content locks, guarded resources, verified proof bundles, or any Pubky canonical resource.
- **Synonyms to AVOID**: service data, private domain data, app database
- **Related terms**: Verification Task, Verification Task Claim, Access Credential Lookup Key

### Verification Worker
- **Definition**: Lock Server runtime component that claims verification tasks and invokes worker-owned completion use cases; currently in-process until all completion dependencies are shared/durable.
- **NOT**: The HTTP API route, a per-request child process, or a Pubky homeserver feature.
- **Synonyms to AVOID**: job runner, background processor, async manager
- **Related terms**: Verification Task, Verification Task Claim, Lock Server Runtime State

### Health Check
- **Definition**: Lock Server runtime liveness endpoint, exposed as `GET /healthz`, that reports whether the HTTP process is serving.
- **NOT**: Dependency readiness, proof verification, entitlement status, Pubky discovery, or database inspection.
- **Synonyms to AVOID**: readiness check, status page, diagnostics dump
- **Related terms**: Readiness Check, Lock Server Runtime State

### Readiness Check
- **Definition**: Lock Server runtime dependency check, exposed as `GET /readyz`, that reports whether the composed runtime can serve traffic; persisted (currently Postgres-backed) runtime readiness requires a successful pool ping.
- **NOT**: Health check/liveness, migration runner, task queue depth, worker claim inspection, or credential validation.
- **Synonyms to AVOID**: health check, status endpoint, database dump
- **Related terms**: Health Check, Lock Server Runtime State

### Task ID
- **Definition**: Server-generated UUID v4 operational identifier for a verification task, serialized as a canonical lowercase hyphenated UUID string and distinct from the viewer-generated Bundle ID.
- **NOT**: A durable recovery handle, entitlement identifier, bearer secret, or credible-exit handle.
- **Synonyms to AVOID**: bundle id, proof id, job id
- **Related terms**: Verification Task, Bundle ID

### Crockford Identifier Normalization
- **Definition**: Parsing behavior for Crockford-encoded Locks identifiers implemented by the Rust `base32` crate's Crockford decoder; Locks relies on the crate's built-in lowercase and ambiguous-character normalization, rejects hyphens/readability separators, and emits uppercase canonical form from `base32::encode`.
- **NOT**: Custom Locks-specific ambiguous-character mapping, a checksum scheme, or permission to serialize non-canonical lowercase identifiers.
- **Synonyms to AVOID**: fuzzy ID parsing, loose ID format
- **Related terms**: Lock ID, Bundle ID

### Pubky Identity Wrapper
- **Definition**: v0 `locks-core` domain role wrapper for creator and Lock Server Pubky identities; it delegates parsing and canonical validation to Pubky/common public-key parsing and emits the canonical `pubky<z32>` form.
- **NOT**: A custom Locks z32 parser or a reason to erase creator-vs-Lock-Server role intent from domain types.
- **Synonyms to AVOID**: pubky validator, identity parser
- **Related terms**: Content Creator, Lock Server

### Verification Result
- **Definition**: Minimal criterion-level entitlement evidence stored after successful verification; it contains only criterion results necessary to satisfy the lock logic.
- **NOT**: Raw proof material, failed attempt history, arbitrary verifier metadata, or independently re-verifiable audit evidence in v0.
- **Synonyms to AVOID**: proof status, validation result, decision, audit evidence
- **Related terms**: Criterion Verification Result, Verified Proof Bundle, Verified At

### Criterion Verification Result
- **Definition**: Successful verification evidence for one criterion, including criterion ID, satisfaction, verification time, verifying Lock Server, and verifier type.
- **NOT**: The submitted proof itself or a failed attempt record.
- **Synonyms to AVOID**: criterion proof result, validation detail, verifier metadata
- **Related terms**: Verification Result, Criterion, Verifier Type

### Verifier Type
- **Definition**: Protocol-facing enum value for the kind of verifier used for a criterion, such as `dev-static` or `paykit-payment`; it must survive migration and not expose implementation class/module names.
- **NOT**: The Lock Server identity or a Rust module path.
- **Synonyms to AVOID**: verifier class, verifier implementation, checker name
- **Related terms**: Criterion Verification Result, Lock Type

### Paykit Payment Verifier
- **Definition**: Production-shaped payment verifier with public wire value `paykit-payment`; in v1 it must be the content lock's sole criterion and the sole lock-logic reference, creates invoices through configured Paykit Server during proof submission, and verifies payment status through worker-owned status checks.
- **NOT**: The Paykit Server itself, wallet/xpub custody, a generic `payment` verifier, or the stale `paykit` verifier value.
- **Synonyms to AVOID**: paykit verifier, payment verifier, paid lock verifier
- **Related terms**: Verifier Type, Submitted Proof Bundle, Reader Public Key, Paykit Server

### Reader Public Key
- **Definition**: Top-level `SubmittedProofBundle.reader_public_key` Pubky identity required for `paykit-payment` submissions; it must resolve through Pubky/PKARR/homeserver discovery before invoice creation.
- **NOT**: A proof payload field, content creator identity, access credential binding, or final non-transferability guarantee.
- **Synonyms to AVOID**: reader, viewer key, payer key
- **Related terms**: Submitted Proof Bundle, Paykit Payment Verifier, Content Viewer

### Paykit Server
- **Definition**: Standalone payment service configured under Lock Server `[paykit]`; Locks calls `POST /invoices` and `POST /transactions/status` with Lock-Server-signed requests.
- **NOT**: Lock Server private runtime state, Locks-owned access decision logic, or creator-owned Pubky data.
- **Synonyms to AVOID**: payment backend, wallet server, invoice server
- **Related terms**: Paykit Payment Verifier, Lock Server, Reader Public Key

### Dev Static Verifier
- **Definition**: Non-production verifier used by the first implementation slice in dev/test runtime mode; it reads `params.satisfied: true/false` from a criterion and requires no meaningful submitted proof. Production-mode workers do not register it.
- **NOT**: A production lock type or real proof verifier.
- **Synonyms to AVOID**: mock verifier, test lock type, static lock
- **Related terms**: Verifier Type, Criterion

### Verified At
- **Definition**: Timestamp recording when a submitted proof bundle was successfully verified.
- **NOT**: Access credential expiry time or entitlement revocation time.
- **Synonyms to AVOID**: created at, checked at, validation time
- **Related terms**: Verification Result, Verified Proof Bundle

---

## Entitlement Context

### Verified Proof Bundle
- **Definition**: Creator-guarded record stored after successful verification, containing minimal criterion-level verification result evidence and a content lock path for a submitted proof bundle.
- **NOT**: Raw proof storage by default, an access credential, or a Lock Server private database record.
- **Synonyms to AVOID**: stored proof, verified bundle, proof record
- **Related terms**: Entitlement Record, Bundle ID, Verification Result, Content Lock Path

### Entitlement Record
- **Definition**: The access-eligibility meaning of a verified proof bundle stored under the creator's guarded Locks path.
- **NOT**: A short-lived access credential.
- **Synonyms to AVOID**: access token record, permission record, authorization record
- **Related terms**: Verified Proof Bundle, Access Credential, Revocation

### Entitlement Lifetime
- **Definition**: Lock-type-specific rule describing how long an entitlement remains usable.
- **NOT**: The TTL of a Lock-Server-issued access credential.
- **Synonyms to AVOID**: token lifetime, expiry, session duration
- **Related terms**: Entitlement Record, Access Credential

### Revocation
- **Definition**: Creator action or content lock file removal that prevents an existing entitlement from being honored in the future; direct entitlement revocation is performed by deleting the verified proof bundle.
- **NOT**: Automatic invalidation caused by creating a new content lock file.
- **Synonyms to AVOID**: deletion, cancellation, invalidation
- **Related terms**: Entitlement Record, Content Creator

---

## Access Context

### Access Credential
- **Definition**: Reusable-until-expiry opaque bearer credential issued by the Lock Server after successful verification or after resolving an existing entitlement record; server-side state resolves it to creator and Bundle ID.
- **NOT**: A durable entitlement record or the Bundle ID.
- **Synonyms to AVOID**: entitlement token, proof token, permanent access token
- **Related terms**: Entitlement Record, Proxy Read

### Credential TTL
- **Definition**: Requested lifetime for an access credential, specified by the content lock access policy and rejected by the Lock Server if above its configured maximum.
- **NOT**: Entitlement lifetime or native Pubky session lifetime.
- **Synonyms to AVOID**: token lifetime, entitlement expiry, session duration
- **Related terms**: Access Credential, Access Policy, Entitlement Lifetime

### Proxy Read
- **Definition**: Lock Server read of guarded content from the creator homeserver followed by passing the response to an authorized viewer.
- **NOT**: Direct viewer access to the creator's guarded path.
- **Synonyms to AVOID**: proxy pass, fetch, relay
- **Related terms**: Lock Server, Guarded Resource, Access Credential

---

## Implementation Context

### Protocol Payload
- **Definition**: Top-level JSON payload exchanged or persisted by Locks, versioned at the top level and owned by `locks-core` when it represents shared domain/protocol data.
- **NOT**: Lock Server private runtime state or an unversioned nested object.
- **Synonyms to AVOID**: DTO, wire object, data model
- **Related terms**: Content Lock, Submitted Proof Bundle, Verified Proof Bundle

### Lock Server Runtime State
- **Definition**: Lock-Server-private operational state needed to run the service, such as verification task rows, task claims, attempt metadata, and access credential lookup records.
- **NOT**: Creator-owned content locks, guarded resources, verified proof bundles, or any canonical Pubky-owned domain resource.
- **Synonyms to AVOID**: domain state, Pubky data, dev storage
- **Related terms**: Verification Task, Verification Task Claim, Access Credential

### Canonical JSON
- **Definition**: RFC 8785/JCS-compatible JSON serialization used as the hash input for content locks; the Rust implementation uses `serde_json_canonicalizer`.
- **NOT**: Pretty-printed JSON, insertion-order JSON, or service-local serialization.
- **Synonyms to AVOID**: stable JSON, sorted JSON
- **Related terms**: Lock Hash, Lock ID, Protocol Payload

### Protocol Timestamp
- **Definition**: RFC3339 JSON timestamp represented with `time::OffsetDateTime` in Rust protocol/domain payloads.
- **NOT**: Local time, naive datetime, Unix timestamp by default, or `chrono`-specific type.
- **Synonyms to AVOID**: datetime string, timestamp field
- **Related terms**: Verified At, Protocol Payload

### Workspace Member
- **Definition**: Top-level Cargo workspace package in the Locks repository, such as `locks-core` or `locks-service`.
- **NOT**: A nested `crates/` package or placeholder for an undefined future product.
- **Synonyms to AVOID**: crate folder, package module, component directory
- **Related terms**: Protocol Payload, Lock Server

---

## Pubky Integration Context

### Capability Grant
- **Definition**: Creator authorization allowing the Lock Server to read and/or write specific Pubky paths on the creator's homeserver; creator-facing grant UX is handled by Pubky Ring.
- **NOT**: Ownership transfer or blanket trust unless the grant is broad.
- **Synonyms to AVOID**: permission, scope, grant token
- **Related terms**: Lock Server, Guarded Path, Public Lock Path

### Creator Authority
- **Definition**: Creator-granted homeserver authority held by the Lock Server for Locks public and private namespaces, currently stored as encrypted runtime state and used for Lock Server -> creator homeserver Pubky I/O.
- **NOT**: pubky.app's own homeserver session, a frontend session, a viewer access credential, or manual operator provisioning.
- **Synonyms to AVOID**: creator login, lock-server login, operator session, homeserver token
- **Related terms**: Capability Grant, Creator Authority Acquisition, Native Session Secret, Frontend Session

### Creator Authority Acquisition
- **Definition**: Lock-Server-owned process for obtaining or refreshing creator-granted homeserver authority for Locks public and private namespaces.
- **NOT**: pubky.app's own homeserver login, viewer access, iframe UI, or manual operator provisioning.
- **Synonyms to AVOID**: creator login, lock-server login, auth iframe, operator session import
- **Related terms**: Creator Authority, Frontend Session, Capability Grant, Lock Server

### Native Session Secret
- **Definition**: Persisted bearer secret used by the Lock Server to maintain creator authority; current expected session lifetime is 6 months.
- **NOT**: A frontend session, viewer access credential, Bundle ID, or entitlement record.
- **Synonyms to AVOID**: JWT, grant token, access token
- **Related terms**: Creator Authority, Capability Grant, Lock Server

### Guarded Path
- **Definition**: Creator homeserver path requiring authorized access, used for guarded resources and verified proof bundles.
- **NOT**: Public `/pub` data.
- **Synonyms to AVOID**: private path, protected path, locked path
- **Related terms**: Guarded Resource, Verified Proof Bundle

### Public Lock Path
- **Definition**: Public creator homeserver path where content locks and Locks configuration are published.
- **NOT**: A location for bearer secrets or entitlement records.
- **Synonyms to AVOID**: lock url, policy path, public policy path
- **Related terms**: Content Lock, Lock Service Pointer

### Credible Exit
- **Definition**: Ability for a creator and viewer to migrate from one Lock Server to another using creator-owned public configuration and guarded entitlement records, without relying on the old Lock Server's private database.
- **NOT**: Full trustlessness; the active Lock Server remains a trusted proxy.
- **Synonyms to AVOID**: migration, portability, service switching
- **Related terms**: Lock Service Pointer, Bundle ID, Verified Proof Bundle

---

## Forbidden Lexicon

Avoid these in domain-bearing names unless quoted from an external spec:

- `manager`
- `helper`
- `util`
- `data`
- `info`
- `record` except `Entitlement Record`
- `model`
- `entity`
- `DTO` in domain code
- `lock file` when the concept is `Content Lock`
- `protected content` when the concept is `Guarded Resource` or `Guarded Content`
- `proof id` when the concept is `Bundle ID`
- `entitlement token` when the concept is `Access Credential`

## Unresolved Terms

### Lock Type
- **Status**: Intentionally deferred.
- **Working meaning**: A category of criterion with its own proof format and verification behavior. Current model should remain lock-type agnostic.
