# ADR 0016: Creator Publishing Uses Local Dev HTTP API and Locks-Native Spec Objects

## Status

Accepted

## Context

The first retrieval/access slices proved viewer-side verification, entitlement-backed credential issuance, and proxy-read using in-memory content-lock and guarded-resource repositories. Until Pubky-Core confirms private-path write support, private namespace rules, and event visibility semantics, production Pubky-backed creator publishing must stay behind ports.

The product still needs a creator-side local publishing flow so a creator can add guarded content and publish a content lock through public HTTP API during development and E2E testing. This flow should exercise Locks-native domain objects rather than reusing `pubky-app-specs`, whose schemas are specific to Pubky.app posts/files/follows and `/pub/pubky.app/...` paths.

## Decision

Creator publishing is a separate Creator Publishing / Lock Authoring milestone, implemented locally before real Pubky I/O.

The first local HTTP API is creator-prefixed:

```text
POST /creator/priv-resources
POST /creator/content-locks
POST /creator/lock-service-config
```

`POST /creator/priv-resources` and `POST /creator/content-locks` form the two-step guarded-resource/content-lock publishing flow. `POST /creator/lock-service-config` stores the creator default Lock Service Pointer whose canonical future Pubky path is `/pub/locks.app/config.json`.

These routes are mounted only in dev/test runtime when an explicit creator-publishing flag is enabled. Production mode must refuse to expose them. The routes are unauthenticated in the local skeleton; do not add fake shared-secret auth or admin tokens. Real creator authorization remains a future Pubky capability/session concern.

`locks-core` remains the Locks-native spec authority for now. Do not use `pubky-app-specs` objects directly. Do not create a separate `pubky-lock-specs` crate yet; keep extraction easy by putting protocol payloads, validation, path rules, canonical JSON, and hash derivation in `locks-core`.

`GuardedResource` hard-breaks in v0 to require:

```json
{
  "path": "/priv/locks.app/content/example.txt",
  "hash": "<guarded_resource_hash>",
  "content_type": "text/plain",
  "size": 5
}
```

`content_type` must parse as MIME. `size` is the exact byte length of the guarded resource and must be greater than zero. Validation lives in `locks-core::GuardedResource::new(...)`; fields may remain public for now to minimize fixture churn, but new code must use the constructor.

`POST /creator/priv-resources` accepts creator, path, MIME content type, and base64 content bytes. It computes the guarded resource hash and size, stores/replaces the current resource for `(creator, path)`, and returns the resulting guarded resource descriptor. Guarded resource mutation semantics are overwrite-by-path: uploading a new payload at the same creator/path replaces the current bytes and metadata.

`GuardedResourceRepository` stores current guarded resource metadata and bytes by `(creator, path)`. Reads for proxy access still require the expected hash from the content lock; if the current stored hash does not match, the read misses and proxy-read returns guarded-resource unavailable. This preserves hash safety while allowing creator overwrite semantics.

`POST /creator/content-locks` creates a content lock for an already-registered guarded resource. The local use case verifies the referenced guarded resource currently exists for the creator and that path, hash, content type, and size match before storing the content lock. Content lock identity remains derived from canonical JSON: identical content lock creation is idempotent, while changed content creates a different lock ID and content lock path.

`/pub/locks.app/config.json` is the creator-owned default Locks config path, represented locally by a Locks-native `LockServicePointer` spec object. The local `POST /creator/lock-service-config` route stores the creator default Lock Server pointer. Content lock creation does not require this config to exist: content locks may use `lock_server.override`; when no override is present, viewer discovery depends on the creator's Lock Service Pointer.

Proxy-read returns raw guarded resource bytes and uses the stored guarded resource `content_type` for the HTTP `Content-Type` header. Successful proxy-read remains a byte response, not JSON/base64.

## Consequences

- Local creator-to-viewer E2E can be exercised without test-only seeding endpoints.
- Creator publishing route shape becomes public development API surface, but not production API surface until creator authorization is solved.
- The default Lock Service Pointer can be authored locally without requiring every content lock to carry `lock_server.override`.
- Existing fixtures and tests must be updated for required `GuardedResource.content_type` and `GuardedResource.size`.
- Docs must distinguish implemented local creator publishing from still-deferred production Pubky-backed publishing.
- The local in-memory guarded resource repository no longer models versioned storage by `(creator, path, hash)`; it models one current resource per `(creator, path)` plus hash-checked reads.
- Older content locks can stop resolving if the creator overwrites the guarded resource path with different bytes. This is intentional for the local overwrite model and must be visible through proxy-read failure rather than stale-byte access.
- Future Pubky-backed publishing may replace local storage verification with homeserver state verification while preserving the same Locks-native spec object shape.

## Deferred

- Real creator authentication/authorization through Pubky capability grants.
- Pubky-backed writes under `/priv/locks.app/content/` and `/pub/locks.app/`.
- Multipart or streaming uploads.
- Range requests and content disposition.
- Separate published `pubky-lock-specs` crate or npm/WASM package.
