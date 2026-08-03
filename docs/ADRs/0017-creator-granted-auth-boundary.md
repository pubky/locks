# ADR 0017: Creator-Granted Auth Boundary

## Status

Accepted

## Context

Production Locks must use Pubky-Core homeserver auth/capability semantics rather than inventing a parallel authorization model.

The Lock Server acts as a Pubky ecosystem application. Creators grant it authority to operate under Locks-owned public and private namespaces on the creator's homeserver. Viewers do not authenticate to the creator homeserver directly for guarded access; they satisfy Locks criteria and receive Locks-issued access credentials. The Lock Server then uses its creator-granted authority to read guarded resources and write Locks records.

This makes Locks a trusted proxy: creators trust it with delegated access to Locks-owned private paths, and viewers trust it to release content only after successful verification.

## Decision

Production Locks uses one creator-granted Locks app session per creator per Lock Server. The session is reused across that creator's locks; it is not per-lock.

The required creator-granted scopes are:

```text
/pub/locks.app/:rw
/priv/locks.app/:rw
```

`/pub/locks.app/:rw` covers public Locks resources such as the Lock Service Pointer and content lock files. `/priv/locks.app/:rw` covers guarded Locks resources such as guarded content, verified proof bundles / entitlement records, and proxy reads after viewer authorization succeeds. Locks stores guarded content bytes under `/priv/locks.app/content/` and verified proof bundles under `/priv/locks.app/proofs/<bundle_id>.json`. The `/priv/locks.app/:rw` scope is sufficient for read/write/delete on all Locks private children; any Pubky SDK transport quirks stay inside infrastructure adapters rather than domain/use-case logic.

Current Pubky-Core code supports the capability syntax and `/pub/locks.app/:rw` through existing `/pub/` homeserver write authorization. The confirmed homeserver path for Locks private data is `/priv/locks.app/`, so creator-granted Locks authority must include `/priv/locks.app/:rw` for private resources. Locks depends on Pubky homeserver private storage rather than implementing a parallel guarded storage/auth path. Current Pubky-Core emits events for public file writes/deletes and supports path-prefix event filtering. Private writes under `/priv/locks.app/...` emit no public events, and private paths are non-public; Locks discovery must use public `/pub/locks.app/...` resources rather than private write events.

The first implemented creator authorization path is the existing Pubky QR/deeplink auth flow using the legacy/cookie session variant. After that legacy path is working, Locks should migrate creator authorization to the SDK grant flow (`PubkyGrantAuthFlow` / `GrantCredential`) as the durable production auth primitive. A Locks-specific UX/API wrapper is acceptable, but Pubky-Core does not need a new auth primitive for the first implementation. Manual operator provisioning and direct client submission of raw session material are not production acquisition paths.

Locks treats redirect, popup, iframe, and native pubky.app rendering as presentation shells over the same Creator Authority Acquisition state machine. The application protocol owns pending flow state, one-time code exchange, frontend sessions, and creator authority status. The first production integration contract should be JSON/API plus secret-free status; iframe HTML can be added later without changing domain or application state.

Current Pubky-Core code supports grant-backed app sessions: user-signed Grant JWS + Proof-of-Possession exchanged at `/auth/grant/session` for an opaque bearer. The SDK exposes `PubkyGrantAuthFlow`, `GrantCredential`, `PubkySession::from_grant_credential`, and export/import of durable grant secret material. Current SDK flow is deeplink + relay + homeserver exchange; a direct Locks callback endpoint would be additional product/API work rather than the shape currently dictated by `../pubky-core/`.

Creator-granted session material is secret-bearing Lock Server private runtime state. Store it in Postgres private runtime storage, encrypted at rest immediately with a server-side key supplied through environment/config secret. Do not store the encryption key in Pubky-owned resources or committed config examples. Do not store session material in Pubky-owned resources. Do not expose it through logs, debug formatting, readiness responses, error envelopes, or viewer-facing DTOs.

Current Pubky-Core SDK export/import support indicates the long-term persisted material should be the durable grant credential secret, or equivalent parts: grant JWS, PoP client secret, and homeserver public key. `GrantCredential::export_secret()` intentionally omits the short-lived bearer and can mint a fresh bearer on import; the exported value is bearer-equivalent until the underlying grant expires or is revoked. The interim legacy QR auth implementation persists legacy cookie/session secret material instead, behind the same creator-authority storage boundary. SDK refresh is proactive around bearer expiry; homeserver revocation deletes grant sessions; `PubkySession::revalidate()` can detect missing, expired, or invalidated sessions.

The first production Pubky-backed creator publishing slice has two separate authorization checks. `pubky.app/browser -> Lock Server` calls use a Locks-local frontend session token, and creator identity is derived from that session rather than trusted from request bodies. `Lock Server -> creator homeserver` writes use the creator-granted Locks app session. Production creator routes should fail clearly when the frontend session is missing/expired, when a request attempts to spoof a different creator, or when the Lock Server lacks valid creator authority for the authenticated creator. A future external-authoring mode remains possible: another creator app may pre-publish guarded content and content locks, while Locks verifies proof bundles, writes entitlement records, and proxy-reads through its creator-granted session.

Creator authority status-check UX/API semantics are resolved by the authentication layer between `pubky-app` and `lock-server`, not by a standalone Locks domain endpoint. If a creator-facing status endpoint is exposed later, it must rely on the pubky-app/lock-server authenticated context and must derive the creator from that context rather than accepting an arbitrary public key query parameter. It may return secret-free status such as authorization state, granted scopes, and expiry.

Default viewer proxy-read access requires only a Locks-issued access credential. Anonymous-compatible bearer access remains the default global model. Viewer Pubky identity is not a global access-route requirement. Future lock criteria may require Pubky viewer identity and signed requests as criterion-specific behavior; those identity-bound criteria are optional per-criterion extensions.

Locks-issued access credentials are valid only against the Lock Server that issued them. Credible exit and cross-server portability come from the durable creator-owned verified proof bundle / entitlement record. A migrated or replacement Lock Server should resolve the durable entitlement and issue its own fresh access credential.

Before returning guarded bytes to a viewer, Locks must read the public content lock and verify the proxy-read guarded bytes against the lock's guarded resource descriptor: hash, exact byte size, and MIME content type. Correctness takes priority over caching/latency in the first production implementation.

The v1 access model keeps the Lock Server as a trusted plaintext proxy. Encrypted guarded payloads, key release, or key unwrapping designs are deferred research and are not blockers for production Pubky repository wiring.

If creator-granted authority is missing, expired, revoked, or lacks sufficient capability during proxy-read or write, Locks treats it as an operational creator-authority failure, not as viewer authorization failure. Locks revalidates creator sessions lazily before Pubky writes and proxy reads. If the SDK can refresh or revalidate an expired/refreshable session, Locks attempts one refresh/revalidation before failing. Refresh/revalidation failure returns a stable secret-free operational error such as `503 creator_authority_unavailable`. Scheduled background revalidation is not required for the first implementation.

Revoking the creator-granted Locks app session stops the Lock Server from serving or writing until reauthorized, but does not by itself delete or invalidate existing verified proof bundles / entitlements. Entitlements remain valid unless manually revoked by the creator, expired by entitlement rules, invalidated by content lock removal/change, or blocked by guarded resource integrity failure.

Existing Lock-Server-local access credential records may remain until their own TTL expires. While creator authority is unavailable they cannot be used to serve bytes and should fail with creator-authority-unavailable semantics. After creator reauthorization, the same access credential may resume working if still unexpired and entitlement/integrity checks pass.


### Interim legacy-auth implementation note

Until grant-based auth is available in Pubky-Core and integrated into `pubky.app`, Locks has an interim backend authority slice for legacy cookie sessions. The application model is intentionally generic: `creator_authorities.auth_kind` currently accepts `legacy_cookie` and `grant`, and the secret-bearing value is stored as Lock-Server-private Postgres runtime state.

For the legacy-cookie implementation, crates.io `pubky` 0.9.x restores/revalidates stored session material through `PubkySession::import_secret(...)`. Browser connect-flow creation still must be Lock-Server-owned because `PubkyAuthFlow` / `AuthFlowKind::signin()` authorization URLs carry client secret material. `pubky.app` must not start that flow or forward the resulting session secret.

The implemented stable API error for missing/invalid/revoked creator authority is secret-free `503 creator_authority_unavailable`.

## Consequences

Positive:

- Locks relies on Pubky-Core homeserver auth/capability semantics instead of inventing separate creator auth.
- The grant cardinality is operationally simple: one creator-granted session per creator per Lock Server.
- The namespace-scoped grant bounds creator trust to Locks-owned public and private paths.
- Viewers can remain anonymous-compatible by default.
- Credible exit is anchored in creator-owned entitlement records, not Lock-Server-local access credentials.
- Creator authority outages are represented accurately as operational failures instead of false viewer authorization failures.

Negative:

- The Lock Server stores high-value creator-granted session material.
- `/priv/locks.app/:rw` gives Locks broad read/write authority within the Locks private namespace.
- Proxy-read integrity verification adds read/hash cost to the first production implementation.
- Creator status UX requires real Pubky-Core/Ring token validation before it can be safely exposed.

## Open Follow-ups

- Track SDK refresh/revalidation behavior as Pubky-Core evolves, but keep retry policy lazy and single-attempt unless a concrete UX/operational need appears.
