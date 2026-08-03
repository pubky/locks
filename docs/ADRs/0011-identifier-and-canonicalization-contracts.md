# ADR 0011: Identifier and Canonicalization Contracts

## Status

Accepted

## Context

The workspace skeleton is in place and `locks-core` implementation will start with value objects and protocol payloads. Before writing TDD tests, the project needs exact text-format contracts for identifiers, content lock paths, Pubky identity wrappers, canonical JSON, and protocol timestamps.

These details affect wire compatibility, path safety, test vectors, and SDK behavior.

## Decision

### Lock ID

`LockId` is derived from the full 32-byte BLAKE3 hash of the canonical serialized content lock payload.

Encoding and parsing rules:

- Rust `base32` crate, Crockford alphabet.
- Fixed length: 52 characters.
- No prefix.
- No Crockford checksum.
- Canonical form is uppercase output from `base32::encode`.
- Parsing uses `base32::decode` with the Crockford alphabet, including that crate's built-in lowercase and ambiguous-character normalization.
- Parsing rejects hyphens/readability separators.
- Display and serialization emit canonical uppercase form.

### Bundle ID

`BundleId` is a 128-bit cryptographically random viewer-generated bearer secret.

Encoding and parsing rules:

- Rust `base32` crate, Crockford alphabet.
- Fixed length: 26 characters.
- No prefix.
- No Crockford checksum.
- Canonical form is uppercase output from `base32::encode`.
- Same `base32::decode` parsing and normalization behavior as `LockId`.
- Rejects hyphens/readability separators.
- Display and serialization emit canonical uppercase form.
- Safe as a path segment after validation.

### Task ID

`TaskId` is a server-generated UUID v4.

Rules:

- Serialized as canonical lowercase hyphenated UUID string.
- Operational identifier only.
- Not a bearer secret.
- Not a durable recovery handle.
- Not used for credible exit.
- Identifies short-lived verification task state retained for a few hours.

### Pubky identity wrappers

`CreatorPubky` and `LockServerPubky` are domain role wrappers around Pubky public-key identities.

Rules:

- Parsing delegates validation to `pubky::PublicKey`.
- Display and serialization store the canonical `pubky<z32>` rendering.
- Bare z32 public keys may be accepted by the underlying Pubky parser, but Locks emits the `pubky`-prefixed form.

### Content lock path

`ContentLockPath` is a creator-homeserver-relative path.

Rules:

- Must be exactly `/pub/locks.app/<lock_id>.json`.
- Not a full Pubky URL.
- Not a homeserver URL.
- Not any other `/pub/...` path.
- Embedded `<lock_id>` must parse as a valid `LockId`.
- Display and serialization normalize the embedded Lock ID to canonical uppercase form.

### Pubky lock resource

`PubkyLockResource` is the protocol-facing addressed Pubky resource for a public content lock.

Rules:

- Must be exactly `pubky<creator_pubky>/pub/locks.app/<lock_id>.json`, matching the preferred `PubkyResource` identifier form from the `pubky` crate.
- Must not use the alternate `pubky://...` form in Locks protocol payloads.
- Must not be an HTTP(S) homeserver transport URL.
- Embedded creator must parse as `CreatorPubky`.
- Embedded path must parse as `ContentLockPath`.
- Display and serialization normalize the embedded content lock path while preserving the creator Pubky identity value.

### Canonical JSON and timestamps

Content lock hashing uses RFC 8785/JCS-compatible canonical JSON.

- Rust implementation uses `serde_json_canonicalizer`.
- Hash input is the canonical JSON bytes of the serialized content lock payload.
- Lock ID and lock hash are derived values, not serialized content lock fields.

Protocol timestamps use `time::OffsetDateTime` and serialize as RFC3339 JSON strings. Do not use `chrono` unless a later ADR revisits this.

## Consequences

- `base32` crate behavior is part of the implementation contract and should be covered by tests for lowercase, ambiguous Crockford characters, and hyphen rejection.
- `BundleId` and `TaskId` are visibly distinct wherever both appear internally; public HTTP API payloads use the `{ creator, bundle_id }` verification handle and do not expose `TaskId`.
- Content lock paths have one canonical creator-relative shape and do not accept arbitrary Pubky URLs.
- Pubky lock resources have one canonical protocol-facing shape: `pubky<creator_pubky>/pub/locks.app/<lock_id>.json`.
- `locks-core` relies on `pubky::PublicKey` for Pubky identity parsing instead of reimplementing syntax checks.
- Canonical JSON and timestamp behavior are explicit protocol contracts, not incidental implementation choices.
