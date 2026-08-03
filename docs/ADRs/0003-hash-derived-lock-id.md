# ADR 0003: Lock ID is derived from the content lock hash

- **Status**: Accepted
- **Date**: 2026-05-28

## Context

Content locks are public creator-owned policy files. If a lock changes, existing entitlement semantics should not silently change underneath previously verified viewers.

The product needs a deterministic Lock ID that is safe as a Pubky path segment and identifies the exact lock version.

## Decision

The Lock ID is derived from the hash of the content lock's canonical representation.

Canonicalization and encoding:

- Canonical representation: RFC 8785/JCS-compatible canonical JSON.
- Rust implementation: `serde_json_canonicalizer`.
- Hash function: BLAKE3 over the canonical JSON bytes.
- Hash input: all serialized content lock payload fields.
- Lock ID encoding: Rust `base32` crate, Crockford alphabet, over the full 32-byte BLAKE3 hash.
- Lock ID length: fixed 52 characters.
- Lock ID spelling: encoded hash only, with no `lock_` readability prefix.
- Lock ID canonical form: uppercase output from `base32::encode`.
- Lock ID parsing uses `base32::decode` with the Crockford alphabet, including that crate's built-in lowercase and ambiguous-character normalization.
- Lock ID parsing rejects hyphens/readability separators.
- Lock ID uses no Crockford checksum.
- Public lock path: `/pub/locks.app/<lock_id>.json`.

Lock ID and lock hash are derived values, not serialized fields inside the content lock payload, to avoid circular hashing.

Changing any content lock field changes the hash, therefore creates a new content lock file with a new Lock ID.

Existing entitlements remain intact unless the verified proof bundle is deleted by the content creator or the corresponding content lock file is removed.

## Consequences

Positive:

- Lock changes are explicit.
- Entitlements can be tied to the exact lock version that was satisfied.
- A replacement Lock Server can reason about old entitlements during credible exit.
- The path format is deterministic and path-safe.

Negative:

- Canonicalization is security- and interoperability-sensitive.
- Including all fields means operational metadata changes also change Lock ID.
- Lock updates create new files rather than mutating identity in place.

## Open Follow-ups

- Add test vectors for canonical JSON, BLAKE3, Crockford-base32 Lock ID encoding, and final `/pub/locks.app/<lock_id>.json` path.
