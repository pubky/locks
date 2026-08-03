# ADR 0007: locks-core Owns Protocol Payload Serialization

## Status

Accepted

## Context

Locks protocol/domain payloads will be used by multiple workspace members: the Lock Server, future SDK, future admin tooling, and likely creator-facing UI code or generated bindings.

If JSON serialization is treated as an incidental service detail, Rust field renames or service implementation changes could accidentally change the protocol shape.

## Decision

`locks-core` owns shared protocol/domain payloads and their JSON serialization/deserialization.

Rules for v0 protocol/domain payloads in `locks-core`:

- Use explicit serde naming policy with snake_case field names.
- Reject unknown JSON fields.
- Include `version` on top-level persisted/protocol payloads.
- Do not include `version` on nested objects.
- Require JSON-shape tests for serialization and deserialization.
- Keep dependencies minimal and protocol/domain oriented. Accepted dependencies include `serde`, `serde_json`, `serde_json_canonicalizer`, `thiserror`, `blake3`, `base32`, `uuid`, and `time`.

Protocol timestamps use `time::OffsetDateTime` and serialize as RFC3339 JSON strings. Do not use `chrono` unless a later ADR revisits this.

`SubmittedProofBundle` is a top-level protocol payload and includes `version` even though it is not stored as an entitlement by default.

`locks-core` must not contain Lock Server runtime state, repositories, application ports, verifier traits, fake/dev verifier implementations, in-memory stores, HTTP routes, Pubky client code, database dependencies, HTTP clients, or async orchestration.

## Consequences

- `locks-core` types become the initial JSON contract source of truth.
- Future Rust refactors must preserve JSON shape unless deliberately changed.
- Unknown fields fail in v0, reducing semantic ambiguity while the protocol is young.
- Extension/compatibility mechanisms can be added later through an explicit ADR if needed.
