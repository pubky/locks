# ADR 0021: Opaque Relying-Service Client Reference on Proof Bundles

## Status

Accepted

## Context

`{ pubky_lock_resource, criterion_id, reader_public_key }` binds a verification
task to a content lock and its terms, not to one instance of a relying service's
business object. A relying service that sells access per order needs to prove
that a Locks task was paid for exactly one order instance. Without a
per-instance binding, a buyer who completed an identical bundle earlier (same
lock, same reader, same terms) could register that old completed task against a
new order for the same item.

Locks must not grow per-merchant semantics: it does not know what an order,
payment, or instance is, and any interpreted field would drag Locks into
relying-service business logic.

## Decision

Accept an optional opaque `client_reference` on `SubmittedProofBundle`, persist
it immutably with the verification task, and echo it on the public lifecycle
lookup.

### The value is inert

`ClientReference` is a newtype over `String` validated on construction and on
deserialize:

- 1..=64 bytes of UTF-8 (bytes, not characters);
- no control characters;
- no interpretation: no trimming, no case folding, no normalization.

Locks stores and compares the value exactly as submitted. Minting, meaning, and
equality checks are the relying service's responsibility; the relying service
mints the reference server-side per payment before the buyer submits and
requires exact equality at completion.

### Persistence and replay identity

The reference lives inside the existing `submitted_proof_bundle` JSONB column,
so no schema migration is required. It participates in the exact persisted
replay comparison for `{ creator, bundle_id }`: a replay with an identical
bundle stays idempotent, and a replay with a different `client_reference`
(present vs absent, or a different value) returns the existing
`409 task_state_conflict`. The stored reference never changes after insert:
both verification task repositories treat updates as lifecycle-only
(`status`, `started_at`, `completed_at`, `failure_message`) and never write
the stored bundle, so no status transition can rewrite it.

### Lifecycle lookup echo

The handle-based lifecycle view returned by `POST /proof-bundles`,
`POST /verification-task-lookups`, and `POST /verification-task-completions`
gains four fields echoed from the stored submitted bundle:

- `pubky_lock_resource`
- `criterion_ids` — the submitted proofs' `criterion_id`s in submission order
- `reader_public_key`
- `client_reference`

The possession boundary is unchanged: the `{ creator, bundle_id }` handle
remains the only lookup key, and the response still excludes `task_id`, raw
proof payloads, invoice data, entitlement evidence, and bearer credentials.

## Consequences

Positive:

- A relying service can bind a Locks verification task to exactly one instance
  of its own business object and verify that binding at completion.
- Locks stays uninterpreting: the reference carries no Locks semantics.
- No persistence migration; the reference rides the existing bundle JSONB.
- No auth boundary change: same handle, same possession model, same secret
  exclusions.

Negative:

- The request contract grows one optional field; bundles that carry it are
  pinned to it for replay (a changed reference conflicts rather than silently
  reusing the task).
- The lifecycle response grows four fields. The new SDK reads them as
  optional (`Option` / `#[serde(default)]`), so an upgraded SDK works against
  both the old and the new server. The only safe rollout order is: release
  the SDK, let consumers upgrade, then deploy the server. Pre-existing SDK
  releases keep the repository's `deny_unknown_fields` parser posture (and
  CONTRIBUTING states maintainers may change APIs without backwards
  compatibility), so they reject the new server's lifecycle responses:
  consumers on a strict pre-change SDK release must upgrade before the server
  deploys. There is no server-first order.

## Rejected alternatives

### Interpret the reference or namespace it per verifier

Rejected. Any interpreted structure pulls relying-service business logic into
Locks and breaks verifier neutrality.

### A separate mutable metadata column or side table

Rejected. The reference is part of the submitted bundle's identity; storing it
outside the bundle JSONB would invite drift between replay comparison and
stored state, and would require a migration for no benefit.

### Bind instances via new bundle IDs per order

Rejected. The buyer generates the Bundle ID, so the relying service cannot
control or predict it; it needs a value it mints itself and can compare for
exact equality.
