# ADR 0021: Relying-service client reference

- Status: Proposed
- Date: 2026-11-04
- Scope: `locks-core`, `locks-service`, `locks-server`, `locks-sdk`

## Context

`{ creator, bundle_id }` identifies one verification attempt, and the lifecycle lookup reports its status. A relying service that sells access per business object, for example a shop that takes one payment per order, needs more than status: it must know that a completed task was submitted for this order, under the lock terms this order priced.

Status alone cannot tell the relying service that:

- A completed bundle that the reader created for the same lock in another application can be registered against a new order. The relying service never saw that bundle, so it cannot refuse it as a duplicate.
- A bundle that a buyer completes against a cheaper lock of the same creator carries the same creator and a valid bundle ID. Only the lock resource tells it apart from the lock the order priced.

The relying service can only tell these cases apart with a value it mints itself before the reader submits, carried by the task and echoed back, plus the lock terms the task verified.

`POST /verification-task-lookups` is authorized only by the `{ creator, bundle_id }` handle, whose one secret is the bundle ID, and `POST /access-credentials` issues a content credential for the same handle once the task completes. A leaked bundle ID therefore already exposes the lock's content. The reader's identity is the one fact it does not expose today.

## Decision

### Submitted bundle

- `SubmittedProofBundle` gains optional `client_reference: ClientReference`, serialized only when present.
- `ClientReference` is 1 to 64 bytes of UTF-8 with no control characters. It is stored and echoed verbatim, never trimmed, case-folded, or otherwise interpreted.
- Bundles without the field are unchanged, including bundles already stored before the field existed.

### Replay identity and immutability

- `client_reference` is part of the submitted bundle, so exact-replay comparison covers it on every admission path (`insert_verification_task` and `insert_invoice_pending_task`). A changed, added, or removed reference under an existing `{ creator, bundle_id }` is `409 task_state_conflict`; an exact replay stays idempotent.
- Task identity (`task_id`, `creator`, `submitted_proof_bundle`, `submitted_at`) is written once at insert. Every update path writes lifecycle fields only: the PostgreSQL update statement, the in-memory single and atomic batch updates, and the in-memory claimer's transition. A stored reference cannot be rewritten whatever record a caller passes.
- Deletion fences are unchanged: a new bundle for a lock under deletion is refused before any reference is stored, and exact pre-cutoff replays still return the existing lifecycle.

### Lifecycle echo

- When the stored bundle carries a reference, submission, lookup, and dev-completion responses add `client_binding: { client_reference, pubky_lock_resource, criterion_ids }`. `criterion_ids` lists the submitted proofs' criterion IDs in submission order.
- When it does not, responses carry no `client_binding` key, so they keep the exact shape existing SDK releases parse.
- Lifecycle responses never carry `reader_public_key`.

### Relying-service rule

A relying service accepts a `completed` task for one of its objects only when `client_reference` equals the value it minted for that object and `pubky_lock_resource` and `criterion_ids` equal the terms it expects. An absent `client_binding` never matches.

### SDK and rollout

- SDK response parsers ignore unknown fields while still rejecting secret-bearing ones, and that release ships before any server that returns `client_binding`.
- The SDK that types `client_binding` (`ClientBindingResponse`) accepts responses with and without it.
- Bundles without a reference never receive the new key, so consumers that never send a reference are unaffected by server rollout order. A consumer that sends a reference must run an SDK release that parses lenient or typed `client_binding` responses, because the submission response itself carries the binding.

## Consequences

### Positive

- A relying service can bind a completed task to exactly one of its objects and to the lock terms it priced.
- The binding is enforced by data the relying service minted, not by uniqueness assumptions about bundles it never saw.
- The unauthenticated lookup reveals no reader identity, with or without a reference.

### Negative and risks

- A leaked bundle ID with a reference reveals which lock the task verifies. The same bundle ID already yields a content credential for that lock, so the lookup adds no new content exposure.
- A relying service that compares only `client_reference` leaves the cheaper-lock case open; the rule requires all three fields.

## Rejected alternatives

- **Uniqueness of bundle IDs on the relying-service side only:** rejected because it cannot refuse a bundle the relying service has never seen, such as one completed in another application for the same lock and reader.
- **Echo only `client_reference`:** rejected because a reader can pair the reference with a cheaper lock of the same creator.
- **Echo `reader_public_key` with the lock fields:** rejected because the lookup is authorized only by the `{ creator, bundle_id }` handle, and a leaked bundle ID would name the reader.
- **Return the binding only to holders of an access credential:** rejected because the credential is issued for the same bundle ID with no further authentication, so it adds no protection, and the relying service does not hold it.
- **Let Locks interpret references or orders:** rejected because order semantics belong to the relying service.

## Related records

- [ADR 0005](0005-bundle-id-and-verification-task-lifecycle.md) defines the Bundle ID and the one-attempt lifecycle.
- [ADR 0015](0015-verification-task-public-handle.md) defines the public `{ creator, bundle_id }` handle.
