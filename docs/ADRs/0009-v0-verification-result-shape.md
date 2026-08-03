# ADR 0009: v0 Verification Result Shape

## Status

Accepted

## Context

A verified proof bundle must contain enough evidence to honor an entitlement after successful verification, including during credible exit to another Lock Server. At the same time, v0 should not store raw proof material or lock-type-specific private details in creator-owned guarded entitlement storage.

Locks must also support content locks with multiple criteria, where each criterion may be verified at a different time and possibly by a different verifier type.

## Decision

For v0, `VerificationResult` is minimal criterion-level entitlement evidence, not independently re-verifiable audit evidence.

A successful verified proof bundle stores only criterion results necessary to satisfy the content lock's lock logic. Failed criterion attempts are not stored in the entitlement record.

Each criterion verification result includes:

- `criterion_id`
- `satisfied`
- `verified_at`
- `verified_by`
- `verifier_type`

`verified_by` records the Lock Server identity that produced the result. `verifier_type` records the kind of verifier used and must be protocol-facing, not an implementation module or class name. In code this is represented as the `VerifierType` enum and serialized as strings such as `dev-static`.

v0 criterion results do not include arbitrary lock-type-specific metadata. The verified proof bundle instead references the exact content lock file using `pubky_lock_resource`.

`VerifiedProofBundle` uses `pubky_lock_resource`, not `content_lock`, because the field value is the protocol-facing addressed Pubky resource: `pubky<creator_pubky>/pub/locks.app/<lock_id>.json`. To honor an entitlement, the Lock Server derives creator, content lock path, and Lock ID from this resource, reads the content lock file, and verifies that it hashes to the embedded Lock ID.

## Consequences

- v0 credible exit trusts the stored successful verification result rather than re-verifying original raw proof.
- Creator-owned entitlement storage avoids raw proof and failed attempt history by default.
- The content lock file remains necessary context for interpreting criterion IDs and lock logic.
- Missing or hash-mismatched content lock files cause the entitlement not to be honored.
