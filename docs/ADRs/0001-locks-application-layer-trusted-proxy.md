# ADR 0001: Locks is application-layer trusted proxy logic

- **Status**: Accepted
- **Date**: 2026-05-28

## Context

Locks gates access to creator-owned guarded content in the Pubky ecosystem. The product should avoid requiring homeservers to understand lock-type-specific verification such as payment, subscription, password, follower status, or time-window logic.

## Decision

Locks will be implemented as application-layer logic in a Lock Server. The Lock Server receives creator-granted authority, verifies viewer proof, stores successful entitlement records, issues access credentials, and proxy-reads guarded content.

Homeservers remain responsible for Pubky sessions, path-scoped capabilities, and storage semantics. They do not become aware of lock-type-specific verification.

## Consequences

Positive:

- Minimal disturbance to the Pubky protocol.
- Lock types can evolve independently.
- The domain model can stay application-owned.

Negative:

- The Lock Server is trusted with any guarded content it is authorized to read.
- Broad interim grants such as `/priv/:rw` are high-trust and should be avoided when narrower capabilities are available.

## Open Follow-ups

- Confirm the final Pubky Homeserver path capability model for `/priv/app.locks/`.
- Confirm whether app sessions can write verified proof bundles under `/priv/app.locks/proofs/`.
