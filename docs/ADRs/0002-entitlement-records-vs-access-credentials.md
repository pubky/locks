# ADR 0002: Entitlement records are distinct from access credentials

- **Status**: Accepted
- **Date**: 2026-05-28

## Context

A viewer may need durable proof that they satisfied a content lock, especially for credible exit and Lock Server migration. At the same time, content retrieval should use short-lived credentials to limit damage from bearer-token sharing.

## Decision

Locks separates durable entitlement records from short-lived access credentials.

A verified proof bundle stored under the creator's guarded Locks path is the entitlement record. It records minimal criterion-level verification result evidence and is resolved by Bundle ID.

An access credential is a reusable-until-expiry opaque bearer credential issued by the Lock Server after explicit credential issuance against a currently valid entitlement. It is not minted by read-only task polling and is not implied by verification task completion. Server-side credential state resolves to at least creator and Bundle ID.

Entitlement revocation is represented by deleting the verified proof bundle. A replacement Lock Server must not honor an entitlement if the verified proof bundle is absent.

A missing corresponding content lock file also means the entitlement should not be honored. Historical lock archives are not used for entitlement resolution. Homeserver error codes distinguish intentional absence from temporary read failure.

## Consequences

Positive:

- Credible exit does not depend on the old Lock Server's private database.
- Access credentials can be short-lived without destroying durable entitlement state.
- Access credential lifecycle details are defined in ADR 0008.
- The model can support anonymous-compatible flows.
- Revocation is simple and portable: absence of the verified proof bundle is enough.

Negative:

- There are two sensitive handles: Bundle ID and access credential.
- Implementations must not confuse entitlement lifetime with access credential TTL.
- Deletion-based revocation has limited auditability unless a separate audit mechanism is added.
- Temporary homeserver read failures must not be mistaken for deletion or content lock file removal.

## Open Follow-ups

- Define exact homeserver error handling for missing content lock file vs temporary read failure.
