# Locks Open Questions

This file tracks only unresolved decisions before production implementation. Confirmed product decisions belong in `docs/DOMAIN_MODEL.md`, `docs/THESAURUS.md`, ADRs, or implementation notes — not here.

## Current status

Plan 0018 SDK-backed Pubky repository runtime composition is implemented through the server binary: persisted runtime can compose `creator_repositories.backend = "pubky-homeserver"` using encrypted creator-authority records and session-scoped SDK storage. Plan 0019 implemented the production-shaped `legacy-connect` Creator Authority Acquisition JSON protocol. Plan 0024 removes the abandoned `legacy-self-relay` / `srvr/caps/sign` auth surface; `legacy-connect` is the only accepted acquisition method for now, and `method` remains as the future extension point for homeserver grant-based auth.

There are no unresolved Pubky-Core or deferred design questions blocking repository runtime composition or the backend acquisition protocol itself.

Resolved decisions from the previous open-question set are captured in:

- [`docs/DOMAIN_MODEL.md`](DOMAIN_MODEL.md)
- [`docs/ADRs/0017-creator-granted-auth-boundary.md`](ADRs/0017-creator-granted-auth-boundary.md)
- [`docs/RUNTIME.md`](RUNTIME.md)

## Next decision point

The Creator Authority Acquisition shell decision is resolved in [`docs/ADRs/0019-creator-authority-acquisition-shell.md`](ADRs/0019-creator-authority-acquisition-shell.md): current `legacy-connect` uses a Lock-Server-hosted redirect/popup page because the existing Pubky authorization URL is secret-bearing.

The self-relay auth experiment is no longer active. The compatible Ring path is the current legacy Pubky auth URL flow (`relay`, `secret`, `caps`) approved by Pubky Ring / homeserver through the encrypted relay payload. PKARR publication remains required for Lock Server discovery and is not tied to self-relay auth.

A live Pubky smoke test remains deferred until Pubky/testnet setup and credentials are available.

Resolved role-wrapper decision: keep `CreatorPubky` / `LockServerPubky` as domain role wrappers for now, with validation and canonical parsing delegated to Pubky/common public-key parsing. Revisit only if a concrete cross-crate API simplification requires replacing them.

If new Pubky-Core, pubky-app, or lock-type-specific questions arise during the next integration slice, add only the unresolved question here and move the answer into the relevant ADR/domain/runtime doc once resolved.
