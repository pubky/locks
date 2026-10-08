# Locks Open Questions

This file tracks only unresolved decisions before production implementation. Confirmed product decisions belong in `docs/DOMAIN_MODEL.md`, `docs/THESAURUS.md`, ADRs, or implementation notes — not here.

## Current status

Plan 0018 SDK-backed Pubky repository runtime composition is implemented through the server binary using encrypted creator-authority records and session-scoped SDK storage. Plan 0019 implemented the hosted Creator Authority Acquisition shell. Plan 0024 removed the abandoned `legacy-self-relay` / `srvr/caps/sign` surface. Production now selects `grant-connect`; `legacy-connect` remains explicit compatibility mode and is never a fallback from grant mode.

There are no unresolved Pubky integration or deferred design questions blocking repository runtime composition or the backend acquisition protocol itself.

Resolved decisions from the previous open-question set are captured in:

- [`docs/DOMAIN_MODEL.md`](DOMAIN_MODEL.md)
- [`docs/ADRs/0017-creator-granted-auth-boundary.md`](ADRs/0017-creator-granted-auth-boundary.md)
- [`docs/RUNTIME.md`](RUNTIME.md)

## Next decision point

The Creator Authority Acquisition shell decision is resolved in [`docs/ADRs/0019-creator-authority-acquisition-shell.md`](ADRs/0019-creator-authority-acquisition-shell.md): Lock Server hosts the redirect/popup shell and preserves the one-time `state` + `code` callback contract for both explicit acquisition methods.

The self-relay auth experiment is no longer active. Ring 2.0 and Bitkit use the `signin_grant` flow (`relay`, `secret`, `caps`, `cid`, `cpk`). PKARR publication remains required for Lock Server discovery and is not tied to auth relay choice.

A live Ring/Bitkit grant approval and Postgres restart/storage smoke remains deferred until device and Pubky/testnet fixtures are available. Current pinned Pubky SDK delegated signer/restore APIs are `#[doc(hidden)]`; making this server-held use case public upstream remains dependency hardening, not a protocol-design blocker.

Resolved role-wrapper decision: keep `CreatorPubky` / `LockServerPubky` as domain role wrappers for now, with validation and canonical parsing delegated to Pubky/common public-key parsing. Revisit only if a concrete cross-crate API simplification requires replacing them.

If new Pubky integration, pubky-app, or lock-type-specific questions arise during the next integration slice, add only the unresolved question here and move the answer into the relevant ADR/domain/runtime doc once resolved.
