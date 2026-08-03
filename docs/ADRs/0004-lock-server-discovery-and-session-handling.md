# ADR 0004: Lock Server discovery and creator session handling

- **Status**: Accepted
- **Date**: 2026-05-28

## Context

Viewers need to discover which Lock Server handles a creator's content locks. The Lock Server also needs creator-granted authority to read guarded resources and write verified proof bundles.

The design considered whether Lock Server discovery should use creator-owned public config or a property specified on the corresponding content lock.

## Decision

The default Lock Server pointer stays in creator-owned public config:

```text
/pub/locks.app/config.json
```

It can be overridden by a Lock Server location specified in a concrete content lock using this nested field shape:

```json
{
  "lock_server": {
    "override": "pubky<lock_server_z32>"
  }
}
```

Absence of `lock_server.override` means viewers use the default Lock Server pointer from `/pub/locks.app/config.json`.

Because all serialized content lock payload fields participate in the lock hash, changing `lock_server.override` creates a new Lock ID.

A Lock Server is Pubky-addressed and uses the same `_pubky.<raw_z32>` transport mapping as homeserver resources.

The current external app auth flow is considered production-ready for external services such as a Lock Server. The expected session lifetime and renewal model is 6 months. The Lock Server should persist native session secrets. Creator-facing UX for broad guarded capability grants is handled by Pubky Ring.

## Consequences

Positive:

- Creator migration remains a public config update instead of a PKDNS/Pkarr change.
- Viewers have one default place to resolve the creator's Lock Server.
- Per-lock override is available as an escape hatch and is implemented in v0 resolution logic.
- Lock Server addressing follows the existing Pubky transport model.
- Implementation can rely on native session persistence rather than inventing a separate service grant mechanism.

Negative:

- Persisted native session secrets are high-value bearer secrets and need secure server-side storage.
- Six-month sessions require renewal and expiry handling.
- Broad guarded grants still require careful UX and trust framing, even if Pubky Ring handles the grant UI.
- Per-lock override is not the default migration mechanism because changing it creates a new content lock file and makes migration proportional to the number of overridden locks.

## Open Follow-ups

- Define secure storage requirements for native session secrets.
- Define renewal behavior before a 6-month session expires.
- Confirm private path write support and namespace with Pubky-Core.
