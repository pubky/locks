# ADR 0008: v0 Access Credential Lifecycle

## Status

Accepted

## Context

Access credentials must remain separate from durable entitlement records. The Bundle ID is the durable viewer-held recovery handle and entitlement anchor. The access credential is a Lock-Server-issued convenience credential for proxy access.

The first implementation needs a concrete lifecycle so application services, in-memory stores, and later HTTP routes can be modeled without waiting on Pubky-Core questions.

## Decision

For v0, an access credential is a reusable-until-expiry opaque bearer credential.

The Lock Server stores credential state server-side. The credential resolves to at least:

```text
{ creator, bundle_id }
```

The credential does not duplicate-bind to Lock ID or guarded resource hash. Those are reached by resolving the verified proof bundle anchored by Bundle ID and then reading the referenced content lock file.

The default requested credential TTL is 15 minutes. Longer TTLs, including TTLs measured in days, are allowed by the model.

The requested TTL is part of the content lock's access policy. If the requested TTL exceeds the Lock Server's configured maximum, the Lock Server rejects explicitly. It does not silently clamp.

Access credential validation must re-check the underlying entitlement state, including whether the verified proof bundle still exists and whether the corresponding content lock file is still valid.

## Consequences

- Leaked credentials are usable until expiry, so TTL policy matters.
- Expired credentials can be replaced by resolving the durable entitlement through Bundle ID.
- A Lock Server may refuse to serve a content lock whose access policy exceeds its safety limits.
- Changing access policy TTL changes the content lock hash and therefore creates a new Lock ID.
