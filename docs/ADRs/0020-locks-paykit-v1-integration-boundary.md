# ADR 0020: Locks–Paykit v1 integration boundary

- Status: Accepted
- Date: 2026-07-15
- Scope: `locks`, with a coordinated contract in `paykit-server`

## Context

The `paykit-payment` verifier requires a standalone Paykit Server to create payment requests and report factual payment observations. Locks must remain the access authority and must not acquire wallet material, Paykit delivery state, or payment-transport policy.

The integration also needs retry-safe invoice creation, creator-scoped identity, authenticated status lookup, and explicit ownership of payment-policy decisions. Those decisions are durable architecture rather than implementation-plan steps.

## Decision

### Ownership boundary

- Locks owns content-lock authoring and validation, verification-task lifecycle, confirmation-threshold policy, entitlement publication, access credentials, and private-resource access decisions.
- Paykit Server owns payment-request creation and delivery, supported assets and base-unit semantics, payment endpoints, wallet/watch-only material, and factual payment observation.
- Locks stores no xpub, wallet, address-derivation, Electrum, Encrypted-Link, or Paykit outbox state.
- Paykit Server never grants Locks entitlements, issues Locks access credentials, reads private locked content, or returns an access decision.

### Payment criterion

The v1 content-lock criterion has verifier wire value `paykit-payment` and params exactly:

```json
{
  "recipient_pubky": "pubky<creator>",
  "amount": "50000",
  "asset": "BTC"
}
```

- `recipient_pubky` must equal the canonical content-lock creator.
- `amount` is a positive decimal integer string in the asset's base unit.
- `asset` is an opaque, non-empty string to Locks. Paykit Server owns deployment-specific asset support and base-unit interpretation.
- V1 permits exactly one payment criterion, referenced exactly once by the lock logic, and exactly one submitted payment proof.
- The submitted payment proof payload is `{}`. `reader_public_key` is top-level submission data.
- Content-lock authoring does not require runtime Paykit configuration or availability.

### Locks submission lifecycle

`{ creator, bundle_id }` is a permanent, one-attempt Locks lifecycle identity. Submission processing occurs in this order:

1. apply rate limiting;
2. validate proof shape;
3. load and validate the current canonical Lock Resource and payment policy;
4. resolve the current reader through Pubky discovery;
5. compare any persisted lifecycle under `{ creator, bundle_id }`;
6. reject changed submitted proof material with `409 task_state_conflict`;
7. require configured Paykit and call its idempotent invoice endpoint only for new payment submissions; and
8. insert the verification task with post-invoice race reconciliation.

Exact persisted payment replay returns the stored lifecycle without calling Paykit or repeating invoice side effects. Changed replay does not call Paykit. Clients needing another attempt must generate a new Bundle ID. Live Noise observation uses the dedicated task-bound lookup below.

### Invoice request

For a new lifecycle identity, Locks sends RFC 8785 canonical JSON to `POST /invoices`:

```json
{
  "bundle_id": "<reader-generated bundle id>",
  "lock_resource": "pubky<creator>/<canonical lock path>.json",
  "reader": "pubky<reader>"
}
```

Locks signs the exact canonical body bytes with its existing Ed25519 keypair and sends the unpadded-base64url signature in `X-Paykit-Signature`.

The durable Paykit invoice identity is `(creator, bundle_id)`, where Paykit derives `creator` from `lock_resource`. Exact proof submission replay returns the persisted verification lifecycle without calling Paykit. A different binding under the same identity returns Paykit `409 Conflict`, which Locks maps to `409 task_state_conflict`. Invoice creation and Noise state observation are separate operations.

For connection observation, the Reader sends its existing `{ creator, bundle_id }` task handle to Locks `POST /paykit-connection-state-lookups`. Locks loads the persisted task, rejects non-payment tasks, then sends signed canonical `{ creator, bundle_id }` to Paykit `POST /connections/status`. Paykit resolves its persisted invoice binding and returns exactly one local state: `none`, `handshake`, `connected`, `recovery_required`, or `blocked`. Locks rejects missing, malformed, unknown, or extra successful response fields as `502 paykit_connection_state_unavailable`. Browser callers cannot probe arbitrary peer keys or receiver paths.

### Status request and access policy

Locks sends RFC 8785 canonical JSON to `POST /transactions/status`:

```json
{
  "creator": "pubky<creator>",
  "bundle_id": "<reader-generated bundle id>"
}
```

The status body uses the same `X-Paykit-Signature` authentication as invoice creation. The only v1 factual statuses are:

- `undetected`;
- `detected`; and
- `confirmed`.

The response also contains non-negative `confirmations` and `amount_matched`. Paykit reports those facts; Locks alone applies `minimum_confirmations` and decides whether access is satisfied.

V1 invoices have no local TTL or `expires_at`. Paykit lifecycle reconciliation later added terminal `cancelled` for canonical SDK rejection/cancellation and terminal `expired` for canonical proposal expiry. Every status-call transport, timeout, HTTP, authentication/authorization, protocol, tuple-validation, and decoding failure still leaves verification pending and schedules durable retry. This includes `404` and malformed successful responses.

### Runtime boundary

- New payment submissions require `[paykit]`; exact replay returns persisted lifecycle without calling Paykit. Dedicated connection-state lookup requires `[paykit]`.
- Paykit HTTP connect timeout is 5 seconds and whole-request timeout is 20 seconds.
- An enabled in-process Paykit worker requires `claim_timeout_seconds > 20`.
- `worker.poll_interval_ms` must be greater than zero whether the worker is enabled or disabled.
- Configured Paykit `server_url` is a canonical exact HTTP(S) origin without credentials, path, query, fragment, or trailing slash; endpoint paths are appended only after validation.

## Consequences

### Positive

- Access authority remains inside Locks.
- Payment transport and asset policy remain inside Paykit.
- Creator-scoped invoice identity supports tenant isolation and Bundle ID reuse across creators.
- Durable idempotency makes ambiguous and concurrent invoice submission recoverable.
- Status failures cannot incorrectly become permanent payment denials.

### Negative and risks

- Both services must implement the same canonical-body signing contract.
- Paykit must parse the public Locks payment criterion and therefore depends on its versioned shape.
- Exact submission replay intentionally performs current canonical lock and reader preflight before returning persisted lifecycle state.
- Unpaid invoices and pending Locks tasks have no protocol expiry in v1 and therefore require operational retention policy outside the payment-status contract.

## Rejected alternatives

- **Let Paykit decide access:** rejected because payment observation is not entitlement policy.
- **Use globally unique Bundle IDs:** rejected because the durable identity is creator-scoped.
- **Trust caller-supplied payment terms:** rejected because terms come from the canonical Lock Resource.
- **Use unsigned or bundle-only status lookup:** rejected because it is unauthenticated and ambiguous across creators.
- **Terminalize status transport/protocol failures:** rejected because those failures are not payment facts.
- **Store wallet or xpub material in Locks:** rejected because Paykit owns payment transport and derivation.

## Related records

The Paykit-owned counterpart is `paykit-server/docs/ADRs/0003-locks-integration-v1-contract.md`. That record is self-contained for Paykit Server implementation and verification.
