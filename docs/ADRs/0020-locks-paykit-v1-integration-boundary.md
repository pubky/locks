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
  "asset": "BTC",
  "payment_in": 24
}
```

- `recipient_pubky` must equal the canonical content-lock creator.
- `amount` is a positive decimal integer string in the asset's base unit.
- `asset` is an opaque, non-empty string to Locks. Paykit Server owns deployment-specific asset support and base-unit interpretation.
- `payment_in` is a required, nonzero JSON `u64` number of whole hours in Locks policy.
- V1 permits exactly one payment criterion, referenced exactly once by the lock logic, and exactly one submitted payment proof.
- The submitted payment proof payload is `{}`. `reader_public_key` is top-level submission data.
- Content-lock authoring does not require runtime Paykit configuration or availability.

### Price denomination and payment methods

The criterion's `asset` and `amount` describe the requested value, not the required
payment method. Creator UIs offer BTC prices in satoshis or USD prices in cents.
Locks preserves that criterion unchanged. Paykit Server reads it from the canonical
Lock Resource and builds the request from the creator's enabled receiving methods:

| Price | Bitcoin enabled | Arbitrum USDT enabled |
| --- | --- | --- |
| BTC | Exact BTC amount, no conversion | Fixed BTC-to-USDT quote |
| USD | Fixed USD-to-BTC quote | Fixed 1:1 USD-to-USDT quote |

Without Paykit Server's optional `[usdt]` configuration, setup requests only the Bitcoin account and invoices offer only Bitcoin. Choosing a USD price does not request a USDT address.

A disabled receiving method is omitted from the request. Paykit owns rate selection,
quote expiry and exact amount verification. Locks does not fetch exchange rates or
compare a payment asset to the criterion's denomination; it requires a full on-time
payment on one accepted method using Paykit's authenticated invoice status.

### Locks submission lifecycle

`{ creator, bundle_id }` is a permanent, one-attempt Locks lifecycle identity. Submission processing occurs in this order:

1. apply rate limiting;
2. validate proof shape;
3. compare any durable lifecycle under `{ creator, bundle_id }` before mutable Lock Resource lookup;
4. return an exact persisted replay or reject changed submitted proof material with `409 task_state_conflict`;
5. only for a genuinely new identity, load and validate the current canonical Lock Resource and payment policy;
6. require configured Paykit and atomically persist a pending verification task plus immutable invoice-admission intent under the per-lock deletion/admission fence, before Reader homeserver or Paykit I/O;
7. return the pending lifecycle immediately; and
8. let a separately claimed invoice-admission worker create or idempotently replay the Paykit invoice, apply durable retry or terminal-failure policy, then mark successful admission ready so the verification worker may claim it.

Exact persisted replay does not call Paykit and does not require configured Paykit; browser clients poll the existing lifecycle rather than resubmitting to drive invoice admission. Worker retry uses the persisted intent and stable task UUID as `X-Request-ID`. Changed replay conflicts without calling Paykit. Terminal lifecycle state is not restarted under the same identity; clients needing another attempt must generate a new Bundle ID. Live Noise observation uses the dedicated task-bound lookup below.

### Invoice request

For a claimed invoice-admission task, Locks sends RFC 8785 canonical JSON to `POST /invoices`:

```json
{
  "bundle_id": "<reader-generated bundle id>",
  "lock_resource": "pubky<creator>/<canonical lock path>.json",
  "reader": "pubky<reader>"
}
```

Locks signs `b"paykit-http-signature-v1\0" + uppercase_method + b"\0" + exact_query_free_path + b"\0" + exact_canonical_body` with its existing Ed25519 keypair and sends the unpadded-base64url signature in `X-Paykit-Signature`. Body-only signatures are not supported.

The durable Paykit invoice identity is `(creator, bundle_id)`, where Paykit derives `creator` from `lock_resource`. Exact replay must return the original success without repeating mutable lookups, allocation, address creation, or delivery side effects. Success is exactly `200 OK` with a closed body containing RFC 3339 `invoice_created_at` and `payment_deadline`; Locks validates both timestamps and requires `payment_deadline > invoice_created_at` before marking admission ready. Transport failures, invalid or ambiguous success responses, typed Reader setup or registry failures, `429`, and `5xx` responses remain worker-owned durable retries within the DB-owned admission deadline. Paykit conflicts and other terminal admission errors make the pending lifecycle fail with a viewer-safe message; they are not synchronous `POST /proof-bundles` errors.

For connection observation, the Reader sends its existing `{ creator, bundle_id }` task handle to Locks `POST /paykit-connection-state-lookups`. Locks loads the persisted task, rejects non-payment tasks, then sends signed canonical `{ creator, bundle_id }` to Paykit `POST /connections/status`. Paykit resolves its persisted invoice binding and returns exactly one local state: `none`, `handshake`, `connected`, `recovery_required`, or `blocked`. Locks rejects missing, malformed, unknown, or extra successful response fields as `502 paykit_connection_state_unavailable`. Browser callers cannot probe arbitrary peer keys or receiver paths.

### Status request and access policy

Locks sends RFC 8785 canonical JSON to `POST /payment-requests/status`:

```json
{
  "creator": "pubky<creator>",
  "bundle_id": "<reader-generated bundle id>"
}
```

The status body uses the same `X-Paykit-Signature` authentication as invoice creation. It is closed and keeps canonical axes separate: `request_state` is `proposed`, `proposal_expired`, `accepted`, `rejected`, `canceled`, `proof_submitted`, or `active_recurring`; `payment_state` is `undetected`, `detected`, `confirmed`, or `expired`. The response also contains `invoice_created_at`, `payment_deadline`, and nullable `bitcoin` / `usdt_arbitrum` observations, each with `confirmations`, `amount_matched`, and `paid_on_time` (USDT additionally has `finalized`). Locks requires a full on-time payment on one rail, applies `minimum_confirmations` only to Bitcoin, and accepts USDT after verified Arbitrum inclusion. Paykit verifies the payment; Locks decides access.

`rejected`, `canceled`, `proposal_expired`, and accepted plus payment `expired` terminalize the current Locks attempt as `expired` with the corresponding typed reason. They issue no entitlement and do not retry the same task, even when confirmed payment facts exist. Transport failures, timeouts, response-body read failures, and non-`200` responses remain no-entitlement and pending for durable retry; `409 Conflict` is operator-visible rather than reclassified as payment rejection. A `200` response with malformed JSON, missing required or unknown fields, unknown states, an oversized body, or invalid or misordered timestamps is a permanent contract failure: Locks terminalizes the attempt as `failed` with a viewer-safe failure message and no entitlement. Both `failed` and `expired` require a new Bundle ID for another attempt. Locks validates timestamp syntax and requires `payment_deadline > invoice_created_at`, but does not compare either timestamp to its local clock.

### Runtime boundary

- A genuinely new payment lifecycle requires `[paykit]`; every exact persisted replay does not. Dedicated connection-state lookup also requires `[paykit]`.
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
- Retryable status availability failures cannot incorrectly become permanent payment denials.

### Negative and risks

- Both services must implement the same canonical-body signing contract.
- Paykit must parse the public Locks payment criterion and therefore depends on its versioned shape.
- Snapshotted `invoice_pending` tasks must resolve before deletion may start the Paykit drain.
- Terminal payment attempts require a fresh Bundle ID for retry; payment evidence remains owned and queryable by Paykit.

## Rejected alternatives

- **Let Paykit decide access:** rejected because payment observation is not entitlement policy.
- **Use globally unique Bundle IDs:** rejected because the durable identity is creator-scoped.
- **Trust caller-supplied payment terms:** rejected because terms come from the canonical Lock Resource.
- **Use unsigned or bundle-only status lookup:** rejected because it is unauthenticated and ambiguous across creators.
- **Terminalize status transport or availability failures:** rejected because those failures are not payment facts.
- **Retry malformed or unsupported status contracts:** rejected because retrying the same invalid contract under the same Bundle ID cannot establish entitlement safely.
- **Store wallet or xpub material in Locks:** rejected because Paykit owns payment transport and derivation.

## Related records

The Paykit-owned counterpart is `paykit-server/docs/ADRs/0003-locks-integration-v1-contract.md`. That record is self-contained for Paykit Server implementation and verification.
