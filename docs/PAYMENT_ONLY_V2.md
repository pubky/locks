# Payment-only Content Lock v2

Payment-only v2 supports one Bitcoin payment policy without Graph predicates. Mixed Graph+payment policies remain unsupported.

## Closed policy and submission shapes

Content Lock v2 payment mode contains exactly one `payment` object and no `graph` object:

```json
{
  "payment": {
    "recipient_pubky": "<content creator Pubky>",
    "amount": "50000",
    "asset": "BTC",
    "minimum_confirmations": 1
  },
  "verification_window_seconds": 300
}
```

Rules:

- `asset` is exactly `BTC`.
- `amount` is a non-zero canonical unsigned 64-bit decimal string.
- `recipient_pubky` equals Content Lock creator.
- `minimum_confirmations` is `0..=6`.
- `verification_window_seconds` is `60..=604800`.
- Submitted Proof Bundle v2 carries `reader_pubky`; it carries no payment proof payload.

## Viewer flow

JS/WASM exposes payment-specific methods so callers cannot accidentally select Graph response parsing:

```js
const lifecycle = await locks.viewer.submitPaymentProofBundle({
  version: 2,
  bundle_id,
  pubky_lock_resource,
  reader_pubky,
});

const current = await locks.viewer.lookupPaymentVerificationTask(
  new VerificationTaskHandleOptions(creator, bundle_id),
);
```

Rust callers use `ViewerLocks::submit_payment_proof_bundle`, `ViewerLocks::lookup_payment_verification_task`, and `ViewerLocks::parse_payment_lifecycle_response`.

Both methods use existing public routes:

- `POST /proof-bundles` with `{ "submitted_proof_bundle": ... }`
- `POST /verification-task-lookups` with `{ "creator", "bundle_id" }`

Lifecycle responses use existing `pending | in_progress | publishing_entitlement | completed | failed | expired` vocabulary. `publishing_entitlement` remains retryable until durable publication completes. Payment terminal reasons are closed to payment outcomes. Parsers ignore additive viewer-safe fields but reject known secret-bearing fields.

## Admission, provider, and deadline behavior

Locks classifies exact replay or conflict before mutable lock lookup. New admission atomically persists immutable snapshot, `evaluator_mode = payment`, and Locks-owned deadline before outbound I/O. Provider calls use signed canonical JSON:

- `POST /v2/invoices`
- `POST /v2/invoices/status`

External calls may start only while `now < deadline`. Provider evidence with `observed_at == deadline` is timely even when response arrives later. `current_sample` can decrease or disappear after reorg. First threshold-satisfying evidence is immutable; later current-sample changes cannot revoke satisfaction.

HTTP `400`, `401`, `403`, or malformed success data terminalize as `invalid_verifier_response`; `409` terminalizes as `verification_conflict`. `404`, `429`, `5xx`, and transport failures retry only until Locks deadline. Proposal expiry is distinct from Locks verification deadline.

Payment task claims, retries, snapshot writes, and publication use revision/lease fencing. Satisfied entitlement publication is durable and restart-safe. Graph-only v2 and payment v1 stay in separate evaluator lanes.

## Provider-v2 deletion

Graceful deletion requires Paykit Server `v0.1.1-rc3` or newer. Locks freezes exact payment evaluator facts, posts/replays `/payment-request-drains`, then reconciles provider-v2 invoices only after drain acknowledgment and completion. Reconciliation uses exact `/v2/invoices` replay plus `/v2/invoices/status`; it never creates a fresh invoice or uses legacy payment status. Live provider status defers deletion. Conflict plus status `404` proves absence; conflict plus a found invoice, changed binding/timestamps, malformed success, or changed Paykit authority fails closed.

Deletion cutoff blocks unpublished entitlement effects. Completed published entitlement payloads remain unchanged. Graceful purge replays `/payment-request-drain-cleanups` with durable cleanup token under live deletion claim before operational state is removed; lost responses are safe to replay. Force deletion makes no provider call, abandons provider obligation, terminalizes unpublished task as `expired/content_lock_deleted`, preserves completed task, and leaves permanent same-resource task/publication fences. External invoice may remain active after force.

Paykit Server rc3 also keeps its provider-v2 creation fence permanently after graceful cleanup.
The same canonical lock resource may replay its exact old invoice, but cannot create a fresh invoice.
Publish under a new Lock ID instead; Locks has no provider-fence reset.

## Runtime and verification

Payment-only v2 reuses existing `[paykit]` server URL and Lock Server signing key. V2 threshold comes from immutable lock policy; existing `[paykit].minimum_confirmations` remains v1-only.

Migration `0031_payment_only_v2.sql` adds explicit `graph | payment` evaluator mode plus payment snapshot constraints/indexes. Run DB-backed tests against PostgreSQL 16.

Generated browser package verification:

```bash
npm --prefix locks-sdk/bindings/js run test
npm --prefix locks-sdk/bindings/js pack --dry-run
```

Repository verification:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
```

Deterministic provider-v2/PostgreSQL flow: [`PAYMENT_ONLY_V2_COMPOSE_E2E.md`](PAYMENT_ONLY_V2_COMPOSE_E2E.md).
