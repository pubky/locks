# Mixed Graph + Payment V2

Mixed Content Lock v2 combines one graph policy with one Paykit payment policy. It is an acceleration lane, not debt forgiveness:

1. Locks durably admits or exact-replays payment obligation before evaluator work.
2. Folded auth derives Reader server-side. Mixed submissions MUST omit wire `reader_pubky`.
3. Each evaluation wave keeps payment evidence live and eligible. Payment unavailability, conflict, ineligibility, or expiry blocks Graph bypass.
4. Timely payment threshold **or** Graph success can satisfy lock. Graph success never cancels, erases, or forgives payment.
5. Canonical `satisfied_by` values are `payment`, `zero_conf_graph`, or `[payment, zero_conf_graph]` in that order.

## Lifecycle

Mixed tasks use one deadline, revision, claim lease, CAS transition stream, publication fence, and immutable entitlement payload. Public states are `pending`, `in_progress`, `publishing_entitlement`, `completed`, `expired`, and `failed`.

At deadline, terminal precedence is:

1. payment rejected/canceled/proposal-expired/deadline-expired -> `expired`;
2. payment conflict/invalid response -> `failed`;
3. payment or Graph dependency unavailable -> `failed/verification_unavailable_by_deadline`;
4. Graph invalid response -> `failed/invalid_verifier_response`;
5. Graph false -> `expired/graph_condition_not_observed_by_deadline`.

First timely payment threshold and first true Graph observations are sticky audit evidence. Current provider status remains authoritative for payment eligibility. Evidence observed after deadline is ignored.

## Public API

Viewer:

- `POST /proof-bundles` starts folded auth for mixed v2.
- `POST /proof-bundles/auth-status` polls folded auth.
- `POST /verification-task-lookups` reads lifecycle by Creator + bundle ID.

Creator:

- `POST /creator/verification-evidence` returns closed `mode: "mixed"` evidence with both `graph` and `payment` blocks.

Exact replay returns stored state without Paykit or Nexus calls. Changed mode, lock hash, graph policy, payment policy, or bundle binding conflicts.

Rust SDK exposes `submit_mixed_proof_bundle`, `lookup_mixed_verification_task`, `parse_mixed_lifecycle_response`, and `parse_mixed_verification_evidence_response`. JS/WASM exposes `submitMixedProofBundle`, `pollMixedFoldedAuth`, and `lookupMixedVerificationTask`; `verificationEvidence` dispatches its closed parser by `mode`.

## Deletion

Mixed tasks are provider-v2 deletion obligations.

Graceful deletion freezes exact mixed, Graph, and payment evidence; completes provider drain/reconciliation before final credentials; then terminalizes unpublished work as `expired/content_lock_deleted` without rewriting roots. Completed payload is immutable. After purge, lookup returns not found.

Force deletion makes no Paykit or Nexus calls. It preserves completed work, blocks deferred publication, terminalizes unpublished work as `expired/content_lock_deleted`, preserves roots, and retains permanent replay fencing.

## Runtime dependencies

Paykit Server must support immutable provider-v2 create/status/drain contract (`v0.1.1-rc3`, commit `725a4cd6578cdc149b4f38c8577bb9937a81bbb6`). Nexus uses signed Locks HTTP client rules already documented for Graph Stage 1. Missing Nexus does not strand payment-satisfiable mixed tasks; it records Graph dependency unavailability. Missing Paykit prevents mixed admission.

## Verification

Migration `0033_mixed_graph_payment_v2.sql` is additive. Existing graph/payment/v1 snapshots and migrations `0001..0032` remain unchanged. Required release evidence includes fresh PostgreSQL 16 migration, populated `0032 -> 0033`, memory/PostgreSQL parity, worker/API/SDK tests, and protocol-real mixed Compose twice with restart/replay checks.
