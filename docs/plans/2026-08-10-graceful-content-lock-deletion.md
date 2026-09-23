# Graceful Content-Lock Deletion and Payment Deadline Implementation Plan

> **For Hermes:** Use subagent-driven-development to implement this plan one review-gated commit slice at a time. Stop after each slice; the user commits before the next slice.

**Goal:** Add a bounded `paykit-payment` deadline and creator-authorized graceful content-lock deletion that withdraws the public lock immediately and drains accepted payment and access obligations durably. Graceful cleanup never deletes concurrent Pubky replacements, but tombstone publication has the explicitly accepted non-atomic overwrite limitation recorded below.

**Architecture:** Locks owns the public tombstone, admission cutoff, verification tasks, credentials, guarded content, path ownership, and overall deletion job. Paykit Server owns invoice timestamps, Payment Request lifecycle classification, cancellation, Bitcoin observation, and a durable lock-wide payment drain. PostgreSQL stores retryable Locks workflow state; Pubky remains authoritative for public lock/tombstone and private guarded bytes.

**Tech Stack:** Rust 2024, Axum, Tokio, SQLx/PostgreSQL, Pubky homeserver storage, `time`, AEAD via `chacha20poly1305`, existing Locks SDK and JS/WASM bindings.

**Sibling plan:** Paykit Server `docs/plans/2026-08-10-lock-payment-draining.md`. Both plans repeat the shared wire contract deliberately.

---

## Status and provenance

- Plan status: **accepted product design; Tasks 1–7 committed; Tasks 8–11 remain**.
- Repository inspected: `/home/u/Projects/Synonym/Pubky/locks-public`.
- Planning base when written: clean `master` at `ba49a77`.
- There has been no production deployment. New persistence may require a clean pre-production database; no historical backfill is required.
- No Paykit Rust protocol change is planned. `proposal_expires_at` retains its existing pre-acceptance meaning.

### Explicit requirements and confirmed decisions

1. `paykit-payment` criterion params gain required `payment_in`.
2. `payment_in` is a nonzero JSON `u64` integer measured in whole hours. Zero, fractional, negative, string, and out-of-range values are invalid. There is no product maximum beyond checked duration/timestamp representation.
3. Locks includes `payment_in` in the signed invoice request. Paykit independently reads the canonical lock and rejects a mismatch before side effects.
4. Paykit commits `invoice_created_at` and `payment_deadline = checked(invoice_created_at + payment_in hours)` atomically with invoice creation. Exact replay returns the original timestamps.
5. Locks persists the returned timestamps before admitting the verification task. Retry never restarts the payment window.
6. Paykit sets Payment Request `proposal_expires_at` to `payment_deadline`, but the field remains proposal-only. Locks and Paykit Server enforce the post-acceptance deadline as application state.
7. Payment is timely when Paykit’s durable `first_amount_matched_observed_at <= payment_deadline`. An earlier underpayment does not lend its timestamp to a later qualifying output. Polling latency is accepted.
8. At the deadline, undetected and underpaid invoices expire and stop active observation. A timely amount-matched payment may continue confirmation observation after the deadline without a second timeout.
9. Locks alone applies configured `minimum_confirmations`; it is not sent to Paykit’s drain endpoint.
10. Payment after application expiry never opens the lock. Reader UI blocks/removes payment instructions at the deadline and warns that late payment receives no access or automatic refund.
11. Graceful deletion is the default creator DELETE mode. `graceful=true` is an alias; `force=true` is mutually exclusive and explicit.
12. Graceful deletion is irreversible once its durable job is persisted. There is no cancellation API.
13. Public withdrawal replaces `/pub/locks.app/{lock_id}.json` with an exact tombstone after durably storing the original canonical lock:

```json
{
  "version": 1,
  "type": "content_lock_deletion",
  "lock_id": "<lock_id>",
  "deletion_started_at": "<RFC3339 UTC>"
}
```

14. Persisting the deletion job is the proof-admission cutoff. New Bundle IDs are rejected; exact replay/status for previously persisted tasks remains available.
15. Paykit atomically classifies Payment Requests at drain start: accepted/rejected persisted before the cutoff retain that state; unanswered requests are durably canceled; later acceptance loses.
16. Durable cancellation enqueue is enough to stop blocking; delivery/acknowledgment is not awaited.
17. Rejected and canceled requests do not block. Accepted requests block until payment expires or satisfies Locks’ frozen rule. Timely amount-matched payment continues through required confirmations.
18. Existing access credentials remain reusable until their original expiry.
19. Existing and final drain credentials resolve authorization and resource descriptors from the deletion job’s frozen canonical manifest while the public path contains a tombstone. The tombstone is never treated as a valid Content Lock, and callers outside the persisted drain receive no new access.
20. Every already-paid entitlement lacking an active credential at tombstoning, plus every payment completed during draining, may obtain exactly one final drain credential.
21. Default final-credential issuance window is 15 minutes; configured maximum is one hour. Default read window is 15 minutes; configured maximum is one hour. Retry does not extend either.
22. Final credential permits one successful GET per frozen resource. Each path uses an atomic claim. Consumption occurs after upstream bytes are fetched/validated and a `200` response is constructed; a later disconnect does not restore it. Pre-response fetch failure releases the claim.
23. Exact credential issuance replay returns the same random bearer. Persist a versioned encrypted envelope using a domain-separated key derived from the existing runtime master key; bind creator, Bundle ID, deletion job, and envelope version as AEAD context.
24. Deletion worker retries transient failures with durable exponential backoff: one second initial, five-minute cap, full jitter, ten attempts per phase by default. Attempts reset on phase advance.
25. Creator-visible job status is only `queued|running|completed|failed`; failed responses include a stable secret-free `failure_code` only.
26. Missing or replaced tombstone before final verification halts as failed. Creator restores the exact tombstone and repeats graceful DELETE to resume the same job.
27. Guarded paths are exclusive to one managed lock. Enforce unique `(creator, guarded_path)` ownership in PostgreSQL. There is no historical backfill.
28. Lock publication uses best-effort ownership compensation and a durable opaque per-lock publication intent under the same PostgreSQL fence as deletion admission. Graceful/force deletion cannot start while publication is in flight. The intent is cleared only after ownership is durably published or failed publication is safely compensated. Process death can leave operator-reconciled intent/ownership state; do not claim cross-system atomicity.
29. Graceful finalization non-destructively verifies every frozen guarded-resource generation and the exact tombstone, then purges Locks authorization/task/job state and asks Paykit to remove operational drain state. The tombstone and guarded bytes remain on Pubky because its unconditional DELETE API cannot safely remove an expected generation in the presence of out-of-band creator writes. Later republication requires an explicit follow-up design rather than an unsafe graceful delete.
30. Paykit retains terminal financial invoice/payment history; delayed old lifecycle events cannot reactivate a fresh publication.
31. New force deletion is synchronous: persist a permanent minimal blocking receipt, delete lock/tombstone first, then best-effort guarded resources. Do not drain Paykit/tasks/credentials. Return failed paths. A force-deleted Lock ID can never be republished.
32. `force=true` against an active graceful job persists `force_requested`, revokes the current claim token/lease, requeues the same frozen job, and returns `202`; a fresh worker claim escalates asynchronously under exclusive action ownership, skips drains, deletes tombstone then content, and finishes forced.
33. Graceful job insertion/resume and permanent force-receipt establishment acquire the same canonical per-lock PostgreSQL fence. The durable result is either an active graceful job or a permanent force receipt, never both. Failed graceful replay requeues the same job and frozen manifest. Force against a terminal job atomically replaces that operational row with the permanent receipt before synchronous external deletion.
34. Any Content Lock fetched from Pubky for deletion must hash to the requested Lock ID and name the authenticated creator before its manifest is frozen or used for resource verification or force deletion.
35. Runtime encryption uses one environment-only 32-byte unpadded-base64url master key selected by `secrets.runtime_master_key_env`. Creator-authority and final-credential encryption keys are derived from it with distinct fixed domain labels. The retired `creator_authority_key_env` key is rejected as unknown configuration; no compatibility alias is retained.
36. The closed `[deletion]` configuration contract is `retry_max_attempts = 10`, `retry_initial_backoff_seconds = 1`, `retry_max_backoff_seconds = 300`, `final_credential_issuance_window_seconds = 900`, and `final_read_window_seconds = 900` by default. All values are positive; initial backoff cannot exceed maximum backoff; both credential windows are bounded to at most 3600 seconds. Retry jitter remains an implementation policy rather than a configurable field.
37. Deletion admission immutably records whether each paid snapshot Bundle had any active credential at cutoff and enrolls every such ordinary credential with its original expiry. Enrolled ordinary credentials remain reusable against the frozen manifest until that expiry; they do not acquire one-shot resource-read rows. When the claimed job first enters `issue_final_credentials`, it persists `final_issuance_started_at`, `final_credential_issuance_deadline = final_issuance_started_at + final_credential_issuance_window`, and `final_read_deadline = final_credential_issuance_deadline + final_read_window` once; replay and later config changes never extend them. A paid snapshot resolved completed without an active ordinary credential at cutoff becomes durably final-credential eligible and receives exactly one encrypted replayable final credential expiring at `final_read_deadline`. Every final credential receives one claimable row per frozen manifest path. Final-read claims precede Pubky fetch, are released on pre-response failure, expire for crash recovery, and are consumed only after the complete HTTP response is constructed; consumption is permanent. Phase advancement waits until every enrolled ordinary credential is expired and every final resource is consumed or its credential/read window is expired.
38. Ordinary credential insertion and deletion admission acquire the same canonical per-lock fence. Deletion-first rejects the insert; insertion-first is attached and classified at cutoff. Database lock order is canonical per-lock fence, deletion job row, snapshot/credential row, then resource-read row. No transaction spans Pubky I/O. Final read claims use fixed 30-second leases clamped to credential expiry; stale claim tokens cannot consume or release a reclaimed row.
39. This pre-production migration intentionally has no creator-authority ciphertext compatibility path. Moving the same bytes to `runtime_master_key_env` changes the derived creator-authority key; existing local encrypted authority rows must be discarded and reacquired or the local database recreated.
40. Deletion-worker runtime configuration is a separate closed `[deletion_worker]` section with `enabled`, `poll_interval_ms`, `claim_timeout_seconds`, `shutdown_timeout_seconds`, and `worker_id`. Defaults are `true`, `250`, `60`, `30`, and `"deletion-worker"`. Enabled verification and deletion workers are independently tracked: starting, stopping, or unexpected exit is `not_ready`; a transient deletion dependency failure is `degraded` until successful dependency evidence; ordinary pending work, advisory-lock contention, and a correctly terminalized failed job do not degrade readiness.
41. **Accepted Pubky tombstone TOCTOU limitation:** the pinned Pubky 0.9.3 SDK exposes ETag metadata but no conditional write API, and the matching homeserver enforces `If-None-Match` for reads but not `If-Match` for writes. Graceful withdrawal therefore reads and compares the frozen canonical lock before an unconditional tombstone `PUT`. A replacement already visible at that read fails closed and is preserved; crash/reclaim replay also preserves any replacement it observes. However, an out-of-band creator replacement written after the comparison and before the `PUT` can be overwritten by the tombstone. Product explicitly accepts this race until Pubky provides atomic conditional writes. This exception does not authorize graceful deletion of replacement bytes; active force remains the only unconditional delete path.

### Source-derived constraints

- Lock ID is BLAKE3 over complete canonical lock JSON. A mutable `deleting` field cannot be added under the same ID.
- Readers fetch the public lock directly from the creator homeserver; a Locks Server GET gate cannot withdraw it.
- Guarded content and public lock JSON are separate Pubky records; no delete cascade exists.
- Current creation validates resource descriptors but does not enforce cross-lock path exclusivity (`locks-service/src/application/use_cases/create_content_lock.rs`).
- Current verification tasks use PostgreSQL leases and fresh claim tokens; deletion needs a separate queue but the same fenced-transition discipline.
- Current access-credential storage keeps only a bearer lookup hash; exact replay requires new encrypted bearer persistence.
- `proposal_expires_at` expires only `Proposed` Paykit SDK state and has no accepted-payment effect.
- Pubky, Locks PostgreSQL, and Paykit PostgreSQL cannot participate in one atomic transaction.

### Explicitly accepted risks

- Late Bitcoin payment may receive no content and no refund.
- Paykit polling latency can make a pre-deadline broadcast late.
- Timely amount-matched payment can block deletion indefinitely while confirmations/reorg state remains unresolved.
- Durable cancellation enqueue may precede actual counterparty delivery.
- Best-effort lock-publication reservation compensation can leave operator-cleaned orphan ownership after process death.
- Force deletion deliberately abandons active payment/access obligations and may orphan content after a crash.

## Repository ownership matrix

| Contract/state | Owner |
| --- | --- |
| `payment_in` criterion schema and validation | Locks Core |
| Signed invoice request producer and response persistence | Locks Server/Service |
| `invoice_created_at`, `payment_deadline`, proposal expiry | Paykit Server |
| Payment Request acceptance/rejection/cancellation projection | Paykit Server |
| Bitcoin first-observation and confirmations | Paykit Server |
| `minimum_confirmations` entitlement decision | Locks |
| Public tombstone and frozen lock manifest | Locks |
| Proof admission cutoff and task transitions | Locks |
| Credentials, per-path consumption, content serving | Locks |
| Lock-wide payment drain and aggregate status | Paykit Server |
| Overall deletion orchestration and final cleanup | Locks |
| Terminal financial history | Paykit Server |

## Shared service-to-service contract

All requests use existing `X-Paykit-Signature` over canonical JSON. Secret/correlation identifiers stay in POST bodies and must not be logged.

### Invoice creation

```http
POST /invoices

{
  "bundle_id": "...",
  "lock_resource": "pubky.../pub/locks.app/<lock_id>.json",
  "reader": "pubky...",
  "payment_in": 24
}
```

Success changes from ignored-body 2xx to closed JSON:

```json
{
  "invoice_created_at": "<RFC3339 UTC>",
  "payment_deadline": "<RFC3339 UTC>"
}
```

Paykit compares request `payment_in` with canonical criterion `payment_in`. Exact replay returns the original response.

### Lock-wide drain

```http
POST /payment-request-drains
{ "lock_resource": "..." }
```

Starts or exactly replays an atomic persisted classification. No `minimum_confirmations` field.

```http
POST /payment-request-drain-lookups
{ "lock_resource": "..." }
```

Both drain endpoints return `200` with the same closed aggregate body:

```json
{
  "status": "active",
  "accepted_count": 0,
  "terminal_count": 0,
  "cancellation_enqueued_count": 0,
  "cleanup_token": "<43-character-unpadded-base64url>"
}
```

`status` is exactly `active` or `completed`. The response contains no drain ID, replay flag, Bundle ID, reader, Payment Request ID, address, payment reference, or raw error. Replay preserves the frozen drain identity, cancellation count, and cleanup token while returning its latest monotonic aggregate progress.

### Per-Bundle status

```http
POST /payment-requests/status
{ "creator": "pubky...", "bundle_id": "..." }
```

Returns this exact closed body with orthogonal lifecycle and payment facts:

```json
{
  "request_state": "proposed",
  "payment_state": "undetected",
  "invoice_created_at": "<RFC3339 UTC>",
  "payment_deadline": "<RFC3339 UTC>",
  "confirmations": 0,
  "amount_matched": false
}
```

The canonical persisted `request_state` is one of these exact closed snake-case values, mapped one-to-one from Paykit SDK lifecycle state:

- `proposed`
- `proposal_expired`
- `accepted`
- `rejected`
- `canceled`
- `proof_submitted`
- `active_recurring`
- `recovery_required`
- `invalid_conflict`

`payment_state` is exactly one of:

- `undetected`
- `detected`
- `confirmed`
- `expired`

`expired` is returned when the invoice has a durable `payment_expired_at`; otherwise the persisted observation state maps one-to-one to `undetected`, `detected`, or `confirmed`. `confirmations` and `amount_matched` remain orthogonal factual fields.

Locks maps `rejected`, `canceled`, and `proposal_expired` requests to `VerificationTaskStatus::Expired`. An `accepted` request whose `payment_state` is `expired` also maps to `Expired`. These terminal outcomes carry no failure message and never map to `Failed`.

Drain classification uses the persisted state without inference from invoice delivery or Bitcoin observation:

- `accepted` is accepted and blocking;
- `rejected`, `canceled`, and `proposal_expired` are terminal and non-blocking;
- `proposed` is unanswered and requires durable cancellation enqueue;
- `recovery_required`, `invalid_conflict`, `proof_submitted`, and `active_recurring` fail drain classification rather than being collapsed into another lifecycle.

For the later HTTP slice, `recovery_required` maps to `503 unavailable`; `invalid_conflict`, `proof_submitted`, and `active_recurring` map to `409 conflict`. These mappings do not alter the canonical lifecycle persisted by this projection.

The stable drain-classification error envelopes are:

- `409 {"error":{"code":"conflict","message":"request conflicts with persisted payment state"}}`
- `503 {"error":{"code":"unavailable","message":"payment request state is unavailable"}}`

Absent drain lookups and absent per-Bundle statuses reuse `404 {"error":{"code":"not_found","message":"requested resource was not found"}}`.

### Operational drain cleanup

Drain creation and lookup responses include an opaque `cleanup_token`: the canonical unpadded base64url encoding of 32 server-keyed, domain-separated bytes bound to the immutable drain identity. The token is not an internal drain ID, is not reversible, is stable across restart/exact replay, and must never be logged. Add `POST /payment-request-drain-cleanups`, authenticated by the existing canonical Locks signature boundary. It accepts the exact query-free body:

```json
{"cleanup_token":"<43-character-unpadded-base64url>","lock_resource":"pubky<creator>/pub/locks.app/<lock_id>.json"}
```

On success it returns the exact closed response `200 {"status":"removed"}`. Cleanup is cycle-bound and idempotent: deleting the matching completed drain advances the publication generation once and durably retains the consumed token as the generation boundary's cleanup receipt; replay of that token while no newer drain exists returns the same response without advancing again. A token that cannot be verified against either the current drain or its retained cleanup receipt—including an arbitrary token for a never-known lock or a delayed old token after a newer drain exists—returns the existing coarse `409 conflict` envelope and cannot delete or advance the newer cycle. An active matching drain also returns `409 conflict`. Authenticated envelope mismatch, corrupt receipt/generation state, or unavailable persistence returns the existing coarse `503 unavailable` envelope. The operation rejects query strings, unknown body fields, padding, non-canonical base64url, and token lengths other than exactly 32 decoded bytes. It deletes only a completed operational drain after Locks external cleanup succeeds and must never delete invoices, Bitcoin observations, Payment Request events, cancellation intents, or financial audit history. No lock, Bundle, internal drain ID, invoice, reader, or Payment Request identifier appears in the response or error envelope.

## HTTP creator contract

```http
DELETE /creator/content-locks/{lock_id}
DELETE /creator/content-locks/{lock_id}?graceful=true
```

Starts/replays/resumes graceful deletion and returns `202` for queued/running work. A completed-and-forgotten absent lock is an idempotent absent postcondition.

Queued/running deletion and deletion status use the closed body `{ "lock_id": "...", "status": "queued|running|completed|failed", "failure_code"?: "..." }`. If both the canonical lock and deletion job are absent, graceful DELETE returns `200` with `{ "lock_id": "...", "status": "completed" }`.

```http
DELETE /creator/content-locks/{lock_id}?force=true
```

- No graceful job: synchronous `200` force summary.
- Existing graceful job: persist `force_requested`, return `202` job status.

The synchronous force summary is exactly `{ "lock_id": "...", "lock_deleted": true, "failed_resource_paths": ["..."] }`. It does not expose a force mode or internal receipt.

Reject `force=true&graceful=true`, unknown fields, malformed booleans, and duplicate conflicting query values.

```http
GET /creator/content-locks/{lock_id}/deletion
```

Authenticated response contains Lock ID and `status`; include `failure_code` only for failed jobs. The closed stable vocabulary is exactly `tombstone_missing`, `tombstone_replaced`, `resource_replaced`, `retry_exhausted`, and `state_corrupt`. `resource_replaced` means a frozen guarded-resource path no longer contains the admitted generation and therefore graceful finalization failed closed without deleting the replacement. Do not expose phases, leases, retries, Bundle IDs, readers, credentials, paths, Paykit IDs, or dependency errors.

If no job or force receipt exists, status returns `404 content_lock_deletion_not_found`. A permanent force receipt projects as `{ "lock_id": "...", "status": "completed" }` without exposing force mode.

## Internal state model

Internal phase names are not public API. The implementation should represent at least:

1. `withdraw`: persist frozen payload/job/admission cutoff, write tombstone, read back exact bytes.
2. `start_payment_drain`: exact Paykit drain creation.
3. `drain_payments`: poll aggregate drain and per-Bundle statuses; transition frozen tasks.
4. `drain_existing_credentials`: wait for credentials active at cutoff to expire.
5. `issue_final_credentials`: allow bounded issuance for eligible entitlements.
6. `drain_final_reads`: enforce per-path claims/consumption and read deadlines.
7. `delete_content`: non-destructively verify every frozen resource generation while the tombstone remains exact.
8. `delete_tombstone`: non-destructively verify that the exact tombstone remains published before the purge handoff.
9. `purge_operational_state`: remove Paykit operational drain, then atomically purge Locks lock-scoped authorization/task/job state and release path ownership.

Use separate durable `state`, `phase`, `attempt_count`, `next_attempt_at`, claim owner/token/expiry, and force-request fields. Use a per-job PostgreSQL advisory action lock where lease expiry must not permit overlapping external effects. SQLx advisory-lock connections must be close-on-drop and explicitly unlocked/closed.

## Implementation sequence

Each task is a separate review/commit checkpoint. Do not commit automatically.

### Task 1: Lock the `payment_in` core contract

**Objective:** Make the content-addressed lock schema reject every non-approved timing shape.

**Files:**
- Modify: `locks-core/src/lock_policy.rs`
- Modify: `locks-core/src/creator_publishing.rs`
- Modify: `locks-sdk/bindings/js/src/creator.rs`
- Test: neighboring unit/public API tests in those files and `locks-sdk/tests/public_api.rs`

**RED:** Add serialization/validation tests for required nonzero JSON `u64`, unknown/missing field rejection, zero/fraction/string/overflow rejection, and canonical Lock ID sensitivity.

**GREEN:** Extend the closed `paykit-payment` params parser/typed accessors and JS creator builder.

**Verify:**

```bash
cargo test -p locks-core
cargo test -p locks-sdk
cargo test -p locks-sdk-wasm
cargo test --workspace --no-run
```

**Suggested commit:** `feat(core): add paykit payment deadline hours`

### Task 2: Persist exclusive guarded-path ownership

**Objective:** Enforce one managed Content Lock per creator/path and retain ownership safely across deletion failures.

**Files:**
- Modify: `locks-service/src/infrastructure/postgres/migrations.rs`
- Create: `locks-service/src/application/models/content_lock_ownership.rs`
- Create: `locks-service/src/application/ports/content_lock_ownership.rs`
- Create: `locks-service/src/infrastructure/postgres/content_lock_ownership.rs`
- Modify: relevant `mod.rs` exports
- Modify: `locks-service/src/application/use_cases/create_content_lock.rs`
- Modify: in-memory test adapters
- Test: `locks-e2e/tests/postgres_runtime.rs`
- Test: `locks-e2e/tests/production_creator_publishing_http.rs`

**RED:** Prove duplicate `(creator,path)` rejection, atomic all-path reservation, ordinary-error compensation, retained ownership after failed deletion, and clean-database rollout.

**GREEN:** Add unique ownership rows carrying creator, full path, intended Lock ID, and status. Reserve before Pubky publication; best-effort compensate ordinary publication failure. Do not invent historical backfill.

**Verify:**

```bash
TEST_DATABASE_URL="$TEST_DATABASE_URL" cargo test -p locks-e2e --test postgres_runtime
TEST_DATABASE_URL="$TEST_DATABASE_URL" cargo test -p locks-e2e --test production_creator_publishing_http
cargo test --workspace --no-run
```

**Suggested commit:** `feat(service): enforce guarded path ownership`

### Task 3: Upgrade the Locks-to-Paykit invoice boundary

**Objective:** Send `payment_in`, require the closed timestamp response, and durably bind it to the verification task before admission.

**Files:**
- Modify: `locks-server/src/paykit_http_client.rs`
- Modify: `locks-service/src/application/models/verification.rs`
- Modify: `locks-service/src/application/ports/verification.rs`
- Modify: verification task PostgreSQL/memory adapters and migration
- Modify: `locks-service/src/application/use_cases/submit_proof_bundle.rs`
- Test: `locks-server/src/api/routes/tests.rs`
- Test: `locks-e2e/tests/postgres_runtime.rs`

**Dependency gate:** Implement only after the Paykit Server invoice-response slice is reviewed and committed.

**RED:** Test canonical signed request body, strict timestamp response decoding, checked ordering (`created <= deadline`), exact task replay preserving timestamps, and rollback/no-task on invoice rejection.

**GREEN:** Persist immutable invoice timestamps with the task in the same local transaction that admits it. Do not recompute on retry.

**Verify:** focused unit tests, PostgreSQL E2E, then `cargo test --workspace --no-run`.

**Suggested commit:** `feat(paykit): persist invoice payment deadlines`

### Task 4: Add deletion/tombstone domain and persistence

**Objective:** Persist frozen manifests, cutoff state, leases, retry scheduling, force receipts, and minimal public DTOs.

**Files:**
- Create: `locks-core/src/content_lock_deletion.rs`
- Modify: `locks-core/src/lib.rs`
- Create: `locks-service/src/application/models/content_lock_deletion.rs`
- Create: `locks-service/src/application/ports/content_lock_deletion.rs`
- Create: `locks-service/src/infrastructure/postgres/content_lock_deletions.rs`
- Create: `locks-service/src/infrastructure/memory/content_lock_deletions.rs`
- Modify: PostgreSQL migration/module exports
- Modify: `locks-service/src/application/errors.rs`

**RED:** Test exact tombstone JSON, strict unknown-field rejection, frozen payload integrity, unique creator/Lock ID job identity, due claims, lease reclaim/fresh tokens, stale-token rejection, per-phase attempt reset, and permanent force receipt.

**GREEN:** Implement the minimal state model. Keep public status conversion separate from internal phases.

**Suggested commit:** `feat(service): persist content lock deletion jobs`

### Task 5: Serialize deletion start against proof admission

**Objective:** Make database commit order the authoritative cutoff for new Bundle IDs.

**Files:**
- Modify: `locks-service/src/application/use_cases/submit_proof_bundle.rs`
- Create: `locks-service/src/application/use_cases/start_content_lock_deletion.rs`
- Modify: relevant repositories/PostgreSQL transaction helpers
- Test: `locks-e2e/tests/postgres_runtime.rs`
- Test: `locks-server/src/api/routes/tests.rs`

**RED:** Concurrent tests prove task-first commit joins snapshot, deletion-first commit rejects a new Bundle, exact old replay succeeds, and conflicting replay remains rejected.

**GREEN:** Use per-lock database serialization and one transaction for job persistence/task snapshot. For Paykit-backed submissions, atomically persist a hidden, unclaimable admission reservation before the external invoice call; classify durable exact replay/conflict before mutable lock lookup or reader resolution, and resume an unready reservation from its persisted canonical fields. Paykit success makes it ready. This guarantees that deletion either snapshots the durable obligation or commits first and prevents any Paykit call. Do not permit transition to `start_payment_drain` while a snapshotted reservation is unready: Paykit's active drain accepts exact replay only for invoices already created before drain start. Do not hold a database transaction across HTTP, and do not use viewer timestamps or tombstone publication as the cutoff.

**Suggested commit:** `feat(service): enforce deletion admission cutoff`

### Task 6: Add creator deletion/status APIs and SDKs

**Objective:** Expose authenticated graceful default, explicit force, and minimal status consistently across Rust and JS.

**Files:**
- Modify: `locks-server/src/api/creator_publishing.rs`
- Modify: `locks-server/src/api/dtos.rs`
- Modify: `locks-server/src/api/errors.rs`
- Modify: `locks-server/src/api/routes.rs`
- Modify: `locks-sdk/src/creator.rs`
- Modify: `locks-sdk/src/transport.rs`
- Modify: `locks-sdk/bindings/js/src/creator.rs`
- Test: `locks-server/src/api/routes/tests.rs`
- Test: `locks-sdk/tests/public_api.rs`
- Test: `locks-e2e/tests/production_creator_publishing_http.rs`

**RED:** Cover query matrix, auth creator binding, 202 replay/resume/escalation, synchronous 200 force, permanent force receipt, absent postcondition, and redacted status.

**GREEN:** Implement the closed routes exactly as documented. No immediate force through an omitted query option.

**Suggested commit:** `feat(api): add creator content lock deletion`

### Task 7: Integrate Paykit drain/status client

**Objective:** Start/poll Paykit’s lock-wide drain and resolve each existing verification task from factual status.

**Files:**
- Modify: `locks-server/src/paykit_http_client.rs`
- Modify: `locks-server/src/app_state/mod.rs`
- Create: `locks-service/src/application/ports/payment_drain.rs`
- Create: `locks-service/src/application/ports/payment_drain_repository.rs`
- Create: `locks-service/src/application/use_cases/drain_lock_payments.rs`
- Create: `locks-service/src/infrastructure/postgres/payment_drains.rs`
- Create: `locks-service/migrations/0015_content_lock_payment_drains.sql`
- Modify: `locks-service/src/infrastructure/postgres/content_lock_deletions.rs`
- Modify: `locks-service/src/infrastructure/postgres/verification_task_claims.rs`
- Test: `locks-server/src/paykit_http_client.rs`
- Test: deletion use-case tests and HTTP integration fixtures

**Dependency gate:** Patch both plans with exact per-Bundle enums, error mappings, and drain-cleanup route before RED tests. Then implement Paykit Server routes first.

**RED:** Test exact signed JSON, no `minimum_confirmations` leak, aggregate redaction, local application of confirmations, canceled/rejected/expired transitions, timely matched confirmation continuation, and retryable transport errors.

**GREEN:** Freeze Paykit obligation identity, criterion, cutoff status, and authoritative invoice window in the deletion snapshot; persist the opaque drain token and aggregate under the deletion claim fence. Paykit aggregate progress is monotonic: `accepted_count` may only decrease to zero, `terminal_count` may only increase by the same amount, and `cancellation_enqueued_count` plus the opaque cleanup token remain immutable. `completed` requires `accepted_count == 0` and cannot regress to `active`. Locks persists each newer aggregate under the live deletion lease, but still requires every frozen local obligation to become terminal before phase advancement; aggregate completion never bypasses Locks-local confirmation or reorg reconciliation. Exclude ordinary verification workers for every surviving deletion snapshot. Immediately before external entitlement storage, an ordinary claimed worker durably marks entitlement publication under its exact lease. Deletion admission locks and owns every matching task row before snapshot/reset: a committed publication marker blocks deletion, while committed deletion ownership blocks publication and every ordinary claim/retry/terminal write. Ambiguous entitlement publication retains the marker until an ordinary retry reconciles an equivalent entitlement and terminalizes the task. Publish entitlements before terminalizing their task and reconcile only an equivalent existing entitlement. Compose the client and repository in `AppState`. Task 9 supplies the queue polling/supervision that invokes this phase use case.

**Suggested commit:** `feat(paykit): drain deleting lock payments`

### Task 8: Implement final credential/read draining

**Objective:** Preserve existing credential TTL behavior while giving eligible paid entitlements one bounded per-resource final read.

**Files:**
- Modify: `locks-service/src/application/models/access.rs`
- Modify: `locks-service/src/application/ports/access.rs`
- Modify: `locks-service/src/infrastructure/postgres/access_credentials.rs`
- Modify: `locks-service/src/infrastructure/postgres/migrations.rs`
- Modify: `locks-service/src/application/use_cases/issue_access_credential.rs`
- Modify: `locks-service/src/application/use_cases/proxy_read_guarded_resource.rs`
- Modify: `locks-server/src/storage.rs` and secret composition as needed
- Test: `locks-service/src/application/use_cases/credential_flow_tests.rs`
- Test: `locks-service/src/application/use_cases/retrieval_access_flow_tests.rs`
- Test: `locks-e2e/tests/retrieval_access_http.rs`

**RED:** Cover exact encrypted replay, wrong-key/corrupt/version rejection, no secret Debug/log output, issuance/read deadlines, no deadline extension, existing/final access through the frozen manifest while the public path is a tombstone, denial outside the persisted drain, one concurrent success per path, claim release before response construction, consumption after construction, and automatic revocation when complete/expired.

**GREEN:** Snapshot and enroll active credentials atomically at deletion admission, and initialize final-window timestamps once when entering final issuance under the deletion lease. Fence ordinary insertion against deletion admission. Use versioned AEAD and domain-separated key derivation; retain lookup hashes. Enroll cutoff-active credentials at their original expiry and create one final credential only for an eligible completed snapshot without one. Resolve draining reads from the frozen manifest rather than parsing the tombstone. Claim each credential/path before fetch, release on pre-response failure, consume only after the server constructs the complete response, and allow only expired claims to be reclaimed. Do not store plaintext bearer.

**Suggested commit:** `feat(access): drain final deletion credentials`

### Task 9: Implement and supervise the deletion worker

**Objective:** Execute external phases retryably without overlapping external actions or breaking shutdown.

**Files:**
- Create: `locks-server/src/deletion_worker.rs`
- Modify: `locks-server/src/main.rs`
- Modify: `locks-server/src/config/schema.rs`
- Modify: `locks-server/src/config/defaults.rs`
- Modify: `locks-server/src/config/validation.rs`
- Modify: `locks-server/src/app_state/readiness.rs`
- Modify: `locks-server/src/api/runtime.rs`
- Modify: deletion application ports/use cases and PostgreSQL/Pubky/in-memory adapters required for exact tombstone I/O, per-job advisory action ownership, non-failure deferral, and worker materialization of final credentials
- Test: worker unit tests and `locks-e2e/tests/postgres_runtime.rs`

**RED:** Crash/reclaim tests after every external side effect; PostgreSQL advisory ownership exclusion; exact tombstone publication/read-back and failure on replacements observed before publication or during replay; retry exhaustion/resume; force escalation; non-destructive sorted/deduplicated verification of every frozen guarded-resource generation followed by exact retained-tombstone verification; missing or replaced frozen resources and missing or replaced tombstones fail closed; active force deletes the canonical public path before best-effort private cleanup; readiness degradation; shutdown stops claims before HTTP drain and bounds the complete worker/HTTP join. These tests do not claim atomic replacement safety across the accepted read-to-unconditional-`PUT` window in decision 41.

**GREEN:** Reuse existing worker configuration conventions but keep queue cadence and retry due time separate. Never log manifest, resource paths, Bundle IDs, credentials, readers, or Paykit payloads.

**Task 9 PostgreSQL crash/reclaim acceptance coverage map:**

- Graceful public tombstone publication/read-back: `locks-e2e/tests/postgres_runtime.rs::postgres_graceful_withdraw_crash_reclaims_without_republishing_or_stale_advance` executes the production phase executor over the PostgreSQL job/lease repository, simulates process loss after publication, proves a fresh claim and advisory owner resume from exact read-back without a second publication, and fences the stale phase write. Exact missing/replaced byte classification remains covered by `locks-service/tests/content_lock_tombstones.rs` and the phase-executor failure tests.
- Payment-drain start and reconciliation: `payment_drain_reclaim_reconciles_external_start_before_local_persistence` covers remote start before local persistence and fresh-claim lookup reconciliation; `start_phase_replay_persists_monotonic_progress_after_crash_before_phase_advance`, `reclaim_first_fences_stale_initial_payment_drain_store`, `concurrent_force_winner_fences_stale_payment_drain_reconciliation`, and `concurrent_reclaim_winner_fences_stale_terminal_obligation_persistence` cover persisted aggregate replay plus stale start/reconcile/task writes against real PostgreSQL.
- Final credential generation/persistence/replay: `final_credentials_to_materialize_revalidates_exact_live_issue_claim_and_deadline` and `worker_final_issuance_is_exact_claim_fenced_in_winner_transaction` cover live-claim enumeration, fresh reclaimed ownership, stale/forced/deadline fencing, encrypted winner persistence, exact replay, and one-row cardinality against real PostgreSQL; `concurrent_final_issuers_replay_one_winner` independently covers concurrent winner replay.
- Frozen guarded-resource generation and retained-tombstone verification: `locks-e2e/tests/postgres_runtime.rs::postgres_guarded_generation_verification_crash_reclaims_and_replays_without_deletion` demonstrates two distinct loss points: after a frozen generation read but before its phase advance, and after exact retained-tombstone read-back but before the purge-handoff advance. Each boundary drops advisory ownership, recreates the PostgreSQL repository/runtime executor, reclaims with a fresh token, fences the stale advance, and idempotently replays without deleting private bytes.
- Active-force public-first/private cleanup: `locks-e2e/tests/postgres_runtime.rs::postgres_active_force_public_delete_crash_reclaims_before_private_cleanup` loses ownership immediately after canonical public deletion, before any private cleanup, then recreates the PostgreSQL repository/runtime executor, fences stale completion, replays public absence, performs private cleanup, and persists the terminal force receipt. `locks-e2e/tests/postgres_runtime.rs::postgres_active_force_private_delete_crash_reclaims_to_terminal_receipt` separately loses ownership after private deletion, then proves fresh-token reclaim, stale completion fencing, idempotent public/private replay, permanent receipt persistence, job removal, and no further claimable work. `execute_forced_content_lock_deletion::tests` supplies the sorted/deduplicated multi-resource and best-effort error matrix at the same production use-case seam.
- Every composed E2E case above drops the detached PostgreSQL advisory guard as the crash boundary and proves a later independent acquisition succeeds; `postgres_deletion_action_ownership_excludes_overlap_and_reacquires_after_release` separately proves overlap exclusion.

**Suggested commit:** `feat(server): run graceful deletion worker`

### Task 10: Purge graceful state and preserve force blocks

**Objective:** Purge graceful operational state without removing the durable external tombstone, while permanently blocking force-deleted Lock IDs.

**Files:**
- Create/modify: lock-scoped purge repository/use case in `locks-service/src/`
- Modify: deletion worker
- Modify: content-lock creation ownership/force-receipt checks
- Test: PostgreSQL E2E and creator publishing HTTP E2E

**RED:** Prove all Locks task/proof/entitlement/credential/job rows are gone after graceful completion, ownership is released only after external cleanup, fresh same-ID publication accepts only new Bundle IDs, late old task replay cannot reactivate, force receipt blocks same-ID publication forever, and failed force paths retain ownership.

**Suggested commit:** `feat(service): finalize lock deletion lifecycle`

### Task 11: Reader UX and documentation

**Objective:** Make the application deadline visible and prevent accidental late manual payment.

**Files:**
- Modify only currently active reader/demo files discovered at implementation time; audit `examples/js-sdk/`, `README.md`, and `docs/LOCAL_OPERATOR_DEMO.md` before naming exact files.
- Modify: protocol/API documentation for criterion and deletion routes.

**RED:** Browser/demo test with injected clock proves payment action disabled at equality boundary only after the inclusive deadline has passed, warning is visible, and no automatic payment is initiated.

**GREEN:** Display Paykit-returned absolute deadline; do not derive from browser clock plus duration.

**Suggested commit:** `docs: document payment deadlines and lock deletion`

## Cross-repository implementation/review order

1. Commit synchronized plan-only changes separately in Locks and Paykit Server.
2. Locks Task 1 (`payment_in`) and publish/review the exact Locks Core revision Paykit will consume.
3. Paykit Server invoice persistence/response and deadline observation slices.
4. Resolve and patch the exact per-Bundle enums and operational-drain cleanup route in both plans.
5. Paykit Server drain/status API slices.
6. Locks invoice persistence and payment-drain client slices.
7. Locks deletion persistence/API/worker/credential slices.
8. Cross-service E2E and docs.

No repository may claim the sibling contract implemented until pinned dependency/revision and live tests prove it.

## Verification

Repository-local final verification:

```bash
cargo fmt --all
cargo test -p locks-core
cargo test -p locks-service
cargo test -p locks-server
cargo test -p locks-sdk
cargo test -p locks-sdk-wasm
TEST_DATABASE_URL="$TEST_DATABASE_URL" cargo test -p locks-e2e --test postgres_runtime
TEST_DATABASE_URL="$TEST_DATABASE_URL" cargo test -p locks-e2e --test production_creator_publishing_http
TEST_DATABASE_URL="$TEST_DATABASE_URL" cargo test -p locks-e2e --test retrieval_access_http
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
git diff --check
```

Cross-service acceptance must additionally prove:

- invoice timestamp exact replay;
- canonical lock/request `payment_in` mismatch rejection with no side effects;
- inclusive first amount-matched-observation deadline;
- underpayment expiry and matched-payment confirmation continuation;
- atomic acceptance/cancellation drain cutoff;
- cancellation enqueue without delivery wait;
- Locks-only minimum-confirmation decision;
- deletion crash recovery after every remote effect;
- exact tombstone replacement halt/resume;
- existing/final credential drain and concurrent per-path consumption;
- graceful tombstone preservation with no old authorization revival;
- permanent force same-ID block.

## Remaining implementation-contract gates

None. The exact Locks-only deletion configuration and runtime-master-key contracts are fixed above. Paykit Server has no corresponding credential or deletion-worker configuration.

## Out of scope

- Paykit protocol `payment_due_at` field or accepted-expiry event.
- Automatic refunds or late-payment access.
- Manual/automatic Bitcoin payment from the reader.
- Cross-system transactions or exactly-once external effects.
- Historical production-data migration/backfill.
- Republishing force-deleted Lock IDs.
- Deleting reader-downloaded copies.
