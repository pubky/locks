# Graceful Content-Lock Deletion and Payment Deadline Implementation Plan

> **For Hermes:** Use subagent-driven-development to implement this plan one review-gated commit slice at a time. Stop after each slice; the user commits before the next slice.

**Goal:** Add a bounded `paykit-payment` deadline and creator-authorized graceful content-lock deletion that withdraws the public lock immediately, drains accepted payment and access obligations durably, removes guarded content, and safely permits later republication after complete graceful cleanup.

**Architecture:** Locks owns the public tombstone, admission cutoff, verification tasks, credentials, guarded content, path ownership, and overall deletion job. Paykit Server owns invoice timestamps, Payment Request lifecycle classification, cancellation, Bitcoin observation, and a durable lock-wide payment drain. PostgreSQL stores retryable Locks workflow state; Pubky remains authoritative for public lock/tombstone and private guarded bytes.

**Tech Stack:** Rust 2024, Axum, Tokio, SQLx/PostgreSQL, Pubky homeserver storage, `time`, AEAD via `chacha20poly1305`, existing Locks SDK and JS/WASM bindings.

**Sibling plan:** Paykit Server `docs/plans/2026-08-10-lock-payment-draining.md`. Both plans repeat the shared wire contract deliberately.

---

## Status and provenance

- Plan status: **accepted product design; implementation not started**.
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
26. Missing or replaced tombstone before destructive work halts as failed. Creator restores the exact tombstone and repeats graceful DELETE to resume the same job.
27. Guarded paths are exclusive to one managed lock. Enforce unique `(creator, guarded_path)` ownership in PostgreSQL. There is no historical backfill.
28. Lock publication uses best-effort reservation compensation and accepts crash-orphaned ownership requiring operator cleanup; do not claim cross-system atomicity.
29. Graceful final cleanup deletes guarded content first and tombstone last, purges Locks authorization/task/job state, asks Paykit to remove operational drain state, and releases path ownership. It forgets the deletion so the same canonical Lock ID may later be published fresh with new Bundle IDs.
30. Paykit retains terminal financial invoice/payment history; delayed old lifecycle events cannot reactivate a fresh publication.
31. New force deletion is synchronous: persist a permanent minimal blocking receipt, delete lock/tombstone first, then best-effort guarded resources. Do not drain Paykit/tasks/credentials. Return failed paths. A force-deleted Lock ID can never be republished.
32. `force=true` against an active graceful job persists `force_requested` and returns `202`; the worker escalates asynchronously under exclusive action ownership, skips drains, deletes tombstone then content, and finishes forced.

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

Returns aggregate factual state only; no Bundle IDs, readers, Payment Request IDs, addresses, or raw errors.

### Per-Bundle status

```http
POST /payment-requests/status
{ "creator": "pubky...", "bundle_id": "..." }
```

Returns orthogonal `request_state` and `payment_state`, immutable invoice/deadline timestamps, confirmations, and amount match. Exact enum spellings and invalid/recovery-state HTTP mapping are an implementation-contract gate and must be synchronized in both plans before code.

### Drain cleanup

Paykit Server needs an idempotent signed operation to remove only the completed operational drain row after Locks has completed all external deletion effects. Exact route/body is an implementation-contract gate; it must not remove financial invoice/payment history.

## HTTP creator contract

```http
DELETE /creator/content-locks/{lock_id}
DELETE /creator/content-locks/{lock_id}?graceful=true
```

Starts/replays/resumes graceful deletion and returns `202` for queued/running work. A completed-and-forgotten absent lock is an idempotent absent postcondition.

```http
DELETE /creator/content-locks/{lock_id}?force=true
```

- No graceful job: synchronous `200` force summary.
- Existing graceful job: persist `force_requested`, return `202` job status.

Reject `force=true&graceful=true`, unknown fields, malformed booleans, and duplicate conflicting query values.

```http
GET /creator/content-locks/{lock_id}/deletion
```

Authenticated response contains Lock ID and `status`; include `failure_code` only for failed jobs. Do not expose phases, leases, retries, Bundle IDs, readers, credentials, paths, Paykit IDs, or dependency errors.

## Internal state model

Internal phase names are not public API. The implementation should represent at least:

1. `withdraw`: persist frozen payload/job/admission cutoff, write tombstone, read back exact bytes.
2. `start_payment_drain`: exact Paykit drain creation.
3. `drain_payments`: poll aggregate drain and per-Bundle statuses; transition frozen tasks.
4. `drain_existing_credentials`: wait for credentials active at cutoff to expire.
5. `issue_final_credentials`: allow bounded issuance for eligible entitlements.
6. `drain_final_reads`: enforce per-path claims/consumption and read deadlines.
7. `delete_content`: idempotently delete every frozen resource while tombstone remains exact.
8. `delete_tombstone`: persist intent-to-remove phase before external delete so missing-on-retry is success.
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
cargo test -p locks-sdk-js
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

**GREEN:** Use per-lock database serialization and one transaction for job persistence/task snapshot. Do not use viewer timestamps or tombstone publication as the cutoff.

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
- Create: `locks-service/src/application/use_cases/drain_lock_payments.rs`
- Test: `locks-server/src/paykit_http_client.rs`
- Test: deletion use-case tests and HTTP integration fixtures

**Dependency gate:** Patch both plans with exact per-Bundle enums, error mappings, and drain-cleanup route before RED tests. Then implement Paykit Server routes first.

**RED:** Test exact signed JSON, no `minimum_confirmations` leak, aggregate redaction, local application of confirmations, canceled/rejected/expired transitions, timely matched confirmation continuation, and retryable transport errors.

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

**GREEN:** Use versioned AEAD and domain-separated key derivation; retain lookup hashes. Resolve draining reads from the frozen manifest rather than parsing the tombstone. Do not store plaintext bearer.

**Suggested commit:** `feat(access): drain final deletion credentials`

### Task 9: Implement and supervise the deletion worker

**Objective:** Execute external phases retryably without overlapping destructive actions or breaking shutdown.

**Files:**
- Create: `locks-server/src/deletion_worker.rs`
- Modify: `locks-server/src/main.rs`
- Modify: `locks-server/src/config/schema.rs`
- Modify: `locks-server/src/config/defaults.rs`
- Modify: `locks-server/src/config/validation.rs`
- Modify: `locks-server/src/app_state/readiness.rs`
- Modify: `locks-server/src/api/runtime.rs`
- Test: worker unit tests and `locks-e2e/tests/postgres_runtime.rs`

**RED:** Crash/reclaim tests after every external side effect; advisory ownership exclusion; tombstone read-back/replacement failure; retry exhaustion/resume; force escalation; content-first/tombstone-last; missing tombstone allowed only after durable final-removal phase; readiness degradation; shutdown stops claims and bounds worker join.

**GREEN:** Reuse existing worker configuration conventions but keep queue cadence and retry due time separate. Never log manifest, resource paths, Bundle IDs, credentials, readers, or Paykit payloads.

**Suggested commit:** `feat(server): run graceful deletion worker`

### Task 10: Purge graceful state and preserve force blocks

**Objective:** Complete graceful forget/republication without reactivating old authority, while permanently blocking force-deleted Lock IDs.

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
cargo test -p locks-sdk-js
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
- graceful same-ID republication with no old authorization revival;
- permanent force same-ID block.

## Remaining implementation-contract gates

These do not reopen accepted product semantics, but code must not start for the affected slice until both plans are patched identically:

1. Exact `request_state` and `payment_state` wire enum values and mappings for Paykit recovery/conflict conditions.
2. Exact aggregate drain response fields/status values.
3. Exact signed route/body for deleting completed Paykit operational drain state.
4. Exact Locks stable `failure_code` vocabulary.
5. Exact configuration keys for retry attempts/backoff and final credential windows, within the accepted defaults/maxima.

## Out of scope

- Paykit protocol `payment_due_at` field or accepted-expiry event.
- Automatic refunds or late-payment access.
- Manual/automatic Bitcoin payment from the reader.
- Cross-system transactions or exactly-once external effects.
- Historical production-data migration/backfill.
- Republishing force-deleted Lock IDs.
- Deleting reader-downloaded copies.
