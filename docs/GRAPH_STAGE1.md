# Graph Stage 1

Graph Stage 1 is bounded support for graph-only Content Lock v2 policies. It binds Reader identity through folded auth, evaluates selected Nexus `app_spec_v0_7` predicates, durably publishes entitlement, and exposes redacted lifecycle/evidence. It does not compose Graph with Paykit.

Architecture decisions: [ADR 0022](ADRs/0022-graph-stage1-folded-auth-nexus.md).

## Wire flow

1. Creator publishes Content Lock v2 with `graph` policy and no `payment` policy.
2. Viewer generates cryptographically random `bundle_id`, keeps it local, and sends Submitted Proof Bundle v2 to `POST /proof-bundles`.
3. Server returns `202 auth_pending` with public folded-auth `state`, `auth_url`, and `expires_at`.
4. Viewer approves in folded-auth UI, then calls `POST /proof-bundles/auth-status` with `state`.
5. `pending` means poll later. Failure before dependency dispatch is retryable. Ambiguous failure after attempted dispatch returns `restart_required`; resubmit same immutable bundle to create fresh auth state. Expired state returns `expired`.

Lock Server renews durable ownership while an approval poll is live. Cleanup recovers a crashed or abandoned poll after one minute without renewal; concurrent status requests remain `pending` and cannot invalidate active ownership.
6. Approved auth atomically binds Reader and creates durable graph task. Viewer polls `POST /verification-task-lookups` with `{ creator, bundle_id }`.
7. Worker evaluates Nexus. Operational failures retry until deadline. Invalid successful response fails closed. Valid false remains pending until policy can no longer be satisfied. Valid true is retained monotonically.
8. Satisfied policy enters `publishing_entitlement`; publication retries under claim fencing. Completed lifecycle enables existing access-credential flow.
9. Authenticated Creator reads redacted result from `POST /creator/verification-evidence`.

Public responses never include internal task UUID, claim token, folded-auth secret, raw Nexus body, dependency config, runtime master key, or unredacted failure history.

## Policy and Nexus mapping

Stage 1 supports bounded predicates from `GraphPredicateV2`:

| Predicate | Nexus `app_spec_v0_7` read | Meaning |
| --- | --- | --- |
| `follows { follower, followed }` | `/v0/user/{followed}/relationship/{follower}` | Followed identity is subject; follower is viewer. Preserve participant direction exactly. |
| `tagged { tagger, tagged, label }` | `/v0/user/{tagged}/taggers/{label}?viewer_id={tagger}` | Label is trimmed, Unicode-lowercased, then path-segment encoded. |

`true` and `false` are valid observations. Timeout, transport error, or non-success HTTP is operational. Malformed successful JSON is invalid. Unknown/missing/extra contract fields fail closed.

## Rust SDK

Use canonical request planners and response parsers; do not rebuild route bodies manually:

```rust
use locks_sdk::{FoldedAuthStatusRequest, ViewerLocks};

let submitted_request = viewer.submit_graph_proof_bundle(bundle_v2);
// Execute request with caller transport, then parse closed response.
let submitted = ViewerLocks::parse_graph_submit_response(submitted_json)?;

if let Some((_auth_url, state, _expires_at)) = submitted.auth_pending_parts() {
    let poll_request = viewer.poll_folded_auth(FoldedAuthStatusRequest::new(state));
    let auth = ViewerLocks::parse_folded_auth_status_response(auth_json)?;
    // pending -> poll later; restart_required -> resubmit immutable bundle.
}

let status_request = viewer.lookup_graph_verification_task(handle);
let lifecycle = ViewerLocks::parse_graph_lifecycle_response(status_json)?;
```

Creator session uses `CreatorLocks::verification_evidence(bundle_id)` and its closed response parser. Bundle ID and auth state are sensitive caller-managed values.

## JS/WASM SDK

Generated package exposes same contract as plain JavaScript objects:

```js
import { VerificationTaskHandleOptions } from "@synonymdev/locks-sdk";

const submitted = await viewer.submitProofBundle(bundleV2);
let state = submitted.state;

while (submitted.status === "auth_pending") {
  const auth = await viewer.pollFoldedAuth(state);
  if (auth.status === "pending") continue;
  if (auth.status === "restart_required") {
    // Re-submit same immutable bundle. Replace old state with returned state.
    break;
  }
  if (auth.status === "accepted") break;
}

const handle = new VerificationTaskHandleOptions(creator, bundleId);
const lifecycle = await viewer.lookupGraphVerificationTask(handle);
const evidence = await creator.verificationEvidence(bundleId);
```

Never put `bundleId` or folded-auth state in URL/query, analytics, exception telemetry, or shared browser storage. SDK does not expose folded-auth flow secret.

## Deterministic local E2E

Prerequisites: Docker Engine with Compose v2. No real auth, Nexus, Pubky, Paykit, or live network service is called by test logic.

```bash
./scripts/test-graph-stage1-compose.sh
```

Harness creates isolated Compose project `locks-graph-stage1-e2e`:

- `postgres:16-alpine@sha256:0cefaabd882e42bcb2a2f3dc56d3ce4770560ba62fd7f197f1a2e008fd5731ab` with disposable tmpfs data;
- `python:3.13-alpine@sha256:7984cf56ceaddebe4fefeb75d64000b9f45d8b77b11a09b24524da6cdc3af0c8` serving deterministic folded-auth on loopback port `18080` and deterministic Nexus on loopback port `18081`;
- `rust:1.91.1-bookworm@sha256:c1e5f19e773b7878c3f7a805dd00a495e747acbdc76fb2337a4ebf0418896b33` running only `locks-e2e/tests/graph_stage1_compose.rs` in fake service network namespace.

Script validates Compose config, starts healthy dependencies, returns test exit code, and always removes containers/network. Cargo registry/target named volumes are cache only; PostgreSQL data never persists. Remove caches if a byte-clean toolchain rebuild is desired:

```bash
docker volume rm \
  locks-graph-stage1-e2e_graph-cargo-registry \
  locks-graph-stage1-e2e_graph-cargo-target
```

Expected E2E assertions:

- first attempted ambiguous auth poll -> `restart_required`;
- resubmission rotates public state;
- pending approval survives reconstruction of private auth/task adapters over same PostgreSQL; deterministic in-memory content/entitlement fakes remain one external-dependency fixture, while Pubky publication durability is covered by its owning adapter tests;
- approved flow creates pending graph task;
- worker makes exact Nexus path, completes publication, lifecycle returns `completed`, creator evidence is graph-only;
- selected direction, normalized/encoded label, valid false, malformed, unavailable, and timeout paths are classified correctly;
- graph row remains `invoice_admission_phase = ready` with no invoice intent;
- payment-drain table stays empty and fake dependency records zero Paykit calls;
- responses contain no denied secret/internal fields.

## Fault and ownership matrix

| Behavior | Owning verification |
| --- | --- |
| Folded-auth pending/approved/restart/expiry/encrypted restore | server route tests, Postgres folded-auth repository tests, Compose E2E |
| Nexus true/false/path/direction/label/malformed/unavailable/timeout | Nexus client tests, graph worker tests, Compose E2E |
| Durable deadline expiry and retry schedule | graph task repository and graph worker tests |
| Claim loss, stale-worker completion, publication retry/fencing | graph task repository and graph worker tests |
| Deletion admission fence, publication drain, retry, force behavior | deletion use-case/worker and Postgres repository tests |
| Rust/JS closed parsers, generated declarations, browser secret storage | SDK Rust tests, JS/WASM behavior/package/generated smoke |
| Payment v1 unchanged | existing Paykit route, worker, SDK, E2E, and JS example tests |

## Reproducibility gates

Run from repository root:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
./scripts/test-graph-stage1-compose.sh
./scripts/test-graph-stage1-compose.sh
npm --prefix locks-sdk/bindings/js test
npm --prefix locks-sdk/bindings/js run build
npm --prefix locks-sdk/bindings/js run smoke:pkg
npm --prefix locks-sdk/bindings/js run smoke:examples
git diff --check
git status --short
```

PostgreSQL-backed repository tests additionally require disposable `TEST_DATABASE_URL`. CI runs those with PostgreSQL 16. Generated `pkg/` remains ignored; reproducibility means repeated build/smoke produces byte-equivalent checked public declarations/package contract and leaves tracked tree unchanged.

Dependency security evidence:

```bash
cargo audit
cargo tree --workspace --locked > /tmp/locks-cargo-tree.txt
sha256sum Cargo.lock examples/js-sdk/package-lock.json
```

Record exact commands, counts, hashes, container/image receipts, and any unavailable gate. Do not record credentials; redact database passwords and bearer values.

## Boundaries

Included: graph-only Content Lock v2, folded auth, selected Nexus `app_spec_v0_7`, durable graph worker/publication, redacted evidence, deterministic fake-service E2E.

Excluded: Stage 2 zero-confirmation Graph+Paykit composition, Stage 3 generalized policy/migration, live dependency smoke, Paykit changes, credential semantic changes, production deployment.
