# ADR 0022: Graph Stage 1 uses folded auth and bounded Nexus verification

- Status: Accepted
- Date: 2026-11-03
- Scope: graph-only Content Lock v2 verification

## Context

A graph-gated Lock must bind an anonymous browser submission to a Reader Pubky before Locks can ask Nexus about Creator/Reader relationships. The binding can require human approval, spans process restarts, and must not expose auth secrets. Graph observations can also be unavailable or malformed; absence of a trustworthy observation must never grant access.

Stage 1 must coexist with `paykit-payment` v1 without importing, calling, or creating Paykit state for graph-only tasks. Later zero-confirmation Graph+Paykit composition and general mixed-verifier policy remain separate work.

## Decision

### D1: one attempted ambiguous poll requires restart

`POST /proof-bundles` starts folded auth and returns `auth_pending` with public `state`, `auth_url`, and `expires_at`. `POST /proof-bundles/auth-status` owns polling.

A poll which was dispatched to the folded-auth dependency but does not produce a trustworthy `pending` or `approved` result consumes that auth attempt and returns `restart_required`. Clients start a fresh flow by resubmitting the same graph bundle. Locks does not silently retry an attempted ambiguous poll because doing so could consume or reinterpret a one-shot upstream approval.

While a dependency poll is live, Lock Server renews its durable dispatch ownership every 20 seconds. Cleanup treats an unrenewed dispatch as abandoned at one minute, marks it `restart_required`, and generation-fences any later stale completion. Concurrent status requests observe `pending`; they do not take ownership from the active poll.

A failure before dispatch is retryable on the same public state. Expired state returns `expired`. Neither route returns folded-auth flow secret, encrypted state, internal attempt key, raw dependency body, or Reader identity before approval.

### D2: minimum durable restore state is encrypted

PostgreSQL stores only state needed to restore the pending flow after restart: a one-way hash of public state, creator/bundle/generation identity, immutable submitted bundle and lock/policy snapshot, creation/expiry times, lifecycle marker, and encrypted folded-auth flow secret. Auth secret encryption uses the runtime master key boundary; plaintext public state and plaintext flow secrets are not persisted and never enter public DTOs, logs, or evidence.

After approval, Locks canonicalizes and stores the Reader binding, atomically admits the graph verification task, then consumes the pending auth state. Reconstructing runtime adapters over the same DB must therefore continue a pending flow without asking the browser to restart.

### D3: Nexus contract is `app_spec_v0_7`

Stage 1 uses only selected `app_spec_v0_7` reads:

- follows: `/v0/user/{followed}/relationship/{follower}` (`followed` is subject; `follower` is viewer)
- tagged: `/v0/user/{tagged}/taggers/{normalized-percent-encoded-label}?viewer_id={tagger}`

Participant direction is semantic, not cosmetic. `Reader -> Creator` and `Creator -> Reader` produce different paths. Labels are trimmed, Unicode-lowercased, then path-segment encoded. Nexus origins are exact HTTP(S) origins; insecure HTTP is accepted only for explicit loopback test configuration.

A valid `false` is a real observation, not dependency failure. Non-success, timeout, or transport failure is operational and retryable while task deadline remains. Malformed successful response is invalid and terminal. If no required predicate becomes true before durable deadline, task expires/fails closed with no entitlement.

### Evaluation and publication

Graph task state, predicate snapshot, deadline, claims, and publication ownership are durable. Worker claims fence stale writers. Required predicates are monotonic within one attempt: first trustworthy `true` is retained; later `false` does not erase it. Once policy is satisfied, entitlement publication is a separate retryable phase. A stale worker cannot publish or complete after losing claim ownership. Publication faults do not discard satisfied graph evidence.

Content Lock deletion freezes admission, snapshots affected work, drains publication, and only then removes lock/resource state. Force deletion explicitly abandons unresolved obligations. Graph tasks follow same admission/deletion fences as payment tasks but create no payment drain.

### Public clients

Rust SDK uses canonical planners and closed parsers:

```rust
let submitted = viewer.submit_graph_proof_bundle(bundle_v2);
let auth = viewer.poll_folded_auth(auth_request);
let status = viewer.lookup_graph_verification_task(handle);
let evidence = creator.verification_evidence(bundle_id);
```

Generated JS/WASM exposes same flow as plain objects:

```js
import { VerificationTaskHandleOptions } from "@synonymdev/locks-sdk";

const submitted = await viewer.submitProofBundle(bundleV2);
const auth = await viewer.pollFoldedAuth(submitted.state);
const handle = new VerificationTaskHandleOptions(creator, bundleId);
const status = await viewer.lookupGraphVerificationTask(handle);
const evidence = await creator.verificationEvidence(bundleId);
```

Browsers store `bundle_id` and folded-auth `state` as sensitive local application state. They never store folded-auth flow secret or runtime master key. `restart_required` means resubmit same immutable bundle; terminal graph task retry requires fresh Bundle ID.

## Security and redaction

- `bundle_id` is bearer-secret-like and stays in JSON bodies, never URL path/query.
- Folded-auth flow secrets are encrypted at rest and omitted from API, evidence, and logs.
- Nexus bodies and operational details are not returned to viewer responses.
- Creator evidence is session-authenticated and redacted to policy/predicate outcome data.
- Graph-only runtime and tests set `paykit = None`; graph admission writes no invoice intent, payment drain, Paykit request, or status link.

## Consequences

Positive:

- Auth survives server restart without exposing approval material.
- Ambiguous attempted polls fail safe.
- Nexus contract, direction, label normalization, retries, and deadlines are deterministic.
- Entitlement publication remains fenced from stale workers.
- Graph and Paykit boundaries stay independent.

Costs:

- Browser must handle an explicit auth restart state.
- Pending auth depends on runtime master-key continuity.
- Availability failures can delay decision until deadline rather than producing immediate denial.

## Out of scope

Stage 2 zero-confirmation Graph+Paykit composition, Stage 3 generalized policy/migration, live auth/Nexus/Paykit integration, and changes to payment v1 or credential semantics are excluded.

## Verification

`compose.graph-stage1-e2e.yaml` and `scripts/test-graph-stage1-compose.sh` run disposable PostgreSQL plus deterministic fake folded-auth/Nexus listeners. They cover submit -> ambiguous attempted poll -> restart -> private auth/task adapter reconstruction over PostgreSQL -> approval -> status -> worker publication -> creator evidence, selected Nexus true/false/path/direction/label/malformed/unavailable/timeout behavior, redaction, and zero-Paykit DB/call assertions. Content/entitlement fakes remain one deterministic external-dependency fixture; Pubky publication durability and broader fault/fencing behavior remain in owning Rust adapter/module tests. See [`GRAPH_STAGE1.md`](../GRAPH_STAGE1.md).
