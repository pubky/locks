# Paykit-backed viewer flow

Locks' signed Paykit lifecycle integration adds `VerificationTaskLifecycleResponse.terminal_reason` and the public Rust `VerificationTerminalReason` enum. JS/WASM consumers receive the wire string on the existing lifecycle object; no new JS class export is needed. Lock Server owns invoice creation, payment observation, and verification.

Runnable browser example: [`examples/js-sdk/paykit-viewer-flow.js`](../examples/js-sdk/paykit-viewer-flow.js). Its smoke test executes lifecycle handling with public generated-package method names and verifies credential placement.

## JS/WASM

```js
import { unlockPaykitContent } from './paykit-viewer-flow.js';
import init, { BundleId } from '../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js';

await init();
const bundleId = BundleId.generate().toString();
await persistBundleId(bundleId); // application-owned durable, secret-safe storage

const { credentialExpiresAt, response } = await unlockPaykitContent({
  resource: 'pubky.../pub/app.locks/<lock_id>.json',
  readerPublicKey: 'pubky...',
  guardedPath: 'primary.txt',
  bundleId,
  pkarrRelays: ['http://127.0.0.1:15411'], // omit on normal public network
  onConnectionState(state) {
    // none | handshake | connected | recovery_required | blocked
    renderConnectionState(state);
  },
  onConnectionError(error) {
    // Connection observation is informational. Keep lifecycle polling.
    console.warn('Paykit connection state unavailable', error);
  },
});

// Bundle ID was durable before invoice creation. Consume body once.
console.log(bundleId, credentialExpiresAt, await response.arrayBuffer());
```

Await the generated package's default `init` export before using any wasm-backed
export. Later `init()` calls return the already initialized module, so the helper's
defensive initialization is safe. `bundleId` is bearer-like recovery state.
Generate and persist it before calling
`unlockPaykitContent`; the helper validates it before submission. If submission,
polling, credential issuance, or proxy-read fails, reuse that same Bundle ID with
the same resource, reader, and proof. Exact proof-bundle replay is idempotent and
returns the existing verification lifecycle, so it does not create another invoice.
Generate a new Bundle ID only for a new verification attempt.

Copy `unlockPaykitContent` and `runPaykitViewerFlow` from the linked example into an application module. They use only generated public exports:

- `LocksOptions`
- `Locks.forContentLockWithOptions`
- `Locks.readContentLockWithOptions`
- `BundleId.generate`
- `VerificationTaskHandleOptions`
- `Viewer.submitProofBundle`
- `Viewer.lookupVerificationTask`
- `Viewer.lookupPaykitConnectionState`
- `Viewer.issueAccessCredential`
- `Viewer.proxyReadGuardedResourceResponse`

The submitted proof has one `paykit-payment` proof. Reader identity is top-level; proof payload is empty:

```json
{
  "version": 1,
  "bundle_id": "000G40R40M30E209185GR38E1W",
  "pubky_lock_resource": "pubky.../pub/app.locks/<lock_id>.json",
  "reader_public_key": "pubky...",
  "proofs": [
    {
      "criterion_id": "payment-criterion",
      "verifier_type": "paykit-payment",
      "payload": {}
    }
  ]
}
```

Payment amount, asset, and recipient come from content-lock criterion params. Do not place them in proof payload.

## Request and response contracts

| SDK call | Request | Success response |
| --- | --- | --- |
| `submitProofBundle(bundle)` | `POST /proof-bundles` with `{ "submitted_proof_bundle": bundle }` | lifecycle object |
| `lookupVerificationTask(handle)` | `POST /verification-task-lookups` with `{ creator, bundle_id }` | lifecycle object |
| `lookupPaykitConnectionState(handle)` | `POST /paykit-connection-state-lookups` with `{ creator, bundle_id }` | `{ "state": "..." }` |
| `issueAccessCredential(handle)` | `POST /access-credentials` with `{ creator, bundle_id }` | `{ credential, expires_at }` |
| `proxyReadGuardedResourceResponse(credential, path)` | `GET /priv-resources/content/<encoded-path>` with bearer header | native `Response` |

Lifecycle response:

```json
{
  "creator": "pubky...",
  "bundle_id": "000G40R40M30E209185GR38E1W",
  "status": "pending",
  "submitted_at": "2026-09-29T12:00:00Z",
  "started_at": null,
  "completed_at": null,
  "failure_message": null,
  "status_message": "Reader wallet setup needed",
  "admission_deadline_at": "2026-09-29T12:10:00Z",
  "terminal_reason": null
}
```

Handle lifecycle states as closed vocabulary:

| Status | Required fields | Consumer action |
| --- | --- | --- |
| `pending` | `started_at`, `completed_at`, `failure_message`, and `terminal_reason` are `null`; `status_message` and `admission_deadline_at` are nullable | If `status_message` is `Reader wallet setup needed`, show it. Use deadline as DB-owned progress cutoff, not synthetic failure. Keep polling same task; do not resubmit. |
| `in_progress` | `started_at` is set; `completed_at`, `failure_message`, `admission_deadline_at`, and `terminal_reason` are `null` | Wait, then call `lookupVerificationTask` again. |
| `completed` | `started_at` and `completed_at` are set; `failure_message`, `admission_deadline_at`, and `terminal_reason` are `null` | Call `issueAccessCredential`, then proxy-read an authorized relative path. |
| `failed` | `started_at` and `completed_at` are set; `failure_message` is non-empty; `admission_deadline_at` and `terminal_reason` are `null` | Stop polling. Do not issue a credential. |
| `expired` | `started_at` and `completed_at` are set; `failure_message` and `admission_deadline_at` are `null`; `terminal_reason` is set | Stop polling. Do not issue a credential. |
| anything else or any invalid tuple | Fail closed; client and server contract are incompatible. |

`status_message` is viewer-safe progress text, not proof of invoice readiness or entitlement. Current non-null value is exactly `Reader wallet setup needed`, projected while backend retries missing or malformed/oversized Reader wallet registry state inside original 10-minute admission deadline. Once observed it remains sticky across unrelated transient retries until ready or failed. `admission_deadline_at` is persisted DB-owned time exposed only while invoice admission is pending; an overdue value can indicate a stalled worker but does not mean worker ran or task failed. Keep polling. Browser/host clock skew makes local countdowns approximate.

`expired` identifies a normal terminal payment-request outcome, unlike `failed`, which carries a safe verification failure message. Its exact `terminal_reason` wire values are:

| Wire value | Meaning |
| --- | --- |
| `payment_request_rejected` | Reader rejected payment request. |
| `payment_request_canceled` | Payment request was canceled. |
| `proposal_expired` | Payment proposal expired before acceptance. |
| `payment_deadline_expired` | Accepted payment request reached its payment deadline. |

All four outcomes are terminal: stop browser/Rust polling, issue no access credential, and grant no entitlement. To retry, submit a new attempt with a fresh Bundle ID and payment request; replaying same Bundle ID returns same terminal lifecycle.

JS/WASM methods reject unknown statuses, unknown terminal reasons, extra private fields, and invalid tuples. Keep application handling closed too:

```js
function nextAction(lifecycle) {
  switch (lifecycle.status) {
    case 'pending':
    case 'in_progress':
      return 'poll';
    case 'completed':
      return 'issue-credential';
    case 'failed':
      throw new Error(`verification failed: ${lifecycle.failure_message}`);
    case 'expired':
      switch (lifecycle.terminal_reason) {
        case 'payment_request_rejected':
        case 'payment_request_canceled':
        case 'proposal_expired':
        case 'payment_deadline_expired':
          throw new Error(
            `${lifecycle.terminal_reason}; retry with a fresh Bundle ID and payment request`,
          );
        default:
          throw new Error('invalid verification terminal reason');
      }
    default:
      throw new Error('invalid verification lifecycle status');
  }
}
```

Migration note: lifecycle JSON includes `terminal_reason` on every response (`null` unless status is `expired`), nullable `status_message`, and nullable `admission_deadline_at`. Consumers with exact object comparisons, JSON schemas, TypeScript interfaces, or destructuring assumptions for older payloads must accept these fields before deploying against this server version.

Connection state is independent from verification lifecycle:

| State | Meaning |
| --- | --- |
| `none` | Handshake has not started. |
| `handshake` | Link setup is in progress. |
| `connected` | Paykit link is usable; payment is not necessarily complete. |
| `recovery_required` | Runtime must recover or relink. Do not resubmit proof to refresh state. |
| `blocked` | Operator action is required; stop connection-state polling. |

A timeout or error from `lookupPaykitConnectionState` must not delay or stop authoritative lifecycle polling. Never replay `submitProofBundle` merely to refresh connection state.

Access credential is returned exactly once. Keep it out of URLs, JSON bodies, logs, and durable analytics. Pass it only to `proxyReadGuardedResource` or `proxyReadGuardedResourceResponse`; SDK places it in the `Authorization` bearer header.

## Rust request planner

Rust `locks-sdk` builds canonical requests and parses closed responses. It does not execute HTTP; caller sends each `SdkViewerRequest` through its transport.

```rust
use locks_core::verification::SubmittedProofBundle;
use locks_sdk::{
    Result, SdkViewerRequest, VerificationTaskHandleRequest,
    VerificationTaskStatus, VerificationTerminalReason, ViewerLocks,
};
use serde_json::Value;

enum NextRequest {
    Poll(SdkViewerRequest),
    IssueCredential(SdkViewerRequest),
    Failed(String),
    Expired(VerificationTerminalReason),
}

fn plan_after_submit(
    submitted_proof_bundle: SubmittedProofBundle,
    submit_response_json: Value,
) -> Result<(SdkViewerRequest, NextRequest)> {
    let viewer = ViewerLocks::new();
    let submit_request = viewer.submit_proof_bundle(submitted_proof_bundle);
    let lifecycle =
        ViewerLocks::parse_submit_proof_bundle_response(submit_response_json)?;
    let handle = VerificationTaskHandleRequest {
        creator: lifecycle.creator,
        bundle_id: lifecycle.bundle_id,
    };
    let next = match lifecycle.status {
        VerificationTaskStatus::Pending | VerificationTaskStatus::InProgress => {
            NextRequest::Poll(viewer.lookup_verification_task(handle))
        }
        VerificationTaskStatus::Completed => {
            NextRequest::IssueCredential(viewer.issue_access_credential(handle))
        }
        VerificationTaskStatus::Failed => {
            NextRequest::Failed(lifecycle.failure_message.expect("validated failed response"))
        }
        VerificationTaskStatus::Expired => {
            NextRequest::Expired(
                lifecycle.terminal_reason.expect("validated expired response"),
            )
        }
    };
    Ok((submit_request, next))
}

fn expired_message(reason: VerificationTerminalReason) -> &'static str {
    match reason {
        VerificationTerminalReason::PaymentRequestRejected => "payment request rejected",
        VerificationTerminalReason::PaymentRequestCanceled => "payment request canceled",
        VerificationTerminalReason::ProposalExpired => "proposal expired",
        VerificationTerminalReason::PaymentDeadlineExpired => "payment deadline expired",
    }
}
```

Send returned request through caller transport. Parse each later lifecycle response with `parse_lifecycle_response`; it rejects unknown/invalid response shapes. Parse credential response with `parse_access_credential_response`, then pass only its `credential` field to `proxy_read_guarded_resource`. `Failed` and `Expired` are terminal and must not issue a credential; a retry needs a fresh Bundle ID and payment request.

Compile-enforced public consumer contract: [`locks-sdk/tests/paykit_viewer_flow.rs`](../locks-sdk/tests/paykit_viewer_flow.rs). It verifies exact request bodies/routes, every lifecycle and connection state, invalid-state rejection, and bearer-only credential placement.
