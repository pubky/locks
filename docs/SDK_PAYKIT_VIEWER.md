# Paykit-backed viewer flow

Locks' signed Paykit lifecycle integration adds no new `locks-sdk` or JS/WASM export. SDK consumers compose existing public viewer calls; Lock Server owns invoice creation, payment observation, and verification.

Runnable browser example: [`examples/js-sdk/paykit-viewer-flow.js`](../examples/js-sdk/paykit-viewer-flow.js). Its smoke test executes lifecycle handling with public generated-package method names and verifies credential placement.

## JS/WASM

```js
import { unlockPaykitContent } from './paykit-viewer-flow.js';
import { BundleId } from '../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js';

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

`bundleId` is bearer-like recovery state. Generate and persist it before calling
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
  "failure_message": null
}
```

Handle lifecycle states as closed vocabulary:

| Status | Consumer action |
| --- | --- |
| `pending`, `in_progress` | Wait, then call `lookupVerificationTask` again. |
| `completed` | Call `issueAccessCredential`, then proxy-read an authorized relative path. |
| `failed`, `expired` | Terminal failure. Do not issue a credential. |
| anything else | Fail closed; client and server contract are incompatible. |

Connection state is independent from verification lifecycle:

| State | Meaning |
| --- | --- |
| `none` | Handshake has not started. |
| `handshake` | Link setup is in progress. |
| `connected` | Paykit link is usable; payment is not necessarily complete. |
| `recovery_required` | Runtime must recover or relink. Do not resubmit proof to refresh state. |
| `blocked` | Operator action is required; stop connection-state polling. |

A timeout or error from `lookupPaykitConnectionState` must not delay or stop authoritative lifecycle polling. Never replay `submitProofBundle` merely to refresh connection state.

Access credential is returned exactly once. Keep it out of URLs, JSON bodies, logs, and durable analytics. Pass it only to `proxyReadGuardedResource` or `proxyReadGuardedResourceResponse`; SDK places it in `Authorization: Bearer <credential>`.

## Rust request planner

Rust `locks-sdk` builds canonical requests and parses closed responses. It does not execute HTTP; caller sends each `SdkViewerRequest` through its transport.

```rust
use locks_core::verification::SubmittedProofBundle;
use locks_sdk::{
    Result, SdkViewerRequest, VerificationTaskHandleRequest,
    VerificationTaskStatus, ViewerLocks,
};
use serde_json::Value;

enum NextRequest {
    Poll(SdkViewerRequest),
    IssueCredential(SdkViewerRequest),
    Terminal,
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
        VerificationTaskStatus::Failed | VerificationTaskStatus::Expired => {
            NextRequest::Terminal
        }
    };
    Ok((submit_request, next))
}
```

Send returned request through caller transport. Parse each later lifecycle response with `parse_lifecycle_response`; parse credential response with `parse_access_credential_response`, then pass only its `credential` field to `proxy_read_guarded_resource`.

Compile-enforced public consumer contract: [`locks-sdk/tests/paykit_viewer_flow.rs`](../locks-sdk/tests/paykit_viewer_flow.rs). It verifies exact request bodies/routes, every lifecycle and connection state, invalid-state rejection, and bearer-only credential placement.
