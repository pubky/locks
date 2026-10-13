use std::str::FromStr;

use locks_core::{
    ids::{BundleId, CreatorPubky},
    verification::SubmittedProofBundle,
};
use locks_sdk::{
    PaykitConnectionState, VerificationTaskHandleRequest, VerificationTaskStatus,
    VerificationTerminalReason, ViewerLocks,
};
use serde_json::{Value, json};

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo";
const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
const LOCK_RESOURCE: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json";

#[test]
fn public_api_builds_documented_paykit_viewer_requests() {
    let viewer = ViewerLocks::new();
    let submitted: SubmittedProofBundle = serde_json::from_value(json!({
        "version": 1,
        "bundle_id": BUNDLE_ID,
        "pubky_lock_resource": LOCK_RESOURCE,
        "reader_public_key": READER,
        "proofs": [{
            "criterion_id": "payment-criterion",
            "verifier_type": "paykit-payment",
            "payload": {}
        }]
    }))
    .unwrap();

    let submit = viewer.submit_proof_bundle(submitted);
    assert_eq!(submit.method, "POST");
    assert_eq!(submit.path, "/proof-bundles");
    assert_eq!(submit.authorization, None);
    assert_eq!(
        submit.body,
        json!({
            "submitted_proof_bundle": {
                "version": 1,
                "bundle_id": BUNDLE_ID,
                "pubky_lock_resource": LOCK_RESOURCE,
                "reader_public_key": READER,
                "proofs": [{
                    "criterion_id": "payment-criterion",
                    "verifier_type": "paykit-payment",
                    "payload": {}
                }]
            }
        })
    );

    let handle = handle();
    for (request, path) in [
        (
            viewer.lookup_verification_task(handle.clone()),
            "/verification-task-lookups",
        ),
        (
            viewer.lookup_paykit_connection_state(handle.clone()),
            "/paykit-connection-state-lookups",
        ),
        (
            viewer.issue_access_credential(handle),
            "/access-credentials",
        ),
    ] {
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, path);
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "creator": CREATOR, "bundle_id": BUNDLE_ID })
        );
    }
}

#[test]
fn public_api_parses_every_documented_lifecycle_and_connection_state() {
    for (wire, expected) in [
        ("pending", VerificationTaskStatus::Pending),
        ("in_progress", VerificationTaskStatus::InProgress),
        ("completed", VerificationTaskStatus::Completed),
        ("failed", VerificationTaskStatus::Failed),
        ("expired", VerificationTaskStatus::Expired),
    ] {
        let response = ViewerLocks::parse_lifecycle_response(lifecycle_json(wire)).unwrap();
        assert_eq!(response.status, expected);
    }

    for (wire, expected) in [
        ("none", PaykitConnectionState::None),
        ("handshake", PaykitConnectionState::Handshake),
        ("connected", PaykitConnectionState::Connected),
        ("recovery_required", PaykitConnectionState::RecoveryRequired),
        ("blocked", PaykitConnectionState::Blocked),
    ] {
        let response = ViewerLocks::parse_paykit_connection_state_response(json!({
            "state": wire
        }))
        .unwrap();
        assert_eq!(response.state, expected);
    }

    assert!(ViewerLocks::parse_lifecycle_response(lifecycle_json("future")).is_err());
    assert!(
        ViewerLocks::parse_paykit_connection_state_response(json!({ "state": "future" })).is_err()
    );
}

#[test]
fn public_api_parses_every_terminal_reason_and_valid_terminal_tuple() {
    for (wire, expected) in [
        (
            "payment_request_rejected",
            VerificationTerminalReason::PaymentRequestRejected,
        ),
        (
            "payment_request_canceled",
            VerificationTerminalReason::PaymentRequestCanceled,
        ),
        (
            "proposal_expired",
            VerificationTerminalReason::ProposalExpired,
        ),
        (
            "payment_deadline_expired",
            VerificationTerminalReason::PaymentDeadlineExpired,
        ),
    ] {
        let response = ViewerLocks::parse_lifecycle_response(json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "expired",
            "submitted_at": "2026-09-29T12:00:00Z",
            "started_at": "2026-09-29T12:00:01Z",
            "completed_at": "2026-09-29T12:00:02Z",
            "failure_message": null,
            "terminal_reason": wire
        }))
        .unwrap();
        assert_eq!(response.terminal_reason, Some(expected));
    }

    let completed = ViewerLocks::parse_lifecycle_response(lifecycle_json("completed")).unwrap();
    assert_eq!(completed.status, VerificationTaskStatus::Completed);
    assert!(completed.failure_message.is_none());
    assert!(completed.terminal_reason.is_none());

    let failed = ViewerLocks::parse_lifecycle_response(lifecycle_json("failed")).unwrap();
    assert_eq!(failed.status, VerificationTaskStatus::Failed);
    assert_eq!(
        failed.failure_message.as_deref(),
        Some("verification failed")
    );
    assert!(failed.terminal_reason.is_none());
}

#[test]
fn public_api_rejects_invalid_terminal_tuples_and_private_fields() {
    for invalid in [
        json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "expired",
            "submitted_at": "2026-09-29T12:00:00Z",
            "started_at": "2026-09-29T12:00:01Z",
            "completed_at": "2026-09-29T12:00:02Z",
            "failure_message": null,
            "terminal_reason": null
        }),
        json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "failed",
            "submitted_at": "2026-09-29T12:00:00Z",
            "started_at": "2026-09-29T12:00:01Z",
            "completed_at": "2026-09-29T12:00:02Z",
            "failure_message": "verification failed",
            "terminal_reason": "payment_request_rejected"
        }),
        json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "expired",
            "submitted_at": "2026-09-29T12:00:00Z",
            "started_at": "2026-09-29T12:00:01Z",
            "completed_at": "2026-09-29T12:00:02Z",
            "failure_message": null,
            "terminal_reason": "future_reason"
        }),
        json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "pending",
            "submitted_at": "2026-09-29T12:00:00Z",
            "started_at": null,
            "completed_at": null,
            "failure_message": null,
            "terminal_reason": null,
            "task_id": "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            "credential": "secret-access-credential"
        }),
    ] {
        assert!(ViewerLocks::parse_lifecycle_response(invalid).is_err());
    }
}

#[test]
fn public_api_places_access_credential_only_in_bearer_header() {
    let viewer = ViewerLocks::new();
    let issued = ViewerLocks::parse_access_credential_response(json!({
        "credential": "secret-access-credential",
        "expires_at": "2026-09-29T12:15:00Z"
    }))
    .unwrap();

    let request = viewer.proxy_read_guarded_resource(&issued.credential, "primary file.txt");

    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/priv-resources/content/primary%20file.txt");
    assert_eq!(
        request.authorization.as_deref(),
        Some("Bearer secret-access-credential")
    );
    assert_eq!(request.body, Value::Null);
    assert!(!request.path.contains(&issued.credential));
}

fn handle() -> VerificationTaskHandleRequest {
    VerificationTaskHandleRequest {
        creator: CreatorPubky::from_str(CREATOR).unwrap(),
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
    }
}

fn lifecycle_json(status: &str) -> Value {
    let (started_at, completed_at, failure_message, terminal_reason) = match status {
        "pending" => (Value::Null, Value::Null, Value::Null, Value::Null),
        "in_progress" => (
            json!("2026-09-29T12:00:01Z"),
            Value::Null,
            Value::Null,
            Value::Null,
        ),
        "completed" => (
            json!("2026-09-29T12:00:01Z"),
            json!("2026-09-29T12:00:02Z"),
            Value::Null,
            Value::Null,
        ),
        "failed" => (
            json!("2026-09-29T12:00:01Z"),
            json!("2026-09-29T12:00:02Z"),
            json!("verification failed"),
            Value::Null,
        ),
        "expired" => (
            json!("2026-09-29T12:00:01Z"),
            json!("2026-09-29T12:00:02Z"),
            Value::Null,
            json!("payment_request_rejected"),
        ),
        _ => (Value::Null, Value::Null, Value::Null, Value::Null),
    };
    json!({
        "creator": CREATOR,
        "bundle_id": BUNDLE_ID,
        "status": status,
        "submitted_at": "2026-09-29T12:00:00Z",
        "started_at": started_at,
        "completed_at": completed_at,
        "failure_message": failure_message,
        "terminal_reason": terminal_reason
    })
}
