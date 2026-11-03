use std::str::FromStr;

use locks_core::ids::{BundleId, CreatorPubky};
use locks_core::verification::SubmittedProofBundleV2;
use locks_sdk::{
    FoldedAuthStatusRequest, FoldedAuthStatusResponse, GraphSubmitResponse,
    GraphVerificationTaskLifecycleResponse, LocksSession, VerificationTaskHandleRequest,
    VerificationTaskStatus, VerificationTerminalReason, ViewerLocks,
};
use serde_json::json;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo";
const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
const LOCK_RESOURCE: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json";

fn graph_bundle() -> SubmittedProofBundleV2 {
    serde_json::from_value(json!({
        "version": 2,
        "bundle_id": BUNDLE_ID,
        "pubky_lock_resource": LOCK_RESOURCE
    }))
    .unwrap()
}

fn graph_task(status: &str) -> serde_json::Value {
    let mut value = json!({
        "creator": CREATOR,
        "bundle_id": BUNDLE_ID,
        "status": status,
        "submitted_at": "2026-11-03T12:00:00Z",
        "admission_deadline_at": "2026-11-03T12:10:00Z"
    });
    if matches!(status, "completed" | "failed" | "expired") {
        value["completed_at"] = json!("2026-11-03T12:01:00Z");
    }
    value
}

fn graph_evidence(
    status: &str,
    terminal_reason: Option<&str>,
    satisfied_by: &[&str],
) -> serde_json::Value {
    let mut value = json!({
        "creator": CREATOR,
        "reader": READER,
        "bundle_id": BUNDLE_ID,
        "pubky_lock_resource": LOCK_RESOURCE,
        "mode": "graph",
        "status": status,
        "submitted_at": "2026-11-03T12:00:00Z",
        "deadline_at": "2026-11-03T12:10:00Z",
        "satisfied_by": satisfied_by,
        "graph": {"predicates": [{
            "index": 0,
            "predicate": {"type":"follows", "follower":"reader", "followed":"creator"},
            "state": "pending"
        }]}
    });
    if matches!(status, "completed" | "failed" | "expired") {
        value["completed_at"] = json!("2026-11-03T12:01:00Z");
    }
    if let Some(reason) = terminal_reason {
        value["terminal_reason"] = json!(reason);
    }
    value
}

#[test]
fn graph_create_and_folded_auth_poll_requests_are_exact_and_secret_free() {
    let viewer = ViewerLocks::new();
    let create = viewer.submit_graph_proof_bundle(graph_bundle());
    assert_eq!(create.method, "POST");
    assert_eq!(create.path, "/proof-bundles");
    assert_eq!(create.authorization, None);
    assert_eq!(create.body["submitted_proof_bundle"]["version"], 2);
    assert!(create.body.get("state").is_none());

    let poll = viewer.poll_folded_auth(FoldedAuthStatusRequest::new("opaque-state"));
    assert_eq!(poll.method, "POST");
    assert_eq!(poll.path, "/proof-bundles/auth-status");
    assert_eq!(poll.authorization, None);
    assert_eq!(poll.body, json!({ "state": "opaque-state" }));
    assert!(
        !format!("{:?}", FoldedAuthStatusRequest::new("opaque-state")).contains("opaque-state")
    );
}

#[test]
fn graph_submit_and_folded_auth_responses_parse_all_closed_variants() {
    let pending = ViewerLocks::parse_graph_submit_response(json!({
        "status": "auth_pending",
        "auth_url": "pubkyauth://secret-flow",
        "state": "opaque-state",
        "expires_at": "2026-11-03T12:05:00Z"
    }))
    .unwrap();
    assert!(matches!(pending, GraphSubmitResponse::AuthPending { .. }));
    assert!(!format!("{pending:?}").contains("secret-flow"));
    assert!(!format!("{pending:?}").contains("opaque-state"));

    let accepted = ViewerLocks::parse_graph_submit_response(json!({
        "status": "accepted",
        "task": graph_task("in_progress")
    }))
    .unwrap();
    assert!(matches!(accepted, GraphSubmitResponse::Accepted { .. }));

    for (value, expected) in [
        (
            json!({"status":"pending", "expires_at":"2026-11-03T12:05:00Z"}),
            "pending",
        ),
        (
            json!({"status":"accepted", "task":graph_task("in_progress")}),
            "accepted",
        ),
        (json!({"status":"restart_required"}), "restart_required"),
        (
            json!({"status":"expired", "expired_at":"2026-11-03T12:05:00Z"}),
            "expired",
        ),
    ] {
        let parsed = ViewerLocks::parse_folded_auth_status_response(value).unwrap();
        assert_eq!(parsed.status_name(), expected);
    }

    for invalid in [
        json!({"status":"future"}),
        json!({"status":"restart_required", "state":"secret"}),
        json!({"status":"pending", "expires_at":null}),
        json!({"status":"accepted"}),
    ] {
        assert!(
            ViewerLocks::parse_folded_auth_status_response(invalid.clone()).is_err(),
            "accepted invalid folded-auth response: {invalid}"
        );
    }
}

#[test]
fn graph_lifecycle_requires_omission_and_closed_status_reason_tuples() {
    let in_progress =
        ViewerLocks::parse_graph_lifecycle_response(graph_task("in_progress")).unwrap();
    assert_eq!(in_progress.status, VerificationTaskStatus::InProgress);
    assert!(in_progress.completed_at.is_none());
    assert!(in_progress.terminal_reason.is_none());

    let mut expired = graph_task("expired");
    expired["terminal_reason"] = json!("graph_condition_not_observed_by_deadline");
    let expired = ViewerLocks::parse_graph_lifecycle_response(expired).unwrap();
    assert_eq!(
        expired.terminal_reason,
        Some(VerificationTerminalReason::GraphConditionNotObservedByDeadline)
    );

    let mut null_optional = graph_task("in_progress");
    null_optional["completed_at"] = serde_json::Value::Null;
    assert!(ViewerLocks::parse_graph_lifecycle_response(null_optional).is_err());

    let mut unknown = graph_task("in_progress");
    unknown["task_id"] = json!("internal");
    assert!(ViewerLocks::parse_graph_lifecycle_response(unknown).is_err());

    let mut bad_completed = graph_task("completed");
    bad_completed["terminal_reason"] = json!("invalid_verifier_response");
    assert!(ViewerLocks::parse_graph_lifecycle_response(bad_completed).is_err());
}

#[test]
fn graph_lifecycle_accepts_only_exact_graph_terminal_reason_matrix() {
    let graph_reasons = [
        "graph_condition_not_observed_by_deadline",
        "verification_unavailable_by_deadline",
        "invalid_verifier_response",
    ];
    let all_reasons = [
        "payment_request_rejected",
        "payment_request_canceled",
        "proposal_expired",
        "payment_deadline_expired",
        "content_lock_deleted",
        graph_reasons[0],
        graph_reasons[1],
        graph_reasons[2],
    ];

    for (status, reason) in [
        ("expired", graph_reasons[0]),
        ("failed", graph_reasons[1]),
        ("failed", graph_reasons[2]),
    ] {
        let mut value = graph_task(status);
        value["terminal_reason"] = json!(reason);
        assert!(
            ViewerLocks::parse_graph_lifecycle_response(value).is_ok(),
            "rejected valid graph tuple: {status} + {reason}"
        );
    }

    for status in ["expired", "failed"] {
        for reason in all_reasons {
            let allowed = (status == "expired" && reason == graph_reasons[0])
                || (status == "failed" && graph_reasons[1..].contains(&reason));
            if allowed {
                continue;
            }
            let mut value = graph_task(status);
            value["terminal_reason"] = json!(reason);
            assert!(
                ViewerLocks::parse_graph_lifecycle_response(value).is_err(),
                "accepted impossible graph tuple: {status} + {reason}"
            );
        }
    }
}

#[test]
fn graph_creator_evidence_ties_satisfaction_to_exact_status_reason_matrix() {
    for value in [
        graph_evidence("pending", None, &[]),
        graph_evidence("in_progress", None, &[]),
        graph_evidence("publishing_entitlement", None, &["graph"]),
        graph_evidence("completed", None, &["graph"]),
        graph_evidence(
            "expired",
            Some("graph_condition_not_observed_by_deadline"),
            &[],
        ),
        graph_evidence("failed", Some("verification_unavailable_by_deadline"), &[]),
        graph_evidence("failed", Some("invalid_verifier_response"), &[]),
    ] {
        assert!(
            locks_sdk::CreatorLocks::parse_verification_evidence_response(value.clone()).is_ok(),
            "rejected valid graph evidence: {value}"
        );
    }

    let all_reasons = [
        "payment_request_rejected",
        "payment_request_canceled",
        "proposal_expired",
        "payment_deadline_expired",
        "content_lock_deleted",
        "graph_condition_not_observed_by_deadline",
        "verification_unavailable_by_deadline",
        "invalid_verifier_response",
    ];
    for status in ["expired", "failed"] {
        for reason in all_reasons {
            let value = graph_evidence(status, Some(reason), &[]);
            let allowed = (status == "expired"
                && reason == "graph_condition_not_observed_by_deadline")
                || (status == "failed"
                    && matches!(
                        reason,
                        "verification_unavailable_by_deadline" | "invalid_verifier_response"
                    ));
            assert_eq!(
                locks_sdk::CreatorLocks::parse_verification_evidence_response(value.clone())
                    .is_ok(),
                allowed,
                "wrong graph evidence reason decision: {value}"
            );
        }
    }

    let satisfaction_paths: [&[&str]; 4] = [&[], &["graph"], &["payment"], &["zero_conf_graph"]];
    for status in [
        "pending",
        "in_progress",
        "publishing_entitlement",
        "completed",
    ] {
        for satisfied_by in satisfaction_paths {
            let value = graph_evidence(status, None, satisfied_by);
            let allowed = matches!(status, "pending" | "in_progress") && satisfied_by.is_empty()
                || matches!(status, "publishing_entitlement" | "completed")
                    && satisfied_by == ["graph"];
            assert_eq!(
                locks_sdk::CreatorLocks::parse_verification_evidence_response(value.clone())
                    .is_ok(),
                allowed,
                "wrong graph evidence satisfaction decision: {value}"
            );
        }
    }

    for value in [
        graph_evidence(
            "expired",
            Some("graph_condition_not_observed_by_deadline"),
            &["graph"],
        ),
        graph_evidence(
            "failed",
            Some("verification_unavailable_by_deadline"),
            &["graph"],
        ),
    ] {
        assert!(
            locks_sdk::CreatorLocks::parse_verification_evidence_response(value.clone()).is_err(),
            "accepted terminal graph evidence with satisfaction: {value}"
        );
    }
}

#[test]
fn graph_status_and_creator_result_requests_complete_public_flow() {
    let viewer = ViewerLocks::new();
    let handle = VerificationTaskHandleRequest {
        creator: CreatorPubky::from_str(CREATOR).unwrap(),
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
    };
    assert_eq!(
        viewer.lookup_graph_verification_task(handle).path,
        "/verification-task-lookups"
    );

    let creator = LocksSession::new("frontend-session-secret").creator();
    let result = creator.verification_evidence(BundleId::from_str(BUNDLE_ID).unwrap());
    assert_eq!(result.method, "POST");
    assert_eq!(result.path, "/creator/verification-evidence");
    assert_eq!(result.authorization, "Bearer frontend-session-secret");
    assert_eq!(
        result.body,
        locks_sdk::SdkRequestBody::Json(json!({"bundle_id":BUNDLE_ID}))
    );

    let evidence = locks_sdk::CreatorLocks::parse_verification_evidence_response(json!({
        "creator": CREATOR,
        "reader": READER,
        "bundle_id": BUNDLE_ID,
        "pubky_lock_resource": LOCK_RESOURCE,
        "mode": "graph",
        "status": "completed",
        "submitted_at": "2026-11-03T12:00:00Z",
        "deadline_at": "2026-11-03T12:10:00Z",
        "completed_at": "2026-11-03T12:01:00Z",
        "satisfied_by": ["graph"],
        "graph": {
            "predicates": [{
                "index": 0,
                "predicate": {"type":"follows", "follower":"reader", "followed":"creator"},
                "state": "satisfied",
                "latest_observation": {
                    "outcome": "true",
                    "observed_at": "2026-11-03T12:00:30Z",
                    "origin": "https://nexus.example",
                    "http_status": 200
                },
                "first_true_observation": {
                    "outcome": "true",
                    "observed_at": "2026-11-03T12:00:30Z",
                    "origin": "https://nexus.example",
                    "http_status": 200
                }
            }]
        }
    }))
    .unwrap();
    assert_eq!(evidence.status, VerificationTaskStatus::Completed);
    assert_eq!(evidence.graph.predicates.len(), 1);

    let mut leaked = serde_json::to_value(evidence).unwrap();
    leaked["flow_secret"] = json!("secret");
    assert!(locks_sdk::CreatorLocks::parse_verification_evidence_response(leaked).is_err());

    let mut invalid = serde_json::to_value(
        locks_sdk::CreatorLocks::parse_verification_evidence_response(json!({
            "creator": CREATOR,
            "reader": READER,
            "bundle_id": BUNDLE_ID,
            "pubky_lock_resource": LOCK_RESOURCE,
            "mode": "graph",
            "status": "completed",
            "submitted_at": "2026-11-03T12:00:00Z",
            "deadline_at": "2026-11-03T12:10:00Z",
            "completed_at": "2026-11-03T12:01:00Z",
            "satisfied_by": ["graph"],
            "graph": {"predicates": [{
                "index": 0,
                "predicate": {"type":"follows", "follower":"reader", "followed":"creator"},
                "state": "satisfied"
            }]}
        }))
        .unwrap(),
    )
    .unwrap();
    invalid["graph"]["predicates"][0]["latest_observation"] = serde_json::Value::Null;
    assert!(locks_sdk::CreatorLocks::parse_verification_evidence_response(invalid).is_err());
}

#[test]
fn graph_types_are_exported_at_crate_root() {
    let _: Option<GraphVerificationTaskLifecycleResponse> = None;
    let _: Option<FoldedAuthStatusResponse> = None;
}
