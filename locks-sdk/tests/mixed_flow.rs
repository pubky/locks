use std::str::FromStr;

use locks_core::ids::{BundleId, CreatorPubky};
use locks_core::verification::SubmittedProofBundleV2;
use locks_sdk::{CreatorLocks, VerificationTaskHandleRequest, VerificationTaskStatus, ViewerLocks};
use serde_json::json;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo";
const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
const LOCK_RESOURCE: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json";

fn mixed_bundle() -> SubmittedProofBundleV2 {
    serde_json::from_value(json!({
        "version": 2,
        "bundle_id": BUNDLE_ID,
        "pubky_lock_resource": LOCK_RESOURCE
    }))
    .unwrap()
}

#[test]
fn explicit_mixed_request_surface_omits_reader_and_uses_shared_routes() {
    let viewer = ViewerLocks::new();
    let submit = viewer.submit_mixed_proof_bundle(mixed_bundle());
    assert_eq!(submit.method, "POST");
    assert_eq!(submit.path, "/proof-bundles");
    assert!(
        submit.body["submitted_proof_bundle"]
            .get("reader_pubky")
            .is_none()
    );

    let handle = VerificationTaskHandleRequest {
        creator: CreatorPubky::from_str(CREATOR).unwrap(),
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
    };
    let lookup = viewer.lookup_mixed_verification_task(handle);
    assert_eq!(lookup.path, "/verification-task-lookups");
}

#[test]
fn mixed_lifecycle_parser_accepts_closed_terminal_matrix_and_rejects_nulls() {
    let completed = ViewerLocks::parse_mixed_lifecycle_response(json!({
        "creator": CREATOR,
        "bundle_id": BUNDLE_ID,
        "status": "completed",
        "submitted_at": "2026-11-03T12:00:00Z",
        "completed_at": "2026-11-03T12:01:00Z",
        "admission_deadline_at": "2026-11-03T12:10:00Z"
    }))
    .unwrap();
    assert_eq!(completed.status, VerificationTaskStatus::Completed);

    assert!(
        ViewerLocks::parse_mixed_lifecycle_response(json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "in_progress",
            "submitted_at": "2026-11-03T12:00:00Z",
            "completed_at": null,
            "admission_deadline_at": "2026-11-03T12:10:00Z"
        }))
        .is_err()
    );
}

#[test]
fn mixed_creator_evidence_requires_both_blocks_and_zero_conf_discriminator() {
    let evidence = json!({
        "creator": CREATOR,
        "reader": READER,
        "bundle_id": BUNDLE_ID,
        "pubky_lock_resource": LOCK_RESOURCE,
        "mode": "mixed",
        "status": "completed",
        "submitted_at": "2026-11-03T12:00:00Z",
        "deadline_at": "2026-11-03T12:10:00Z",
        "completed_at": "2026-11-03T12:01:00Z",
        "satisfied_by": ["zero_conf_graph"],
        "graph": {"predicates": [{
            "index": 0,
            "predicate": {"type":"follows", "follower":"reader", "followed":"creator"},
            "state": "satisfied",
            "first_true_observation": {
                "outcome": "true",
                "observed_at": "2026-11-03T12:00:30Z",
                "origin": "https://nexus.example",
                "http_status": 200
            }
        }]},
        "payment": {
            "invoice": {
                "status": "created",
                "invoice_created_at": "2026-11-03T12:00:05Z",
                "proposal_expires_at": "2026-11-03T12:05:00Z"
            },
            "provider_status": {
                "creator": CREATOR,
                "bundle_id": BUNDLE_ID,
                "request_state": "accepted",
                "payment_state": "undetected",
                "invoice_created_at": "2026-11-03T12:00:05Z",
                "proposal_expires_at": "2026-11-03T12:05:00Z",
                "deadline": "2026-11-03T12:10:00Z",
                "asset": "BTC",
                "minimum_confirmations": 1,
                "current_sample": null,
                "first_threshold_satisfied": null
            },
            "operational_failure_count": 0,
            "operational_failure_total": 0
        }
    });
    CreatorLocks::parse_mixed_verification_evidence_response(evidence.clone()).unwrap();

    let mut impossible_sample = evidence.clone();
    impossible_sample["payment"]["provider_status"]["current_sample"] = json!({
        "observed_at": "2026-11-03T12:00:06Z",
        "status": "confirmed",
        "outpoint": null,
        "confirmations": 0,
        "amount_matched": true
    });
    assert!(CreatorLocks::parse_mixed_verification_evidence_response(impossible_sample).is_err());

    let mut invalid_confirmations = evidence.clone();
    invalid_confirmations["payment"]["provider_status"]["minimum_confirmations"] = json!(7);
    assert!(
        CreatorLocks::parse_mixed_verification_evidence_response(invalid_confirmations).is_err()
    );

    let mut missing_payment = evidence.clone();
    missing_payment.as_object_mut().unwrap().remove("payment");
    assert!(CreatorLocks::parse_mixed_verification_evidence_response(missing_payment).is_err());

    let mut wrong_discriminator = evidence;
    wrong_discriminator["satisfied_by"] = json!(["graph"]);
    assert!(CreatorLocks::parse_mixed_verification_evidence_response(wrong_discriminator).is_err());
}

#[test]
fn mixed_creator_evidence_rejects_completion_without_live_bound_provider_status() {
    let impossible = json!({
        "creator": CREATOR,
        "reader": READER,
        "bundle_id": BUNDLE_ID,
        "pubky_lock_resource": LOCK_RESOURCE,
        "mode": "mixed",
        "status": "completed",
        "submitted_at": "2026-11-03T12:00:00Z",
        "deadline_at": "2026-11-03T12:10:00Z",
        "completed_at": "2026-11-03T12:01:00Z",
        "satisfied_by": ["zero_conf_graph"],
        "graph": {"predicates": [{
            "index": 0,
            "predicate": {"type":"follows", "follower":"reader", "followed":"creator"},
            "state": "satisfied",
            "first_true_observation": {
                "outcome": "true",
                "observed_at": "2026-11-03T12:00:30Z",
                "origin": "https://nexus.example",
                "http_status": 200
            }
        }]},
        "payment": {
            "invoice": {"status": "pending"},
            "operational_failure_count": 0,
            "operational_failure_total": 0
        }
    });

    assert!(CreatorLocks::parse_mixed_verification_evidence_response(impossible).is_err());
}
