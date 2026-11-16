use std::str::FromStr;

use locks_core::ids::{BundleId, CreatorPubky};
use locks_core::verification::SubmittedProofBundleV2;
use locks_sdk::{CreatorLocks, VerificationTaskHandleRequest, VerificationTaskStatus, ViewerLocks};
use locks_server::api::dtos::CreatorPaymentEvidenceHttpResponse;
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

    let mut publishing = evidence.clone();
    publishing["status"] = json!("publishing_entitlement");
    publishing.as_object_mut().unwrap().remove("completed_at");
    CreatorLocks::parse_mixed_verification_evidence_response(publishing).unwrap();

    let mut created_before_status = evidence.clone();
    created_before_status["status"] = json!("in_progress");
    created_before_status["satisfied_by"] = json!([]);
    created_before_status
        .as_object_mut()
        .unwrap()
        .remove("completed_at");
    created_before_status["payment"]
        .as_object_mut()
        .unwrap()
        .remove("provider_status");
    CreatorLocks::parse_mixed_verification_evidence_response(created_before_status.clone())
        .unwrap();

    let mut scheduled_retry_before_status = created_before_status.clone();
    scheduled_retry_before_status["status"] = json!("pending");
    CreatorLocks::parse_mixed_verification_evidence_response(scheduled_retry_before_status)
        .unwrap();

    let mut unavailable_before_status = created_before_status.clone();
    unavailable_before_status["status"] = json!("failed");
    unavailable_before_status["completed_at"] = json!("2026-11-03T12:10:00Z");
    unavailable_before_status["terminal_reason"] = json!("verification_unavailable_by_deadline");
    CreatorLocks::parse_mixed_verification_evidence_response(unavailable_before_status).unwrap();

    for reason in ["invalid_verifier_response", "verification_conflict"] {
        let mut failed_before_status = created_before_status.clone();
        failed_before_status["status"] = json!("failed");
        failed_before_status["completed_at"] = json!("2026-11-03T12:10:00Z");
        failed_before_status["terminal_reason"] = json!(reason);
        CreatorLocks::parse_mixed_verification_evidence_response(failed_before_status).unwrap();
    }

    created_before_status["status"] = json!("expired");
    created_before_status["completed_at"] = json!("2026-11-03T12:01:00Z");
    created_before_status["terminal_reason"] = json!("content_lock_deleted");
    CreatorLocks::parse_mixed_verification_evidence_response(created_before_status.clone())
        .unwrap();

    created_before_status["terminal_reason"] = json!("payment_request_rejected");
    assert!(
        CreatorLocks::parse_mixed_verification_evidence_response(created_before_status).is_err()
    );

    let mut wrong_expired_reason = evidence.clone();
    wrong_expired_reason["status"] = json!("expired");
    wrong_expired_reason["completed_at"] = json!("2026-11-03T12:01:00Z");
    wrong_expired_reason["terminal_reason"] = json!("invalid_verifier_response");
    wrong_expired_reason["satisfied_by"] = json!([]);
    assert!(
        CreatorLocks::parse_mixed_verification_evidence_response(wrong_expired_reason).is_err()
    );

    let mut zero_failures_with_metadata = evidence.clone();
    zero_failures_with_metadata["payment"]["operational_last_category"] = json!("timeout");
    assert!(
        CreatorLocks::parse_mixed_verification_evidence_response(zero_failures_with_metadata)
            .is_err()
    );

    let mut nonzero_failures_without_metadata = evidence.clone();
    nonzero_failures_without_metadata["payment"]["operational_failure_count"] = json!(1);
    nonzero_failures_without_metadata["payment"]["operational_failure_total"] = json!(1);
    assert!(
        CreatorLocks::parse_mixed_verification_evidence_response(nonzero_failures_without_metadata)
            .is_err()
    );

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

fn completed_usdt_evidence() -> serde_json::Value {
    json!({
        "creator": CREATOR,
        "reader": READER,
        "bundle_id": BUNDLE_ID,
        "pubky_lock_resource": LOCK_RESOURCE,
        "mode": "mixed",
        "status": "completed",
        "submitted_at": "2026-11-03T12:00:00Z",
        "deadline_at": "2026-11-03T12:10:00Z",
        "completed_at": "2026-11-03T12:01:00Z",
        "satisfied_by": ["payment"],
        "graph": {"predicates": [{
            "index": 0,
            "predicate": {"type":"follows", "follower":"reader", "followed":"creator"},
            "state": "pending"
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
                "payment_state": "confirmed",
                "invoice_created_at": "2026-11-03T12:00:05Z",
                "proposal_expires_at": "2026-11-03T12:05:00Z",
                "deadline": "2026-11-03T12:10:00Z",
                "asset": "USDT",
                "minimum_confirmations": 6,
                "current_sample": null,
                "first_threshold_satisfied": null,
                "bitcoin": {
                    "confirmations": 6,
                    "amount_matched": true,
                    "paid_on_time": true
                },
                "usdt_arbitrum": {
                    "confirmations": 1,
                    "amount_matched": true,
                    "paid_on_time": true,
                    "finalized": false
                }
            },
            "operational_failure_count": 0,
            "operational_failure_total": 0
        }
    })
}

fn serialize_server_payment(evidence: &mut serde_json::Value) {
    let payment = CreatorPaymentEvidenceHttpResponse {
        invoice: serde_json::from_value(evidence["payment"]["invoice"].clone()).unwrap(),
        provider_status: Some(
            serde_json::from_value(evidence["payment"]["provider_status"].clone()).unwrap(),
        ),
        operational_failure_count: 0,
        operational_failure_total: 0,
        operational_first_failure_at: None,
        operational_last_failure_at: None,
        operational_last_category: None,
        operational_last_http_status: None,
        next_due_at: None,
    };
    evidence["payment"] = serde_json::to_value(payment).unwrap();
}

#[test]
fn mixed_creator_evidence_accepts_completed_usdt_payment() {
    let parsed =
        CreatorLocks::parse_mixed_verification_evidence_response(completed_usdt_evidence())
            .unwrap();
    assert_eq!(
        parsed.satisfied_by,
        vec![locks_core::verification::SatisfiedByV2::Payment]
    );
}

#[test]
fn mixed_creator_evidence_accepts_pending_usdt_payment_with_explicit_null_rails() {
    let mut evidence = completed_usdt_evidence();
    evidence["status"] = json!("in_progress");
    evidence["satisfied_by"] = json!([]);
    evidence.as_object_mut().unwrap().remove("completed_at");
    evidence["payment"]["provider_status"]["payment_state"] = json!("undetected");
    evidence["payment"]["provider_status"]["bitcoin"] = serde_json::Value::Null;
    evidence["payment"]["provider_status"]["usdt_arbitrum"] = serde_json::Value::Null;

    CreatorLocks::parse_mixed_verification_evidence_response(evidence).unwrap();
}

#[test]
fn server_usdt_evidence_serialization_crosses_sdk_parser_with_nullable_rails() {
    for pending in [true, false] {
        let mut evidence = completed_usdt_evidence();
        if pending {
            evidence["status"] = json!("in_progress");
            evidence["satisfied_by"] = json!([]);
            evidence.as_object_mut().unwrap().remove("completed_at");
            evidence["payment"]["provider_status"]["payment_state"] = json!("undetected");
            evidence["payment"]["provider_status"]["bitcoin"] = serde_json::Value::Null;
            evidence["payment"]["provider_status"]["usdt_arbitrum"] = serde_json::Value::Null;
        } else {
            evidence["payment"]["provider_status"]["bitcoin"] = serde_json::Value::Null;
        }

        serialize_server_payment(&mut evidence);

        assert_eq!(
            evidence["payment"]["provider_status"]["bitcoin"],
            serde_json::Value::Null
        );
        assert!(
            evidence["payment"]["provider_status"]
                .as_object()
                .unwrap()
                .contains_key("usdt_arbitrum")
        );
        CreatorLocks::parse_mixed_verification_evidence_response(evidence).unwrap();
    }

    let mut btc = completed_usdt_evidence();
    btc["status"] = json!("in_progress");
    btc["satisfied_by"] = json!([]);
    btc.as_object_mut().unwrap().remove("completed_at");
    btc["payment"]["provider_status"]["payment_state"] = json!("undetected");
    btc["payment"]["provider_status"]["asset"] = json!("BTC");
    let provider = btc["payment"]["provider_status"].as_object_mut().unwrap();
    provider.remove("bitcoin");
    provider.remove("usdt_arbitrum");
    serialize_server_payment(&mut btc);
    assert!(
        !btc["payment"]["provider_status"]
            .as_object()
            .unwrap()
            .contains_key("bitcoin")
    );
    assert!(
        !btc["payment"]["provider_status"]
            .as_object()
            .unwrap()
            .contains_key("usdt_arbitrum")
    );
    CreatorLocks::parse_mixed_verification_evidence_response(btc).unwrap();
}

#[test]
fn mixed_creator_evidence_rejects_missing_or_malformed_usdt_rail_fields() {
    for missing in ["bitcoin", "usdt_arbitrum"] {
        let mut evidence = completed_usdt_evidence();
        evidence["payment"]["provider_status"]
            .as_object_mut()
            .unwrap()
            .remove(missing);
        assert!(
            CreatorLocks::parse_mixed_verification_evidence_response(evidence).is_err(),
            "accepted USDT evidence missing {missing}"
        );
    }

    let mut malformed = completed_usdt_evidence();
    malformed["payment"]["provider_status"]["usdt_arbitrum"]
        .as_object_mut()
        .unwrap()
        .remove("paid_on_time");
    assert!(CreatorLocks::parse_mixed_verification_evidence_response(malformed).is_err());

    let mut unknown = completed_usdt_evidence();
    unknown["payment"]["provider_status"]["usdt_arbitrum"]["unsupported"] = json!(true);
    assert!(CreatorLocks::parse_mixed_verification_evidence_response(unknown).is_err());

    let mut invalid_asset = completed_usdt_evidence();
    invalid_asset["payment"]["provider_status"]["asset"] = json!("usdt");
    assert!(CreatorLocks::parse_mixed_verification_evidence_response(invalid_asset).is_err());
}

#[test]
fn mixed_creator_evidence_never_uses_bitcoin_offer_to_qualify_usdt() {
    let mut evidence = completed_usdt_evidence();
    evidence["payment"]["provider_status"]["usdt_arbitrum"]["confirmations"] = json!(0);
    evidence["payment"]["provider_status"]["first_threshold_satisfied"] = json!({
        "observed_at": "2026-11-03T12:00:30Z",
        "status": "confirmed",
        "outpoint": {
            "txid": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "vout": 0
        },
        "confirmations": 6,
        "amount_matched": true
    });

    assert!(CreatorLocks::parse_mixed_verification_evidence_response(evidence).is_err());
}

#[test]
fn mixed_creator_evidence_requires_qualifying_usdt_for_graph_eligibility() {
    let mut evidence = completed_usdt_evidence();
    evidence["satisfied_by"] = json!(["zero_conf_graph"]);
    evidence["graph"]["predicates"][0]["state"] = json!("satisfied");
    evidence["graph"]["predicates"][0]["first_true_observation"] = json!({
        "outcome": "true",
        "observed_at": "2026-11-03T12:00:30Z",
        "origin": "https://nexus.example",
        "http_status": 200
    });
    evidence["payment"]["provider_status"]["payment_state"] = json!("undetected");
    evidence["payment"]["provider_status"]["bitcoin"] = serde_json::Value::Null;
    evidence["payment"]["provider_status"]["usdt_arbitrum"] = serde_json::Value::Null;

    assert!(CreatorLocks::parse_mixed_verification_evidence_response(evidence).is_err());
}

#[test]
fn mixed_creator_evidence_rejects_rail_fields_for_btc_status() {
    let mut evidence = completed_usdt_evidence();
    evidence["payment"]["provider_status"]["asset"] = json!("BTC");

    assert!(CreatorLocks::parse_mixed_verification_evidence_response(evidence).is_err());
}
