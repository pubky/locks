use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;

use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode, header};
use locks_core::ids::{
    BundleId, ClientReference, ContentLockPath, CreatorPubky, GuardedResourceHash, LockServerPubky,
    PubkyLockResource,
};
use locks_core::lock_policy::{
    AccessPolicy, CONTENT_LOCK_V2_VERSION, CONTENT_LOCK_VERSION, ContentLock, ContentLockPayload,
    ContentLockV2, Criterion, GraphParticipantV2, GraphPolicyV2, GraphPredicateV2, GuardedResource,
    LockLogic, LockServerConfig, PaymentPolicyV2, VerifierType,
};
use locks_core::verification::{
    Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SatisfiedByV2, SubmittedProofBundle,
    VERIFIED_PROOF_BUNDLE_V2_VERSION, VerifiedProofBundleV2,
};
use locks_server::testing::TestServerApp;
use locks_service::application::models::{GuardedResourceRecord, VerificationTaskStatus};
use serde_json::{Value, json};
use time::macros::datetime;
use tower::ServiceExt;

const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
const CLIENT_REFERENCE: &str = "x9Kq_3-jW0sN8uYv1tB6rZpLmE4cA2dHfG7iOoQwXyU";

#[tokio::test]
async fn retrieval_access_http_flow_returns_seeded_guarded_resource_bytes() {
    let test_app = TestServerApp::new_default_in_memory();
    let content_lock = content_lock();
    let guarded_bytes = b"retrieval access e2e bytes".to_vec();
    test_app
        .seed_content_lock(content_lock.clone())
        .await
        .unwrap();
    test_app
        .seed_guarded_resource(&content_lock, guarded_bytes.clone())
        .await
        .unwrap();
    let router = test_app.router();

    let submit_response = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted_proof_bundle_for(&content_lock) }),
        ))
        .await
        .unwrap();
    assert_eq!(submit_response.status(), StatusCode::OK);
    let submit_json = response_json(submit_response).await;
    assert_eq!(submit_json["creator"], creator().to_string());
    assert_eq!(submit_json["bundle_id"], BUNDLE_ID);
    assert_eq!(submit_json["status"], "pending");
    assert!(submit_json.get("task_id").is_none());
    let submitted_resource = submitted_proof_bundle_for(&content_lock)
        .pubky_lock_resource
        .to_string();
    let expected_binding = json!({
        "client_reference": CLIENT_REFERENCE,
        "pubky_lock_resource": submitted_resource,
        "criterion_ids": ["criterion-1"],
    });
    assert_eq!(submit_json["client_binding"], expected_binding);

    let task = test_app
        .state()
        .verification_tasks()
        .get_verification_task_by_handle(&creator(), &bundle_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, VerificationTaskStatus::Pending);

    let complete_response = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/verification-task-completions",
            handle_request(),
        ))
        .await
        .unwrap();
    assert_eq!(complete_response.status(), StatusCode::OK);
    let complete_json = response_json(complete_response).await;
    assert_eq!(complete_json["creator"], creator().to_string());
    assert_eq!(complete_json["bundle_id"], BUNDLE_ID);
    assert_eq!(complete_json["status"], "completed");
    assert!(complete_json.get("task_id").is_none());
    assert_eq!(complete_json["client_binding"], expected_binding);
    assert!(complete_json.get("reader_public_key").is_none());

    let completed_response = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/verification-task-lookups",
            handle_request(),
        ))
        .await
        .unwrap();
    assert_eq!(completed_response.status(), StatusCode::OK);
    let completed_json = response_json(completed_response).await;
    assert_eq!(completed_json["status"], "completed");
    assert_eq!(completed_json["client_binding"], expected_binding);
    assert!(completed_json.get("reader_public_key").is_none());

    let credential_response = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/access-credentials",
            json!({ "creator": creator(), "bundle_id": BUNDLE_ID }),
        ))
        .await
        .unwrap();
    assert_eq!(credential_response.status(), StatusCode::OK);
    let credential = response_json(credential_response).await["credential"]
        .as_str()
        .unwrap()
        .to_owned();

    let guarded_response = router
        .oneshot(auth_request(
            "GET",
            "/priv-resources/content/e2e.txt",
            &format!("Bearer {credential}"),
        ))
        .await
        .unwrap();
    assert_eq!(guarded_response.status(), StatusCode::OK);
    assert_eq!(
        guarded_response.headers()[header::CONTENT_TYPE],
        "text/plain"
    );
    assert_eq!(response_bytes(guarded_response).await, guarded_bytes);
}

#[tokio::test]
async fn mixed_completion_issues_credential_and_serves_guarded_resource() {
    let test_app = TestServerApp::new_default_in_memory();
    let content_lock = mixed_content_lock();
    let lock_path = ContentLockPath::from_lock_id(content_lock.lock_id().unwrap());
    let lock_resource = PubkyLockResource::new(content_lock.creator.clone(), lock_path.clone());
    let guarded = content_lock.primary_resource.clone().unwrap();
    let guarded_bytes = b"mixed entitlement bytes".to_vec();

    test_app
        .state()
        .content_locks()
        .upsert_content_lock_payload(
            content_lock.creator.clone(),
            lock_path,
            ContentLockPayload::V2(content_lock.clone()),
        )
        .await
        .unwrap();
    test_app
        .state()
        .guarded_resources()
        .upsert_guarded_resource(GuardedResourceRecord {
            creator: content_lock.creator.clone(),
            path: guarded.path.clone(),
            hash: guarded.hash,
            content_type: guarded.content_type.clone(),
            size: guarded.size,
            bytes: guarded_bytes.clone(),
        })
        .await
        .unwrap();
    test_app
        .state()
        .entitlements()
        .insert_verified_proof_bundle_v2(VerifiedProofBundleV2 {
            version: VERIFIED_PROOF_BUNDLE_V2_VERSION,
            creator: content_lock.creator.clone(),
            reader: CreatorPubky::from_str(
                "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky",
            )
            .unwrap(),
            bundle_id: bundle_id(),
            pubky_lock_resource: lock_resource,
            verified_at: datetime!(2026-05-29 12:01:00 UTC),
            satisfied_by: vec![SatisfiedByV2::Payment, SatisfiedByV2::ZeroConfGraph],
        })
        .await
        .unwrap();
    let router = test_app.router();

    let credential_response = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/access-credentials",
            json!({ "creator": creator(), "bundle_id": BUNDLE_ID }),
        ))
        .await
        .unwrap();
    assert_eq!(credential_response.status(), StatusCode::OK);
    let credential = response_json(credential_response).await["credential"]
        .as_str()
        .unwrap()
        .to_owned();

    let guarded_response = router
        .oneshot(auth_request(
            "GET",
            "/priv-resources/content/mixed.txt",
            &format!("Bearer {credential}"),
        ))
        .await
        .unwrap();
    assert_eq!(guarded_response.status(), StatusCode::OK);
    assert_eq!(response_bytes(guarded_response).await, guarded_bytes);
}

fn submitted_proof_bundle_for(content_lock: &ContentLock) -> SubmittedProofBundle {
    SubmittedProofBundle {
        version: SUBMITTED_PROOF_BUNDLE_VERSION,
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: PubkyLockResource::new(
            content_lock.creator.clone(),
            content_lock.content_lock_path().unwrap(),
        ),
        reader_public_key: None,
        client_reference: Some(ClientReference::from_str(CLIENT_REFERENCE).unwrap()),
        proofs: vec![Proof {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::DevStatic,
            payload: json!({ "satisfied": true }),
        }],
    }
}

fn content_lock() -> ContentLock {
    ContentLock {
        version: CONTENT_LOCK_VERSION,
        creator: creator(),
        primary_resource: Some(GuardedResource {
            path: "/priv/app.locks/content/e2e.txt".to_owned(),
            hash: GuardedResourceHash::from_bytes([9; 32]),
            content_type: "text/plain".to_owned(),
            size: 26,
        }),
        secondary_resources: Default::default(),
        criteria: vec![Criterion {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::DevStatic,
            params: json!({ "satisfied": true }),
        }],
        lock_logic: LockLogic::All {
            criteria: vec!["criterion-1".to_owned()],
        },
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 900,
        },
        lock_server: LockServerConfig {
            override_: Some(
                LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
            ),
        },
        created_at: datetime!(2026-05-29 12:00:00 UTC),
    }
}

fn mixed_content_lock() -> ContentLockV2 {
    ContentLockV2 {
        version: CONTENT_LOCK_V2_VERSION,
        creator: creator(),
        primary_resource: Some(GuardedResource {
            path: "/priv/app.locks/content/mixed.txt".to_owned(),
            hash: GuardedResourceHash::from_bytes([7; 32]),
            content_type: "text/plain".to_owned(),
            size: 23,
        }),
        secondary_resources: Default::default(),
        payment: Some(PaymentPolicyV2 {
            recipient_pubky: creator(),
            amount: "50000".to_owned(),
            asset: "BTC".to_owned(),
            minimum_confirmations: 1,
        }),
        graph: Some(GraphPolicyV2 {
            operator: None,
            predicates: vec![GraphPredicateV2::Follows {
                follower: GraphParticipantV2::Reader,
                followed: GraphParticipantV2::Creator,
            }],
        }),
        verification_window_seconds: 300,
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 900,
        },
        lock_server: LockServerConfig {
            override_: Some(
                LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
            ),
        },
        created_at: datetime!(2026-05-29 12:00:00 UTC),
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

fn bundle_id() -> BundleId {
    BundleId::from_str(BUNDLE_ID).unwrap()
}

fn handle_request() -> Value {
    json!({ "creator": creator(), "bundle_id": BUNDLE_ID })
}

fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        12345,
    )));
    request
}

fn auth_request(method: &str, uri: &str, authorization: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, authorization)
        .body(Body::empty())
        .unwrap()
}

async fn response_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&response_bytes(response).await).unwrap()
}

async fn response_bytes(response: axum::response::Response) -> Vec<u8> {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}
