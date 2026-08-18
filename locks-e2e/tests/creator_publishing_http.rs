mod support;

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::post;
use axum::{Json, Router};
use locks_core::ids::{BundleId, ContentLockPath, CreatorPubky, PubkyLockResource};
use locks_core::lock_policy::VerifierType;
use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};
use locks_server::app_state::{AppState, ReaderPubkyResolver};
use locks_server::config::{
    FilesystemLockServerIdentityProvider, LockServerIdentityProvider, PaykitConfig,
};
use locks_server::testing::TestServerApp;
use locks_server::worker::{VerificationWorker, WorkerTick};
use locks_service::application::models::FrontendSessionToken;
use locks_service::application::ports::CriterionVerifier;
use locks_service::infrastructure::memory::content_locks::InMemoryContentLockRepository;
use locks_service::infrastructure::memory::entitlements::InMemoryEntitlementRepository;
use locks_service::infrastructure::memory::guarded_resources::InMemoryGuardedResourceRepository;
use locks_service::infrastructure::memory::lock_service_pointers::InMemoryLockServicePointerRepository;
use locks_service::infrastructure::memory::verification_task_claims::InMemoryVerificationTaskClaimer;
use serde_json::json;
use support::creator_publishing_client::{LocalCreatorPublishingClient, response_bytes};
use time::Duration;
use tokio::sync::Mutex;

const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
const FRONTEND_SESSION_TOKEN: &str = "frontend-session-secret";
const GUARDED_BYTES: &[u8] = b"creator publishing e2e bytes";

#[tokio::test]
async fn creator_publishing_http_flow_registers_locks_verifies_and_proxy_reads_guarded_resource() {
    let config = TestServerApp::default_in_memory_config();
    let test_app =
        TestServerApp::from_state(AppState::new_empty_in_memory_with_creator_repositories(
            config,
            Arc::new(InMemoryContentLockRepository::new()),
            Arc::new(InMemoryGuardedResourceRepository::new()),
            Arc::new(InMemoryLockServicePointerRepository::new()),
            Arc::new(InMemoryEntitlementRepository::new()),
        ));
    test_app
        .insert_frontend_session_for_test(
            FrontendSessionToken::new(FRONTEND_SESSION_TOKEN),
            creator(),
            time::OffsetDateTime::now_utc() + Duration::hours(12),
        )
        .await
        .unwrap();
    let router = test_app.router();
    let client = LocalCreatorPublishingClient::new(router.clone())
        .with_frontend_session_token(FRONTEND_SESSION_TOKEN);

    let pointer_json = client
        .set_lock_service_config("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
        .await
        .unwrap();
    assert_eq!(pointer_json["creator"], creator().to_string());
    assert_eq!(pointer_json["path"], "/pub/locks.app/config.json");
    assert_eq!(
        pointer_json["lock_service_pointer"]["default_lock_server"],
        "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
    );

    let guarded_json = client
        .register_guarded_resource("creator-e2e.txt", "text/plain", GUARDED_BYTES)
        .await
        .unwrap();
    let guarded_resource = guarded_json["guarded_resource"].clone();
    assert_eq!(guarded_json["creator"], creator().to_string());
    assert_eq!(
        guarded_resource["path"],
        "/priv/locks.app/content/creator-e2e.txt"
    );
    assert_eq!(guarded_resource["content_type"], "text/plain");
    assert_eq!(guarded_resource["size"], GUARDED_BYTES.len() as u64);
    assert!(guarded_json.get("content_base64").is_none());
    assert!(guarded_json.get("bytes").is_none());

    let content_lock_json = client
        .create_content_lock(
            guarded_resource,
            json!([{
                "criterion_id": "criterion-1",
                "verifier_type": "dev-static",
                "params": { "satisfied": true }
            }]),
            json!({ "type": "all", "criteria": ["criterion-1"] }),
            json!({ "requested_credential_ttl_seconds": 900 }),
            json!({ "override": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo" }),
        )
        .await
        .unwrap();
    let content_lock_path = ContentLockPath::from_str(
        content_lock_json["content_lock_path"]
            .as_str()
            .expect("content_lock_path is string"),
    )
    .unwrap();
    assert_eq!(
        content_lock_json["content_lock"]["creator"],
        creator().to_string()
    );
    assert!(content_lock_json.get("content_base64").is_none());
    assert!(content_lock_json.get("bytes").is_none());

    let submit_json = client
        .submit_proof_bundle(submitted_proof_bundle_for(content_lock_path))
        .await
        .unwrap();
    assert_eq!(submit_json["creator"], creator().to_string());
    assert_eq!(submit_json["bundle_id"], BUNDLE_ID);
    assert_eq!(submit_json["status"], "pending");
    assert!(submit_json.get("task_id").is_none());

    let completion_json = client
        .dev_complete_verification(creator(), BUNDLE_ID)
        .await
        .unwrap();
    assert_eq!(completion_json["status"], "completed");

    let credential = client
        .issue_access_credential(creator(), BUNDLE_ID)
        .await
        .unwrap();

    let proxy_response = client
        .proxy_read_guarded_resource(&credential, "creator-e2e.txt")
        .await
        .unwrap();
    assert_eq!(proxy_response.status(), StatusCode::OK);
    assert_eq!(proxy_response.headers()[header::CONTENT_TYPE], "text/plain");
    assert_eq!(response_bytes(proxy_response).await, GUARDED_BYTES);
}

#[tokio::test]
async fn creator_publishing_http_reads_primary_and_secondary_resources_with_metadata() {
    let (_test_app, client) = creator_publishing_client().await;
    let primary_bytes = br#"{"title":"post","body":"hello"}"#;
    let secondary_bytes = b"attachment bytes";

    let primary = client
        .register_guarded_resource("post.json", "application/json", primary_bytes.as_slice())
        .await
        .unwrap()["guarded_resource"]
        .clone();
    let secondary = client
        .register_guarded_resource(
            "attachments/a.txt",
            "text/plain",
            secondary_bytes.as_slice(),
        )
        .await
        .unwrap()["guarded_resource"]
        .clone();

    let content_lock_json = client
        .create_content_lock_with_resources(
            Some(primary.clone()),
            secondary_resource_map(&secondary),
            standard_criteria(),
            standard_lock_logic(),
            standard_access_policy(),
            standard_lock_server(),
        )
        .await
        .unwrap();
    let credential = issue_credential_for(&client, &content_lock_json).await;

    let primary_response = client
        .proxy_read_guarded_resource(&credential, "post.json")
        .await
        .unwrap();
    assert_resource_response_headers(
        &primary_response,
        "application/json",
        primary_bytes.len(),
        &primary,
    );
    assert_eq!(response_bytes(primary_response).await, primary_bytes);

    let secondary_response = client
        .proxy_read_guarded_resource(&credential, "attachments/a.txt")
        .await
        .unwrap();
    assert_resource_response_headers(
        &secondary_response,
        "text/plain",
        secondary_bytes.len(),
        &secondary,
    );
    assert_eq!(response_bytes(secondary_response).await, secondary_bytes);
}

#[tokio::test]
async fn creator_publishing_http_supports_secondary_only_content_locks() {
    let (_test_app, client) = creator_publishing_client().await;
    let bytes = b"secondary only bytes";
    let secondary = client
        .register_guarded_resource("only-secondary.txt", "text/plain", bytes.as_slice())
        .await
        .unwrap()["guarded_resource"]
        .clone();

    let content_lock_json = client
        .create_content_lock_with_resources(
            None,
            secondary_resource_map(&secondary),
            standard_criteria(),
            standard_lock_logic(),
            standard_access_policy(),
            standard_lock_server(),
        )
        .await
        .unwrap();
    assert!(
        content_lock_json["content_lock"]
            .get("primary_resource")
            .is_none()
    );
    assert!(content_lock_json["content_lock"]["secondary_resources"].is_object());
    let credential = issue_credential_for(&client, &content_lock_json).await;

    let response = client
        .proxy_read_guarded_resource(&credential, "only-secondary.txt")
        .await
        .unwrap();
    assert_resource_response_headers(&response, "text/plain", bytes.len(), &secondary);
    assert_eq!(response_bytes(response).await, bytes);
}

#[tokio::test]
async fn creator_publishing_http_stale_and_deleted_resources_fail_independently() {
    let (_test_app, client) = creator_publishing_client().await;
    let stale_original = b"original secondary A";
    let unchanged_bytes = b"unchanged secondary B";
    let stale = client
        .register_guarded_resource(
            "attachments/stale.txt",
            "text/plain",
            stale_original.as_slice(),
        )
        .await
        .unwrap()["guarded_resource"]
        .clone();
    let unchanged = client
        .register_guarded_resource(
            "attachments/unchanged.txt",
            "text/plain",
            unchanged_bytes.as_slice(),
        )
        .await
        .unwrap()["guarded_resource"]
        .clone();

    let mut secondary_resources = secondary_resource_map(&stale).as_object().unwrap().clone();
    secondary_resources.extend(
        secondary_resource_map(&unchanged)
            .as_object()
            .unwrap()
            .clone(),
    );
    let content_lock_json = client
        .create_content_lock_with_resources(
            None,
            serde_json::Value::Object(secondary_resources),
            standard_criteria(),
            standard_lock_logic(),
            standard_access_policy(),
            standard_lock_server(),
        )
        .await
        .unwrap();
    let credential = issue_credential_for(&client, &content_lock_json).await;

    client
        .register_guarded_resource(
            "attachments/stale.txt",
            "text/plain",
            b"new bytes".as_slice(),
        )
        .await
        .unwrap();
    let stale_err = client
        .proxy_read_guarded_resource(&credential, "attachments/stale.txt")
        .await
        .unwrap_err();
    assert_eq!(stale_err.status, StatusCode::NOT_FOUND);
    assert_eq!(
        stale_err.body["error"]["code"],
        "guarded_resource_not_found"
    );

    let unchanged_response = client
        .proxy_read_guarded_resource(&credential, "attachments/unchanged.txt")
        .await
        .unwrap();
    assert_eq!(response_bytes(unchanged_response).await, unchanged_bytes);

    client
        .delete_guarded_resource("attachments/unchanged.txt")
        .await
        .unwrap();
    let deleted_err = client
        .proxy_read_guarded_resource(&credential, "attachments/unchanged.txt")
        .await
        .unwrap_err();
    assert_eq!(deleted_err.status, StatusCode::NOT_FOUND);
    assert_eq!(
        deleted_err.body["error"]["code"],
        "guarded_resource_not_found"
    );

    let missing_delete = client
        .delete_guarded_resource("attachments/missing.txt")
        .await
        .unwrap_err();
    assert_eq!(missing_delete.status, StatusCode::NOT_FOUND);
    assert_eq!(
        missing_delete.body["error"]["code"],
        "guarded_resource_not_found"
    );
}

#[tokio::test]
async fn creator_publishing_http_rejects_invalid_guarded_path_before_lock_creation() {
    let config = TestServerApp::default_in_memory_config();
    let test_app =
        TestServerApp::from_state(AppState::new_empty_in_memory_with_creator_repositories(
            config,
            Arc::new(InMemoryContentLockRepository::new()),
            Arc::new(InMemoryGuardedResourceRepository::new()),
            Arc::new(InMemoryLockServicePointerRepository::new()),
            Arc::new(InMemoryEntitlementRepository::new()),
        ));
    test_app
        .insert_frontend_session_for_test(
            FrontendSessionToken::new(FRONTEND_SESSION_TOKEN),
            creator(),
            time::OffsetDateTime::now_utc() + Duration::hours(12),
        )
        .await
        .unwrap();
    let client = LocalCreatorPublishingClient::new(test_app.router())
        .with_frontend_session_token(FRONTEND_SESSION_TOKEN);

    let err = client
        .register_guarded_resource("%2E%2E/not-guarded.txt", "text/plain", GUARDED_BYTES)
        .await
        .unwrap_err();

    assert_eq!(err.status, StatusCode::BAD_REQUEST);
    assert_eq!(err.body["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn creator_publishing_http_rejects_invalid_paykit_payment_params() {
    let (_test_app, client) = creator_publishing_client().await;
    let guarded_resource = client
        .register_guarded_resource("paid.txt", "text/plain", GUARDED_BYTES)
        .await
        .unwrap()["guarded_resource"]
        .clone();

    let invalid_amount = client
        .create_content_lock(
            guarded_resource.clone(),
            json!([{
                "criterion_id": "criterion-1",
                "verifier_type": "paykit-payment",
                "params": {
                    "recipient_pubky": creator().to_string(),
                    "amount": "0",
                    "asset": "BTC",
                    "payment_in": 24
                }
            }]),
            standard_lock_logic(),
            standard_access_policy(),
            standard_lock_server(),
        )
        .await
        .unwrap_err();

    assert_eq!(invalid_amount.status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid_amount.body["error"]["code"], "invalid_request");

    let mismatched_recipient = client
        .create_content_lock(
            guarded_resource,
            json!([{
                "criterion_id": "criterion-1",
                "verifier_type": "paykit-payment",
                "params": {
                    "recipient_pubky": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                    "amount": "50000",
                    "asset": "BTC",
                    "payment_in": 24
                }
            }]),
            standard_lock_logic(),
            standard_access_policy(),
            standard_lock_server(),
        )
        .await
        .unwrap_err();

    assert_eq!(mismatched_recipient.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        mismatched_recipient.body["error"]["code"],
        "invalid_request"
    );
}

#[tokio::test]
async fn creator_publishing_http_rejects_ambiguous_paykit_payment_policies() {
    let (_test_app, client) = creator_publishing_client().await;
    let guarded_resource = client
        .register_guarded_resource("ambiguous-paid.txt", "text/plain", GUARDED_BYTES)
        .await
        .unwrap()["guarded_resource"]
        .clone();
    let cases = [
        (
            json!([
                paykit_criterion_json("payment-1"),
                {
                    "criterion_id": "other",
                    "verifier_type": "dev-static",
                    "params": { "satisfied": true }
                }
            ]),
            json!({ "type": "all", "criteria": ["payment-1", "other"] }),
        ),
        (
            json!([
                paykit_criterion_json("payment-1"),
                paykit_criterion_json("payment-2")
            ]),
            json!({ "type": "any", "criteria": ["payment-1", "payment-2"] }),
        ),
        (
            json!([paykit_criterion_json("payment-1")]),
            json!({ "type": "all", "criteria": ["payment-1", "payment-1"] }),
        ),
    ];

    for (criteria, lock_logic) in cases {
        let error = client
            .create_content_lock(
                guarded_resource.clone(),
                criteria,
                lock_logic,
                standard_access_policy(),
                standard_lock_server(),
            )
            .await
            .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.body["error"]["code"], "invalid_request");
    }
}

#[tokio::test]
async fn creator_publishing_http_paykit_payment_flow_creates_invoice_verifies_and_proxy_reads() {
    let fake_paykit = FakePaykitServer::start().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let secret_path = temp_dir.path().join("lock-server.keypair-seed");
    let public_key = FilesystemLockServerIdentityProvider
        .generate_secret(&secret_path)
        .unwrap();
    let mut config = TestServerApp::default_in_memory_config();
    config.credentials.lock_server_secret_key = secret_path;
    config.credentials.lock_server_public_key = public_key;
    config.paykit = Some(PaykitConfig {
        server_url: fake_paykit.server_url.clone(),
        minimum_confirmations: 0,
    });
    let state = AppState::new_empty_in_memory_with_creator_repositories(
        config,
        Arc::new(InMemoryContentLockRepository::new()),
        Arc::new(InMemoryGuardedResourceRepository::new()),
        Arc::new(InMemoryLockServicePointerRepository::new()),
        Arc::new(InMemoryEntitlementRepository::new()),
    )
    .with_reader_pubky_resolver(Arc::new(AlwaysResolvesReader));
    let test_app = TestServerApp::from_state(state);
    test_app
        .insert_frontend_session_for_test(
            FrontendSessionToken::new(FRONTEND_SESSION_TOKEN),
            creator(),
            time::OffsetDateTime::now_utc() + Duration::hours(12),
        )
        .await
        .unwrap();
    let client = LocalCreatorPublishingClient::new(test_app.router())
        .with_frontend_session_token(FRONTEND_SESSION_TOKEN);

    let guarded_resource = client
        .register_guarded_resource("paid-e2e.txt", "text/plain", GUARDED_BYTES)
        .await
        .unwrap()["guarded_resource"]
        .clone();
    let content_lock_json = client
        .create_content_lock(
            guarded_resource,
            json!([{
                "criterion_id": "criterion-1",
                "verifier_type": "paykit-payment",
                "params": {
                    "recipient_pubky": creator().to_string(),
                    "amount": "50000",
                    "asset": "BTC",
                    "payment_in": 24
                }
            }]),
            standard_lock_logic(),
            standard_access_policy(),
            standard_lock_server(),
        )
        .await
        .unwrap();
    let content_lock_path = ContentLockPath::from_str(
        content_lock_json["content_lock_path"]
            .as_str()
            .expect("content_lock_path is string"),
    )
    .unwrap();
    let submitted = paykit_submitted_proof_bundle_for(content_lock_path);
    let lock_resource = submitted.pubky_lock_resource.to_string();

    let mut invalid = submitted.clone();
    invalid.bundle_id = BundleId::new_random();
    let invalid_bundle_id = invalid.bundle_id.clone();
    invalid.proofs[0].criterion_id = "missing-criterion".to_owned();
    let error = client.submit_proof_bundle(invalid).await.unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.body["error"]["code"], "invalid_request");
    fake_paykit.assert_no_invoice_created().await;
    assert!(
        test_app
            .state()
            .verification_tasks()
            .get_verification_task_by_handle(&creator(), &invalid_bundle_id)
            .await
            .unwrap()
            .is_none()
    );

    let submit_json = client.submit_proof_bundle(submitted.clone()).await.unwrap();
    assert_eq!(submit_json["status"], "pending");
    assert!(submit_json.get("task_id").is_none());
    fake_paykit.assert_invoice_created(&lock_resource).await;
    fake_paykit.assert_invoice_count(1).await;

    let replay_json = client.submit_proof_bundle(submitted.clone()).await.unwrap();
    assert_eq!(replay_json, submit_json);
    fake_paykit.assert_invoice_count(1).await;

    let mut conflicting = submitted;
    conflicting.reader_public_key = Some(
        CreatorPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap(),
    );
    let conflict = client.submit_proof_bundle(conflicting).await.unwrap_err();
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(conflict.body["error"]["code"], "task_state_conflict");
    fake_paykit.assert_invoice_count(1).await;

    let task = test_app
        .state()
        .verification_tasks()
        .get_verification_task_by_handle(&creator(), &bundle_id())
        .await
        .unwrap()
        .expect("submitted task exists");
    let claimer = InMemoryVerificationTaskClaimer::new(vec![task]);
    let worker = VerificationWorker::new(
        test_app.state().verification_tasks().as_ref(),
        &claimer,
        test_app.state().content_locks().as_ref(),
        test_app.state().entitlements().as_ref(),
        test_app.state().dev_static_verifier().as_ref(),
        test_app
            .state()
            .paykit_payment_verifier()
            .map(|verifier| verifier.as_ref() as &dyn CriterionVerifier),
        true,
        test_app.state().clock().as_ref(),
        test_app
            .state()
            .config()
            .credentials
            .lock_server_public_key
            .clone(),
        "paykit-e2e-worker".to_owned(),
        std::time::Duration::from_millis(10),
        60,
    );
    assert!(matches!(
        worker.run_once().await.unwrap(),
        WorkerTick::Completed(_)
    ));
    fake_paykit.assert_status_checked().await;

    let credential = client
        .issue_access_credential(creator(), BUNDLE_ID)
        .await
        .unwrap();
    let proxy_response = client
        .proxy_read_guarded_resource(&credential, "paid-e2e.txt")
        .await
        .unwrap();
    assert_eq!(proxy_response.status(), StatusCode::OK);
    assert_eq!(response_bytes(proxy_response).await, GUARDED_BYTES);
}

async fn creator_publishing_client() -> (TestServerApp, LocalCreatorPublishingClient) {
    let config = TestServerApp::default_in_memory_config();
    let test_app =
        TestServerApp::from_state(AppState::new_empty_in_memory_with_creator_repositories(
            config,
            Arc::new(InMemoryContentLockRepository::new()),
            Arc::new(InMemoryGuardedResourceRepository::new()),
            Arc::new(InMemoryLockServicePointerRepository::new()),
            Arc::new(InMemoryEntitlementRepository::new()),
        ));
    test_app
        .insert_frontend_session_for_test(
            FrontendSessionToken::new(FRONTEND_SESSION_TOKEN),
            creator(),
            time::OffsetDateTime::now_utc() + Duration::hours(12),
        )
        .await
        .unwrap();
    let client = LocalCreatorPublishingClient::new(test_app.router())
        .with_frontend_session_token(FRONTEND_SESSION_TOKEN);
    (test_app, client)
}

async fn issue_credential_for(
    client: &LocalCreatorPublishingClient,
    content_lock_json: &serde_json::Value,
) -> String {
    let content_lock_path = ContentLockPath::from_str(
        content_lock_json["content_lock_path"]
            .as_str()
            .expect("content_lock_path is string"),
    )
    .unwrap();
    let bundle_id = BundleId::new_random();
    let bundle_id_string = bundle_id.to_string();
    client
        .submit_proof_bundle(submitted_proof_bundle_for_bundle(
            content_lock_path,
            bundle_id,
        ))
        .await
        .unwrap();
    client
        .dev_complete_verification(creator(), &bundle_id_string)
        .await
        .unwrap();
    client
        .issue_access_credential(creator(), &bundle_id_string)
        .await
        .unwrap()
}

fn secondary_resource_map(resource: &serde_json::Value) -> serde_json::Value {
    json!({
        resource["path"].as_str().expect("resource path is string"): {
            "hash": resource["hash"].clone(),
            "content_type": resource["content_type"].clone(),
            "size": resource["size"].clone(),
        }
    })
}

fn assert_resource_response_headers(
    response: &axum::response::Response,
    content_type: &str,
    content_length: usize,
    resource: &serde_json::Value,
) {
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], content_type);
    assert_eq!(
        response.headers()[header::CONTENT_LENGTH],
        content_length.to_string()
    );
    assert_eq!(
        response.headers()[header::ETAG],
        format!("\"{}\"", resource["hash"].as_str().unwrap())
    );
}

fn standard_criteria() -> serde_json::Value {
    json!([{
        "criterion_id": "criterion-1",
        "verifier_type": "dev-static",
        "params": { "satisfied": true }
    }])
}

fn standard_lock_logic() -> serde_json::Value {
    json!({ "type": "all", "criteria": ["criterion-1"] })
}

fn standard_access_policy() -> serde_json::Value {
    json!({ "requested_credential_ttl_seconds": 900 })
}

fn standard_lock_server() -> serde_json::Value {
    json!({ "override": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo" })
}

fn submitted_proof_bundle_for_bundle(
    content_lock_path: ContentLockPath,
    bundle_id: BundleId,
) -> SubmittedProofBundle {
    SubmittedProofBundle {
        version: SUBMITTED_PROOF_BUNDLE_VERSION,
        bundle_id,
        pubky_lock_resource: PubkyLockResource::new(creator(), content_lock_path),
        reader_public_key: None,
        proofs: vec![Proof {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: locks_core::lock_policy::VerifierType::DevStatic,
            payload: json!({ "satisfied": true }),
        }],
    }
}

fn submitted_proof_bundle_for(content_lock_path: ContentLockPath) -> SubmittedProofBundle {
    SubmittedProofBundle {
        version: SUBMITTED_PROOF_BUNDLE_VERSION,
        bundle_id: bundle_id(),
        pubky_lock_resource: PubkyLockResource::new(creator(), content_lock_path),
        reader_public_key: None,
        proofs: vec![Proof {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: locks_core::lock_policy::VerifierType::DevStatic,
            payload: json!({ "satisfied": true }),
        }],
    }
}

fn paykit_criterion_json(criterion_id: &str) -> serde_json::Value {
    json!({
        "criterion_id": criterion_id,
        "verifier_type": "paykit-payment",
        "params": {
            "recipient_pubky": creator().to_string(),
            "amount": "50000",
            "asset": "BTC",
            "payment_in": 24
        }
    })
}

fn paykit_submitted_proof_bundle_for(content_lock_path: ContentLockPath) -> SubmittedProofBundle {
    SubmittedProofBundle {
        version: SUBMITTED_PROOF_BUNDLE_VERSION,
        bundle_id: bundle_id(),
        pubky_lock_resource: PubkyLockResource::new(creator(), content_lock_path),
        reader_public_key: Some(creator()),
        proofs: vec![Proof {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::PaykitPayment,
            payload: json!({}),
        }],
    }
}

#[derive(Debug)]
struct AlwaysResolvesReader;

#[async_trait]
impl ReaderPubkyResolver for AlwaysResolvesReader {
    async fn reader_has_homeserver(&self, _reader: &CreatorPubky) -> bool {
        true
    }
}

#[derive(Clone)]
struct FakePaykitServer {
    server_url: String,
    state: Arc<Mutex<FakePaykitState>>,
}

#[derive(Debug, Default)]
struct FakePaykitState {
    invoice_body: Option<serde_json::Value>,
    invoice_signature: Option<String>,
    invoice_count: usize,
    status_body: Option<serde_json::Value>,
    status_signature: Option<String>,
}

impl FakePaykitServer {
    async fn start() -> Self {
        let state = Arc::new(Mutex::new(FakePaykitState::default()));
        let app = Router::new()
            .route("/invoices", post(fake_invoice_handler))
            .route("/transactions/status", post(fake_status_handler))
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { server_url, state }
    }

    async fn assert_invoice_created(&self, lock_resource: &str) {
        let state = self.state.lock().await;
        assert_eq!(
            state.invoice_body,
            Some(json!({
                "bundle_id": BUNDLE_ID,
                "lock_resource": lock_resource,
                "payment_in": 24,
                "reader": creator().to_string(),
            }))
        );
        let signature = state
            .invoice_signature
            .as_deref()
            .expect("invoice request has X-Paykit-Signature");
        assert!(!signature.is_empty());
        assert!(!signature.contains('='));
    }

    async fn assert_no_invoice_created(&self) {
        let state = self.state.lock().await;
        assert_eq!(state.invoice_body, None);
        assert_eq!(state.invoice_signature, None);
        assert_eq!(state.invoice_count, 0);
    }

    async fn assert_invoice_count(&self, expected: usize) {
        let state = self.state.lock().await;
        assert_eq!(state.invoice_count, expected);
    }

    async fn assert_status_checked(&self) {
        let state = self.state.lock().await;
        assert_eq!(
            state.status_body,
            Some(json!({
                "creator": creator().to_string(),
                "bundle_id": BUNDLE_ID
            }))
        );
        let signature = state
            .status_signature
            .as_deref()
            .expect("status request has X-Paykit-Signature");
        assert!(!signature.is_empty());
        assert!(!signature.contains('='));
    }
}

async fn fake_invoice_handler(
    State(state): State<Arc<Mutex<FakePaykitState>>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut state = state.lock().await;
    state.invoice_count += 1;
    state.invoice_body = Some(serde_json::from_slice(&body).unwrap());
    state.invoice_signature = headers
        .get("X-Paykit-Signature")
        .map(|value| value.to_str().unwrap().to_owned());
    (
        StatusCode::CREATED,
        Json(json!({
            "invoice_created_at": "2026-08-12T10:00:00Z",
            "payment_deadline": "2026-08-13T10:00:00Z",
        })),
    )
}

async fn fake_status_handler(
    State(state): State<Arc<Mutex<FakePaykitState>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Json<serde_json::Value> {
    let mut state = state.lock().await;
    state.status_body = Some(serde_json::from_slice(&body).unwrap());
    state.status_signature = headers
        .get("X-Paykit-Signature")
        .map(|value| value.to_str().unwrap().to_owned());
    Json(json!({
        "status": "detected",
        "confirmations": 0,
        "amount_matched": true,
    }))
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

fn bundle_id() -> BundleId {
    BundleId::from_str(BUNDLE_ID).unwrap()
}
