use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{HeaderMap, Request, StatusCode, header};
use locks_core::ids::{
    BundleId, CreatorPubky, GuardedResourceHash, LockServerPubky, PubkyLockResource,
};
use locks_core::lock_policy::{
    AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
    LockServerConfig, VerifierType,
};
use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::{
    CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
    CreatorConnectAuthorizationUrl, CreatorConnectFlowId, FrontendSessionRecord,
    FrontendSessionToken, GuardedResourceRecord, LegacyCreatorConnectFlowApproval,
    PendingCreatorConnectFlowRecord,
};
use locks_service::application::ports::{Clock, LegacyCreatorConnectFlowClient};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use time::macros::datetime;
use tower::ServiceExt;

use super::router;
use crate::api::auth::parse_frontend_session_token;
use crate::api::creator_authority::{escape_html, validate_return_to_url};
use crate::api::dtos::{
    AuthenticatedCreateContentLockHttpRequest, AuthenticatedSetLockServicePointerHttpRequest,
    IssueAccessCredentialHttpRequest, SubmitProofBundleHttpRequest,
    VerificationTaskHandleHttpRequest,
};
use crate::app_state::{AppState, ReaderPubkyResolver};
use crate::config::{
    ContentLocksConfig, CreatorAuthorityAcquisitionConfig, CreatorAuthorityAcquisitionMethod,
    DatabaseConfig, LockServerCredentialsConfig, LockServerRuntimeConfig, LoggingConfig,
    PubkyConfig, RateLimitsConfig, RuntimeConfig, RuntimeEnvironment, SecretsConfig,
    VerificationSubmissionRateLimitConfig, WorkerConfig,
};
use crate::paykit_http_client::{
    PaykitSetupStatusKind, PaykitSetupStatusProvider, PaykitSetupStatusProviderError,
};

use locks_service::infrastructure::memory::content_locks::InMemoryContentLockRepository;
use locks_service::infrastructure::memory::entitlements::InMemoryEntitlementRepository;
use locks_service::infrastructure::memory::guarded_resources::InMemoryGuardedResourceRepository;
use locks_service::infrastructure::memory::lock_service_pointers::InMemoryLockServicePointerRepository;
use locks_service::infrastructure::postgres::CreatorAuthoritySecretCipher;

const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

#[derive(Debug, Clone, Copy)]
enum CreatorRepositoryBackend {
    PubkyHomeserver,
}

#[derive(Debug)]
struct AlwaysResolvesReader;

#[async_trait]
impl ReaderPubkyResolver for AlwaysResolvesReader {
    async fn reader_has_homeserver(&self, _reader: &CreatorPubky) -> bool {
        true
    }
}

#[derive(Debug)]
struct RecordingPaykitSetupStatusProvider {
    result: Result<PaykitSetupStatusKind, PaykitSetupStatusProviderError>,
    creators: Mutex<Vec<CreatorPubky>>,
}

impl RecordingPaykitSetupStatusProvider {
    fn status(status: PaykitSetupStatusKind) -> Self {
        Self {
            result: Ok(status),
            creators: Mutex::new(Vec::new()),
        }
    }

    fn failure(error: PaykitSetupStatusProviderError) -> Self {
        Self {
            result: Err(error),
            creators: Mutex::new(Vec::new()),
        }
    }

    fn creators(&self) -> Vec<CreatorPubky> {
        self.creators.lock().unwrap().clone()
    }
}

#[async_trait]
impl PaykitSetupStatusProvider for RecordingPaykitSetupStatusProvider {
    async fn setup_status(
        &self,
        creator: &CreatorPubky,
    ) -> Result<PaykitSetupStatusKind, PaykitSetupStatusProviderError> {
        self.creators.lock().unwrap().push(creator.clone());
        self.result
    }
}

#[tokio::test]
async fn healthz_returns_process_liveness_without_runtime_details() {
    let response = router(test_state())
        .oneshot(empty_request("GET", "/healthz"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "ok");
    assert_eq!(body.as_object().unwrap().len(), 1);
    assert_no_keys(
        &body,
        &[
            "database_url",
            "lock_server_secret_key",
            "lock_server_public_key",
            "worker_id",
            "worker_enabled",
            "task_count",
            "secret_path",
            "credentials",
            "error",
            "task_id",
            "credential",
            "submitted_proof_bundle",
        ],
    );
}

#[tokio::test]
async fn well_known_locks_server_returns_service_version_and_lock_server_identity() {
    let response = router(test_state())
        .oneshot(empty_request("GET", "/.well-known/locks-server"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["service"], "pubky-locks-server");
    assert_eq!(body["api_version"], "0.1");
    assert_eq!(
        body["lock_server"],
        "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
    );
    assert_eq!(body.as_object().unwrap().len(), 3);
}

#[tokio::test]
async fn cors_preflight_allows_browser_sdk_requests() {
    let response = router(test_state())
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/healthz")
                .header(header::ORIGIN, "https://pubky.app")
                .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .header(
                    header::ACCESS_CONTROL_REQUEST_HEADERS,
                    "authorization,content-type",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some("https://pubky.app")
    );
    assert!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value.contains("authorization") && value.contains("content-type")
            })
    );
}

#[tokio::test]
async fn readyz_returns_ready_for_ephemeral_runtime_without_secrets() {
    let response = router(test_state())
        .oneshot(empty_request("GET", "/readyz"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "ready");
    assert_eq!(body["runtime_storage"], "ephemeral");
    assert_eq!(body["worker_enabled"], true);
    assert_eq!(body.as_object().unwrap().len(), 3);
    assert_no_keys(
        &body,
        &[
            "database_url",
            "lock_server_secret_key",
            "lock_server_public_key",
            "worker_id",
            "task_count",
            "secret_path",
            "credentials",
            "error",
            "task_id",
            "credential",
            "submitted_proof_bundle",
        ],
    );
}

#[tokio::test]
async fn readyz_reports_worker_disabled_for_ephemeral_runtime() {
    let mut config = test_config(RuntimeEnvironment::Development, true);
    config.worker.enabled = false;
    let response = router(AppState::new_empty_in_memory(config))
        .oneshot(empty_request("GET", "/readyz"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "ready");
    assert_eq!(body["runtime_storage"], "ephemeral");
    assert_eq!(body["worker_enabled"], false);
    assert_eq!(body.as_object().unwrap().len(), 3);
}

#[tokio::test]
async fn readyz_returns_not_ready_for_persisted_runtime_when_pool_ping_fails() {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgres://locks:locks@127.0.0.1:1/locks_test")
        .expect("construct lazy unreachable Postgres pool");
    let state = AppState::new_with_postgres_runtime(
        test_config(RuntimeEnvironment::Development, true),
        pool,
        CreatorAuthoritySecretCipher::new([7; 32]),
    );

    let response = router(state)
        .oneshot(empty_request("GET", "/readyz"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response_json(response).await;
    assert_eq!(body["status"], "not_ready");
    assert_eq!(body["runtime_storage"], "persisted");
    assert_eq!(body["worker_enabled"], true);
    assert_eq!(body.as_object().unwrap().len(), 3);
    assert_no_keys(
        &body,
        &[
            "database_url",
            "error",
            "message",
            "lock_server_secret_key",
            "lock_server_public_key",
            "worker_id",
            "task_count",
            "secret_path",
            "credentials",
            "task_id",
            "credential",
            "submitted_proof_bundle",
        ],
    );
}

#[tokio::test]
async fn viewer_access_contract_fixtures_submit_proof_bundle() {
    let request = fixture_value("submit_proof_bundle_request.json");
    serde_json::from_value::<SubmitProofBundleHttpRequest>(request.clone()).unwrap();
    let response_shape = fixture_value("submit_proof_bundle_response_shape.json");

    let app = router(test_state());
    let response = app
        .oneshot(json_request("POST", "/proof-bundles", request))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["creator"], response_shape["creator"]);
    assert_eq!(body["bundle_id"], response_shape["bundle_id"]);
    assert_eq!(body["status"], response_shape["status"]);
    assert_eq!(body["started_at"], response_shape["started_at"]);
    assert_eq!(body["completed_at"], response_shape["completed_at"]);
    assert_eq!(body["failure_message"], response_shape["failure_message"]);
    assert_placeholder_string(&body["submitted_at"], "<submitted_at>");
    assert_no_keys(
        &body,
        &[
            "task_id",
            "pubky_lock_resource",
            "submitted_proof_bundle",
            "proofs",
            "credential",
        ],
    );
}

#[test]
fn viewer_access_contract_fixtures_verification_task_handle_request() {
    let request = fixture_value("verification_task_handle_request.json");
    serde_json::from_value::<VerificationTaskHandleHttpRequest>(request.clone()).unwrap();
    serde_json::from_value::<IssueAccessCredentialHttpRequest>(request.clone()).unwrap();
    assert_eq!(
        request["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
    assert_eq!(request["bundle_id"], BUNDLE_ID);
    assert!(request.get("task_id").is_none());
}

#[tokio::test]
async fn viewer_access_contract_fixtures_access_credential_response() {
    let response_shape = fixture_value("access_credential_response_shape.json");
    let state = test_state().with_clock(Arc::new(FixedClock(datetime!(2026-05-29 12:00:00 UTC))));
    let content_lock = content_lock(true);
    seed_content_lock(&state, content_lock.clone()).await;
    let app = router(state.clone());
    submit_task(&state, &app, submitted_proof_bundle_for(&content_lock)).await;
    complete_task(&app).await;

    let credential_response = app
        .oneshot(json_request(
            "POST",
            "/access-credentials",
            fixture_value("verification_task_handle_request.json"),
        ))
        .await
        .unwrap();

    assert_eq!(credential_response.status(), StatusCode::OK);
    let body = response_json(credential_response).await;
    assert_placeholder_string(&body["credential"], "<credential>");
    assert_placeholder_string(&body["expires_at"], "<expires_at>");
    assert_eq!(response_shape["credential"], "<credential>");
    assert_eq!(response_shape["expires_at"], "<expires_at>");
    assert_no_keys(&body, &["task_id", "raw_credential", "entitlement"]);
}

#[tokio::test]
async fn post_proof_bundles_returns_public_lifecycle_handle_without_task_id() {
    let app = router(test_state());
    let request_body = json!({
        "submitted_proof_bundle": submitted_proof_bundle()
    });

    let response = app
        .oneshot(json_request("POST", "/proof-bundles", request_body))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
    assert_eq!(body["bundle_id"], BUNDLE_ID);
    assert_eq!(body["status"], "pending");
    assert!(body.get("submitted_at").and_then(Value::as_str).is_some());
    assert_no_keys(
        &body,
        &[
            "task_id",
            "pubky_lock_resource",
            "submitted_proof_bundle",
            "proofs",
        ],
    );
}

#[tokio::test]
async fn post_proof_bundles_rejects_paykit_payment_when_paykit_is_not_configured() {
    let mut content_lock = content_lock(true);
    content_lock.criteria[0].verifier_type = VerifierType::PaykitPayment;
    content_lock.criteria[0].params = json!({
        "recipient_pubky": creator().to_string(),
        "amount": "50000",
        "asset": "BTC"
    });
    let mut bundle = submitted_proof_bundle_for(&content_lock);
    bundle.reader_public_key = Some(creator());
    bundle.proofs[0].verifier_type = VerifierType::PaykitPayment;
    bundle.proofs[0].payload = json!({});
    let state = test_state().with_reader_pubky_resolver(Arc::new(AlwaysResolvesReader));
    seed_content_lock(&state, content_lock).await;
    let app = router(state);

    let response = app
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": bundle }),
        ))
        .await
        .unwrap();

    assert_error_response(
        response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "paykit_not_configured",
    )
    .await;
}

#[tokio::test]
async fn post_proof_bundles_rejects_mixed_paykit_payment_proofs() {
    let app = router(test_state());
    let mut bundle = submitted_proof_bundle();
    bundle.reader_public_key = Some(creator());
    bundle.proofs[0].verifier_type = VerifierType::PaykitPayment;
    bundle.proofs[0].payload = json!({});
    bundle.proofs.push(Proof {
        criterion_id: "criterion-2".to_owned(),
        verifier_type: VerifierType::DevStatic,
        payload: json!({ "satisfied": true }),
    });

    let response = app
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": bundle }),
        ))
        .await
        .unwrap();

    assert_error_response(response, StatusCode::BAD_REQUEST, "invalid_request").await;
}

#[tokio::test]
async fn post_proof_bundles_rejects_invalid_request_with_stable_error_envelope() {
    let app = router(test_state());

    let response = app
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "task_id": "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10" }),
        ))
        .await
        .unwrap();

    assert_error_response(response, StatusCode::BAD_REQUEST, "invalid_request").await;
}

#[tokio::test]
async fn duplicate_changed_proof_submission_maps_to_task_state_conflict() {
    let app = router(test_state());
    let mut changed = submitted_proof_bundle();
    changed.proofs[0].payload = json!({ "satisfied": false });

    let first = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted_proof_bundle() }),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = app
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": changed }),
        ))
        .await
        .unwrap();

    assert_error_response(second, StatusCode::CONFLICT, "task_state_conflict").await;
}

#[tokio::test]
async fn post_proof_bundles_returns_rate_limited_after_configured_submission_limit() {
    let app = router(test_state_with_rate_limit(true, 1, 60));

    let first = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted_proof_bundle() }),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = app
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted_proof_bundle_with_bundle_id(
                    "000G40R40M30E209185GR38E1X"
                ) }),
        ))
        .await
        .unwrap();

    assert_eq!(second.headers().get(header::RETRY_AFTER).unwrap(), "60");
    assert_error_response(second, StatusCode::TOO_MANY_REQUESTS, "rate_limited").await;
}

#[tokio::test]
async fn post_proof_bundles_rate_limit_is_per_creator() {
    let app = router(test_state_with_rate_limit(true, 1, 60));
    let other_creator =
        CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
            .unwrap();

    let first = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted_proof_bundle() }),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = app
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted_proof_bundle_for_creator_and_bundle_id(
                    other_creator,
                    "000G40R40M30E209185GR38E1X",
                ) }),
        ))
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::OK);
}

#[tokio::test]
async fn post_proof_bundles_rate_limit_is_per_client_address() {
    let app = router(test_state_with_rate_limit(true, 1, 60));

    let first = app
        .clone()
        .oneshot(json_request_with_client_address(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted_proof_bundle() }),
            [127, 0, 0, 1],
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = app
        .oneshot(json_request_with_client_address(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted_proof_bundle_with_bundle_id(
                    "000G40R40M30E209185GR38E1X"
                ) }),
            [127, 0, 0, 2],
        ))
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::OK);
}

#[tokio::test]
async fn post_proof_bundles_does_not_rate_limit_when_disabled() {
    let app = router(test_state_with_rate_limit(false, 0, 0));

    let first = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted_proof_bundle() }),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = app
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted_proof_bundle_with_bundle_id(
                    "000G40R40M30E209185GR38E1X"
                ) }),
        ))
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::OK);
}

#[tokio::test]
async fn lookup_verification_task_returns_pending_lifecycle_view_without_secrets() {
    let app = router(test_state());
    let submit_response = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted_proof_bundle() }),
        ))
        .await
        .unwrap();
    let submitted = response_json(submit_response).await;
    let submitted_at = submitted["submitted_at"].clone();

    let response = app
        .oneshot(json_request(
            "POST",
            "/verification-task-lookups",
            handle_request(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
    assert_eq!(body["bundle_id"], BUNDLE_ID);
    assert_eq!(body["status"], "pending");
    assert_eq!(body["submitted_at"], submitted_at);
    assert_eq!(body["started_at"], Value::Null);
    assert_eq!(body["completed_at"], Value::Null);
    assert_eq!(body["failure_message"], Value::Null);
    assert_no_keys(&body, &["task_id", "credential", "credential_issuance"]);
}

#[tokio::test]
async fn lookup_missing_verification_task_returns_stable_error_envelope() {
    let app = router(test_state());

    let response = app
        .oneshot(json_request(
            "POST",
            "/verification-task-lookups",
            handle_request(),
        ))
        .await
        .unwrap();

    assert_error_response(
        response,
        StatusCode::NOT_FOUND,
        "verification_task_not_found",
    )
    .await;
}

#[tokio::test]
async fn old_task_id_polling_route_is_not_mounted() {
    let app = router(test_state());

    let response = app
        .oneshot(empty_request(
            "GET",
            "/verification-tasks/018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(!body.contains("verification_task_not_found"));
}

#[tokio::test]
async fn lookup_request_rejects_unknown_fields() {
    let app = router(test_state());

    let response = app
        .oneshot(json_request(
            "POST",
            "/verification-task-lookups",
            json!({
                "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                "bundle_id": BUNDLE_ID,
                "task_id": "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn complete_verification_task_returns_completed_lifecycle_status() {
    let state = test_state();
    let content_lock = content_lock(true);
    seed_content_lock(&state, content_lock.clone()).await;
    let app = router(state.clone());
    submit_task(&state, &app, submitted_proof_bundle_for(&content_lock)).await;

    let response = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/verification-task-completions",
            handle_request(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
    assert_eq!(body["bundle_id"], BUNDLE_ID);
    assert_eq!(body["status"], "completed");
    assert!(body.get("completed_at").and_then(Value::as_str).is_some());
    assert_no_keys(&body, &["task_id", "credential", "credential_issuance"]);

    let polled = app
        .oneshot(json_request(
            "POST",
            "/verification-task-lookups",
            handle_request(),
        ))
        .await
        .unwrap();
    let polled_body = response_json(polled).await;
    assert_eq!(polled_body["status"], "completed");
}

#[tokio::test]
async fn complete_missing_verification_task_returns_stable_error_envelope() {
    let app = router(test_state());

    let response = app
        .oneshot(json_request(
            "POST",
            "/verification-task-completions",
            handle_request(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = response_json(response).await;
    assert_eq!(
        body,
        json!({
            "error": {
                "code": "verification_task_not_found",
                "message": "verification task not found"
            }
        })
    );
}

#[tokio::test]
async fn production_router_omits_dev_completion_route() {
    let app = router(test_state_with_runtime(
        RuntimeEnvironment::Production,
        false,
    ));

    let response = app
        .oneshot(json_request(
            "POST",
            "/verification-task-completions",
            handle_request(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(!body.contains("verification_task_not_found"));
}

#[tokio::test]
async fn production_creator_routes_are_not_mounted_with_local_memory_repositories() {
    let app = router(test_state_with_runtime(
        RuntimeEnvironment::Production,
        false,
    ));

    let response = app
        .oneshot(json_request(
            "POST",
            "/creator/priv-resources",
            legacy_json_guarded_resource_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn production_creator_routes_are_mounted_with_pubky_homeserver_repositories() {
    let app = router(test_state_with_creator_repository_backend(
        RuntimeEnvironment::Production,
        CreatorRepositoryBackend::PubkyHomeserver,
    ));

    let response = app
        .oneshot(raw_upload_request(
            "/creator/priv-resources/content/example.txt",
            Some("text/plain"),
            b"guarded bytes".to_vec(),
        ))
        .await
        .unwrap();

    let body = assert_error_response(
        response,
        StatusCode::UNAUTHORIZED,
        "frontend_session_unavailable",
    )
    .await;
    assert!(!body.to_string().contains("guarded bytes"));
}

#[tokio::test]
async fn dev_pubky_homeserver_routes_mount_authenticated_creator_routes_and_manual_completion() {
    let mut config = test_config(RuntimeEnvironment::Development, true);
    config.creator_authority_acquisition.enabled = true;
    config.creator_authority_acquisition.method = CreatorAuthorityAcquisitionMethod::LegacyConnect;
    let app = router(AppState::new_empty_in_memory_with_creator_repositories(
        config,
        Arc::new(InMemoryContentLockRepository::new()),
        Arc::new(InMemoryGuardedResourceRepository::new()),
        Arc::new(InMemoryLockServicePointerRepository::new()),
        Arc::new(InMemoryEntitlementRepository::new()),
    ));

    let creator_response = app
        .clone()
        .oneshot(raw_upload_request(
            "/creator/priv-resources/content/example.txt",
            Some("text/plain"),
            b"guarded bytes".to_vec(),
        ))
        .await
        .unwrap();
    assert_error_response(
        creator_response,
        StatusCode::UNAUTHORIZED,
        "frontend_session_unavailable",
    )
    .await;

    let completion_response = app
        .oneshot(json_request(
            "POST",
            "/verification-task-completions",
            json!({ "creator": creator().to_string(), "bundle_id": BUNDLE_ID }),
        ))
        .await
        .unwrap();
    assert_error_response(
        completion_response,
        StatusCode::NOT_FOUND,
        "verification_task_not_found",
    )
    .await;
}

#[tokio::test]
async fn production_creator_routes_do_not_accept_frontend_session_query_tokens() {
    let app = router(test_state_with_creator_repository_backend(
        RuntimeEnvironment::Production,
        CreatorRepositoryBackend::PubkyHomeserver,
    ));

    let response = app
        .oneshot(raw_upload_request(
            "/creator/priv-resources/content/example.txt?frontend_session_token=query-secret",
            Some("text/plain"),
            b"guarded bytes".to_vec(),
        ))
        .await
        .unwrap();

    let body = assert_error_response(
        response,
        StatusCode::UNAUTHORIZED,
        "frontend_session_unavailable",
    )
    .await;
    assert!(!body.to_string().contains("query-secret"));
}

#[tokio::test]
async fn authenticated_creator_old_json_guarded_resource_route_is_not_mounted() {
    let state = test_state_with_creator_repository_backend(
        RuntimeEnvironment::Production,
        CreatorRepositoryBackend::PubkyHomeserver,
    );
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);
    let mut request = legacy_json_guarded_resource_payload();
    request["creator"] = json!(other_creator().to_string());

    let response = app
        .oneshot(authenticated_json_request(
            "POST",
            "/creator/priv-resources",
            request,
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authenticated_creator_body_creator_is_rejected_for_content_lock_creation() {
    let state = test_state_with_creator_repository_backend(
        RuntimeEnvironment::Production,
        CreatorRepositoryBackend::PubkyHomeserver,
    );
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);
    let mut request = creator_content_lock_request(registered_guarded_resource_json());
    request["creator"] = json!(other_creator().to_string());

    let response = app
        .oneshot(authenticated_json_request(
            "POST",
            "/creator/content-locks",
            request,
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    let body = assert_error_response(response, StatusCode::BAD_REQUEST, "invalid_request").await;
    assert_eq!(body["error"]["message"], "invalid request");
    assert!(!body.to_string().contains("frontend-session-secret"));
}

#[tokio::test]
async fn authenticated_creator_body_creator_is_rejected_for_lock_service_config_update() {
    let state = test_state_with_creator_repository_backend(
        RuntimeEnvironment::Production,
        CreatorRepositoryBackend::PubkyHomeserver,
    );
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);
    let mut request = creator_lock_service_config_request();
    request["creator"] = json!(other_creator().to_string());

    let response = app
        .oneshot(authenticated_json_request(
            "POST",
            "/creator/lock-service-config",
            request,
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    let body = assert_error_response(response, StatusCode::BAD_REQUEST, "invalid_request").await;
    assert_eq!(body["error"]["message"], "invalid request");
    assert!(!body.to_string().contains("frontend-session-secret"));
}

#[tokio::test]
async fn authenticated_creator_registers_guarded_resource_without_body_creator() {
    let state = test_state_with_creator_repository_backend(
        RuntimeEnvironment::Production,
        CreatorRepositoryBackend::PubkyHomeserver,
    );
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);
    let response = app
        .oneshot(authenticated_raw_upload_request(
            "/creator/priv-resources/content/example.txt",
            Some("text/plain"),
            b"guarded bytes".to_vec(),
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
}

#[tokio::test]
async fn authenticated_creator_creates_content_lock_without_body_creator() {
    let state = test_state_with_creator_repository_backend(
        RuntimeEnvironment::Production,
        CreatorRepositoryBackend::PubkyHomeserver,
    );
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let mut request = creator_content_lock_request(registered_guarded_resource_json());
    seed_guarded_resource_from_fixture(&state, &request).await;
    request.as_object_mut().unwrap().remove("creator");
    let app = router(state);

    let response = app
        .oneshot(authenticated_json_request(
            "POST",
            "/creator/content-locks",
            request,
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["content_lock"]["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
}

#[tokio::test]
async fn authenticated_creator_content_lock_over_max_resources_returns_invalid_request() {
    let state = test_state_with_content_lock_limits(ContentLocksConfig {
        max_resource_bytes: 10_000_000,
        max_resources: 1,
        max_total_resource_bytes: 100_000_000,
    });
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let mut request = creator_content_lock_request(registered_guarded_resource_json());
    request["secondary_resources"] = json!({
        "/priv/locks.app/content/secondary.txt": {
            "hash": registered_guarded_resource_json()["hash"].clone(),
            "content_type": "text/plain",
            "size": 1
        }
    });
    let app = router(state);

    let response = app
        .oneshot(authenticated_json_request(
            "POST",
            "/creator/content-locks",
            request,
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    let body = assert_error_response(response, StatusCode::BAD_REQUEST, "invalid_request").await;
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("at most 1 resources")
    );
}

#[tokio::test]
async fn authenticated_creator_content_lock_over_total_size_returns_invalid_request() {
    let state = test_state_with_content_lock_limits(ContentLocksConfig {
        max_resource_bytes: 10_000_000,
        max_resources: 10,
        max_total_resource_bytes: 12,
    });
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let request = creator_content_lock_request(registered_guarded_resource_json());
    let app = router(state);

    let response = app
        .oneshot(authenticated_json_request(
            "POST",
            "/creator/content-locks",
            request,
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    let body = assert_error_response(response, StatusCode::BAD_REQUEST, "invalid_request").await;
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("total resource size")
    );
}

#[tokio::test]
async fn authenticated_creator_delete_guarded_resource_returns_204_then_404() {
    let state = test_state_with_creator_repository_backend(
        RuntimeEnvironment::Production,
        CreatorRepositoryBackend::PubkyHomeserver,
    );
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let request = creator_content_lock_request(registered_guarded_resource_json());
    seed_guarded_resource_from_fixture(&state, &request).await;
    let app = router(state);

    let response = app
        .clone()
        .oneshot(auth_request(
            "DELETE",
            "/creator/priv-resources/content/example.txt",
            "Bearer frontend-session-secret",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(response_bytes(response).await.is_empty());

    let response = app
        .oneshot(auth_request(
            "DELETE",
            "/creator/priv-resources/content/example.txt",
            "Bearer frontend-session-secret",
        ))
        .await
        .unwrap();
    assert_error_response(
        response,
        StatusCode::NOT_FOUND,
        "guarded_resource_not_found",
    )
    .await;
}

#[tokio::test]
async fn authenticated_creator_sets_lock_service_pointer_without_body_creator() {
    let state = test_state_with_creator_repository_backend(
        RuntimeEnvironment::Production,
        CreatorRepositoryBackend::PubkyHomeserver,
    );
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);
    let mut request = creator_lock_service_config_request();
    request.as_object_mut().unwrap().remove("creator");

    let response = app
        .oneshot(authenticated_json_request(
            "POST",
            "/creator/lock-service-config",
            request,
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
}

#[tokio::test]
async fn dev_creator_routes_do_not_mount_unauthenticated_publishing_shape() {
    let state = test_state_with_creator_publishing(true);
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);

    let response = app
        .oneshot(json_request(
            "POST",
            "/creator/priv-resources",
            legacy_json_guarded_resource_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn production_router_does_not_expose_dev_completion_route() {
    let app = router(test_state_with_runtime(
        RuntimeEnvironment::Production,
        false,
    ));

    let response = app
        .oneshot(json_request(
            "POST",
            "/verification-task-completions",
            handle_request(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(!body.contains("verification_task_not_found"));
}

#[tokio::test]
async fn failed_completion_returns_error_and_task_polls_as_failed() {
    let state = test_state();
    let content_lock = content_lock(false);
    seed_content_lock(&state, content_lock.clone()).await;
    let app = router(state.clone());
    submit_task(&state, &app, submitted_proof_bundle_for(&content_lock)).await;

    let response = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/verification-task-completions",
            handle_request(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let error = response_json(response).await;
    assert_eq!(error["error"]["code"], "entitlement_not_authorized");

    let polled = app
        .oneshot(json_request(
            "POST",
            "/verification-task-lookups",
            handle_request(),
        ))
        .await
        .unwrap();
    let polled_body = response_json(polled).await;
    assert_eq!(polled_body["status"], "failed");
    assert!(
        polled_body["failure_message"]
            .as_str()
            .is_some_and(|message| message.contains("entitlement not satisfied"))
    );
}

#[tokio::test]
async fn completed_entitlement_can_issue_access_credential() {
    let state = test_state();
    let content_lock = content_lock(true);
    seed_content_lock(&state, content_lock.clone()).await;
    let app = router(state.clone());
    submit_task(&state, &app, submitted_proof_bundle_for(&content_lock)).await;
    complete_task(&app).await;

    let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/access-credentials",
                json!({ "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy", "bundle_id": BUNDLE_ID }),
            ))
            .await
            .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert!(
        body["credential"]
            .as_str()
            .is_some_and(|credential| !credential.is_empty())
    );
    assert!(body["expires_at"].as_str().is_some());
    assert_no_keys(&body, &["task_id", "creator", "bundle_id"]);

    let polled = app
        .oneshot(json_request(
            "POST",
            "/verification-task-lookups",
            handle_request(),
        ))
        .await
        .unwrap();
    let polled_body = response_json(polled).await;
    assert_no_keys(
        &polled_body,
        &["task_id", "credential", "credential_issuance"],
    );
}

#[tokio::test]
async fn access_credential_request_rejects_task_id_shortcut() {
    let app = router(test_state());

    let response = app
        .oneshot(json_request(
            "POST",
            "/access-credentials",
            json!({ "task_id": "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10" }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn missing_entitlement_cannot_issue_access_credential() {
    let app = router(test_state());

    let response = app
            .oneshot(json_request(
                "POST",
                "/access-credentials",
                json!({ "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy", "bundle_id": BUNDLE_ID }),
            ))
            .await
            .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "entitlement_not_authorized");
}

#[tokio::test]
async fn proxy_read_with_valid_bearer_credential_returns_raw_guarded_resource_bytes() {
    let state = test_state();
    let content_lock = content_lock(true);
    seed_content_lock(&state, content_lock.clone()).await;
    seed_guarded_resource(&state, &content_lock, b"guarded bytes".to_vec()).await;
    let app = router(state.clone());
    submit_task(&state, &app, submitted_proof_bundle_for(&content_lock)).await;
    complete_task(&app).await;
    let credential = issue_credential(&app).await;

    let response = app
        .oneshot(auth_request(
            "GET",
            "/priv-resources/content/hello.txt",
            &format!("Bearer {credential}"),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "text/plain");
    assert_eq!(response.headers()[header::CONTENT_LENGTH], "13");
    let expected_hash = serde_json::to_value(content_lock.primary_resource.as_ref().unwrap().hash)
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        response.headers()[header::ETAG],
        format!("\"{expected_hash}\"")
    );
    assert!(
        response
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .is_none()
    );
    assert_eq!(response_bytes(response).await, b"guarded bytes".to_vec());
}

#[tokio::test]
async fn proxy_read_accepts_bearer_scheme_case_insensitively() {
    let state = test_state();
    let content_lock = content_lock(true);
    seed_content_lock(&state, content_lock.clone()).await;
    seed_guarded_resource(&state, &content_lock, b"case insensitive".to_vec()).await;
    let app = router(state.clone());
    submit_task(&state, &app, submitted_proof_bundle_for(&content_lock)).await;
    complete_task(&app).await;
    let credential = issue_credential(&app).await;

    let response = app
        .oneshot(auth_request(
            "GET",
            "/priv-resources/content/hello.txt",
            &format!("bearer {credential}"),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_bytes(response).await, b"case insensitive".to_vec());
}

#[test]
fn frontend_session_authorization_accepts_single_case_insensitive_bearer_header() {
    let token =
        parse_frontend_session_token(&authorization_headers(&["bearer frontend-session-token"]))
            .unwrap();

    assert_eq!(token.expose_token(), "frontend-session-token");
}

#[test]
fn frontend_session_authorization_rejects_missing_or_malformed_authorization_header() {
    for header_value in [
        None,
        Some(""),
        Some("Bearer"),
        Some("Bearer "),
        Some("Basic token"),
        Some("Bearer one two"),
    ] {
        let headers = match header_value {
            Some(value) => authorization_headers(&[value]),
            None => HeaderMap::new(),
        };
        let error = parse_frontend_session_token(&headers).unwrap_err();

        assert_eq!(error.status_code(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            error.error_response().error.code,
            "frontend_session_unavailable",
            "header: {header_value:?}"
        );
    }
}

#[test]
fn frontend_session_authorization_rejects_multiple_authorization_headers() {
    let error = parse_frontend_session_token(&authorization_headers(&[
        "Bearer frontend-session-token",
        "Bearer another-token",
    ]))
    .unwrap_err();

    assert_eq!(error.status_code(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        error.error_response().error.code,
        "frontend_session_unavailable"
    );
}

#[test]
fn frontend_session_authorization_errors_do_not_expose_raw_token_values() {
    let error = parse_frontend_session_token(&authorization_headers(&[
        "Bearer frontend-session-token",
        "Bearer another-token",
    ]))
    .unwrap_err();
    let body = serde_json::to_value(error.error_response()).unwrap();

    assert!(!body.to_string().contains("frontend-session-token"));
    assert!(!body.to_string().contains("another-token"));
}

#[tokio::test]
async fn paykit_setup_status_route_derives_creator_and_projects_all_statuses() {
    for (status, wire_status) in [
        (PaykitSetupStatusKind::Ready, "ready"),
        (PaykitSetupStatusKind::SetupRequired, "setup_required"),
        (PaykitSetupStatusKind::Unavailable, "unavailable"),
    ] {
        let provider = Arc::new(RecordingPaykitSetupStatusProvider::status(status));
        let state = test_state().with_paykit_setup_status_provider(Some(provider.clone()));
        seed_frontend_session(&state, "frontend-session-token", creator()).await;

        let response = router(state)
            .oneshot(auth_request(
                "GET",
                "/creator/paykit/setup-status",
                "Bearer frontend-session-token",
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body, json!({ "status": wire_status }));
        assert_eq!(provider.creators(), vec![creator()]);
    }
}

#[tokio::test]
async fn paykit_setup_status_route_uses_creator_from_the_presented_session() {
    let provider = Arc::new(RecordingPaykitSetupStatusProvider::status(
        PaykitSetupStatusKind::Ready,
    ));
    let state = test_state().with_paykit_setup_status_provider(Some(provider.clone()));
    seed_frontend_session(&state, "other-session-token", other_creator()).await;

    let response = router(state)
        .oneshot(auth_request(
            "GET",
            "/creator/paykit/setup-status",
            "Bearer other-session-token",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(provider.creators(), vec![other_creator()]);
}

#[tokio::test]
async fn paykit_setup_status_route_projects_missing_config_and_upstream_failures_as_unavailable() {
    let state = test_state();
    seed_frontend_session(&state, "frontend-session-token", creator()).await;
    let response = router(state)
        .oneshot(auth_request(
            "GET",
            "/creator/paykit/setup-status",
            "Bearer frontend-session-token",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_json(response).await,
        json!({ "status": "unavailable" })
    );

    for error in [
        PaykitSetupStatusProviderError::Timeout,
        PaykitSetupStatusProviderError::NonSuccess,
        PaykitSetupStatusProviderError::InvalidResponse,
        PaykitSetupStatusProviderError::Unavailable,
    ] {
        let provider = Arc::new(RecordingPaykitSetupStatusProvider::failure(error));
        let state = test_state().with_paykit_setup_status_provider(Some(provider));
        seed_frontend_session(&state, "frontend-session-token", creator()).await;

        let response = router(state)
            .oneshot(auth_request(
                "GET",
                "/creator/paykit/setup-status",
                "Bearer frontend-session-token",
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response_json(response).await,
            json!({ "status": "unavailable" })
        );
    }
}

#[tokio::test]
async fn paykit_setup_status_route_rejects_query_and_body_after_authentication() {
    let provider = Arc::new(RecordingPaykitSetupStatusProvider::status(
        PaykitSetupStatusKind::Ready,
    ));
    let state = test_state().with_paykit_setup_status_provider(Some(provider.clone()));
    seed_frontend_session(&state, "frontend-session-token", creator()).await;
    let app = router(state);

    for request in [
        auth_request(
            "GET",
            &format!("/creator/paykit/setup-status?creator={}", other_creator()),
            "Bearer frontend-session-token",
        ),
        Request::builder()
            .method("GET")
            .uri("/creator/paykit/setup-status")
            .header(header::AUTHORIZATION, "Bearer frontend-session-token")
            .body(Body::from("{}"))
            .unwrap(),
    ] {
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(response).await["error"]["code"],
            "invalid_request"
        );
    }
    assert!(provider.creators().is_empty());
}

#[tokio::test]
async fn paykit_setup_status_route_preserves_frontend_session_auth_errors() {
    for authorization in [None, Some("Basic token"), Some("Bearer missing-token")] {
        let request = match authorization {
            Some(value) => auth_request("GET", "/creator/paykit/setup-status", value),
            None => empty_request("GET", "/creator/paykit/setup-status"),
        };
        let response = router(test_state()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response_json(response).await["error"]["code"],
            "frontend_session_unavailable"
        );
    }

    let state = test_state();
    seed_expired_frontend_session(&state, "expired-token", creator()).await;
    let response = router(state)
        .oneshot(auth_request(
            "GET",
            "/creator/paykit/setup-status",
            "Bearer expired-token",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response_json(response).await["error"]["code"],
        "frontend_session_expired"
    );
}

#[tokio::test]
async fn paykit_setup_status_route_authenticates_before_reading_an_oversized_body() {
    let response = router(test_state_with_runtime(
        RuntimeEnvironment::Production,
        false,
    ))
    .oneshot(
        Request::builder()
            .method("GET")
            .uri("/creator/paykit/setup-status")
            .body(Body::from(vec![0_u8; 2 * 1024 * 1024 + 1]))
            .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response_json(response).await["error"]["code"],
        "frontend_session_unavailable"
    );
}

#[test]
fn connect_shell_return_to_accepts_configured_origin() {
    let allowed = vec!["https://pubky.app".to_owned()];

    let return_to =
        validate_return_to_url("https://pubky.app/locks/callback?next=/reader", &allowed).unwrap();

    assert_eq!(return_to, "https://pubky.app/locks/callback?next=/reader");
}

#[test]
fn connect_shell_return_to_accepts_wildcard_policy() {
    let allowed = vec!["*".to_owned()];

    let return_to = validate_return_to_url(
        "https://creator.example/callback?state=from-caller",
        &allowed,
    )
    .unwrap();

    assert_eq!(
        return_to,
        "https://creator.example/callback?state=from-caller"
    );
}

#[test]
fn connect_shell_return_to_accepts_localhost_configured_origin() {
    let allowed = vec!["http://localhost:3000".to_owned()];

    let return_to =
        validate_return_to_url("http://localhost:3000/locks/callback", &allowed).unwrap();

    assert_eq!(return_to, "http://localhost:3000/locks/callback");
}

#[test]
fn connect_shell_return_to_rejects_empty_allowlist_and_unconfigured_origin() {
    for (return_to, allowed) in [
        ("https://pubky.app/locks/callback", vec![]),
        (
            "https://evil.example/callback?code=steal-me",
            vec!["https://pubky.app".to_owned()],
        ),
    ] {
        let error = validate_return_to_url(return_to, &allowed).unwrap_err();

        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(error.error_response().error.code, "invalid_request");
        assert!(
            !serde_json::to_string(&error.error_response())
                .unwrap()
                .contains(return_to)
        );
    }
}

#[test]
fn connect_shell_return_to_rejects_malformed_and_non_http_urls() {
    for return_to in [
        "not a url",
        "/locks/callback",
        "//evil.example/callback",
        "pubky://app/callback",
        "mailto:creator@example.com",
        "https:///missing-host",
    ] {
        let error = validate_return_to_url(return_to, &["*".to_owned()]).unwrap_err();

        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(error.error_response().error.code, "invalid_request");
        assert!(
            !serde_json::to_string(&error.error_response())
                .unwrap()
                .contains(return_to)
        );
    }
}

#[tokio::test]
async fn proxy_read_rejects_missing_or_malformed_authorization_header() {
    let app = router(test_state());

    for header_value in [
        None,
        Some(""),
        Some("Bearer"),
        Some("Bearer "),
        Some("Basic token"),
        Some("Bearer one two"),
    ] {
        let response = match header_value {
            Some(value) => app
                .clone()
                .oneshot(auth_request(
                    "GET",
                    "/priv-resources/content/hello.txt",
                    value,
                ))
                .await
                .unwrap(),
            None => app
                .clone()
                .oneshot(empty_request("GET", "/priv-resources/content/hello.txt"))
                .await
                .unwrap(),
        };
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "header: {header_value:?}"
        );
        let body = response_json(response).await;
        assert_eq!(body["error"]["code"], "invalid_access_credential");
    }
}

#[tokio::test]
async fn proxy_read_rejects_multiple_authorization_headers() {
    let state = test_state();
    let content_lock = content_lock(true);
    seed_content_lock(&state, content_lock.clone()).await;
    seed_guarded_resource(&state, &content_lock, b"guarded bytes".to_vec()).await;
    let app = router(state.clone());
    submit_task(&state, &app, submitted_proof_bundle_for(&content_lock)).await;
    complete_task(&app).await;
    let credential = issue_credential(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/priv-resources/content/hello.txt")
                .header(header::AUTHORIZATION, format!("Bearer {credential}"))
                .header(header::AUTHORIZATION, "Bearer another-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "invalid_access_credential");
}

#[tokio::test]
async fn proxy_read_rejects_query_credential_even_with_valid_authorization_header() {
    let state = test_state();
    let content_lock = content_lock(true);
    seed_content_lock(&state, content_lock.clone()).await;
    seed_guarded_resource(&state, &content_lock, b"guarded bytes".to_vec()).await;
    let app = router(state.clone());
    submit_task(&state, &app, submitted_proof_bundle_for(&content_lock)).await;
    complete_task(&app).await;
    let credential = issue_credential(&app).await;

    let response = app
        .oneshot(auth_request(
            "GET",
            "/priv-resources/content/hello.txt?credential=query-secret",
            &format!("Bearer {credential}"),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "invalid_access_credential");
}

#[tokio::test]
async fn proxy_read_rejects_credentials_outside_authorization_header() {
    let app = router(test_state());

    let query_response = app
        .clone()
        .oneshot(empty_request(
            "GET",
            "/priv-resources/content/hello.txt?credential=secret",
        ))
        .await
        .unwrap();
    assert_eq!(query_response.status(), StatusCode::UNAUTHORIZED);

    let body_response = app
        .oneshot(json_request(
            "GET",
            "/priv-resources/content/hello.txt",
            json!({ "credential": "secret" }),
        ))
        .await
        .unwrap();
    assert_eq!(body_response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn creator_publishing_contract_fixtures_register_guarded_resource() {
    let expected = fixture_value("register_guarded_resource_response_shape.json");
    let state = test_state_with_creator_publishing(true);
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);

    let response = app
        .oneshot(authenticated_raw_upload_request(
            "/creator/priv-resources/content/example.txt",
            Some("text/plain"),
            b"guarded bytes".to_vec(),
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["creator"], expected["creator"]);
    assert_eq!(
        body["guarded_resource"]["path"],
        expected["guarded_resource"]["path"]
    );
    assert_eq!(
        body["guarded_resource"]["content_type"],
        expected["guarded_resource"]["content_type"]
    );
    assert_eq!(
        body["guarded_resource"]["size"],
        expected["guarded_resource"]["size"]
    );
    assert_placeholder_string(
        &body["guarded_resource"]["hash"],
        expected["guarded_resource"]["hash"].as_str().unwrap(),
    );
}

#[tokio::test]
async fn creator_publishing_contract_fixtures_create_content_lock() {
    let request = fixture_value("create_content_lock_request.json");
    serde_json::from_value::<AuthenticatedCreateContentLockHttpRequest>(request.clone()).unwrap();
    let expected = fixture_value("create_content_lock_response_shape.json");
    let state = test_state_with_creator_publishing(true)
        .with_clock(Arc::new(FixedClock(datetime!(2026-06-03 12:00:00 UTC))));
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    seed_guarded_resource_from_fixture(&state, &request).await;
    let app = router(state);

    let response = app
        .oneshot(json_request("POST", "/creator/content-locks", request))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_placeholder_string(&body["lock_id"], expected["lock_id"].as_str().unwrap());
    let lock_id = body["lock_id"].as_str().unwrap();
    assert_eq!(
        body["content_lock_path"],
        format!("/pub/locks.app/{lock_id}.json")
    );
    assert_eq!(
        body["content_lock"]["version"],
        expected["content_lock"]["version"]
    );
    assert_eq!(
        body["content_lock"]["creator"],
        expected["content_lock"]["creator"]
    );
    assert_eq!(
        body["content_lock"]["primary_resource"],
        expected["content_lock"]["primary_resource"]
    );
    assert_eq!(
        body["content_lock"]["criteria"],
        expected["content_lock"]["criteria"]
    );
    assert_eq!(
        body["content_lock"]["lock_logic"],
        expected["content_lock"]["lock_logic"]
    );
    assert_eq!(
        body["content_lock"]["access_policy"],
        expected["content_lock"]["access_policy"]
    );
    assert_eq!(
        body["content_lock"]["lock_server"],
        expected["content_lock"]["lock_server"]
    );
    assert_placeholder_string(
        &body["content_lock"]["created_at"],
        expected["content_lock"]["created_at"].as_str().unwrap(),
    );
}

#[tokio::test]
async fn creator_publishing_contract_fixtures_set_lock_service_config() {
    let request = fixture_value("set_lock_service_config_request.json");
    serde_json::from_value::<AuthenticatedSetLockServicePointerHttpRequest>(request.clone())
        .unwrap();
    let expected = fixture_value("set_lock_service_config_response_shape.json");
    let state = test_state_with_creator_publishing(true)
        .with_clock(Arc::new(FixedClock(datetime!(2026-06-03 00:00:00 UTC))));
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);

    let response = app
        .oneshot(json_request(
            "POST",
            "/creator/lock-service-config",
            request,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["creator"], expected["creator"]);
    assert_eq!(body["path"], expected["path"]);
    assert_eq!(
        body["lock_service_pointer"]["version"],
        expected["lock_service_pointer"]["version"]
    );
    assert_eq!(
        body["lock_service_pointer"]["default_lock_server"],
        expected["lock_service_pointer"]["default_lock_server"]
    );
    assert_placeholder_string(
        &body["lock_service_pointer"]["created_at"],
        expected["lock_service_pointer"]["created_at"]
            .as_str()
            .unwrap(),
    );
}

#[tokio::test]
async fn creator_guarded_resources_route_is_not_mounted_when_disabled() {
    let app = router(test_state());

    let response = app
        .oneshot(json_request(
            "POST",
            "/creator/priv-resources",
            legacy_json_guarded_resource_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn creator_guarded_resources_route_is_not_mounted_in_production_shape() {
    let config = test_config(RuntimeEnvironment::Production, false);
    let app = router(AppState::new_empty_in_memory(config));

    let response = app
        .oneshot(json_request(
            "POST",
            "/creator/priv-resources",
            legacy_json_guarded_resource_payload(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn creator_guarded_resources_registers_resource_when_enabled() {
    let state = test_state_with_creator_publishing(true);
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state.clone());

    let response = app
        .oneshot(authenticated_raw_upload_request(
            "/creator/priv-resources/content/example.txt",
            Some("text/plain"),
            b"guarded bytes".to_vec(),
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
    assert_eq!(
        body["guarded_resource"]["path"],
        "/priv/locks.app/content/example.txt"
    );
    assert_eq!(body["guarded_resource"]["content_type"], "text/plain");
    assert_eq!(body["guarded_resource"]["size"], 13);
    assert!(body["guarded_resource"]["hash"].as_str().unwrap().len() >= 52);
    assert!(body.get("bytes").is_none());
    assert!(body.get("content_base64").is_none());

    let stored = state
        .guarded_resources()
        .get_current_guarded_resource(&creator(), "/priv/locks.app/content/example.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.content_type, "text/plain");
    assert_eq!(stored.size, 13);
    assert_eq!(stored.bytes, b"guarded bytes".to_vec());
}

#[tokio::test]
async fn creator_guarded_resources_missing_content_type_returns_stable_error() {
    let state = test_state_with_creator_publishing(true);
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);

    let response = app
        .oneshot(authenticated_raw_upload_request(
            "/creator/priv-resources/content/example.txt",
            None,
            b"guarded bytes".to_vec(),
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    let body = assert_error_response(response, StatusCode::BAD_REQUEST, "invalid_request").await;
    assert_eq!(body["error"]["message"], "missing content-type");
}

#[tokio::test]
async fn creator_guarded_resources_invalid_path_returns_stable_error() {
    let state = test_state_with_creator_publishing(true);
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);

    for uri in [
        "/creator/priv-resources/content/%2E%2E/secret.txt",
        "/creator/priv-resources/content/images//example.txt",
        "/creator/priv-resources/content/https://evil.example/x",
    ] {
        let response = app
            .clone()
            .oneshot(authenticated_raw_upload_request(
                uri,
                Some("text/plain"),
                b"guarded bytes".to_vec(),
                "frontend-session-secret",
            ))
            .await
            .unwrap();

        assert_error_response(response, StatusCode::BAD_REQUEST, "invalid_request").await;
    }
}

#[tokio::test]
async fn creator_guarded_resources_invalid_mime_returns_stable_error() {
    let state = test_state_with_creator_publishing(true);
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);
    let response = app
        .oneshot(authenticated_raw_upload_request(
            "/creator/priv-resources/content/example.txt",
            Some("not mime"),
            b"guarded bytes".to_vec(),
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn creator_guarded_resources_empty_content_returns_stable_error() {
    let state = test_state_with_creator_publishing(true);
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);

    let response = app
        .oneshot(authenticated_raw_upload_request(
            "/creator/priv-resources/content/example.txt",
            Some("text/plain"),
            Vec::new(),
            "frontend-session-secret",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn creator_content_locks_route_requires_frontend_session() {
    let app = router(test_state());

    let response = app
        .oneshot(json_request(
            "POST",
            "/creator/content-locks",
            creator_content_lock_request(registered_guarded_resource_json()),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn creator_content_locks_create_lock_for_registered_guarded_resource() {
    let state = test_state_with_creator_publishing(true);
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state.clone());
    let guarded_resource = register_guarded_resource_through_creator_route(&app).await;

    let response = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/creator/content-locks",
            creator_content_lock_request(guarded_resource.clone()),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let lock_id = body["lock_id"].as_str().unwrap();
    let path = body["content_lock_path"].as_str().unwrap();
    assert!(path.starts_with("/pub/locks.app/"));
    assert!(path.ends_with(".json"));
    assert!(path.contains(lock_id));
    assert_eq!(
        body["content_lock"]["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
    assert_eq!(body["content_lock"]["primary_resource"], guarded_resource);
    assert!(body.get("bytes").is_none());
    assert!(body.get("content_base64").is_none());
    assert!(body["content_lock"].get("bytes").is_none());
    assert!(body["content_lock"].get("content_base64").is_none());

    let stored = state
        .content_locks()
        .get_content_lock(&creator(), &path.parse().unwrap())
        .await
        .unwrap();
    assert!(stored.is_some());
}

#[tokio::test]
async fn creator_content_locks_missing_guarded_resource_returns_stable_error() {
    let state = test_state_with_creator_publishing(true);
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);

    let response = app
        .oneshot(json_request(
            "POST",
            "/creator/content-locks",
            creator_content_lock_request(registered_guarded_resource_json()),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "guarded_resource_not_found");
}

#[tokio::test]
async fn creator_content_locks_stale_guarded_resource_returns_stable_error() {
    let state = test_state_with_creator_publishing(true);
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);
    let mut guarded_resource = register_guarded_resource_through_creator_route(&app).await;
    guarded_resource["size"] = json!(guarded_resource["size"].as_u64().unwrap() + 1);

    let response = app
        .oneshot(json_request(
            "POST",
            "/creator/content-locks",
            creator_content_lock_request(guarded_resource),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn creator_content_locks_identical_request_returns_same_lock_id_and_path() {
    let state = test_state_with_creator_publishing(true)
        .with_clock(Arc::new(FixedClock(datetime!(2026-06-03 12:00:00 UTC))));
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);
    let guarded_resource = register_guarded_resource_through_creator_route(&app).await;
    let request = creator_content_lock_request(guarded_resource);

    let first = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/creator/content-locks",
            request.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = response_json(first).await;

    let second = app
        .oneshot(json_request("POST", "/creator/content-locks", request))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = response_json(second).await;

    assert_eq!(second_body["lock_id"], first_body["lock_id"]);
    assert_eq!(
        second_body["content_lock_path"],
        first_body["content_lock_path"]
    );
    assert_eq!(second_body["content_lock"], first_body["content_lock"]);
}

#[tokio::test]
async fn creator_lock_service_config_route_requires_frontend_session() {
    let app = router(test_state());

    let response = app
        .oneshot(json_request(
            "POST",
            "/creator/lock-service-config",
            creator_lock_service_config_request(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn creator_authority_status_route_requires_frontend_session_bearer() {
    let app = router(test_state());

    let response = app
        .oneshot(empty_request("GET", "/creator/authority-status"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "frontend_session_unavailable");
    assert!(!body.to_string().contains("frontend-session-token"));
}

#[tokio::test]
async fn creator_authority_status_route_derives_creator_from_frontend_session() {
    let state = test_state();
    seed_frontend_session(&state, "frontend-session-token", other_creator()).await;
    let app = router(state);

    let response = app
        .oneshot(auth_request(
            "GET",
            "/creator/authority-status",
            "Bearer frontend-session-token",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["creator"],
        "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky"
    );
    assert_eq!(body["authorized"], false);
    assert_eq!(body["auth_kind"], Value::Null);
    assert_eq!(body["granted_scopes"], json!([]));
    assert_eq!(body["session_expires_at"], Value::Null);
}

#[tokio::test]
async fn creator_authority_status_route_returns_missing_status_without_secret_fields() {
    let state = test_state();
    seed_frontend_session(&state, "frontend-session-token", creator()).await;
    let app = router(state);

    let response = app
        .oneshot(auth_request(
            "GET",
            "/creator/authority-status",
            "Bearer frontend-session-token",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
    assert_eq!(body["authorized"], false);
    assert_eq!(body["auth_kind"], Value::Null);
    assert_eq!(body["granted_scopes"], json!([]));
    assert_eq!(body["session_expires_at"], Value::Null);
    assert_no_keys(
        &body,
        &[
            "secret",
            "token",
            "frontend_session_token",
            "authorization_url",
            "code",
            "database_url",
        ],
    );
}

#[tokio::test]
async fn creator_authority_status_route_returns_authorized_status_without_secret_fields() {
    let state = test_state();
    seed_frontend_session(&state, "frontend-session-token", creator()).await;
    seed_creator_authority(&state).await;
    let app = router(state);

    let response = app
        .oneshot(auth_request(
            "GET",
            "/creator/authority-status",
            "Bearer frontend-session-token",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
    assert_eq!(body["authorized"], true);
    assert_eq!(body["auth_kind"], "legacy_cookie");
    assert_eq!(
        body["granted_scopes"],
        json!(["/pub/locks.app/:rw", "/priv/locks.app/:rw"])
    );
    assert_eq!(body["session_expires_at"], Value::Null);
    assert!(!body.to_string().contains("creator-authority-secret"));
    assert_no_keys(
        &body,
        &[
            "secret",
            "token",
            "frontend_session_token",
            "authorization_url",
            "code",
            "database_url",
        ],
    );
}

#[tokio::test]
async fn frontend_session_signout_revokes_current_session() {
    let mut config = test_config(RuntimeEnvironment::Production, false);
    config.creator_authority_acquisition.enabled = true;
    let state = AppState::new_empty_in_memory(config);
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state);

    let signout = app
        .clone()
        .oneshot(auth_request(
            "DELETE",
            "/frontend-sessions/current",
            "Bearer frontend-session-secret",
        ))
        .await
        .unwrap();

    assert_eq!(signout.status(), StatusCode::NO_CONTENT);

    let after_signout = app
        .oneshot(auth_request(
            "GET",
            "/creator/authority-status",
            "Bearer frontend-session-secret",
        ))
        .await
        .unwrap();

    let body = assert_error_response(
        after_signout,
        StatusCode::UNAUTHORIZED,
        "frontend_session_unavailable",
    )
    .await;
    assert!(!body.to_string().contains("frontend-session-secret"));
}

#[tokio::test]
async fn frontend_session_signout_requires_bearer_token() {
    let mut config = test_config(RuntimeEnvironment::Production, false);
    config.creator_authority_acquisition.enabled = true;
    let app = router(AppState::new_empty_in_memory(config));

    let response = app
        .oneshot(empty_request("DELETE", "/frontend-sessions/current"))
        .await
        .unwrap();

    let body = assert_error_response(
        response,
        StatusCode::UNAUTHORIZED,
        "frontend_session_unavailable",
    )
    .await;
    assert!(!body.to_string().contains("Authorization"));
}

#[tokio::test]
async fn creator_connect_flow_routes_are_not_mounted_when_disabled() {
    for (method, path, body) in [
        (
            "GET",
            "/connect?return_to=https%3A%2F%2Fpubky.app%2Flocks%2Fconnected&state=opaque-state",
            None,
        ),
        ("POST", "/connect/flow-123/complete", None),
        (
            "POST",
            "/creator/connect-flows",
            Some(
                json!({"return_to": "https://pubky.app/locks/connected", "state": "opaque-state"}),
            ),
        ),
        (
            "POST",
            "/creator/connect-flows/flow-123/completions",
            Some(json!({})),
        ),
        (
            "POST",
            "/frontend-sessions",
            Some(json!({"code": "one-time-code", "state": "opaque-state"})),
        ),
    ] {
        let request = match body {
            Some(body) => json_request(method, path, body),
            None => empty_request(method, path),
        };
        let response = router(test_state_with_creator_connect(false))
            .oneshot(request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {path}");
    }
}

#[tokio::test]
async fn connect_shell_rejects_unallowlisted_return_to_without_starting_flow() {
    let client = Arc::new(CountingLegacyConnectFlowClient::default());
    let state =
        test_state_with_creator_connect_return_origins(vec!["https://pubky.app".to_owned()])
            .with_legacy_creator_connect_flow_client(client.clone());

    let response = router(state)
        .oneshot(empty_request(
            "GET",
            "/connect?return_to=https%3A%2F%2Fevil.example%2Fcallback&state=opaque-state",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "invalid_request");
    assert!(!body.to_string().contains("evil.example"));
    assert_eq!(client.start_call_count(), 0);
}

#[tokio::test]
async fn connect_shell_starts_flow_and_renders_auth_url_only_on_lock_server_origin() {
    let client = Arc::new(CountingLegacyConnectFlowClient::default());
    let state =
        test_state_with_creator_connect_return_origins(vec!["https://pubky.app".to_owned()])
            .with_legacy_creator_connect_flow_client(client.clone());
    let response = router(state)
            .oneshot(empty_request(
                "GET",
                "/connect?return_to=https%3A%2F%2Fpubky.app%2Flocks%2Fconnected&state=opaque-%3Cstate%3E",
            ))
            .await
            .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/html; charset=utf-8"
    );
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("Enable Locks"));
    assert!(body.contains("pubkyauth://fake-secret-flow-url"));
    assert!(body.contains("data-testid=\"pubky-auth-qr\""));
    assert!(body.contains("<svg"));
    assert!(body.contains("aria-label=\"Pubky authorization QR code\""));
    assert!(body.contains("/connect/"));
    assert!(body.contains("/complete"));
    // `state` is retained server-side on the pending flow and echoed back only on the completion
    // redirect (see `connect_shell_completion_redirects_to_return_to_with_state_and_code_only`),
    // so the shell must not carry it in any form — raw or HTML-escaped.
    assert!(!body.contains("opaque-"));
    assert!(!body.contains("session_secret"));
    assert!(!body.contains("frontend_session_token"));
    assert!(!body.contains("one-time-code"));
    assert_eq!(client.start_call_count(), 1);
}

#[test]
fn connect_shell_escape_html_escapes_interpolated_text() {
    assert_eq!(escape_html("<&>\"'"), "&lt;&amp;&gt;&quot;&#x27;");
}

#[tokio::test]
async fn connect_shell_completion_redirects_to_return_to_with_state_and_code_only() {
    let state =
        test_state_with_creator_connect_return_origins(vec!["https://pubky.app".to_owned()])
            .with_legacy_creator_connect_flow_client(Arc::new(
                CountingLegacyConnectFlowClient::default(),
            ));
    seed_pending_creator_connect_flow(
        &state,
        "flow-redirect",
        "https://pubky.app/locks/connected?existing=1",
        "opaque value&x=%",
    )
    .await;

    let response = router(state)
        .oneshot(empty_request("POST", "/connect/flow-redirect/complete"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let location = response
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(location.starts_with(
        "https://pubky.app/locks/connected?existing=1&state=opaque%20value%26x%3D%25&code="
    ));
    assert!(!location.contains("authorization_url"));
    assert!(!location.contains("pubkyauth"));
    assert!(!location.contains("<svg"));
    assert!(!location.contains("session_secret"));
    assert!(!location.contains("frontend_session_token"));
}

#[tokio::test]
async fn connect_shell_completion_revalidates_return_to_allowlist_before_redirect() {
    let state =
        test_state_with_creator_connect_return_origins(vec!["https://pubky.app".to_owned()])
            .with_legacy_creator_connect_flow_client(Arc::new(
                CountingLegacyConnectFlowClient::default(),
            ));
    seed_pending_creator_connect_flow(
        &state,
        "flow-evil-return",
        "https://evil.example/steal",
        "opaque-state",
    )
    .await;

    let response = router(state)
        .oneshot(empty_request("POST", "/connect/flow-evil-return/complete"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "invalid_request");
    assert!(!body.to_string().contains("evil.example"));
}

#[tokio::test]
async fn connect_shell_completion_redirect_does_not_include_auth_url_or_session_secret() {
    let state = test_state_with_creator_connect_return_origins(vec!["*".to_owned()])
        .with_legacy_creator_connect_flow_client(Arc::new(
            CountingLegacyConnectFlowClient::default(),
        ));
    seed_pending_creator_connect_flow(
        &state,
        "flow-secret-free-location",
        "https://creator.example/callback",
        "opaque-state",
    )
    .await;

    let response = router(state)
        .oneshot(empty_request(
            "POST",
            "/connect/flow-secret-free-location/complete",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let location = response
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(location.matches("state=").count(), 1);
    assert_eq!(location.matches("code=").count(), 1);
    assert!(!location.contains("authorization_url"));
    assert!(!location.contains("pubkyauth"));
    assert!(!location.contains("session-secret"));
    assert!(!location.contains("frontend_session_token"));
}

#[tokio::test]
async fn self_relay_acquisition_routes_are_not_mounted() {
    let mut config = test_config(RuntimeEnvironment::Production, false);
    config.creator_authority_acquisition.enabled = true;
    config.creator_authority_acquisition.method = CreatorAuthorityAcquisitionMethod::LegacyConnect;

    let response = router(AppState::new_empty_in_memory(config))
        .oneshot(json_request(
            "POST",
            "/creator-authority/acquisitions",
            json!({}),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn raw_creator_connect_flow_routes_are_never_mounted() {
    let mut config = test_config(RuntimeEnvironment::Production, false);
    config.creator_authority_acquisition = CreatorAuthorityAcquisitionConfig {
        enabled: true,
        method: CreatorAuthorityAcquisitionMethod::LegacyConnect,
        frontend_session_ttl_seconds: 86_400,
        frontend_session_code_ttl_seconds: 120,
        legacy_connect: crate::config::LegacyConnectAcquisitionConfig {
            allowed_return_origins: Vec::new(),
        },
    };
    let app = router(AppState::new_empty_in_memory(config));

    for (method, path, body) in [
        (
            "POST",
            "/creator/connect-flows",
            Some(
                json!({"return_to": "https://pubky.app/locks/connected", "state": "opaque-state"}),
            ),
        ),
        (
            "POST",
            "/creator/connect-flows/flow-123/completions",
            Some(json!({})),
        ),
    ] {
        let response = app
            .clone()
            .oneshot(json_request(method, path, body.unwrap()))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {path}");
    }
}

#[tokio::test]
async fn raw_creator_connect_flow_routes_are_not_mounted_when_enabled_in_dev() {
    let response = router(test_state_with_creator_connect(true))
        .oneshot(json_request(
            "POST",
            "/creator/connect-flows",
            json!({"return_to": "https://pubky.app/locks/connected", "state": "opaque-state"}),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn creator_lock_service_config_sets_pointer_when_enabled() {
    let state = test_state_with_creator_publishing(true)
        .with_clock(Arc::new(FixedClock(datetime!(2026-06-03 00:00:00 UTC))));
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state.clone());

    let response = app
        .oneshot(json_request(
            "POST",
            "/creator/lock-service-config",
            creator_lock_service_config_request(),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["creator"],
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
    );
    assert_eq!(body["path"], "/pub/locks.app/config.json");
    assert_eq!(body["lock_service_pointer"]["version"], 1);
    assert_eq!(
        body["lock_service_pointer"]["default_lock_server"],
        "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
    );
    assert_eq!(
        body["lock_service_pointer"]["created_at"],
        "2026-06-03T00:00:00Z"
    );
    assert!(body.get("task_id").is_none());
    assert!(body.get("secret").is_none());
    assert!(body.get("session").is_none());

    let stored = state
        .lock_service_pointers()
        .get_lock_service_pointer(&creator())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.default_lock_server.to_string(),
        "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
    );
}

#[tokio::test]
async fn creator_lock_service_config_replaces_pointer_when_enabled() {
    let state = test_state_with_creator_publishing(true)
        .with_clock(Arc::new(FixedClock(datetime!(2026-06-03 00:00:00 UTC))));
    seed_frontend_session(&state, "frontend-session-secret", creator()).await;
    let app = router(state.clone());

    let first = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/creator/lock-service-config",
            creator_lock_service_config_request(),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let replacement = app
        .oneshot(json_request(
            "POST",
            "/creator/lock-service-config",
            json!({
                "default_lock_server": "pubky3kj4afafdba8diu5oxd96dz6orrqt5nfgbmi473go6ju8s64z36y"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(replacement.status(), StatusCode::OK);
    let body = response_json(replacement).await;
    assert_eq!(
        body["lock_service_pointer"]["default_lock_server"],
        "pubky3kj4afafdba8diu5oxd96dz6orrqt5nfgbmi473go6ju8s64z36y"
    );

    let stored = state
        .lock_service_pointers()
        .get_lock_service_pointer(&creator())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.default_lock_server.to_string(),
        "pubky3kj4afafdba8diu5oxd96dz6orrqt5nfgbmi473go6ju8s64z36y"
    );
}

#[tokio::test]
async fn proxy_read_missing_guarded_resource_returns_stable_error_envelope() {
    let state = test_state();
    let content_lock = content_lock(true);
    seed_content_lock(&state, content_lock.clone()).await;
    let app = router(state.clone());
    submit_task(&state, &app, submitted_proof_bundle_for(&content_lock)).await;
    complete_task(&app).await;
    let credential = issue_credential(&app).await;

    let response = app
        .oneshot(auth_request(
            "GET",
            "/priv-resources/content/hello.txt",
            &format!("Bearer {credential}"),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "guarded_resource_not_found");
}

fn test_state() -> AppState {
    test_state_with_runtime(RuntimeEnvironment::Development, true)
}

fn test_state_with_rate_limit(enabled: bool, max_requests: u32, window_seconds: u64) -> AppState {
    let mut config = test_config(RuntimeEnvironment::Development, true);
    config.rate_limits = RateLimitsConfig {
        verification_submission: VerificationSubmissionRateLimitConfig {
            enabled,
            max_requests,
            window_seconds,
        },
    };
    AppState::new_empty_in_memory(config)
}

fn test_state_with_runtime(
    environment: RuntimeEnvironment,
    expose_dev_completion_route: bool,
) -> AppState {
    let config = test_config(environment, expose_dev_completion_route);
    AppState::new_empty_in_memory_with_creator_repositories(
        config,
        Arc::new(InMemoryContentLockRepository::new()),
        Arc::new(InMemoryGuardedResourceRepository::new()),
        Arc::new(InMemoryLockServicePointerRepository::new()),
        Arc::new(InMemoryEntitlementRepository::new()),
    )
}

fn test_state_with_creator_publishing(_enabled: bool) -> AppState {
    test_state_with_creator_repository_backend(
        RuntimeEnvironment::Development,
        CreatorRepositoryBackend::PubkyHomeserver,
    )
}

fn test_state_with_creator_connect(expose_creator_connect_routes: bool) -> AppState {
    let mut config = test_config(RuntimeEnvironment::Development, true);
    config.creator_authority_acquisition.enabled = expose_creator_connect_routes;
    AppState::new_empty_in_memory(config)
}

fn test_state_with_creator_connect_return_origins(allowed_return_origins: Vec<String>) -> AppState {
    let mut config = test_config(RuntimeEnvironment::Development, true);
    config
        .creator_authority_acquisition
        .legacy_connect
        .allowed_return_origins = allowed_return_origins;
    AppState::new_empty_in_memory(config)
}

fn test_state_with_content_lock_limits(content_locks: ContentLocksConfig) -> AppState {
    let mut config = test_config(RuntimeEnvironment::Production, false);
    config.content_locks = content_locks;
    AppState::new_empty_in_memory_with_creator_repositories(
        config,
        Arc::new(InMemoryContentLockRepository::new()),
        Arc::new(InMemoryGuardedResourceRepository::new()),
        Arc::new(InMemoryLockServicePointerRepository::new()),
        Arc::new(InMemoryEntitlementRepository::new()),
    )
}

fn test_state_with_creator_repository_backend(
    environment: RuntimeEnvironment,
    _backend: CreatorRepositoryBackend,
) -> AppState {
    let config = test_config(environment, false);
    AppState::new_empty_in_memory_with_creator_repositories(
        config,
        Arc::new(InMemoryContentLockRepository::new()),
        Arc::new(InMemoryGuardedResourceRepository::new()),
        Arc::new(InMemoryLockServicePointerRepository::new()),
        Arc::new(InMemoryEntitlementRepository::new()),
    )
}

fn test_config(
    environment: RuntimeEnvironment,
    _expose_dev_completion_route: bool,
) -> LockServerRuntimeConfig {
    LockServerRuntimeConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        credentials: LockServerCredentialsConfig {
            lock_server_secret_key: PathBuf::from("/tmp/lock-server-test-secret.sess"),
            lock_server_public_key: LockServerPubky::from_str(
                "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            )
            .unwrap(),
            max_ttl_seconds: 900,
        },
        database: DatabaseConfig {
            url: "postgres://locks:locks@localhost/locks_test".to_owned(),
            max_connections: 10,
            run_migrations_on_startup: true,
        },
        worker: WorkerConfig {
            enabled: true,
            poll_interval_ms: 250,
            claim_timeout_seconds: 60,
            worker_id: "test-worker".to_owned(),
        },
        runtime: RuntimeConfig { environment },
        creator_authority_acquisition: CreatorAuthorityAcquisitionConfig::default(),
        secrets: SecretsConfig::default(),
        logging: LoggingConfig::default(),
        pubky: PubkyConfig::default(),
        pkdns: crate::config::PkdnsConfig::default(),
        rate_limits: RateLimitsConfig::default(),
        content_locks: ContentLocksConfig::default(),
        paykit: None,
    }
}

fn submitted_proof_bundle() -> SubmittedProofBundle {
    submitted_proof_bundle_for(&content_lock(true))
}

fn submitted_proof_bundle_with_bundle_id(bundle_id: &str) -> SubmittedProofBundle {
    submitted_proof_bundle_for_creator_and_bundle_id(creator(), bundle_id)
}

fn submitted_proof_bundle_for_creator_and_bundle_id(
    creator: CreatorPubky,
    bundle_id: &str,
) -> SubmittedProofBundle {
    let content_lock = content_lock_for_creator(creator.clone(), true);
    SubmittedProofBundle {
        version: SUBMITTED_PROOF_BUNDLE_VERSION,
        bundle_id: BundleId::from_str(bundle_id).unwrap(),
        pubky_lock_resource: PubkyLockResource::new(
            creator,
            content_lock.content_lock_path().unwrap(),
        ),
        reader_public_key: None,
        proofs: vec![Proof {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::DevStatic,
            payload: json!({ "satisfied": true }),
        }],
    }
}

fn submitted_proof_bundle_for(content_lock: &ContentLock) -> SubmittedProofBundle {
    SubmittedProofBundle {
        version: SUBMITTED_PROOF_BUNDLE_VERSION,
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: PubkyLockResource::new(
            creator(),
            content_lock.content_lock_path().unwrap(),
        ),
        reader_public_key: None,
        proofs: vec![Proof {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::DevStatic,
            payload: json!({ "satisfied": true }),
        }],
    }
}

async fn seed_content_lock(state: &AppState, content_lock: ContentLock) {
    state
        .content_locks()
        .upsert_content_lock(
            creator(),
            content_lock.content_lock_path().unwrap(),
            content_lock,
        )
        .await
        .unwrap();
}

async fn seed_guarded_resource(state: &AppState, content_lock: &ContentLock, bytes: Vec<u8>) {
    let guarded_resource = content_lock.primary_resource.as_ref().unwrap();
    state
        .guarded_resources()
        .upsert_guarded_resource(GuardedResourceRecord {
            creator: content_lock.creator.clone(),
            path: guarded_resource.path.clone(),
            hash: guarded_resource.hash,
            content_type: guarded_resource.content_type.clone(),
            size: guarded_resource.size,
            bytes,
        })
        .await
        .unwrap();
}

async fn seed_guarded_resource_from_fixture(state: &AppState, request: &Value) {
    let guarded_resource = &request["primary_resource"];
    state
        .guarded_resources()
        .upsert_guarded_resource(GuardedResourceRecord {
            creator: creator(),
            path: guarded_resource["path"].as_str().unwrap().to_owned(),
            hash: GuardedResourceHash::from_bytes([7; 32]),
            content_type: guarded_resource["content_type"]
                .as_str()
                .unwrap()
                .to_owned(),
            size: guarded_resource["size"].as_u64().unwrap(),
            bytes: b"guarded bytes".to_vec(),
        })
        .await
        .unwrap();
}

fn fixture_value(file_name: &str) -> Value {
    let content = match file_name {
        "register_guarded_resource_response_shape.json" => include_str!(
            "../../../tests/fixtures/creator_publishing/register_guarded_resource_response_shape.json"
        ),
        "create_content_lock_request.json" => {
            include_str!(
                "../../../tests/fixtures/creator_publishing/create_content_lock_request.json"
            )
        }
        "create_content_lock_response_shape.json" => include_str!(
            "../../../tests/fixtures/creator_publishing/create_content_lock_response_shape.json"
        ),
        "set_lock_service_config_request.json" => include_str!(
            "../../../tests/fixtures/creator_publishing/set_lock_service_config_request.json"
        ),
        "set_lock_service_config_response_shape.json" => include_str!(
            "../../../tests/fixtures/creator_publishing/set_lock_service_config_response_shape.json"
        ),
        "submit_proof_bundle_request.json" => {
            include_str!("../../../tests/fixtures/viewer_access/submit_proof_bundle_request.json")
        }
        "submit_proof_bundle_response_shape.json" => include_str!(
            "../../../tests/fixtures/viewer_access/submit_proof_bundle_response_shape.json"
        ),
        "verification_task_handle_request.json" => {
            include_str!(
                "../../../tests/fixtures/viewer_access/verification_task_handle_request.json"
            )
        }
        "access_credential_response_shape.json" => {
            include_str!(
                "../../../tests/fixtures/viewer_access/access_credential_response_shape.json"
            )
        }
        other => panic!("unknown fixture: {other}"),
    };

    serde_json::from_str(content).unwrap()
}

fn assert_placeholder_string(actual: &Value, placeholder: &str) {
    let actual = actual
        .as_str()
        .expect("dynamic fixture field must be string");
    assert_ne!(actual, placeholder);
    assert!(!actual.is_empty());
}

async fn register_guarded_resource_through_creator_route(app: &axum::Router) -> Value {
    let response = app
        .clone()
        .oneshot(authenticated_raw_upload_request(
            "/creator/priv-resources/content/example.txt",
            Some("text/plain"),
            b"guarded bytes".to_vec(),
            "frontend-session-secret",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await["guarded_resource"].clone()
}

async fn submit_task(state: &AppState, app: &axum::Router, submitted: SubmittedProofBundle) {
    let creator = submitted.pubky_lock_resource.creator().clone();
    let bundle_id = submitted.bundle_id.clone();
    let response = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["creator"], creator.to_string());
    assert_eq!(body["bundle_id"], bundle_id.to_string());
    assert_eq!(body["status"], "pending");
    assert!(body.get("task_id").is_none());

    assert!(
        state
            .verification_tasks()
            .get_verification_task_by_handle(&creator, &bundle_id)
            .await
            .unwrap()
            .is_some()
    );
}

async fn complete_task(app: &axum::Router) {
    let response = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/verification-task-completions",
            handle_request(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

async fn issue_credential(app: &axum::Router) -> String {
    let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/access-credentials",
                json!({ "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy", "bundle_id": BUNDLE_ID }),
            ))
            .await
            .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await["credential"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn content_lock(satisfied: bool) -> ContentLock {
    content_lock_for_creator(creator(), satisfied)
}

fn content_lock_for_creator(creator: CreatorPubky, satisfied: bool) -> ContentLock {
    ContentLock {
        version: CONTENT_LOCK_VERSION,
        creator,
        primary_resource: Some(GuardedResource {
            path: "/priv/locks.app/content/hello.txt".to_owned(),
            hash: GuardedResourceHash::from_bytes([7; 32]),
            content_type: "text/plain".to_owned(),
            size: 13,
        }),
        secondary_resources: Default::default(),
        criteria: vec![Criterion {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::DevStatic,
            params: json!({ "satisfied": satisfied }),
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

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

fn other_creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky").unwrap()
}

async fn seed_frontend_session(state: &AppState, raw_token: &str, creator: CreatorPubky) {
    let now = time::OffsetDateTime::now_utc();
    state
        .frontend_sessions()
        .insert_frontend_session(FrontendSessionRecord {
            token: FrontendSessionToken::new(raw_token),
            creator,
            created_at: now,
            expires_at: now + time::Duration::hours(12),
        })
        .await
        .unwrap();
}

async fn seed_expired_frontend_session(state: &AppState, raw_token: &str, creator: CreatorPubky) {
    let now = time::OffsetDateTime::now_utc();
    state
        .frontend_sessions()
        .insert_frontend_session(FrontendSessionRecord {
            token: FrontendSessionToken::new(raw_token),
            creator,
            created_at: now - time::Duration::hours(2),
            expires_at: now - time::Duration::hours(1),
        })
        .await
        .unwrap();
}

async fn seed_creator_authority(state: &AppState) {
    state
        .creator_authorities()
        .upsert_creator_authority(CreatorAuthorityRecord {
            creator: creator(),
            auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
            granted_scopes: vec![
                "/pub/locks.app/:rw".to_owned(),
                "/priv/locks.app/:rw".to_owned(),
            ],
            secret: CreatorAuthoritySecret::new("creator-authority-secret"),
            session_expires_at: None,
            last_revalidated_at: None,
        })
        .await
        .unwrap();
}

async fn seed_pending_creator_connect_flow(
    state: &AppState,
    flow_id: &str,
    return_to: &str,
    opaque_state: &str,
) {
    let now = time::OffsetDateTime::now_utc();
    state
        .creator_connect_flows()
        .insert_pending_creator_connect_flow(PendingCreatorConnectFlowRecord {
            flow_id: CreatorConnectFlowId::new(flow_id),
            return_to: return_to.to_owned(),
            state: opaque_state.to_owned(),
            authorization_url: CreatorConnectAuthorizationUrl::new(
                "pubkyauth://fake-secret-flow-url",
            ),
            requested_scopes: vec![
                "/pub/locks.app/:rw".to_owned(),
                "/priv/locks.app/:rw".to_owned(),
            ],
            created_at: now,
            expires_at: now + time::Duration::minutes(5),
        })
        .await
        .unwrap();
}

struct FixedClock(time::OffsetDateTime);

impl Clock for FixedClock {
    fn now(&self) -> time::OffsetDateTime {
        self.0
    }
}

#[derive(Debug, Default)]
struct CountingLegacyConnectFlowClient {
    start_calls: AtomicUsize,
}

impl CountingLegacyConnectFlowClient {
    fn start_call_count(&self) -> usize {
        self.start_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl LegacyCreatorConnectFlowClient for CountingLegacyConnectFlowClient {
    async fn start_legacy_creator_connect_flow(
        &self,
        _requested_scopes: &[String],
    ) -> Result<CreatorConnectAuthorizationUrl, ApplicationError> {
        self.start_calls.fetch_add(1, Ordering::SeqCst);
        Ok(CreatorConnectAuthorizationUrl::new(
            "pubkyauth://fake-secret-flow-url",
        ))
    }

    async fn await_legacy_creator_connect_flow_approval(
        &self,
        _authorization_url: &CreatorConnectAuthorizationUrl,
    ) -> Result<LegacyCreatorConnectFlowApproval, ApplicationError> {
        Ok(LegacyCreatorConnectFlowApproval {
            creator: creator(),
            session_secret: CreatorAuthoritySecret::new("session-secret"),
        })
    }
}

fn handle_request() -> Value {
    json!({ "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy", "bundle_id": BUNDLE_ID })
}

fn legacy_json_guarded_resource_payload() -> Value {
    json!({
        "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
        "path": "/priv/locks.app/content/example.txt",
        "content_type": "text/plain",
        "content_base64": "Z3VhcmRlZCBieXRlcw==",
    })
}

fn registered_guarded_resource_json() -> Value {
    serde_json::to_value(GuardedResource {
        path: "/priv/locks.app/content/example.txt".to_owned(),
        hash: GuardedResourceHash::from_bytes([7; 32]),
        content_type: "text/plain".to_owned(),
        size: 13,
    })
    .unwrap()
}

fn creator_content_lock_request(guarded_resource: Value) -> Value {
    json!({
        "primary_resource": guarded_resource,
        "secondary_resources": {},
        "criteria": [{
            "criterion_id": "criterion-1",
            "verifier_type": "dev-static",
            "params": { "satisfied": true }
        }],
        "lock_logic": { "type": "all", "criteria": ["criterion-1"] },
        "access_policy": { "requested_credential_ttl_seconds": 900 },
        "lock_server": { "override": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo" }
    })
}

fn creator_lock_service_config_request() -> Value {
    json!({
        "default_lock_server": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
    })
}

fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
    let mut request = json_request_with_client_address(method, uri, body, [127, 0, 0, 1]);
    if uri.starts_with("/creator/content-locks") || uri.starts_with("/creator/lock-service-config")
    {
        request.headers_mut().insert(
            header::AUTHORIZATION,
            "Bearer frontend-session-secret".parse().unwrap(),
        );
    }
    request
}

fn authenticated_json_request(
    method: &str,
    uri: &str,
    body: Value,
    frontend_session_token: &str,
) -> Request<Body> {
    let mut request = json_request(method, uri, body);
    request.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {frontend_session_token}").parse().unwrap(),
    );
    request
}

fn raw_upload_request(uri: &str, content_type: Option<&str>, bytes: Vec<u8>) -> Request<Body> {
    let mut builder = Request::builder().method("PUT").uri(uri);
    if let Some(content_type) = content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    builder.body(Body::from(bytes)).unwrap()
}

fn authenticated_raw_upload_request(
    uri: &str,
    content_type: Option<&str>,
    bytes: Vec<u8>,
    frontend_session_token: &str,
) -> Request<Body> {
    let mut request = raw_upload_request(uri, content_type, bytes);
    request.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {frontend_session_token}").parse().unwrap(),
    );
    request
}

fn json_request_with_client_address(
    method: &str,
    uri: &str,
    body: Value,
    ip_octets: [u8; 4],
) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::from(ip_octets)),
        12345,
    )));
    request
}

fn empty_request(method: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

fn authorization_headers(values: &[&str]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for value in values {
        headers.append(header::AUTHORIZATION, value.parse().unwrap());
    }
    headers
}

fn auth_request(method: &str, uri: &str, authorization: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, authorization)
        .body(Body::empty())
        .unwrap()
}

async fn assert_error_response(
    response: axum::response::Response,
    status: StatusCode,
    code: &str,
) -> Value {
    assert_eq!(response.status(), status);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], code);
    assert!(body["error"]["message"].is_string());
    body
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

fn assert_no_keys(json: &Value, keys: &[&str]) {
    for key in keys {
        assert!(json.get(*key).is_none(), "unexpected key {key} in {json}");
    }
}
