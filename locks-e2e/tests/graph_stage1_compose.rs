use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode, header};
use locks_core::ids::{
    BundleId, ContentLockPath, CreatorPubky, GuardedResourceHash, PubkyLockResource,
};
use locks_core::lock_policy::{
    AccessPolicy, ContentLockPayload, ContentLockV2, GraphParticipantV2, GraphPolicyV2,
    GraphPredicateV2, GuardedResource, LockServerConfig,
};
use locks_core::verification::SubmittedProofBundleV2;
use locks_server::app_state::{AppState, RuntimeSecretCiphers};
use locks_server::config::{GrantConnectAcquisitionConfig, NexusConfig};
use locks_server::graph_worker::{GraphRetryJitter, GraphVerificationWorker, GraphWorkerTick};
use locks_server::nexus_http_client::{NexusHttpClient, NexusObservation};
use locks_server::testing::TestServerApp;
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::{FoldedAuthFlowSecret, GrantPopKeyId};
use locks_service::application::ports::{
    ContentLockRepository, FoldedAuthApprovalPoll, FoldedAuthApprovalPollError,
    FoldedAuthApprovalPoller, FoldedAuthFlowStarter, FoldedAuthPollCall,
};
use locks_service::infrastructure::final_credentials::FinalCredentialCipher;
use locks_service::infrastructure::folded_auth::FoldedAuthSecretCipher;
use locks_service::infrastructure::memory::{
    content_lock_tombstones::InMemoryContentLockTombstoneRepository,
    content_locks::InMemoryContentLockRepository, entitlements::InMemoryEntitlementRepository,
    guarded_resources::InMemoryGuardedResourceRepository,
    lock_service_pointers::InMemoryLockServicePointerRepository,
};
use locks_service::infrastructure::postgres::{
    CreatorAuthoritySecretCipher, PostgresFoldedAuthRepository, run_migrations,
};
use locks_service::infrastructure::runtime_master_key::RuntimeMasterKey;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use time::OffsetDateTime;
use tower::ServiceExt;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

#[derive(Debug)]
struct DependencyAuth {
    base_url: String,
    client: reqwest::Client,
}

impl FoldedAuthFlowStarter for DependencyAuth {
    fn start(
        &self,
        _key_id: &GrantPopKeyId,
    ) -> Result<(String, FoldedAuthFlowSecret), ApplicationError> {
        Ok((
            format!("{}/approve", self.base_url),
            FoldedAuthFlowSecret::new(b"compose-fake-auth-secret".to_vec()),
        ))
    }
}

#[async_trait]
impl FoldedAuthApprovalPoller for DependencyAuth {
    async fn poll_approval(&self, _flow_secret: &FoldedAuthFlowSecret) -> FoldedAuthPollCall {
        let response = match self
            .client
            .post(format!("{}/poll", self.base_url))
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                return FoldedAuthPollCall::NotAttempted(ApplicationError::Storage {
                    message: format!("fake auth unavailable before dispatch: {error}"),
                });
            }
        };
        let body: Value = match response.json().await {
            Ok(body) => body,
            Err(_) => return FoldedAuthPollCall::Attempted(Err(FoldedAuthApprovalPollError)),
        };
        match body.get("status").and_then(Value::as_str) {
            Some("pending") => FoldedAuthPollCall::Attempted(Ok(FoldedAuthApprovalPoll::Pending)),
            Some("approved") => {
                let reader = body
                    .get("reader")
                    .and_then(Value::as_str)
                    .and_then(|value| CreatorPubky::from_str(value).ok());
                match reader {
                    Some(reader) => {
                        FoldedAuthPollCall::Attempted(Ok(FoldedAuthApprovalPoll::Approved {
                            reader,
                        }))
                    }
                    None => FoldedAuthPollCall::Attempted(Err(FoldedAuthApprovalPollError)),
                }
            }
            _ => FoldedAuthPollCall::Attempted(Err(FoldedAuthApprovalPollError)),
        }
    }
}

#[derive(Debug)]
struct FixedJitter;

impl GraphRetryJitter for FixedJitter {
    fn jitter(&self, base: Duration) -> Duration {
        base
    }
}

#[tokio::test]
async fn graph_stage1_compose_flow_is_restart_safe_fault_closed_and_paykit_free() {
    let database_url =
        std::env::var("TEST_DATABASE_URL").expect("Compose E2E requires TEST_DATABASE_URL");
    let dependency_url = std::env::var("GRAPH_FAKE_DEPENDENCY_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:18080".to_owned());
    let nexus_url = std::env::var("GRAPH_FAKE_NEXUS_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:18081".to_owned());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    client
        .post(format!("{dependency_url}/reset"))
        .send()
        .await
        .unwrap();

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    sqlx::query("TRUNCATE verification_tasks, folded_auth_attempts CASCADE")
        .execute(&pool)
        .await
        .unwrap();

    let content_locks = Arc::new(InMemoryContentLockRepository::new());
    let entitlements = Arc::new(InMemoryEntitlementRepository::new());
    let (lock, bundle) = graph_fixture();
    content_locks
        .upsert_content_lock_payload(
            creator(),
            bundle.pubky_lock_resource.content_lock_path().clone(),
            ContentLockPayload::V2(lock),
        )
        .await
        .unwrap();

    set_mode(&client, &dependency_url, "auth", "ambiguous").await;
    let state = build_state(
        pool.clone(),
        content_locks.clone(),
        entitlements.clone(),
        dependency_url.clone(),
        nexus_url.clone(),
    );
    let app = TestServerApp::from_state(state.clone());
    let router = app.router();
    let submitted = router
        .clone()
        .oneshot(json_request(
            "/proof-bundles",
            json!({"submitted_proof_bundle": bundle}),
        ))
        .await
        .unwrap();
    let submitted_status = submitted.status();
    let submitted = response_json(submitted).await;
    assert_eq!(submitted_status, StatusCode::ACCEPTED, "{submitted}");
    assert_eq!(submitted["status"], "auth_pending");
    assert_no_secret_fields(&submitted);

    let restart = router
        .clone()
        .oneshot(json_request(
            "/proof-bundles/auth-status",
            json!({"state": submitted["state"]}),
        ))
        .await
        .unwrap();
    assert_eq!(restart.status(), StatusCode::OK);
    let restart = response_json(restart).await;
    assert_eq!(restart["status"], "restart_required");
    assert_no_secret_fields(&restart);

    set_mode(&client, &dependency_url, "auth", "approved").await;
    let rotated = router
        .clone()
        .oneshot(json_request(
            "/proof-bundles",
            json!({"submitted_proof_bundle": graph_fixture().1}),
        ))
        .await
        .unwrap();
    assert_eq!(rotated.status(), StatusCode::ACCEPTED);
    let rotated = response_json(rotated).await;
    assert_eq!(rotated["status"], "auth_pending");
    assert_ne!(rotated["state"], submitted["state"]);

    // Rebuild private auth/task adapters around the same PostgreSQL database before polling.
    // Content and entitlement adapters remain one deterministic fake external dependency;
    // production publication durability belongs to the Pubky-backed adapter tests.
    let restarted = build_state(
        pool.clone(),
        content_locks.clone(),
        entitlements.clone(),
        dependency_url.clone(),
        nexus_url.clone(),
    );
    let restarted_app = TestServerApp::from_state(restarted.clone());
    let accepted = restarted_app
        .router()
        .oneshot(json_request(
            "/proof-bundles/auth-status",
            json!({"state": rotated["state"]}),
        ))
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
    let accepted = response_json(accepted).await;
    assert_eq!(accepted["status"], "accepted");
    assert_eq!(accepted["task"]["status"], "pending");
    assert_no_secret_fields(&accepted);

    let lifecycle = restarted_app
        .router()
        .oneshot(json_request(
            "/verification-task-lookups",
            json!({"creator": creator(), "bundle_id": BUNDLE_ID}),
        ))
        .await
        .unwrap();
    assert_eq!(response_json(lifecycle).await["status"], "pending");

    set_mode(&client, &dependency_url, "nexus", "true").await;
    let nexus = Arc::new(NexusHttpClient::new(&nexus_url, true).unwrap());
    let worker = GraphVerificationWorker::new(
        restarted.graph_task_claimer().clone(),
        entitlements.clone(),
        nexus,
        restarted.clock().clone(),
        Arc::new(FixedJitter),
        "compose-graph-worker".to_owned(),
        time::Duration::seconds(30),
    );
    assert!(matches!(
        worker.run_once().await.unwrap(),
        GraphWorkerTick::Terminal(_)
    ));

    let final_state = build_state(
        pool.clone(),
        content_locks,
        entitlements,
        dependency_url.clone(),
        nexus_url.clone(),
    );
    let final_app = TestServerApp::from_state(final_state.clone());
    final_app
        .insert_frontend_session_for_test(
            locks_service::application::models::FrontendSessionToken::new("graph-e2e-creator"),
            creator(),
            OffsetDateTime::now_utc() + time::Duration::minutes(5),
        )
        .await
        .unwrap();
    let status = final_app
        .router()
        .oneshot(json_request(
            "/verification-task-lookups",
            json!({"creator": creator(), "bundle_id": BUNDLE_ID}),
        ))
        .await
        .unwrap();
    let status = response_json(status).await;
    assert_eq!(status["status"], "completed");

    let evidence = final_app
        .router()
        .oneshot(authenticated_json_request(
            "/creator/verification-evidence",
            json!({"bundle_id": BUNDLE_ID}),
            "graph-e2e-creator",
        ))
        .await
        .unwrap();
    let evidence = response_json(evidence).await;
    assert_eq!(evidence["mode"], "graph");
    assert_eq!(evidence["satisfied_by"], json!(["graph"]));
    assert_eq!(evidence["graph"]["predicates"][0]["index"], 0);
    assert_no_secret_fields(&evidence);

    let row: (String, Option<Value>) = sqlx::query_as(
        "SELECT invoice_admission_phase, invoice_admission_intent
         FROM verification_tasks WHERE creator = $1 AND bundle_id = $2",
    )
    .bind(CREATOR)
    .bind(BUNDLE_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        row,
        ("ready".to_owned(), None),
        "graph flow created Paykit intent"
    );
    let drain_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM content_lock_payment_drains")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(drain_count.0, 0, "graph flow created Paykit drain state");

    let observations: Value = client
        .get(format!("{dependency_url}/observations"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(observations["auth_polls"], 2);
    assert_eq!(observations["nexus_calls"], 1);
    assert_eq!(observations["paykit_calls"], 0);
    assert!(
        observations["last_nexus_path"]
            .as_str()
            .unwrap()
            .contains(&format!("/v0/user/{CREATOR}/relationship/{READER}"))
    );

    assert_nexus_fault_matrix(&client, &dependency_url, &nexus_url).await;
    pool.close().await;
}

async fn assert_nexus_fault_matrix(
    client: &reqwest::Client,
    dependency_url: &str,
    nexus_url: &str,
) {
    let nexus = NexusHttpClient::new(nexus_url, true).unwrap();
    let follows = GraphPredicateV2::Follows {
        follower: GraphParticipantV2::Reader,
        followed: GraphParticipantV2::Creator,
    };
    let tagged = GraphPredicateV2::Tagged {
        tagger: GraphParticipantV2::Creator,
        tagged: GraphParticipantV2::Reader,
        label: "  Ä/Tag?  ".to_owned(),
    };

    set_mode(client, dependency_url, "nexus", "false").await;
    assert!(matches!(
        nexus.verify(&follows, &creator(), &reader()).await,
        NexusObservation::Valid { value: false, .. }
    ));
    assert!(matches!(
        nexus.verify(&tagged, &creator(), &reader()).await,
        NexusObservation::Valid { value: false, .. }
    ));
    let observations: Value = client
        .get(format!("{dependency_url}/observations"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        observations["last_nexus_path"]
            .as_str()
            .unwrap()
            .contains(&format!(
                "/v0/user/{READER}/taggers/%C3%A4%2Ftag%3F?viewer_id={CREATOR}"
            ))
    );

    set_mode(client, dependency_url, "nexus", "malformed").await;
    assert!(matches!(
        nexus.verify(&tagged, &creator(), &reader()).await,
        NexusObservation::Invalid { .. }
    ));
    set_mode(client, dependency_url, "nexus", "unavailable").await;
    assert!(matches!(
        nexus.verify(&follows, &creator(), &reader()).await,
        NexusObservation::Operational { .. }
    ));
    set_mode(client, dependency_url, "nexus", "timeout").await;
    assert!(matches!(
        nexus.verify(&follows, &creator(), &reader()).await,
        NexusObservation::Operational { .. }
    ));
}

fn build_state(
    pool: sqlx::PgPool,
    content_locks: Arc<InMemoryContentLockRepository>,
    entitlements: Arc<InMemoryEntitlementRepository>,
    dependency_url: String,
    nexus_url: String,
) -> AppState {
    let mut config = TestServerApp::default_in_memory_config();
    config.database.url = "postgres://redacted".to_owned();
    config.creator_authority_acquisition.grant_connect = Some(GrantConnectAcquisitionConfig {
        client_id: "https://pubky.app".to_owned(),
        allowed_return_origins: vec!["https://pubky.app".to_owned()],
    });
    config.nexus = Some(NexusConfig {
        server_url: nexus_url,
        allow_insecure_loopback: true,
    });
    config.paykit = None;
    let master =
        RuntimeMasterKey::from_base64url("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
    let folded_auth = Arc::new(PostgresFoldedAuthRepository::new(
        pool.clone(),
        FoldedAuthSecretCipher::from_runtime_master_key(&master),
    ));
    let auth = Arc::new(DependencyAuth {
        base_url: dependency_url,
        client: reqwest::Client::new(),
    });
    AppState::new_with_postgres_runtime_and_creator_repositories(
        config,
        pool,
        RuntimeSecretCiphers::new(
            CreatorAuthoritySecretCipher::new([7; 32]),
            FinalCredentialCipher::new([8; 32]),
        ),
        content_locks,
        Arc::new(InMemoryContentLockTombstoneRepository::new()),
        Arc::new(InMemoryGuardedResourceRepository::new()),
        Arc::new(InMemoryLockServicePointerRepository::new()),
        entitlements,
    )
    .with_folded_auth_runtime(folded_auth, auth.clone(), auth)
}

fn graph_fixture() -> (ContentLockV2, SubmittedProofBundleV2) {
    let lock = ContentLockV2 {
        version: 2,
        creator: creator(),
        primary_resource: Some(
            GuardedResource::new(
                "/priv/app.locks/content/graph.txt",
                GuardedResourceHash::from_bytes([3; 32]),
                "text/plain",
                5,
            )
            .unwrap(),
        ),
        secondary_resources: BTreeMap::new(),
        payment: None,
        graph: Some(GraphPolicyV2 {
            predicates: vec![GraphPredicateV2::Follows {
                follower: GraphParticipantV2::Reader,
                followed: GraphParticipantV2::Creator,
            }],
            operator: None,
        }),
        verification_window_seconds: 300,
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 300,
        },
        lock_server: LockServerConfig { override_: None },
        created_at: OffsetDateTime::from_unix_timestamp(1_798_800_000).unwrap(),
    };
    let path = ContentLockPath::from_lock_id(lock.lock_id().unwrap());
    let bundle = SubmittedProofBundleV2 {
        version: 2,
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: PubkyLockResource::new(creator(), path),
        reader_pubky: None,
    };
    (lock, bundle)
}

async fn set_mode(client: &reqwest::Client, base: &str, dependency: &str, mode: &str) {
    client
        .post(format!("{base}/control/{dependency}/{mode}"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

fn json_request(path: &str, value: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 41003))))
        .body(Body::from(serde_json::to_vec(&value).unwrap()))
        .unwrap()
}

fn authenticated_json_request(path: &str, value: Value, token: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 41003))))
        .body(Body::from(serde_json::to_vec(&value).unwrap()))
        .unwrap()
}

async fn response_json(response: axum::response::Response) -> Value {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!(
            "response {status} was not JSON: {error}; body={:?}",
            String::from_utf8_lossy(&bytes)
        )
    })
}

fn assert_no_secret_fields(value: &Value) {
    let text = value.to_string();
    for denied in [
        "flow_secret",
        "claim_token",
        "runtime_master_key",
        "raw_body",
        "history",
        "payment_request",
        "invoice",
    ] {
        assert!(
            !text.contains(denied),
            "response leaked denied field {denied}"
        );
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str(CREATOR).unwrap()
}

fn reader() -> CreatorPubky {
    CreatorPubky::from_str(READER).unwrap()
}
