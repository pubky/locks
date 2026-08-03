use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;

use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode, header};
use locks_core::ids::{
    BundleId, CreatorPubky, GuardedResourceHash, LockServerPubky, PubkyLockResource,
};
use locks_core::lock_policy::{
    AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
    LockServerConfig, VerifierType,
};
use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};
use locks_server::api::routes::router;
use locks_server::app_state::AppState;
use locks_server::config::{
    ContentLocksConfig, CreatorAuthorityAcquisitionConfig, DatabaseConfig,
    LockServerCredentialsConfig, LockServerRuntimeConfig, LoggingConfig, PkdnsConfig, PubkyConfig,
    RateLimitsConfig, RuntimeConfig, RuntimeEnvironment, SecretsConfig, WorkerConfig,
};
use locks_server::worker::{VerificationWorker, WorkerTick};
use locks_service::application::models::{
    AccessCredential, AccessCredentialLookupKey, CreatorAuthorityAuthKind, CreatorAuthorityRecord,
    CreatorAuthoritySecret, VerificationTaskStatus,
};
use locks_service::infrastructure::memory::{
    content_locks::InMemoryContentLockRepository, entitlements::InMemoryEntitlementRepository,
    guarded_resources::InMemoryGuardedResourceRepository,
    lock_service_pointers::InMemoryLockServicePointerRepository,
};
use locks_service::infrastructure::postgres::{CreatorAuthoritySecretCipher, run_migrations};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use time::macros::datetime;
use tower::ServiceExt;

const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

#[tokio::test]
async fn postgres_runtime_state_survives_app_state_recreation() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let content_lock = content_lock();

    let first_state = app_state(database.pool().clone());
    seed_content_lock(&first_state, content_lock.clone()).await;
    let first_router = router(first_state.clone());
    submit_task(&first_router, submitted_proof_bundle_for(&content_lock)).await;

    let recreated_state = app_state(database.pool().clone());
    let recreated_task = recreated_state
        .verification_tasks()
        .get_verification_task_by_handle(&creator(), &bundle_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recreated_task.status, VerificationTaskStatus::Pending);

    seed_content_lock(&recreated_state, content_lock.clone()).await;
    let worker = VerificationWorker::from_state(&recreated_state);
    assert_eq!(
        worker.run_once().await.unwrap(),
        WorkerTick::Completed(recreated_task.task_id)
    );

    let recreated_router = router(recreated_state.clone());
    let credential = issue_credential(&recreated_router).await;
    let credential_key = AccessCredentialLookupKey::derive(&AccessCredential::new(credential));

    let final_state = app_state(database.pool().clone());
    let stored_credential = final_state
        .access_credentials()
        .get_access_credential(&credential_key)
        .await
        .unwrap();
    assert!(stored_credential.is_some());

    database.cleanup().await;
}

#[tokio::test]
async fn postgres_runtime_readyz_returns_ready_without_leaking_runtime_details() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let state = app_state(database.pool().clone());
    let response = router(state)
        .oneshot(empty_request("GET", "/readyz"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "ready");
    assert_eq!(body["runtime_storage"], "persisted");
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

    database.cleanup().await;
}

#[tokio::test]
async fn postgres_runtime_encrypts_creator_authority_secrets_at_rest() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let state = app_state(database.pool().clone());

    state
        .creator_authorities()
        .upsert_creator_authority(creator_authority_record("legacy-cookie-session-secret"))
        .await
        .unwrap();

    let stored_secret: String = sqlx::query_scalar("SELECT secret FROM creator_authorities")
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert!(stored_secret.starts_with("v1.xchacha20poly1305:"));
    assert!(!stored_secret.contains("legacy-cookie-session-secret"));

    let loaded = state
        .creator_authorities()
        .get_creator_authority(&creator())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        loaded.secret.expose_secret(),
        "legacy-cookie-session-secret"
    );

    database.cleanup().await;
}

struct TestDatabase {
    pool: PgPool,
    schema_name: String,
    database_url: String,
}

impl TestDatabase {
    async fn create() -> Option<Self> {
        let Ok(database_url) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("skipping Postgres E2E test because TEST_DATABASE_URL is not set");
            return None;
        };
        let schema_name = format!("locks_e2e_{}", uuid::Uuid::new_v4().simple());
        let mut admin_connection = PgConnection::connect(&database_url)
            .await
            .expect("connect to TEST_DATABASE_URL");
        admin_connection
            .execute(format!("CREATE SCHEMA {schema_name}").as_str())
            .await
            .expect("create isolated schema");

        let search_path = schema_name.clone();
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .after_connect(move |connection, _metadata| {
                let search_path = search_path.clone();
                Box::pin(async move {
                    connection
                        .execute(format!("SET search_path TO {search_path}").as_str())
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await
            .expect("connect isolated schema pool");
        run_migrations(&pool)
            .await
            .expect("run migrations in isolated schema");

        Some(Self {
            pool,
            schema_name,
            database_url,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) {
        self.pool.close().await;
        let mut admin_connection = PgConnection::connect(&self.database_url)
            .await
            .expect("connect to TEST_DATABASE_URL for cleanup");
        admin_connection
            .execute(format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema_name).as_str())
            .await
            .expect("drop isolated schema");
    }
}

fn app_state(pool: PgPool) -> AppState {
    AppState::new_with_postgres_runtime_and_creator_repositories(
        test_config(),
        pool,
        CreatorAuthoritySecretCipher::new([7; 32]),
        std::sync::Arc::new(InMemoryContentLockRepository::new()),
        std::sync::Arc::new(InMemoryGuardedResourceRepository::new()),
        std::sync::Arc::new(InMemoryLockServicePointerRepository::new()),
        std::sync::Arc::new(InMemoryEntitlementRepository::new()),
    )
}

fn creator_authority_record(secret: &str) -> CreatorAuthorityRecord {
    CreatorAuthorityRecord {
        creator: creator(),
        auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
        granted_scopes: vec![
            "/pub/locks.app/:rw".to_owned(),
            "/priv/locks.app/:rw".to_owned(),
        ],
        secret: CreatorAuthoritySecret::new(secret.to_owned()),
        session_expires_at: None,
        last_revalidated_at: Some(datetime!(2026-05-29 12:00:00 UTC)),
    }
}

fn test_config() -> LockServerRuntimeConfig {
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
            worker_id: "e2e-worker".to_owned(),
        },
        runtime: RuntimeConfig {
            environment: RuntimeEnvironment::Development,
        },
        creator_authority_acquisition: CreatorAuthorityAcquisitionConfig::default(),
        secrets: SecretsConfig::default(),
        rate_limits: RateLimitsConfig::default(),
        logging: LoggingConfig::default(),
        pubky: PubkyConfig::default(),
        pkdns: PkdnsConfig::default(),
        content_locks: ContentLocksConfig::default(),
        paykit: None,
    }
}

async fn seed_content_lock(state: &AppState, content_lock: ContentLock) {
    state
        .content_locks()
        .upsert_content_lock(
            content_lock.creator.clone(),
            content_lock.content_lock_path().unwrap(),
            content_lock,
        )
        .await
        .unwrap();
}

async fn submit_task(router: &axum::Router, bundle: SubmittedProofBundle) {
    let response = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": bundle }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(json["creator"], creator().to_string());
    assert_eq!(json["bundle_id"], BUNDLE_ID);
    assert_eq!(json["status"], "pending");
    assert!(json.get("task_id").is_none());
}

async fn issue_credential(router: &axum::Router) -> String {
    let response = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/access-credentials",
            json!({ "creator": creator(), "bundle_id": BUNDLE_ID }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await["credential"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn submitted_proof_bundle_for(content_lock: &ContentLock) -> SubmittedProofBundle {
    SubmittedProofBundle {
        version: SUBMITTED_PROOF_BUNDLE_VERSION,
        bundle_id: bundle_id(),
        pubky_lock_resource: PubkyLockResource::new(
            content_lock.creator.clone(),
            content_lock.content_lock_path().unwrap(),
        ),
        reader_public_key: None,
        proofs: vec![Proof {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::DevStatic,
            payload: json!({ "e2e": true }),
        }],
    }
}

fn content_lock() -> ContentLock {
    ContentLock {
        version: CONTENT_LOCK_VERSION,
        creator: creator(),
        primary_resource: Some(GuardedResource {
            path: "/priv/locks.app/content/postgres-runtime.txt".to_owned(),
            hash: GuardedResourceHash::from_bytes([9; 32]),
            content_type: "text/plain".to_owned(),
            size: 22,
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

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

fn bundle_id() -> BundleId {
    BundleId::from_str(BUNDLE_ID).unwrap()
}

fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    insert_connect_info(&mut request);
    request
}

fn empty_request(method: &str, uri: &str) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    insert_connect_info(&mut request);
    request
}

fn insert_connect_info(request: &mut Request<Body>) {
    request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        12345,
    )));
}

fn assert_no_keys(body: &Value, forbidden_keys: &[&str]) {
    for key in forbidden_keys {
        assert!(body.get(key).is_none(), "response leaked key {key}");
    }
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
