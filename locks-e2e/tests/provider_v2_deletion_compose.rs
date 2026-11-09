use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use locks_core::ids::{
    BundleId, ContentLockPath, CreatorPubky, GuardedResourceHash, PubkyLockResource,
};
use locks_core::lock_policy::{
    AccessPolicy, ContentLockPayload, ContentLockV2, GuardedResource, LockServerConfig,
    PaymentPolicyV2,
};
use locks_core::verification::{SUBMITTED_PROOF_BUNDLE_V2_VERSION, SubmittedProofBundleV2};
use locks_server::api::routes::router;
use locks_server::app_state::{AppState, RuntimeSecretCiphers};
use locks_server::config::{
    FilesystemLockServerIdentityProvider, LockServerIdentityProvider, PaykitConfig,
};
use locks_server::deletion_worker::{
    DeletionWorker, DeletionWorkerConfig, RandomFullJitter, RuntimeClaimedDeletionExecutor,
};
use locks_server::testing::TestServerApp;
use locks_service::application::models::{
    ContentLockDeletionState, FrontendSessionToken, GuardedResourceRecord, VerificationTaskStatus,
};
use locks_service::application::ports::{ContentLockRepository, GuardedResourceRepository};
use locks_service::infrastructure::final_credentials::FinalCredentialCipher;
use locks_service::infrastructure::memory::{
    content_lock_tombstones::InMemoryContentLockTombstoneRepository,
    content_locks::InMemoryContentLockRepository, entitlements::InMemoryEntitlementRepository,
    guarded_resources::InMemoryGuardedResourceRepository,
    lock_service_pointers::InMemoryLockServicePointerRepository,
    public_content_locks::InMemoryPublicContentLockStore,
};
use locks_service::infrastructure::postgres::{CreatorAuthoritySecretCipher, run_migrations};
use reqwest::StatusCode;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;

const SESSION: &str = "provider-v2-deletion-session";
const BUNDLE: &str = "000G40R40M30E209185GR38E1W";

#[test]
fn provider_v2_deletion_compose_uses_real_pinned_services() {
    let compose = include_str!("../../compose.provider-v2-deletion-e2e.yaml");
    assert!(compose.contains("725a4cd6578cdc149b4f38c8577bb9937a81bbb6"));
    assert!(compose.contains("460e541fa10c8da8c761f71177407cd0959ca17d"));
    assert!(compose.contains("postgres:16"));
    assert!(compose.contains("provider-v2-deletion-e2e"));
    assert!(!compose.contains("fake-provider"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_v2_deletion_compose_exercises_real_locks_http_and_paykit_rc3() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("Compose database URL");
    let paykit_url = std::env::var("PAYKIT_SERVER_URL").expect("Compose Paykit URL");
    let scenario = std::env::var("PROVIDER_V2_DELETION_SCENARIO").expect("Compose scenario");
    let coord = PathBuf::from(std::env::var("LOCKS_PAYKIT_COORD_DIR").expect("coord directory"));
    let pool = PgPoolOptions::new()
        .max_connections(12)
        .connect(&database_url)
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();

    let secret_path = PathBuf::from("/private/lock-server.keypair-seed");
    let public_key = FilesystemLockServerIdentityProvider
        .generate_secret(&secret_path)
        .unwrap();
    write_json(
        &coord.join("locks-key.json"),
        json!({ "public_key": public_key }),
    );
    let paykit = wait_json(&coord.join("paykit-ready.json")).await;
    let creator = CreatorPubky::from_str(paykit["creator"].as_str().unwrap()).unwrap();
    let reader = CreatorPubky::from_str(paykit["reader"].as_str().unwrap()).unwrap();

    let lock = ContentLockV2 {
        version: 2,
        creator: creator.clone(),
        primary_resource: Some(
            GuardedResource::new(
                "/priv/app.locks/content/provider-v2.txt",
                GuardedResourceHash::from_bytes([44; 32]),
                "text/plain",
                4,
            )
            .unwrap(),
        ),
        secondary_resources: BTreeMap::new(),
        payment: Some(PaymentPolicyV2 {
            recipient_pubky: creator.clone(),
            amount: "50000".to_owned(),
            asset: "BTC".to_owned(),
            minimum_confirmations: 1,
        }),
        graph: None,
        verification_window_seconds: 3600,
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 1,
        },
        lock_server: LockServerConfig { override_: None },
        created_at: time::OffsetDateTime::now_utc()
            .replace_nanosecond(0)
            .unwrap(),
    };
    let lock_id = lock.lock_id().unwrap();
    let lock_path = ContentLockPath::from_lock_id(lock_id.clone());
    let resource = PubkyLockResource::new(creator.clone(), lock_path.clone());
    let bundle = SubmittedProofBundleV2 {
        version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
        bundle_id: BundleId::from_str(BUNDLE).unwrap(),
        pubky_lock_resource: resource.clone(),
        reader_pubky: Some(reader),
    };

    let public_locks = InMemoryPublicContentLockStore::new();
    let content_locks = Arc::new(InMemoryContentLockRepository::with_public_store(
        public_locks.clone(),
    ));
    content_locks
        .upsert_content_lock_payload(creator.clone(), lock_path, ContentLockPayload::V2(lock))
        .await
        .unwrap();
    let tombstones = Arc::new(InMemoryContentLockTombstoneRepository::with_public_store(
        public_locks,
    ));
    let guarded = Arc::new(InMemoryGuardedResourceRepository::new());
    guarded
        .upsert_guarded_resource(GuardedResourceRecord {
            creator: creator.clone(),
            path: "/priv/app.locks/content/provider-v2.txt".to_owned(),
            hash: GuardedResourceHash::from_bytes([44; 32]),
            content_type: "text/plain".to_owned(),
            size: 4,
            bytes: b"test".to_vec(),
        })
        .await
        .unwrap();
    let pointers = Arc::new(InMemoryLockServicePointerRepository::new());
    let entitlements = Arc::new(InMemoryEntitlementRepository::new());

    let state = build_state(
        pool.clone(),
        &paykit_url,
        secret_path,
        public_key,
        content_locks,
        tombstones,
        guarded,
        pointers,
        entitlements,
    );
    let app = TestServerApp::from_state(state.clone());
    app.insert_frontend_session_for_test(
        FrontendSessionToken::new(SESSION),
        creator.clone(),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(state.clone()).into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .into_future(),
    );
    let http = reqwest::Client::new();
    let submit = http
        .post("http://127.0.0.1:3000/proof-bundles")
        .json(&json!({ "submitted_proof_bundle": bundle }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        submit.status(),
        StatusCode::ACCEPTED,
        "{}",
        submit.text().await.unwrap()
    );
    let task = state
        .payment_tasks()
        .get_payment_task_by_handle(&creator, &BundleId::from_str(BUNDLE).unwrap())
        .await
        .unwrap()
        .unwrap();
    let deadline = task
        .snapshot
        .as_v1()
        .deadline_at
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    write_json(
        &coord.join("locks-ready.json"),
        json!({
            "bundle_id": BUNDLE,
            "lock_resource": resource,
            "deadline": deadline,
        }),
    );
    if scenario == "create-first" {
        wait_file(&coord.join("invoice-outcome")).await;
    }

    let deletion = http
        .delete(format!(
            "http://127.0.0.1:3000/creator/content-locks/{lock_id}"
        ))
        .bearer_auth(SESSION)
        .send()
        .await
        .unwrap();
    assert_eq!(
        deletion.status(),
        StatusCode::ACCEPTED,
        "{}",
        deletion.text().await.unwrap()
    );

    let executor = RuntimeClaimedDeletionExecutor::new(state.clone());
    let jitter = RandomFullJitter;
    let worker = DeletionWorker::new(
        state.content_lock_deletions().as_ref(),
        state.clock().as_ref(),
        &executor,
        &jitter,
        DeletionWorkerConfig {
            worker_id: format!("cross-service-{scenario}"),
            poll_interval: Duration::from_millis(20),
            claim_timeout: Duration::from_secs(5),
            retry_max_attempts: 20,
            retry_initial_backoff: Duration::from_millis(20),
            retry_max_backoff: Duration::from_millis(100),
        },
    );
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let finish_by = tokio::time::Instant::now() + Duration::from_secs(90);
    let mut fence_reported = false;
    loop {
        let _ = worker.run_once(&shutdown_rx).await;
        if !fence_reported {
            let fence: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM content_lock_payment_drains WHERE provider_v2_fence_acknowledged_at IS NOT NULL"
            ).fetch_one(&pool).await.unwrap();
            if fence == 1 {
                std::fs::write(coord.join("fence-acknowledged"), b"ok").unwrap();
                fence_reported = true;
            }
        }
        if scenario == "drain-first" && fence_reported {
            wait_file(&coord.join("invoice-outcome")).await;
        }
        let deletion_job = state
            .content_lock_deletions()
            .get_job(&creator, &lock_id)
            .await
            .unwrap();
        match deletion_job.map(|job| job.state) {
            None | Some(ContentLockDeletionState::Completed) => break,
            Some(ContentLockDeletionState::Failed) => {
                panic!("deletion failed before completion")
            }
            Some(ContentLockDeletionState::Queued | ContentLockDeletionState::Running) => {}
        }
        assert!(
            tokio::time::Instant::now() < finish_by,
            "deletion did not complete"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let replay = http
        .delete(format!(
            "http://127.0.0.1:3000/creator/content-locks/{lock_id}"
        ))
        .bearer_auth(SESSION)
        .send()
        .await
        .unwrap();
    assert_eq!(
        replay.status(),
        StatusCode::OK,
        "{}",
        replay.text().await.unwrap()
    );
    let task = state
        .payment_tasks()
        .get_payment_task_by_handle(&creator, &BundleId::from_str(BUNDLE).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, VerificationTaskStatus::Expired);
    let observations: Value = http
        .get(format!("{paykit_url}/observations"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(observations["cleanup_response_dropped"], true);
    assert!(observations["cleanup_calls"].as_u64().unwrap() >= 2);

    std::fs::write(coord.join("locks-complete"), b"ok").unwrap();
    wait_file(&coord.join("paykit-complete")).await;
    server.abort();
    pool.close().await;
}

#[allow(clippy::too_many_arguments)]
fn build_state(
    pool: sqlx::PgPool,
    paykit_url: &str,
    secret_path: PathBuf,
    public_key: locks_core::ids::LockServerPubky,
    content_locks: Arc<InMemoryContentLockRepository>,
    tombstones: Arc<InMemoryContentLockTombstoneRepository>,
    guarded: Arc<InMemoryGuardedResourceRepository>,
    pointers: Arc<InMemoryLockServicePointerRepository>,
    entitlements: Arc<InMemoryEntitlementRepository>,
) -> AppState {
    let mut config = TestServerApp::default_in_memory_config();
    config.database.url = "postgres://redacted".to_owned();
    config.credentials.lock_server_secret_key = secret_path;
    config.credentials.lock_server_public_key = public_key;
    config.worker.enabled = false;
    config.deletion.retry_max_attempts = 20;
    config.deletion.retry_initial_backoff_seconds = 0;
    config.deletion.retry_max_backoff_seconds = 1;
    config.deletion.final_credential_issuance_window_seconds = 30;
    config.deletion.final_read_window_seconds = 1;
    config.paykit = Some(PaykitConfig {
        server_url: paykit_url.to_owned(),
        minimum_confirmations: 1,
    });
    AppState::new_with_postgres_runtime_and_creator_repositories(
        config,
        pool,
        RuntimeSecretCiphers::new(
            CreatorAuthoritySecretCipher::new([7; 32]),
            FinalCredentialCipher::new([8; 32]),
        ),
        content_locks,
        tombstones,
        guarded,
        pointers,
        entitlements,
    )
}

fn write_json(path: &std::path::Path, value: Value) {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, serde_json::to_vec(&value).unwrap()).unwrap();
    std::fs::rename(temporary, path).unwrap();
}

async fn wait_json(path: &std::path::Path) -> Value {
    tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            if let Ok(bytes) = std::fs::read(path) {
                return serde_json::from_slice(&bytes).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {}", path.display()))
}

async fn wait_file(path: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(180), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {}", path.display()));
}
