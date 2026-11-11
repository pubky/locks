use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::time::Duration;

use locks_core::ids::{
    BundleId, ContentLockPath, CreatorPubky, GuardedResourceHash, PubkyLockResource,
};
use locks_core::lock_policy::{
    AccessPolicy, ContentLockV2, GraphParticipantV2, GraphPolicyV2, GraphPredicateV2,
    GuardedResource, LockServerConfig, PaymentPolicyV2,
};
use locks_core::verification::{SUBMITTED_PROOF_BUNDLE_V2_VERSION, SubmittedProofBundleV2};
use locks_server::config::{FilesystemLockServerIdentityProvider, LockServerIdentityProvider};
use locks_service::application::models::{
    CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
};
use locks_service::application::ports::CreatorAuthorityStore;
use locks_service::infrastructure::postgres::{
    CreatorAuthoritySecretCipher, PostgresCreatorAuthorityStore, run_migrations,
};
use locks_service::infrastructure::runtime_master_key::RuntimeMasterKey;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use time::OffsetDateTime;

const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
const RUNTIME_MASTER_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

#[test]
fn mixed_compose_pins_protocol_real_dependencies() {
    let compose = include_str!("../../compose.mixed-graph-payment-v2-e2e.yaml");
    let runtime_test = include_str!("mixed_graph_payment_v2_compose.rs");
    assert!(compose.contains("725a4cd6578cdc149b4f38c8577bb9937a81bbb6"));
    assert!(compose.contains("460e541fa10c8da8c761f71177407cd0959ca17d"));
    assert!(compose.contains("postgres:16"));
    assert!(compose.contains("graph-e2e-fake-dependencies.py"));
    assert!(compose.contains("network_mode: service:paykit"));
    assert!(!compose.contains("fake-provider"));
    for forbidden in [
        concat!("fn build_", "state("),
        concat!("fn start_", "server("),
        concat!("fn mixed_", "worker("),
        concat!("InMemoryContent", "LockRepository"),
        concat!("InMemoryEntitlement", "Repository"),
    ] {
        assert!(
            !runtime_test.contains(forbidden),
            "production-runtime Compose must not use in-process boundary {forbidden}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn mixed_graph_payment_v2_compose_exercises_locks_http_paykit_rc3_and_nexus() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("Compose database URL");
    let paykit_url = std::env::var("PAYKIT_SERVER_URL").expect("Compose Paykit URL");
    let dependency_url =
        std::env::var("GRAPH_FAKE_DEPENDENCY_URL").expect("Compose dependency URL");
    let nexus_url = std::env::var("GRAPH_FAKE_NEXUS_URL").expect("Compose Nexus URL");
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
    let authority_secret_path = coord.join("creator-authority.secret");
    let authority_secret = wait_secret(&authority_secret_path).await;
    let master = RuntimeMasterKey::from_base64url(RUNTIME_MASTER_KEY).unwrap();
    let authority_store = PostgresCreatorAuthorityStore::new_encrypted(
        pool.clone(),
        CreatorAuthoritySecretCipher::new(master.creator_authority_key()),
    );
    authority_store
        .upsert_creator_authority(CreatorAuthorityRecord {
            creator: creator.clone(),
            auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
            granted_scopes: vec![
                "/pub/app.locks/:rw".to_owned(),
                "/priv/app.locks/:rw".to_owned(),
            ],
            secret: CreatorAuthoritySecret::new(authority_secret),
            session_expires_at: None,
            last_revalidated_at: Some(OffsetDateTime::now_utc()),
        })
        .await
        .unwrap();
    assert!(
        authority_store
            .get_creator_authority(&creator)
            .await
            .unwrap()
            .is_some(),
        "seeded creator authority must be readable before Locks starts"
    );
    std::fs::remove_file(authority_secret_path).unwrap();

    let (lock, bundle) = mixed_fixture(creator.clone());
    write_json(
        &coord.join("locks-fixture.json"),
        json!({
            "content_lock_path": bundle.pubky_lock_resource.content_lock_path(),
            "content_lock": lock,
        }),
    );
    wait_file(&coord.join("content-lock-published")).await;

    let http = reqwest::Client::new();
    http.post(format!("{dependency_url}/reset"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    set_mode(&http, &dependency_url, "nexus", "unavailable").await;

    let config_path = coord.join("locks-runtime.toml");
    write_runtime_config(
        &config_path,
        &public_key.to_string(),
        &nexus_url,
        &paykit_url,
    );
    let server_url = "http://127.0.0.1:3000";
    let mut runtime = spawn_locks_runtime(&config_path, &database_url);
    wait_http_ready(server_url).await;
    assert!(
        authority_store
            .get_creator_authority(&creator)
            .await
            .unwrap()
            .is_some(),
        "Locks startup must preserve seeded creator authority"
    );

    let submission_response = http
        .post(format!("{server_url}/proof-bundles"))
        .json(&json!({"submitted_proof_bundle": bundle}))
        .send()
        .await
        .unwrap();
    let submission_status = submission_response.status();
    let submission_body = submission_response.text().await.unwrap();
    assert!(
        submission_status.is_success(),
        "production Locks admission returned {submission_status}: {submission_body}"
    );
    let submitted: Value = serde_json::from_str(&submission_body).unwrap();
    assert_eq!(submitted["status"], "auth_pending");
    assert_no_secret_fields(&submitted);
    let before_auth = dependency_observations(&http, &dependency_url).await;
    assert_eq!(before_auth["nexus_calls"], 0);

    write_json(
        &coord.join("folded-auth.json"),
        json!({"auth_url": submitted["auth_url"]}),
    );
    wait_file(&coord.join("folded-auth-approved")).await;
    let accepted = wait_for_folded_acceptance(
        &http,
        server_url,
        submitted["state"].as_str().expect("folded-auth state"),
    )
    .await;
    assert_eq!(accepted["status"], "accepted");
    assert_eq!(accepted["task"]["status"], "pending");
    assert_no_secret_fields(&accepted);

    let replayed: Value = http
        .post(format!("{server_url}/proof-bundles"))
        .json(&json!({"submitted_proof_bundle": bundle}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replayed, accepted, "accepted submission must exact-replay");
    assert_no_secret_fields(&replayed);

    write_json(
        &coord.join("locks-ready.json"),
        json!({
            "bundle_id": BUNDLE_ID,
            "lock_resource": bundle.pubky_lock_resource,
        }),
    );
    wait_file(&coord.join("paykit-restarted-and-accepted")).await;
    wait_for_nexus_calls(&http, &dependency_url, 1).await;

    stop_child(&mut runtime);
    set_mode(&http, &dependency_url, "nexus", "true").await;
    runtime = spawn_locks_runtime(&config_path, &database_url);
    wait_http_ready(server_url).await;

    let lifecycle = wait_for_completed_lifecycle(&http, server_url, &creator).await;
    assert_eq!(lifecycle["status"], "completed");
    assert_no_secret_fields(&lifecycle);
    stop_child(&mut runtime);

    runtime = spawn_locks_runtime(&config_path, &database_url);
    wait_http_ready(server_url).await;
    let restarted: Value = http
        .post(format!("{server_url}/verification-task-lookups"))
        .json(&json!({"creator": creator, "bundle_id": BUNDLE_ID}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        restarted, lifecycle,
        "restart must exact-replay completed lifecycle"
    );
    stop_child(&mut runtime);

    std::fs::write(coord.join("locks-complete"), b"ok").unwrap();
    let readback = wait_json(&coord.join("entitlement-readback.json")).await;
    assert_eq!(readback["bundle_id"], BUNDLE_ID);
    assert_eq!(readback["creator"], creator.to_string());
    assert_eq!(readback["reader"], reader.to_string());
    assert_eq!(readback["satisfied_by"], json!(["zero_conf_graph"]));
    wait_file(&coord.join("paykit-complete")).await;

    let observations = dependency_observations(&http, &dependency_url).await;
    assert!(observations["nexus_calls"].as_u64().unwrap() >= 2);
    pool.close().await;
}

fn write_runtime_config(path: &Path, public_key: &str, nexus_url: &str, paykit_url: &str) {
    let config = format!(
        r#"bind_addr = "0.0.0.0:3000"
[credentials]
lock_server_secret_key = "/private/lock-server.keypair-seed"
lock_server_public_key = "{public_key}"
max_ttl_seconds = 900
[database]
url_env = "PUBKY_LOCK_DATABASE_URL"
max_connections = 12
run_migrations_on_startup = true
[worker]
enabled = true
poll_interval_ms = 50
paykit_payment_retry_interval_seconds = 1
claim_timeout_seconds = 30
worker_id = "mixed-production-runtime"
[deletion_worker]
enabled = false
poll_interval_ms = 250
claim_timeout_seconds = 30
shutdown_timeout_seconds = 5
worker_id = "mixed-deletion-worker"
[runtime]
environment = "development"
[paykit]
server_url = "{paykit_url}"
minimum_confirmations = 1
[nexus]
server_url = "{nexus_url}"
allow_insecure_loopback = true
[creator_authority_acquisition]
enabled = false
method = "legacy-connect"
frontend_session_ttl_seconds = 86400
frontend_session_code_ttl_seconds = 120
[creator_authority_acquisition.legacy_connect]
allowed_return_origins = ["https://app.example"]
[creator_authority_acquisition.grant_connect]
client_id = "app.locks"
allowed_return_origins = ["https://app.example"]
[secrets]
runtime_master_key_env = "PUBKY_LOCK_RUNTIME_MASTER_KEY"
[deletion]
retry_max_attempts = 10
retry_initial_backoff_seconds = 1
retry_max_backoff_seconds = 30
final_credential_issuance_window_seconds = 900
final_read_window_seconds = 900
[logging]
level = "warn"
[pubky]
network = "testnet"
resolution = "default"
[pkdns]
public_ip = "127.0.0.1"
public_pubky_tls_port = 6287
public_icann_http_port = 80
icann_domain = "localhost"
key_republisher_interval_seconds = 3600
[rate_limits]
trusted_proxy_hops = 0
[rate_limits.verification_submission]
enabled = true
max_requests = 60
window_seconds = 60
[rate_limits.paykit_connection_state_lookup]
max_requests = 60
window_seconds = 60
max_in_flight = 16
max_entries = 10000
global_requests_per_second = 50
global_burst = 50
[content_locks]
max_resource_bytes = 10000000
max_resources = 10
max_total_resource_bytes = 100000000
"#,
    );
    std::fs::write(path, config).unwrap();
}

fn spawn_locks_runtime(config_path: &Path, database_url: &str) -> Child {
    let binary = std::env::var("LOCKS_SERVER_BINARY")
        .unwrap_or_else(|_| "/target/debug/locks-server".to_owned());
    Command::new(binary)
        .arg("--config")
        .arg(config_path)
        .env("PUBKY_LOCK_DATABASE_URL", database_url)
        .env("PUBKY_LOCK_RUNTIME_MASTER_KEY", RUNTIME_MASTER_KEY)
        .env("RUST_LOG", "locks_server=debug,locks_service=debug")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn production Locks server")
}

fn stop_child(child: &mut Child) {
    child.kill().unwrap();
    child.wait().unwrap();
}

async fn wait_http_ready(base_url: &str) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if reqwest::get(format!("{base_url}/healthz"))
                .await
                .map(|response| response.status().is_success())
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("production Locks server did not become ready");
}

async fn wait_for_folded_acceptance(
    client: &reqwest::Client,
    base_url: &str,
    state: &str,
) -> Value {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let response: Value = client
                .post(format!("{base_url}/proof-bundles/auth-status"))
                .json(&json!({"state": state}))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            match response["status"].as_str() {
                Some("accepted") => return response,
                Some("pending") => tokio::time::sleep(Duration::from_millis(50)).await,
                status => panic!("folded auth reached unexpected status {status:?}: {response}"),
            }
        }
    })
    .await
    .expect("folded auth did not become accepted")
}

async fn wait_for_completed_lifecycle(
    client: &reqwest::Client,
    base_url: &str,
    creator: &CreatorPubky,
) -> Value {
    tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            let response = client
                .post(format!("{base_url}/verification-task-lookups"))
                .json(&json!({"creator": creator, "bundle_id": BUNDLE_ID}))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();
            let lifecycle: Value = response.json().await.unwrap();
            if lifecycle["status"] == "completed" {
                return lifecycle;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("production Locks runtime did not publish mixed entitlement")
}

async fn wait_for_nexus_calls(client: &reqwest::Client, base_url: &str, minimum: u64) {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let observations = dependency_observations(client, base_url).await;
            if observations["nexus_calls"].as_u64().unwrap() >= minimum {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("mixed worker never reached same-wave Nexus call");
}

async fn dependency_observations(client: &reqwest::Client, base_url: &str) -> Value {
    client
        .get(format!("{base_url}/observations"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn mixed_fixture(creator: CreatorPubky) -> (ContentLockV2, SubmittedProofBundleV2) {
    let lock = ContentLockV2 {
        version: 2,
        creator: creator.clone(),
        primary_resource: Some(
            GuardedResource::new(
                "/priv/app.locks/content/mixed.txt",
                GuardedResourceHash::from_bytes([55; 32]),
                "text/plain",
                5,
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
        graph: Some(GraphPolicyV2 {
            predicates: vec![GraphPredicateV2::Follows {
                follower: GraphParticipantV2::Reader,
                followed: GraphParticipantV2::Creator,
            }],
            operator: None,
        }),
        verification_window_seconds: 3600,
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 300,
        },
        lock_server: LockServerConfig { override_: None },
        created_at: OffsetDateTime::now_utc().replace_nanosecond(0).unwrap(),
    };
    let path = ContentLockPath::from_lock_id(lock.lock_id().unwrap());
    let bundle = SubmittedProofBundleV2 {
        version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: PubkyLockResource::new(creator, path),
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

fn write_json(path: &Path, value: Value) {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, serde_json::to_vec(&value).unwrap()).unwrap();
    std::fs::rename(temporary, path).unwrap();
}

async fn wait_json(path: &Path) -> Value {
    tokio::time::timeout(Duration::from_secs(600), async {
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

async fn wait_secret(path: &Path) -> String {
    tokio::time::timeout(Duration::from_secs(600), async {
        loop {
            if let Ok(secret) = std::fs::read_to_string(path) {
                return secret;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for secret handoff"))
}

async fn wait_file(path: &Path) {
    tokio::time::timeout(Duration::from_secs(600), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {}", path.display()));
}

fn assert_no_secret_fields(value: &Value) {
    let text = value.to_string();
    for forbidden in [
        "flow_secret",
        "signing",
        "invoice_address",
        "publication_token",
        "claim_token",
        "bearer",
    ] {
        assert!(
            !text.contains(forbidden),
            "response leaked {forbidden}: {text}"
        );
    }
}
