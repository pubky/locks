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

const GRAPH_BUNDLE: &str = "000G40R40M30E209185GR38E1W";
const PAYMENT_BUNDLE: &str = "000G40R40M30E209185GR38E2W";
const COMBINED_BUNDLE: &str = "000G40R40M30E209185GR38E3W";
const BLOCKED_BUNDLE: &str = "000G40R40M30E209185GR38E4W";
const RUNTIME_MASTER_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

#[test]
fn interrupted_provider_create_requires_replay_before_provider_durability() {
    let pending = json!({
        "payment": {
            "invoice": {"status": "pending"},
            "provider_status": null
        }
    });

    assert!(provider_create_restart_recovery_required(
        "in_progress",
        &pending,
        false,
    ));
    assert!(!provider_create_restart_recovery_required(
        "in_progress",
        &pending,
        true,
    ));
    assert!(!provider_create_restart_crash_state("pending", &pending));
    assert!(!provider_create_restart_crash_state(
        "in_progress",
        &json!({
            "payment": {
                "invoice": {"status": "created"},
                "provider_status": null
            }
        }),
    ));
}

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

#[test]
fn mixed_compose_orders_provider_and_graph_durability_barriers() {
    let runtime = include_str!("mixed_graph_payment_v2_compose.rs");
    let bridge = include_str!("../../fixtures/paykit-mixed-graph-payment-v2-bridge.rs");
    let runner = include_str!("../../scripts/test-mixed-graph-payment-v2-compose.sh");

    assert!(
        runner.contains("locks-mixed-v2-$$"),
        "default Compose prefix must be process-unique"
    );
    assert!(
        runner.find("assert_project_absent \"$project\"").unwrap()
            < runner.find("active_project=\"$project\"").unwrap(),
        "cleanup ownership must begin only after pre-existing project absence is proved"
    );

    assert!(
        bridge.find("arm-provider-create").unwrap() < bridge.find(".approve_auth(").unwrap(),
        "Paykit create barrier must be armed before folded authorization makes work claimable"
    );
    let release = bridge.find("release-provider-create").unwrap();
    let released = bridge.find("provider-create-released").unwrap();
    let canceled = bridge[release..]
        .find("cancel_database_wait(&pool, \"invoice_provider_v2\")")
        .unwrap()
        + release;
    assert!(
        release < canceled && canceled < released,
        "blocked provider transaction must be canceled before replay is released"
    );
    assert!(
        !bridge[release..released].contains("server.kill()"),
        "provider transaction cancellation must not kill Paykit and strand its Pubky lock"
    );
    assert!(
        bridge.contains("assert_eq!((invoice_count, provider_count), (0, 0))"),
        "aborted provider-create transaction must prove zero durable rows before replay"
    );
    let scenario_start = runtime.rfind("for (index, (name, bundle_id").unwrap();
    let scenario = &runtime[scenario_start..];
    assert!(
        scenario.find("arm-provider-create").unwrap()
            < scenario.find("folded-auth-{name}.json").unwrap(),
        "Locks must await provider barrier before handing off folded authorization"
    );

    let created = scenario.find("wait_for_created_without_status").unwrap();
    let stopped = scenario[created..]
        .find("stop_child(&mut runtime)")
        .unwrap()
        + created;
    let paykit_stop = scenario[created..].find("locks-invoice-persisted").unwrap() + created;
    assert!(
        created < stopped && stopped < paykit_stop,
        "Locks must stop at created/no-status boundary before Paykit restart choreography"
    );

    let target_nexus = scenario
        .find("set_mode(&http, &dependency_url, \"nexus\", nexus_mode)")
        .unwrap();
    let accept_gate = scenario[target_nexus..]
        .find("if index != 0 && expected_path.is_some()")
        .unwrap()
        + target_nexus;
    assert!(
        target_nexus < accept_gate,
        "target Nexus mode must be established before provider acceptance for payment/combined waves"
    );
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

    let (lock, base_bundle) = mixed_fixture(creator.clone(), GRAPH_BUNDLE);
    write_json(
        &coord.join("locks-fixture.json"),
        json!({
            "content_lock_path": base_bundle.pubky_lock_resource.content_lock_path(),
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

    let scenarios = [
        (
            "graph",
            GRAPH_BUNDLE,
            false,
            "true",
            Some(json!(["zero_conf_graph"])),
        ),
        (
            "payment",
            PAYMENT_BUNDLE,
            true,
            "false",
            Some(json!(["payment"])),
        ),
        (
            "combined",
            COMBINED_BUNDLE,
            true,
            "true",
            Some(json!(["payment", "zero_conf_graph"])),
        ),
        ("blocked", BLOCKED_BUNDLE, false, "true", None),
    ];

    for (index, (name, bundle_id, paid, nexus_mode, expected_path)) in scenarios.iter().enumerate()
    {
        let mut bundle = base_bundle.clone();
        bundle.bundle_id = BundleId::from_str(bundle_id).unwrap();
        set_mode(&http, &dependency_url, "nexus", "unavailable").await;
        let nexus_calls_before_submission =
            dependency_observations(&http, &dependency_url).await["nexus_calls"]
                .as_u64()
                .unwrap();
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
        assert_eq!(
            before_auth["nexus_calls"].as_u64().unwrap(),
            nexus_calls_before_submission,
            "mixed submission must not call Nexus before folded authorization"
        );

        if index == 0 {
            std::fs::write(coord.join("arm-provider-create"), b"ok").unwrap();
            wait_file(&coord.join("provider-create-armed")).await;
        }

        write_json(
            &coord.join(format!("folded-auth-{name}.json")),
            json!({"auth_url": submitted["auth_url"]}),
        );
        wait_file(&coord.join(format!("folded-auth-approved-{name}"))).await;
        let accepted = wait_for_folded_acceptance(
            &http,
            server_url,
            submitted["state"].as_str().expect("folded-auth state"),
        )
        .await;
        assert_eq!(accepted["status"], "accepted");
        assert_eq!(accepted["task"]["status"], "pending");
        assert_no_secret_fields(&accepted);

        if index == 0 {
            stop_child(&mut runtime);
            assert_locks_task_cardinality(&pool, &creator, bundle_id, 0).await;
            runtime = spawn_locks_runtime(&config_path, &database_url);
            wait_http_ready(server_url).await;
        }

        write_json(
            &coord.join(format!("locks-ready-{name}.json")),
            json!({
                "bundle_id": bundle_id,
                "lock_resource": bundle.pubky_lock_resource,
                "paid": paid,
                "accept": name != &"blocked",
            }),
        );

        if index == 0 {
            wait_file(&coord.join("provider-create-blocked")).await;
            let blocked_observations = dependency_observations(&http, &dependency_url).await;
            assert_eq!(
                blocked_observations["nexus_calls"].as_u64().unwrap(),
                nexus_calls_before_submission,
                "Graph must not run while provider create is blocked before durability"
            );
            stop_child(&mut runtime);
            assert_provider_create_restart_crash_state(&pool, &creator, bundle_id).await;
            std::fs::write(coord.join("release-provider-create"), b"ok").unwrap();
            wait_file(&coord.join("provider-create-released")).await;
            // Killing Locks also cancels its blocked HTTP request. Restart now so
            // exact create replay can make the provider transaction durable.
            runtime = spawn_locks_runtime(&config_path, &database_url);
            wait_http_ready(server_url).await;
        }

        wait_file(&coord.join(format!("provider-ready-{name}"))).await;
        assert_locks_task_cardinality(&pool, &creator, bundle_id, 0).await;

        if index == 0 {
            wait_for_created_without_status(&pool, &creator, bundle_id).await;
            stop_child(&mut runtime);
            wait_for_created_without_status(&pool, &creator, bundle_id).await;
            std::fs::write(coord.join("locks-invoice-persisted"), b"ok").unwrap();
            wait_file(&coord.join("paykit-stopped-after-create")).await;
            std::fs::write(coord.join("resume-paykit-after-create"), b"ok").unwrap();
            wait_file(&coord.join("paykit-resumed-after-create")).await;
            runtime = spawn_locks_runtime(&config_path, &database_url);
            wait_http_ready(server_url).await;
        }

        if index == 0 {
            std::fs::write(coord.join(format!("accept-provider-{name}")), b"ok").unwrap();
            set_mode(&http, &dependency_url, "nexus", "false").await;
            wait_for_graph_observation(&pool, &creator, bundle_id, "false").await;
            stop_child(&mut runtime);
            runtime = spawn_locks_runtime(&config_path, &database_url);
            wait_http_ready(server_url).await;
        }

        let nexus_calls_before =
            dependency_observations(&http, &dependency_url).await["nexus_calls"]
                .as_u64()
                .unwrap();
        if index != 0 && expected_path.is_some() {
            stop_child(&mut runtime);
        }
        set_mode(&http, &dependency_url, "nexus", nexus_mode).await;
        if index != 0 && expected_path.is_some() {
            std::fs::write(coord.join(format!("accept-provider-{name}")), b"ok").unwrap();
            wait_file(&coord.join(format!("provider-evaluation-ready-{name}"))).await;
            runtime = spawn_locks_runtime(&config_path, &database_url);
            wait_http_ready(server_url).await;
        }

        if let Some(expected_path) = expected_path {
            let lifecycle =
                wait_for_completed_lifecycle(&http, server_url, &creator, bundle_id).await;
            assert_eq!(lifecycle["status"], "completed");
            assert_no_secret_fields(&lifecycle);
            assert_locks_task_cardinality(&pool, &creator, bundle_id, 1).await;

            stop_child(&mut runtime);
            runtime = spawn_locks_runtime(&config_path, &database_url);
            wait_http_ready(server_url).await;
            let restarted: Value = http
                .post(format!("{server_url}/verification-task-lookups"))
                .json(&json!({"creator": creator, "bundle_id": bundle_id}))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(restarted, lifecycle, "restart must exact-replay lifecycle");

            std::fs::write(coord.join(format!("locks-complete-{name}")), b"ok").unwrap();
            let readback =
                wait_json(&coord.join(format!("entitlement-readback-{name}.json"))).await;
            assert_eq!(readback["bundle_id"], *bundle_id);
            assert_eq!(readback["creator"], creator.to_string());
            assert_eq!(readback["reader"], reader.to_string());
            assert_eq!(readback["satisfied_by"], *expected_path);
        } else {
            wait_for_provider_request_state(&pool, &creator, bundle_id, "proposed").await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            let lifecycle = lookup_lifecycle(&http, server_url, &creator, bundle_id).await;
            assert!(matches!(
                lifecycle["status"].as_str(),
                Some("pending" | "in_progress")
            ));
            assert_locks_task_cardinality(&pool, &creator, bundle_id, 0).await;
            let after = dependency_observations(&http, &dependency_url).await;
            assert_eq!(
                after["nexus_calls"].as_u64().unwrap(),
                nexus_calls_before,
                "proposed provider state must block Graph even when Nexus would be true"
            );
            std::fs::write(coord.join("locks-complete-blocked"), b"ok").unwrap();
        }
    }

    stop_child(&mut runtime);
    std::fs::write(coord.join("locks-complete"), b"ok").unwrap();
    wait_file(&coord.join("paykit-complete")).await;
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
poll_interval_ms = 1000
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
    bundle_id: &str,
) -> Value {
    tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            let response = client
                .post(format!("{base_url}/verification-task-lookups"))
                .json(&json!({"creator": creator, "bundle_id": bundle_id}))
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

async fn lookup_lifecycle(
    client: &reqwest::Client,
    base_url: &str,
    creator: &CreatorPubky,
    bundle_id: &str,
) -> Value {
    client
        .post(format!("{base_url}/verification-task-lookups"))
        .json(&json!({"creator": creator, "bundle_id": bundle_id}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn assert_locks_task_cardinality(
    pool: &sqlx::PgPool,
    creator: &CreatorPubky,
    bundle_id: &str,
    expected_payloads: i64,
) {
    let (tasks, payloads): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE entitlement_to_publish IS NOT NULL)
         FROM verification_tasks
         WHERE creator = $1 AND bundle_id = $2 AND evaluator_mode = 'mixed'",
    )
    .bind(creator.to_string())
    .bind(bundle_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(tasks, 1, "mixed restart must retain exactly one task");
    assert_eq!(payloads, expected_payloads, "mixed payload cardinality");
}

async fn wait_for_created_without_status(
    pool: &sqlx::PgPool,
    creator: &CreatorPubky,
    bundle_id: &str,
) {
    let started = std::time::Instant::now();
    loop {
        let (
            status,
            claimed_by,
            claim_expires_at,
            next_attempt_at,
            database_now,
            revision,
            snapshot,
        ): (
            String,
            Option<String>,
            Option<OffsetDateTime>,
            Option<OffsetDateTime>,
            OffsetDateTime,
            i64,
            Value,
        ) = sqlx::query_as(
            "SELECT status, claimed_by, claim_expires_at, next_attempt_at,
                    clock_timestamp(), evaluator_revision, evaluator_snapshot
             FROM verification_tasks
             WHERE creator = $1 AND bundle_id = $2 AND evaluator_mode = 'mixed'",
        )
        .bind(creator.to_string())
        .bind(bundle_id)
        .fetch_one(pool)
        .await
        .unwrap();
        if status == "in_progress"
            && snapshot["payment"]["invoice"]["status"] == "created"
            && snapshot["payment"]["provider_status"].is_null()
        {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "invoice-created restart window was not persisted: status={status} claimed_by={claimed_by:?} claim_expires_at={claim_expires_at:?} next_attempt_at={next_attempt_at:?} database_now={database_now} revision={revision} snapshot={snapshot}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_provider_request_state(
    pool: &sqlx::PgPool,
    creator: &CreatorPubky,
    bundle_id: &str,
    expected: &str,
) {
    let started = std::time::Instant::now();
    loop {
        let (status, revision, snapshot): (String, i64, Value) = sqlx::query_as(
            "SELECT status, evaluator_revision, evaluator_snapshot
             FROM verification_tasks
             WHERE creator = $1 AND bundle_id = $2 AND evaluator_mode = 'mixed'",
        )
        .bind(creator.to_string())
        .bind(bundle_id)
        .fetch_one(pool)
        .await
        .unwrap();
        if snapshot["payment"]["provider_status"]["request_state"] == expected {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "provider state {expected} was not persisted: status={status} revision={revision} snapshot={snapshot}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn provider_create_restart_crash_state(status: &str, snapshot: &Value) -> bool {
    status == "in_progress"
        && snapshot["payment"]["invoice"]["status"] == "pending"
        && snapshot["payment"]["provider_status"].is_null()
}

fn provider_create_restart_recovery_required(
    status: &str,
    snapshot: &Value,
    provider_durable: bool,
) -> bool {
    provider_create_restart_crash_state(status, snapshot) && !provider_durable
}

async fn assert_provider_create_restart_crash_state(
    pool: &sqlx::PgPool,
    creator: &CreatorPubky,
    bundle_id: &str,
) {
    let (status, claimed_by, claim_expires_at, database_now, revision, snapshot): (
        String,
        Option<String>,
        Option<OffsetDateTime>,
        OffsetDateTime,
        i64,
        Value,
    ) = sqlx::query_as(
        "SELECT status, claimed_by, claim_expires_at, clock_timestamp(),
                evaluator_revision, evaluator_snapshot
         FROM verification_tasks
         WHERE creator = $1 AND bundle_id = $2 AND evaluator_mode = 'mixed'",
    )
    .bind(creator.to_string())
    .bind(bundle_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert!(
        provider_create_restart_crash_state(&status, &snapshot),
        "provider-create crash boundary diverged: status={status} claimed_by={claimed_by:?} claim_expires_at={claim_expires_at:?} database_now={database_now} revision={revision} snapshot={snapshot}"
    );
    eprintln!(
        "provider-create crash boundary: status={status} claimed_by={claimed_by:?} claim_expires_at={claim_expires_at:?} database_now={database_now} revision={revision} invoice={} provider_status={}",
        snapshot["payment"]["invoice"], snapshot["payment"]["provider_status"]
    );
}

async fn wait_for_graph_observation(
    pool: &sqlx::PgPool,
    creator: &CreatorPubky,
    bundle_id: &str,
    outcome: &str,
) {
    let started = std::time::Instant::now();
    loop {
        let (
            status,
            claimed_by,
            claim_expires_at,
            next_attempt_at,
            database_now,
            revision,
            snapshot,
        ): (
            String,
            Option<String>,
            Option<OffsetDateTime>,
            Option<OffsetDateTime>,
            OffsetDateTime,
            i64,
            Value,
        ) = sqlx::query_as(
            "SELECT status, claimed_by, claim_expires_at, next_attempt_at,
                    clock_timestamp(), evaluator_revision, evaluator_snapshot
             FROM verification_tasks
             WHERE creator = $1 AND bundle_id = $2 AND evaluator_mode = 'mixed'",
        )
        .bind(creator.to_string())
        .bind(bundle_id)
        .fetch_one(pool)
        .await
        .unwrap();
        if snapshot["graph"]["predicates"][0]["latest_observation"]["outcome"] == outcome {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(180),
            "provider-status and Graph evidence were not durably persisted: status={status} claimed_by={claimed_by:?} claim_expires_at={claim_expires_at:?} next_attempt_at={next_attempt_at:?} database_now={database_now} revision={revision} snapshot={snapshot}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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

fn mixed_fixture(
    creator: CreatorPubky,
    bundle_id: &str,
) -> (ContentLockV2, SubmittedProofBundleV2) {
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
        bundle_id: BundleId::from_str(bundle_id).unwrap(),
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
