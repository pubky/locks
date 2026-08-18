use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use locks_core::content_lock_deletion::ContentLockDeletionTombstone;
use locks_core::ids::{ContentLockPath, CreatorPubky, LockId, LockServerPubky};
use sqlx::postgres::PgPoolOptions;
use time::macros::datetime;

use crate::app_state::pubky_clients::{
    PubkyClientConstructor, PubkyHttpClientConstructor, pubky_auth_relay_for_network,
    pubky_client_constructor, pubky_http_client_constructor,
};
use crate::app_state::{AppState, OsRandomTaskIdGenerator, RuntimeStorageKind};
use crate::config::{
    ContentLocksConfig, DatabaseConfig, LockServerCredentialsConfig, LockServerRuntimeConfig,
    LoggingConfig, PubkyConfig, PubkyNetwork, RateLimitsConfig, RuntimeConfig, RuntimeEnvironment,
    SecretsConfig, VerificationSubmissionRateLimitConfig, WorkerConfig,
};
use crate::rate_limit::VerificationSubmissionRateLimitKey;
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::ContentLockDeletionPhase;
use locks_service::application::ports::{
    ContentLockDeletionActionAcquireResult, ContentLockDeletionActionClaim,
    ContentLockDeletionActionOwnership, ContentLockTombstoneRepository,
    CreatorConnectFlowIdGenerator, FrontendSessionCodeGenerator, FrontendSessionTokenGenerator,
    TombstoneReadback, VerificationTaskIdGenerator,
};
use locks_service::infrastructure::final_credentials::FinalCredentialCipher;
use locks_service::infrastructure::postgres::CreatorAuthoritySecretCipher;

#[test]
fn in_memory_state_uses_in_memory_private_runtime_adapters() {
    let state = AppState::new_empty_in_memory(test_config());

    assert_eq!(
        state.private_runtime_storage_kind(),
        RuntimeStorageKind::InMemory
    );
}

#[tokio::test]
async fn postgres_state_uses_postgres_for_private_runtime_adapters() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://locks:locks@localhost/locks_test")
        .unwrap();

    let state = AppState::new_with_postgres_runtime(
        test_config(),
        pool,
        test_creator_authority_cipher(),
        test_final_credential_cipher(),
    );

    assert_eq!(
        state.private_runtime_storage_kind(),
        RuntimeStorageKind::Postgres
    );
    assert_action_ownership_adapter(state.content_lock_deletion_action_ownership().as_ref());
    assert_tombstone_adapter(state.content_lock_tombstones().as_ref());
}

#[tokio::test]
async fn in_memory_state_exposes_callable_deletion_action_and_tombstone_adapters() {
    let state = AppState::new_empty_in_memory(test_config());
    let result = state
        .content_lock_deletion_action_ownership()
        .try_acquire(ContentLockDeletionActionClaim {
            job_id: uuid::Uuid::new_v4(),
            worker_id: "test-worker",
            claim_token: uuid::Uuid::new_v4(),
            expected_phase: ContentLockDeletionPhase::Withdraw,
            force: false,
        })
        .await
        .unwrap();
    assert!(matches!(
        result,
        ContentLockDeletionActionAcquireResult::ClaimLost
    ));

    let lock_id = LockId::from_str("000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG").unwrap();
    let path = ContentLockPath::from_lock_id(lock_id.clone());
    let tombstone = ContentLockDeletionTombstone::new(lock_id, datetime!(2026-08-12 05:00:00 UTC));
    assert_eq!(
        state
            .content_lock_tombstones()
            .read_tombstone(&rate_limit_key().creator, &path, &tombstone)
            .await
            .unwrap(),
        TombstoneReadback::Missing
    );

    let creator = rate_limit_key().creator;
    let content_lock: locks_core::lock_policy::ContentLock =
        serde_json::from_value(serde_json::json!({
            "version": 1,
            "creator": creator,
            "primary_resource": null,
            "secondary_resources": {},
            "criteria": [],
            "lock_logic": { "type": "all", "criteria": [] },
            "access_policy": { "requested_credential_ttl_seconds": 900 },
            "lock_server": { "override": null },
            "created_at": "2026-08-12T04:00:00Z"
        }))
        .unwrap();
    state
        .content_locks()
        .upsert_content_lock(creator.clone(), path.clone(), content_lock.clone())
        .await
        .unwrap();
    state
        .content_lock_tombstones()
        .withdraw_content_lock(creator.clone(), path.clone(), &content_lock, &tombstone)
        .await
        .unwrap();
    assert!(
        state
            .content_locks()
            .get_content_lock(&creator, &path)
            .await
            .is_err()
    );
    assert_eq!(
        state
            .content_lock_tombstones()
            .read_tombstone(&creator, &path, &tombstone)
            .await
            .unwrap(),
        TombstoneReadback::Exact
    );
}

fn assert_action_ownership_adapter(_: &dyn ContentLockDeletionActionOwnership) {}

fn assert_tombstone_adapter(_: &dyn ContentLockTombstoneRepository) {}

#[tokio::test]
async fn postgres_state_wires_legacy_connect_flow_runtime_state() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://locks:locks@localhost/locks_test")
        .unwrap();

    let state = AppState::new_with_postgres_runtime(
        test_config(),
        pool,
        test_creator_authority_cipher(),
        test_final_credential_cipher(),
    );

    assert!(Arc::strong_count(state.creator_connect_flows()) >= 1);
    assert!(Arc::strong_count(state.frontend_session_codes()) >= 1);
    assert!(Arc::strong_count(state.frontend_sessions()) >= 1);
    assert!(Arc::strong_count(state.creator_authority_manager()) >= 1);
    assert!(Arc::strong_count(state.legacy_creator_connect_flow_client()) >= 1);
}

#[tokio::test]
async fn postgres_state_uses_acquisition_gate_to_wire_legacy_connect_client() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://locks:locks@localhost/locks_test")
        .unwrap();
    let mut config = test_config();
    config.creator_authority_acquisition.enabled = true;

    let state = AppState::new_with_postgres_runtime(
        config,
        pool,
        test_creator_authority_cipher(),
        test_final_credential_cipher(),
    );

    let result = state
        .legacy_creator_connect_flow_client()
        .start_legacy_creator_connect_flow(&[])
        .await;
    assert_ne!(result, Err(ApplicationError::CreatorAuthorityUnavailable));
}

#[test]
fn legacy_connect_flow_generators_return_non_reused_secret_bearers() {
    let state = AppState::new_empty_in_memory(test_config());

    let first_flow_id = state
        .creator_connect_flow_id_generator()
        .generate_creator_connect_flow_id();
    let second_flow_id = state
        .creator_connect_flow_id_generator()
        .generate_creator_connect_flow_id();
    assert_ne!(first_flow_id, second_flow_id);

    let first_code = state
        .frontend_session_code_generator()
        .generate_frontend_session_code();
    let second_code = state
        .frontend_session_code_generator()
        .generate_frontend_session_code();
    assert_ne!(first_code.expose_code(), second_code.expose_code());

    let first_token = state
        .frontend_session_token_generator()
        .generate_frontend_session_token();
    let second_token = state
        .frontend_session_token_generator()
        .generate_frontend_session_token();
    assert_ne!(first_token.expose_token(), second_token.expose_token());
}

#[test]
fn ephemeral_state_has_no_postgres_pool_for_readiness() {
    let state = AppState::new_empty_in_memory(test_config());

    assert!(state.postgres_pool().is_none());
}

#[tokio::test]
async fn persisted_state_keeps_postgres_pool_for_readiness() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://locks:locks@localhost/locks_test")
        .unwrap();

    let state = AppState::new_with_postgres_runtime(
        test_config(),
        pool,
        test_creator_authority_cipher(),
        test_final_credential_cipher(),
    );

    assert!(state.postgres_pool().is_some());
}

#[tokio::test]
async fn persisted_state_composes_pubky_homeserver_creator_repositories() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://locks:locks@localhost/locks_test")
        .unwrap();

    let state = AppState::new_with_postgres_runtime(
        test_config(),
        pool,
        test_creator_authority_cipher(),
        test_final_credential_cipher(),
    );

    assert!(Arc::strong_count(state.content_locks()) >= 1);
    assert!(Arc::strong_count(state.guarded_resources()) >= 1);
    assert!(Arc::strong_count(state.lock_service_pointers()) >= 1);
    assert!(Arc::strong_count(state.entitlements()) >= 1);
}

#[tokio::test]
async fn task_id_generator_returns_random_v4_ids_instead_of_restart_local_sequence() {
    let generator = OsRandomTaskIdGenerator;

    let first = generator.generate_task_id().await.unwrap();
    let second = generator.generate_task_id().await.unwrap();

    assert_ne!(first, second);
    assert_eq!(&first.to_string()[14..15], "4");
    assert_eq!(&second.to_string()[14..15], "4");
}

#[test]
fn in_memory_state_has_rate_limiter_configured_from_runtime_config() {
    let state = AppState::new_empty_in_memory(test_config_with_rate_limit(true, 1, 60));
    let key = rate_limit_key();
    let now = datetime!(2026-06-03 12:00:00 UTC);

    assert!(
        state
            .verification_submission_rate_limiter()
            .check(&key, now)
            .allowed
    );
    assert!(
        !state
            .verification_submission_rate_limiter()
            .check(&key, now)
            .allowed
    );
}

#[tokio::test]
async fn postgres_state_has_rate_limiter_configured_from_runtime_config() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://locks:locks@localhost/locks_test")
        .unwrap();
    let state = AppState::new_with_postgres_runtime(
        test_config_with_rate_limit(true, 1, 60),
        pool,
        test_creator_authority_cipher(),
        test_final_credential_cipher(),
    );
    let key = rate_limit_key();
    let now = datetime!(2026-06-03 12:00:00 UTC);

    assert!(
        state
            .verification_submission_rate_limiter()
            .check(&key, now)
            .allowed
    );
    assert!(
        !state
            .verification_submission_rate_limiter()
            .check(&key, now)
            .allowed
    );
}

#[test]
fn pubky_client_constructor_follows_configured_network() {
    assert_eq!(
        pubky_client_constructor(PubkyNetwork::Mainnet),
        PubkyClientConstructor::Mainnet
    );
    assert_eq!(
        pubky_client_constructor(PubkyNetwork::Testnet),
        PubkyClientConstructor::Testnet
    );
}

#[test]
fn pubky_http_client_constructor_follows_configured_network() {
    assert_eq!(
        pubky_http_client_constructor(PubkyNetwork::Mainnet),
        PubkyHttpClientConstructor::Mainnet
    );
    assert_eq!(
        pubky_http_client_constructor(PubkyNetwork::Testnet),
        PubkyHttpClientConstructor::Testnet
    );
}

#[test]
fn testnet_creator_connect_uses_local_pubky_auth_relay() {
    assert!(pubky_auth_relay_for_network(PubkyNetwork::Mainnet).is_none());
    assert_eq!(
        pubky_auth_relay_for_network(PubkyNetwork::Testnet)
            .unwrap()
            .as_str(),
        "http://localhost:15412/inbox/"
    );
}

#[test]
fn disabled_runtime_rate_limiter_in_state_always_allows() {
    let state = AppState::new_empty_in_memory(test_config_with_rate_limit(false, 0, 0));
    let key = rate_limit_key();
    let now = datetime!(2026-06-03 12:00:00 UTC);

    for _ in 0..10 {
        assert!(
            state
                .verification_submission_rate_limiter()
                .check(&key, now)
                .allowed
        );
    }
}

fn test_final_credential_cipher() -> FinalCredentialCipher {
    FinalCredentialCipher::new([8; 32])
}

fn test_creator_authority_cipher() -> CreatorAuthoritySecretCipher {
    CreatorAuthoritySecretCipher::new([7; 32])
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
            worker_id: "test-worker".to_owned(),
        },
        runtime: RuntimeConfig {
            environment: RuntimeEnvironment::Development,
        },
        creator_authority_acquisition: crate::config::CreatorAuthorityAcquisitionConfig::default(),
        secrets: SecretsConfig::default(),
        logging: LoggingConfig::default(),
        pubky: PubkyConfig::default(),
        pkdns: crate::config::PkdnsConfig::default(),
        rate_limits: RateLimitsConfig::default(),
        content_locks: ContentLocksConfig::default(),
        deletion: crate::config::DeletionConfig::default(),
        deletion_worker: crate::config::DeletionWorkerConfig::default(),
        paykit: None,
    }
}

fn test_config_with_rate_limit(
    enabled: bool,
    max_requests: u32,
    window_seconds: u64,
) -> LockServerRuntimeConfig {
    let mut config = test_config();
    config.rate_limits = RateLimitsConfig {
        verification_submission: VerificationSubmissionRateLimitConfig {
            enabled,
            max_requests,
            window_seconds,
        },
    };
    config
}

fn rate_limit_key() -> VerificationSubmissionRateLimitKey {
    VerificationSubmissionRateLimitKey {
        client_address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        creator: CreatorPubky::from_str(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
        )
        .unwrap(),
    }
}
