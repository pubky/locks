use std::str::FromStr;

use base64::Engine;
use locks_core::ids::LockServerPubky;
use tempfile::tempdir;

use crate::config::{
    ConfigError, PaykitConnectionStateLookupRateLimitConfig, PubkyNetwork, RuntimeEnvironment,
    load_existing_config_from_path,
};

#[test]
fn parses_current_development_config_defaults_to_testnet_pubky_and_enabled_creator_authority() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
bind_addr = "127.0.0.1:3000"

[credentials]
lock_server_secret_key = "{}"
lock_server_public_key = "{}"
max_ttl_seconds = 900

[database]
url = "postgres://locks:locks@localhost/locks_test"
max_connections = 10
run_migrations_on_startup = true

[worker]
enabled = true
poll_interval_ms = 250
claim_timeout_seconds = 60
worker_id = "test-worker"

[runtime]
environment = "development"

[creator_authority_acquisition]
method = "legacy-connect"
frontend_session_ttl_seconds = 86400
frontend_session_code_ttl_seconds = 120

[creator_authority_acquisition.legacy_connect]
allowed_return_origins = ["http://localhost:3000"]

[secrets]
runtime_master_key_env = "PUBKY_LOCK_RUNTIME_MASTER_KEY"

[deletion]
retry_max_attempts = 10
retry_initial_backoff_seconds = 1
retry_max_backoff_seconds = 300
final_credential_issuance_window_seconds = 900
final_read_window_seconds = 900

[logging]
level = "info"

[pubky]

[pkdns]
public_ip = "127.0.0.1"
public_pubky_tls_port = 6287
public_icann_http_port = 80
icann_domain = "localhost"
key_republisher_interval_seconds = 3600

[rate_limits.verification_submission]
enabled = true
max_requests = 60
window_seconds = 60

[content_locks]
max_resource_bytes = 10000000
max_resources = 10
max_total_resource_bytes = 100000000
"#,
            secret_path.display(),
            public_key
        ),
    )
    .unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();

    assert_eq!(config.runtime.environment, RuntimeEnvironment::Development);
    assert_eq!(config.pubky.network, PubkyNetwork::Testnet);
    assert!(config.creator_authority_acquisition.enabled);
}

#[test]
fn parses_pubky_pkarr_relay_array_through_runtime_config_loader() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config_text = minimal_config(&secret_path, &public_key, "staging").replace(
        "network = \"testnet\"",
        r#"network = "mainnet"
resolution = "relay-only"
pkarr_relays = ["https://relay.example"]"#,
    );
    std::fs::write(&config_path, config_text).unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();

    assert_eq!(
        config.pubky.pkarr_relays,
        Some(vec!["https://relay.example/".to_owned()])
    );
}

#[test]
fn rejects_removed_pkdns_pkarr_relays() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("locks.toml");
    let config_text = minimal_config(&secret_path, &public_key, "staging").replace(
        "icann_domain = \"localhost\"",
        "icann_domain = \"localhost\"\npkarr_relays = [\"https://relay.example\"]",
    );
    std::fs::write(&config_path, config_text).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(error.to_string().contains("unknown field `pkarr_relays`"));
}

#[test]
fn parses_staging_environment_as_production_shaped_runtime_label() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        minimal_config(&secret_path, &public_key, "staging"),
    )
    .unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();

    assert_eq!(config.runtime.environment, RuntimeEnvironment::Staging);
}

#[test]
fn parses_optional_paykit_runtime_config() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "[content_locks]",
        "[paykit]\nserver_url = \"http://127.0.0.1:3001\"\nminimum_confirmations = 0\n\n[content_locks]",
    );
    std::fs::write(&config_path, config).unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();

    let paykit = config.paykit.expect("paykit config is present");
    assert_eq!(paykit.server_url, "http://127.0.0.1:3001");
    assert_eq!(paykit.minimum_confirmations, 0);
}

#[test]
fn omits_paykit_runtime_config_when_section_is_absent() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        minimal_config(&secret_path, &public_key, "development"),
    )
    .unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();

    assert_eq!(config.paykit, None);
}

#[test]
fn defaults_paykit_connection_state_lookup_admission_when_section_is_absent() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        minimal_config(&secret_path, &public_key, "development"),
    )
    .unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();

    assert_eq!(
        config.rate_limits.paykit_connection_state_lookup,
        PaykitConnectionStateLookupRateLimitConfig {
            max_requests: 60,
            window_seconds: 60,
            max_in_flight: 16,
            max_entries: 10_000,
            global_requests_per_second: 50,
            global_burst: 50,
        }
    );
}

#[test]
fn parses_custom_paykit_connection_state_lookup_admission() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "[content_locks]",
        "[rate_limits.paykit_connection_state_lookup]\nmax_requests = 7\nwindow_seconds = 11\nmax_in_flight = 3\nmax_entries = 101\nglobal_requests_per_second = 5\nglobal_burst = 9\n\n[content_locks]",
    );
    std::fs::write(&config_path, config).unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();

    assert_eq!(
        config.rate_limits.paykit_connection_state_lookup,
        PaykitConnectionStateLookupRateLimitConfig {
            max_requests: 7,
            window_seconds: 11,
            max_in_flight: 3,
            max_entries: 101,
            global_requests_per_second: 5,
            global_burst: 9,
        }
    );
}

#[test]
fn rejects_paykit_connection_state_lookup_concurrency_above_semaphore_limit() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let max_in_flight = tokio::sync::Semaphore::MAX_PERMITS + 1;
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "[content_locks]",
        &format!(
            "[rate_limits.paykit_connection_state_lookup]\nmax_requests = 60\nwindow_seconds = 60\nmax_in_flight = {max_in_flight}\n\n[content_locks]"
        ),
    );
    std::fs::write(&config_path, config).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(matches!(
        error,
        ConfigError::InvalidPaykitConnectionStateLookupMaxInFlight
    ));
}

#[test]
fn rejects_zero_paykit_connection_state_lookup_entry_cap() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "[content_locks]",
        "[rate_limits.paykit_connection_state_lookup]\nmax_entries = 0\n\n[content_locks]",
    );
    std::fs::write(&config_path, config).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(matches!(
        error,
        ConfigError::InvalidPaykitConnectionStateLookupMaxEntries
    ));
}

#[test]
fn rejects_zero_paykit_connection_state_lookup_global_rate() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "[content_locks]",
        "[rate_limits.paykit_connection_state_lookup]\nglobal_requests_per_second = 0\n\n[content_locks]",
    );
    std::fs::write(&config_path, config).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(matches!(
        error,
        ConfigError::InvalidPaykitConnectionStateLookupGlobalRequestsPerSecond
    ));
}

#[test]
fn rejects_zero_paykit_connection_state_lookup_global_burst() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "[content_locks]",
        "[rate_limits.paykit_connection_state_lookup]\nglobal_burst = 0\n\n[content_locks]",
    );
    std::fs::write(&config_path, config).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(matches!(
        error,
        ConfigError::InvalidPaykitConnectionStateLookupGlobalBurst
    ));
}

#[test]
fn rejects_invalid_paykit_server_url() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    for server_url in [
        "ftp://127.0.0.1:3001",
        "http://user:password@127.0.0.1:3001",
        "http://127.0.0.1:3001/",
        "http://127.0.0.1:3001/path",
        "http://127.0.0.1:3001?secret=query",
        "http://127.0.0.1:3001#fragment",
    ] {
        let config = minimal_config(&secret_path, &public_key, "development").replace(
            "[content_locks]",
            &format!(
                "[paykit]\nserver_url = \"{server_url}\"\nminimum_confirmations = 0\n\n[content_locks]"
            ),
        );
        std::fs::write(&config_path, config).unwrap();
        let error = load_existing_config_from_path(&config_path).unwrap_err();
        let message = error.to_string();
        assert_eq!(
            message,
            "paykit.server_url must be an exact HTTP(S) origin without credentials"
        );
        assert!(!message.contains(server_url));
    }
}

#[test]
fn rejects_paykit_when_enabled_worker_claim_timeout_does_not_exceed_request_timeout() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development")
        .replace("claim_timeout_seconds = 60", "claim_timeout_seconds = 20")
        .replace(
            "[content_locks]",
            "[paykit]\nserver_url = \"http://127.0.0.1:3001\"\nminimum_confirmations = 0\n\n[content_locks]",
        );
    std::fs::write(&config_path, config).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(matches!(
        error,
        ConfigError::InvalidPaykitWorkerClaimTimeout {
            request_timeout_seconds: 20
        }
    ));
}

#[test]
fn accepts_paykit_when_enabled_worker_claim_timeout_exceeds_request_timeout() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development")
        .replace("claim_timeout_seconds = 60", "claim_timeout_seconds = 21")
        .replace(
            "[content_locks]",
            "[paykit]\nserver_url = \"http://127.0.0.1:3001\"\nminimum_confirmations = 0\n\n[content_locks]",
        );
    std::fs::write(&config_path, config).unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();

    assert_eq!(config.worker.claim_timeout_seconds, 21);
    assert!(config.paykit.is_some());
}

#[test]
fn rejects_zero_worker_poll_interval() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    for worker_enabled in [true, false] {
        let config_path = temp_dir
            .path()
            .join(format!("config-{worker_enabled}.toml"));
        let config = minimal_config(&secret_path, &public_key, "development")
            .replace(
                "enabled = true\npoll_interval_ms",
                &format!("enabled = {worker_enabled}\npoll_interval_ms"),
            )
            .replace("poll_interval_ms = 250", "poll_interval_ms = 0");
        std::fs::write(&config_path, config).unwrap();

        let error = load_existing_config_from_path(&config_path).unwrap_err();

        assert!(matches!(error, ConfigError::InvalidWorkerPollInterval));
    }
}

#[test]
fn rejects_removed_creator_repositories_section() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        format!(
            "{}\n[creator_repositories]\nbackend = \"local-memory\"\n",
            minimal_config(&secret_path, &public_key, "development")
        ),
    )
    .unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(matches!(error, ConfigError::ParseConfig { .. }));
    assert!(error.to_string().contains("creator_repositories"));
}

#[test]
fn rejects_removed_runtime_mode_keys() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "environment = \"development\"",
        "mode = \"dev\"\nexpose_dev_completion_route = true",
    );
    std::fs::write(&config_path, config).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(matches!(error, ConfigError::ParseConfig { .. }));
    assert!(error.to_string().contains("mode"));
}

#[test]
fn rejects_wildcard_return_origin_in_production() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "production").replace(
        "allowed_return_origins = []",
        r#"allowed_return_origins = ["*"]"#,
    );
    std::fs::write(&config_path, config).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(matches!(
        error,
        ConfigError::WildcardReturnOriginInProduction
    ));
}

#[test]
fn allows_wildcard_return_origin_outside_production() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "staging").replace(
        "allowed_return_origins = []",
        r#"allowed_return_origins = ["*"]"#,
    );
    std::fs::write(&config_path, config).unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();

    assert_eq!(
        config
            .creator_authority_acquisition
            .legacy_connect
            .allowed_return_origins,
        vec!["*".to_owned()]
    );
}

#[test]
fn accepts_closed_deletion_defaults_and_runtime_master_key_contract() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        minimal_config(&secret_path, &public_key, "development"),
    )
    .unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();
    assert_eq!(
        config.secrets.runtime_master_key_env,
        "PUBKY_LOCK_RUNTIME_MASTER_KEY"
    );
    assert_eq!(config.deletion.retry_max_attempts, 10);
    assert_eq!(config.deletion.retry_initial_backoff_seconds, 1);
    assert_eq!(config.deletion.retry_max_backoff_seconds, 300);
    assert_eq!(
        config.deletion.final_credential_issuance_window_seconds,
        900
    );
    assert_eq!(config.deletion.final_read_window_seconds, 900);
}

#[test]
fn rejects_retired_creator_authority_key_env() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "runtime_master_key_env = \"PUBKY_LOCK_RUNTIME_MASTER_KEY\"",
        "creator_authority_key_env = \"PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY\"",
    );
    std::fs::write(&config_path, config).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();
    assert!(matches!(error, ConfigError::ParseConfig { .. }));
    assert!(error.to_string().contains("creator_authority_key_env"));
}

#[test]
fn rejects_zero_or_inverted_deletion_retry_contract() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);

    for (name, from, to, expected) in [
        (
            "zero-attempts",
            "retry_max_attempts = 10",
            "retry_max_attempts = 0",
            ConfigError::InvalidDeletionRetry,
        ),
        (
            "zero-initial",
            "retry_initial_backoff_seconds = 1",
            "retry_initial_backoff_seconds = 0",
            ConfigError::InvalidDeletionRetry,
        ),
        (
            "inverted",
            "retry_initial_backoff_seconds = 1",
            "retry_initial_backoff_seconds = 301",
            ConfigError::InvalidDeletionRetryBackoffOrder,
        ),
    ] {
        let config_path = temp_dir.path().join(format!("{name}.toml"));
        let config = minimal_config(&secret_path, &public_key, "development").replace(from, to);
        std::fs::write(&config_path, config).unwrap();

        let error = load_existing_config_from_path(&config_path).unwrap_err();
        assert_eq!(
            std::mem::discriminant(&error),
            std::mem::discriminant(&expected)
        );
    }
}

#[test]
fn accepts_maximum_deletion_windows_and_rejects_out_of_range_values() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let base = minimal_config(&secret_path, &public_key, "development");

    let max_path = temp_dir.path().join("max.toml");
    let max = base
        .replace(
            "final_credential_issuance_window_seconds = 900",
            "final_credential_issuance_window_seconds = 3600",
        )
        .replace(
            "final_read_window_seconds = 900",
            "final_read_window_seconds = 3600",
        );
    std::fs::write(&max_path, max).unwrap();
    assert!(load_existing_config_from_path(&max_path).is_ok());

    for (name, from, to) in [
        (
            "zero-issuance",
            "final_credential_issuance_window_seconds = 900",
            "final_credential_issuance_window_seconds = 0",
        ),
        (
            "long-issuance",
            "final_credential_issuance_window_seconds = 900",
            "final_credential_issuance_window_seconds = 3601",
        ),
        (
            "zero-read",
            "final_read_window_seconds = 900",
            "final_read_window_seconds = 0",
        ),
        (
            "long-read",
            "final_read_window_seconds = 900",
            "final_read_window_seconds = 3601",
        ),
    ] {
        let config_path = temp_dir.path().join(format!("{name}.toml"));
        std::fs::write(&config_path, base.replace(from, to)).unwrap();
        assert!(matches!(
            load_existing_config_from_path(&config_path).unwrap_err(),
            ConfigError::InvalidDeletionCredentialWindow
        ));
    }
}

fn test_identity(secret_path: &std::path::Path) -> LockServerPubky {
    let keypair = pubky_common::crypto::Keypair::from_secret(&[9; 32]);
    let public_key = LockServerPubky::from_str(&keypair.public_key().to_string()).unwrap();
    std::fs::write(
        secret_path,
        format!(
            "keypair-seed:{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(keypair.secret())
        ),
    )
    .unwrap();
    public_key
}

fn minimal_config(
    secret_path: &std::path::Path,
    public_key: &LockServerPubky,
    environment: &str,
) -> String {
    format!(
        r#"
bind_addr = "127.0.0.1:3000"

[credentials]
lock_server_secret_key = "{}"
lock_server_public_key = "{}"
max_ttl_seconds = 900

[database]
url = "postgres://locks:locks@localhost/locks_test"
max_connections = 10
run_migrations_on_startup = true

[worker]
enabled = true
poll_interval_ms = 250
claim_timeout_seconds = 60
worker_id = "test-worker"

[runtime]
environment = "{}"

[creator_authority_acquisition]
enabled = true
method = "legacy-connect"
frontend_session_ttl_seconds = 86400
frontend_session_code_ttl_seconds = 120

[creator_authority_acquisition.legacy_connect]
allowed_return_origins = []

[secrets]
runtime_master_key_env = "PUBKY_LOCK_RUNTIME_MASTER_KEY"

[deletion]
retry_max_attempts = 10
retry_initial_backoff_seconds = 1
retry_max_backoff_seconds = 300
final_credential_issuance_window_seconds = 900
final_read_window_seconds = 900

[logging]
level = "info"

[pubky]
network = "testnet"

[pkdns]
public_ip = "127.0.0.1"
public_pubky_tls_port = 6287
public_icann_http_port = 80
icann_domain = "localhost"
key_republisher_interval_seconds = 3600

[rate_limits.verification_submission]
enabled = true
max_requests = 60
window_seconds = 60

[content_locks]
max_resource_bytes = 10000000
max_resources = 10
max_total_resource_bytes = 100000000
"#,
        secret_path.display(),
        public_key,
        environment
    )
}
