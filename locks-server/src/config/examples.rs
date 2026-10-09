use std::str::FromStr;

use base64::Engine;
use locks_core::ids::LockServerPubky;
use tempfile::tempdir;

use crate::config::{
    ConfigError, CreatorAuthorityAcquisitionMethod, MAX_TRUSTED_PROXY_HOPS,
    PaykitConnectionStateLookupRateLimitConfig, PubkyNetwork, RateLimitsConfig, RuntimeEnvironment,
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
creator_authority_key_env = "PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY"

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
    assert_eq!(config.worker.paykit_payment_retry_interval_seconds.get(), 3);
}

#[test]
fn parses_custom_paykit_payment_retry_interval() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "poll_interval_ms = 250",
        "poll_interval_ms = 250\npaykit_payment_retry_interval_seconds = 7",
    );
    std::fs::write(&config_path, config).unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();

    assert_eq!(config.worker.paykit_payment_retry_interval_seconds.get(), 7);
}

#[test]
fn rejects_zero_paykit_payment_retry_interval() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "poll_interval_ms = 250",
        "poll_interval_ms = 250\npaykit_payment_retry_interval_seconds = 0",
    );
    std::fs::write(&config_path, config).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(matches!(
        error,
        ConfigError::InvalidPaykitPaymentRetryInterval
    ));
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
fn defaults_trusted_proxy_hops_to_zero_when_absent() {
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

    assert_eq!(config.rate_limits.trusted_proxy_hops, 0);
}

#[test]
fn parses_trusted_proxy_hops_up_to_maximum() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    for trusted_proxy_hops in [0, 1, 2, MAX_TRUSTED_PROXY_HOPS] {
        let config = minimal_config(&secret_path, &public_key, "development").replace(
            "[rate_limits.verification_submission]",
            &format!(
                "[rate_limits]\ntrusted_proxy_hops = {trusted_proxy_hops}\n\n[rate_limits.verification_submission]"
            ),
        );
        std::fs::write(&config_path, config).unwrap();

        let config = load_existing_config_from_path(&config_path).unwrap();

        assert_eq!(config.rate_limits.trusted_proxy_hops, trusted_proxy_hops);
    }
}

#[test]
fn rejects_trusted_proxy_hops_above_maximum() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "[rate_limits.verification_submission]",
        &format!(
            "[rate_limits]\ntrusted_proxy_hops = {}\n\n[rate_limits.verification_submission]",
            MAX_TRUSTED_PROXY_HOPS + 1
        ),
    );
    std::fs::write(&config_path, config).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(matches!(
        error,
        ConfigError::InvalidTrustedProxyHops {
            max: MAX_TRUSTED_PROXY_HOPS
        }
    ));
    assert_eq!(
        error.to_string(),
        "rate_limits.trusted_proxy_hops must not exceed 8"
    );
}

#[test]
fn rejects_negative_trusted_proxy_hops() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development").replace(
        "[rate_limits.verification_submission]",
        "[rate_limits]\ntrusted_proxy_hops = -1\n\n[rate_limits.verification_submission]",
    );
    std::fs::write(&config_path, config).unwrap();

    let error = load_existing_config_from_path(&config_path).unwrap_err();

    assert!(matches!(error, ConfigError::ParseConfig { .. }));
}

#[test]
fn parses_dev_postgres_example_config() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = include_str!("../../config/example.dev.postgres.toml")
        .replace(
            "lock_server_secret_key = \"~/.pubky-lock/secret.sess\"",
            &format!("lock_server_secret_key = \"{}\"", secret_path.display()),
        )
        .replace(
            "lock_server_public_key = \"<derived-on-first-run>\"",
            &format!("lock_server_public_key = \"{public_key}\""),
        )
        .replace(
            "url_env = \"PUBKY_LOCK_DATABASE_URL\"",
            "url = \"postgres://locks:locks@localhost/locks_test\"",
        );
    std::fs::write(&config_path, config).unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();

    assert_eq!(config.rate_limits, RateLimitsConfig::default());
    assert_eq!(config.runtime.environment, RuntimeEnvironment::Development);
    assert!(config.paykit.is_some());
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
fn parses_grant_connect_configuration() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "production")
        .replace("method = \"legacy-connect\"", "method = \"grant-connect\"")
        + r#"
[creator_authority_acquisition.grant_connect]
client_id = "locks.example"
allowed_return_origins = ["https://pubky.app"]
"#;
    std::fs::write(&config_path, config).unwrap();

    let config = load_existing_config_from_path(&config_path).unwrap();
    let grant = config.creator_authority_acquisition.grant_connect.unwrap();

    assert_eq!(
        config.creator_authority_acquisition.method,
        CreatorAuthorityAcquisitionMethod::GrantConnect
    );
    assert_eq!(grant.client_id, "locks.example");
    assert_eq!(grant.allowed_return_origins, ["https://pubky.app"]);
}

#[test]
fn grant_connect_requires_grant_configuration() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development")
        .replace("method = \"legacy-connect\"", "method = \"grant-connect\"");
    std::fs::write(&config_path, config).unwrap();

    assert!(matches!(
        load_existing_config_from_path(&config_path).unwrap_err(),
        ConfigError::MissingGrantConnectConfig
    ));
}

#[test]
fn grant_connect_requires_nonempty_allowed_return_origins() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "development")
        .replace("method = \"legacy-connect\"", "method = \"grant-connect\"")
        .replace(
            "frontend_session_code_ttl_seconds = 120",
            "frontend_session_code_ttl_seconds = 120\nallowed_return_origins = [\"https://legacy.example\"]",
        )
        + r#"
[creator_authority_acquisition.grant_connect]
client_id = "locks.example"
allowed_return_origins = []
"#;
    std::fs::write(&config_path, config).unwrap();

    assert!(matches!(
        load_existing_config_from_path(&config_path).unwrap_err(),
        ConfigError::EmptyGrantConnectAllowedReturnOrigins
    ));
}

#[test]
fn grant_connect_rejects_malformed_client_ids() {
    for client_id in [
        "https://locks.example",
        "locks.example/path",
        "locks.example:8443",
        "user@locks.example",
        "locks example",
    ] {
        let temp_dir = tempdir().unwrap();
        let secret_path = temp_dir.path().join("secret.sess");
        let public_key = test_identity(&secret_path);
        let config_path = temp_dir.path().join("config.toml");
        let config = minimal_config(&secret_path, &public_key, "development")
            .replace("method = \"legacy-connect\"", "method = \"grant-connect\"")
            + &format!(
                r#"
[creator_authority_acquisition.grant_connect]
client_id = "{client_id}"
allowed_return_origins = ["https://pubky.app"]
"#,
            );
        std::fs::write(&config_path, config).unwrap();

        assert!(matches!(
            load_existing_config_from_path(&config_path).unwrap_err(),
            ConfigError::InvalidGrantConnectClientId(value) if value == client_id
        ));
    }
}

#[test]
fn grant_connect_rejects_wildcard_return_origin_in_production() {
    let temp_dir = tempdir().unwrap();
    let secret_path = temp_dir.path().join("secret.sess");
    let public_key = test_identity(&secret_path);
    let config_path = temp_dir.path().join("config.toml");
    let config = minimal_config(&secret_path, &public_key, "production")
        .replace("method = \"legacy-connect\"", "method = \"grant-connect\"")
        + r#"
[creator_authority_acquisition.grant_connect]
client_id = "locks.example"
allowed_return_origins = ["*"]
"#;
    std::fs::write(&config_path, config).unwrap();

    assert!(matches!(
        load_existing_config_from_path(&config_path).unwrap_err(),
        ConfigError::WildcardReturnOriginInProduction
    ));
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
creator_authority_key_env = "PUBKY_LOCK_CREATOR_AUTH_ENCRYPTION_KEY"

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
