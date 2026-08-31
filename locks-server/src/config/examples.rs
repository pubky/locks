use std::str::FromStr;

use base64::Engine;
use locks_core::ids::LockServerPubky;
use tempfile::tempdir;

use crate::config::{
    ConfigError, PubkyNetwork, RuntimeEnvironment, load_existing_config_from_path,
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
pkarr_relays = []
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
pkarr_relays = []
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
