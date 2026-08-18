use std::path::{Path, PathBuf};

use super::defaults::{DEFAULT_CONFIG_FILE, DEFAULT_SECRET_FILE, DEFAULT_SERVICE_HOME};
use super::raw::RawConfig;
use super::schema::{
    ConfigError, ConfigPathResolution, ContentLocksConfig, CreatorAuthorityAcquisitionConfig,
    DatabaseConfig, DeletionConfig, DeletionWorkerConfig, LockServerCredentialsConfig,
    LockServerRuntimeConfig, LoggingConfig, PaykitConfig, PkdnsConfig, PubkyConfig,
    RateLimitsConfig, RuntimeConfig, RuntimeEnvironment, SecretsConfig, WorkerConfig,
};
use super::secrets::{LockServerIdentityProvider, parse_lock_server_keypair_seed};

pub fn resolve_config_path(
    custom_config_path: Option<PathBuf>,
    home_dir: &Path,
) -> Result<ConfigPathResolution, ConfigError> {
    if let Some(config_path) = custom_config_path {
        if !config_path.exists() {
            return Err(ConfigError::MissingCustomConfig(config_path));
        }
        return Ok(ConfigPathResolution::LoadExisting { config_path });
    }

    let service_home = home_dir.join(DEFAULT_SERVICE_HOME);
    let config_path = service_home.join(DEFAULT_CONFIG_FILE);
    let secret_path = service_home.join(DEFAULT_SECRET_FILE);

    if config_path.exists() {
        Ok(ConfigPathResolution::LoadExisting { config_path })
    } else {
        Ok(ConfigPathResolution::InitializeDefault {
            config_path,
            service_home,
            secret_path,
        })
    }
}

pub fn load_existing_config_from_path(
    config_path: &Path,
) -> Result<LockServerRuntimeConfig, ConfigError> {
    let config_text =
        std::fs::read_to_string(config_path).map_err(|source| ConfigError::ReadConfig {
            path: config_path.to_path_buf(),
            source,
        })?;
    let raw: RawConfig =
        toml::from_str(&config_text).map_err(|source| ConfigError::ParseConfig {
            path: config_path.to_path_buf(),
            source,
        })?;
    let config_dir = config_path
        .parent()
        .ok_or_else(|| ConfigError::ConfigPathHasNoParent(config_path.to_path_buf()))?;
    raw.into_runtime_config(config_dir)
}

pub fn load_or_initialize_config(
    custom_config_path: Option<PathBuf>,
    home_dir: &Path,
    identity_provider: &impl LockServerIdentityProvider,
) -> Result<LockServerRuntimeConfig, ConfigError> {
    match resolve_config_path(custom_config_path, home_dir)? {
        ConfigPathResolution::LoadExisting { config_path } => {
            let config = load_existing_config_from_path(&config_path)?;
            validate_existing_config_secret(&config, identity_provider)?;
            Ok(config)
        }
        ConfigPathResolution::InitializeDefault {
            config_path,
            service_home,
            secret_path,
        } => initialize_default_config(config_path, service_home, secret_path, identity_provider),
    }
}

fn validate_existing_config_secret(
    config: &LockServerRuntimeConfig,
    identity_provider: &impl LockServerIdentityProvider,
) -> Result<(), ConfigError> {
    let secret_path = &config.credentials.lock_server_secret_key;
    if !secret_path.exists() {
        return Err(ConfigError::MissingConfiguredSecret(secret_path.clone()));
    }

    let derived_public_key = identity_provider.derive_public_key(secret_path)?;
    if derived_public_key != config.credentials.lock_server_public_key {
        return Err(ConfigError::PublicKeyMismatch);
    }

    if config.paykit.is_some() {
        validate_paykit_signing_seed(secret_path)?;
    }

    Ok(())
}

fn validate_paykit_signing_seed(secret_path: &Path) -> Result<(), ConfigError> {
    let secret =
        std::fs::read_to_string(secret_path).map_err(|source| ConfigError::DerivePublicKey {
            path: secret_path.to_path_buf(),
            message: source.to_string(),
        })?;
    parse_lock_server_keypair_seed(&secret)
        .map(|_| ())
        .map_err(|_| ConfigError::InvalidPaykitSigningSeed)
}

fn initialize_default_config(
    config_path: PathBuf,
    service_home: PathBuf,
    secret_path: PathBuf,
    identity_provider: &impl LockServerIdentityProvider,
) -> Result<LockServerRuntimeConfig, ConfigError> {
    std::fs::create_dir_all(&service_home).map_err(|source| ConfigError::CreateServiceHome {
        path: service_home.clone(),
        source,
    })?;

    let public_key = identity_provider.generate_secret(&secret_path)?;
    let config = LockServerRuntimeConfig {
        bind_addr: "127.0.0.1:3000".parse().expect("static bind addr is valid"),
        credentials: LockServerCredentialsConfig {
            lock_server_secret_key: secret_path,
            lock_server_public_key: public_key,
            max_ttl_seconds: 900,
        },
        database: DatabaseConfig {
            url: std::env::var("PUBKY_LOCK_DATABASE_URL").map_err(|_| {
                ConfigError::MissingDatabaseUrlEnv("PUBKY_LOCK_DATABASE_URL".to_owned())
            })?,
            max_connections: 10,
            run_migrations_on_startup: true,
        },
        worker: WorkerConfig {
            enabled: true,
            poll_interval_ms: 250,
            claim_timeout_seconds: 60,
            worker_id: "default-worker".to_owned(),
        },
        runtime: RuntimeConfig {
            environment: RuntimeEnvironment::Development,
        },
        creator_authority_acquisition: CreatorAuthorityAcquisitionConfig::default(),
        secrets: SecretsConfig::default(),
        logging: LoggingConfig::default(),
        pubky: PubkyConfig::default(),
        pkdns: PkdnsConfig::default(),
        rate_limits: RateLimitsConfig::default(),
        content_locks: ContentLocksConfig::default(),
        deletion: DeletionConfig::default(),
        deletion_worker: DeletionWorkerConfig::default(),
        paykit: Some(PaykitConfig {
            server_url: "http://127.0.0.1:3001/".to_owned(),
            minimum_confirmations: 0,
        }),
    };
    let config_text = format!(
        r#"# Lock Server runtime configuration. Generated on first run; edit intentionally per environment.
bind_addr = "{}" # Socket address the HTTP server binds. Use 127.0.0.1:3000 for local-only; use 0.0.0.0:<port> only behind a trusted proxy/firewall.

[credentials]
lock_server_secret_key = "{}" # Path to the Lock Server signing secret generated beside this config. Keep private; changing it changes the server identity.
lock_server_public_key = "{}" # Public Pubky derived from lock_server_secret_key. Published via /.well-known and PKARR; must match the secret file.
max_ttl_seconds = {} # Maximum signed credential TTL in seconds. Lower limits reduce replay window; higher values keep viewer credentials valid longer.

[database]
url_env = "PUBKY_LOCK_DATABASE_URL" # Environment variable that contains the Postgres URL. Prefer this over inline database.url so secrets stay out of config.
max_connections = {} # Maximum Postgres connections for this process. Raise for more concurrency; lower for small local/dev databases. Must be > 0.
run_migrations_on_startup = {} # true runs embedded DB migrations at startup; false expects migrations handled externally before startup.

[worker]
enabled = {} # true runs the in-process verification worker; false leaves submitted tasks pending unless another worker process handles them.
poll_interval_ms = {} # Worker polling interval in milliseconds. Must be > 0; lower is more responsive but creates more DB traffic.
claim_timeout_seconds = {} # Seconds before another worker can reclaim a stuck task. Too low risks duplicate work; too high delays recovery.
worker_id = "{}" # Stable ID recorded in task claims/logs. Use a unique value per worker process in shared deployments.

[runtime]
environment = "development" # One of: development, staging, production. development enables dev-only verification support; staging/production are production-shaped.

[paykit]
server_url = "{}" # Paykit Server base URL used by paykit-payment locks. Local dev default expects Paykit Server on 127.0.0.1:3001.
minimum_confirmations = {} # Global confirmation threshold for payment satisfaction. 0 allows detected, amount-matched payments before block confirmation.

[creator_authority_acquisition]
enabled = {} # true mounts hosted creator connect/session routes; false disables browser acquisition of creator authority.
method = "legacy-connect" # Currently only legacy-connect is supported. It uses Pubky auth via Ring/signer and stores creator authority server-side.
frontend_session_ttl_seconds = {} # Locks-local browser session lifetime after connect. Higher values reduce reauth; lower values reduce stolen-token lifetime.
frontend_session_code_ttl_seconds = {} # One-time callback code lifetime. Keep short; browser must exchange it quickly for a frontend session.

[creator_authority_acquisition.legacy_connect]
allowed_return_origins = [] # Origins allowed to receive auth callback codes, e.g. ["https://pubky.app"]. Empty rejects all /connect return_to values; ["*"] is dev-only and unsafe for staging/prod.

[secrets]
runtime_master_key_env = "{}" # Environment variable containing the 32-byte base64url runtime master key. Domain-separated keys encrypt creator authority and final credentials. Rotating requires data migration.

[deletion]
retry_max_attempts = {} # Maximum attempts per deletion phase before stable retry_exhausted failure.
retry_initial_backoff_seconds = {} # Initial durable retry delay; must be positive and no greater than the maximum.
retry_max_backoff_seconds = {} # Maximum durable retry delay in seconds.
final_credential_issuance_window_seconds = {} # Bounded final-credential issuance window; must be 1..=3600.
final_read_window_seconds = {} # Bounded one-read-per-resource window; must be 1..=3600.

[deletion_worker]
enabled = {} # true enables the in-process deletion worker; false leaves deletion jobs for another worker process.
poll_interval_ms = {} # Deletion queue polling interval in milliseconds. Must be > 0.
claim_timeout_seconds = {} # Seconds before another deletion worker may reclaim a stuck job. Must be > 0.
shutdown_timeout_seconds = {} # Maximum graceful deletion-worker shutdown wait in seconds. Must be > 0.
worker_id = "{}" # Stable, non-blank deletion-worker identity. Use a unique value per worker process.

[logging]
level = "{}" # Tracing level/filter, e.g. error, warn, info, debug, trace, or EnvFilter syntax. Higher verbosity may expose operational detail in logs.

[pubky]
network = "testnet" # One of: testnet, mainnet. Selects Pubky SDK network defaults; testnet expects local pubky-testnet services.

[pkdns]
public_ip = "{}" # Public IP advertised in PKARR/PKDNS records. Local default is loopback; production must use the externally reachable address.
public_pubky_tls_port = {} # Public PubkyTLS port advertised for the Lock Server. Set to the externally reachable TLS port, or omit only if unsupported by config policy.
public_icann_http_port = {} # Public HTTP port advertised for ICANN/HTTP access. Use the proxy/listener port clients reach, commonly 80 or 443.
icann_domain = "{}" # ICANN DNS name advertised for HTTP access. Local default is localhost; production should be the public hostname.
pkarr_relays = [] # Optional PKARR relay URLs to publish through. Empty uses SDK/default behavior; set explicit relays for controlled staging/prod publication.
key_republisher_interval_seconds = {} # How often the server republishes identity records. Lower improves recovery from relay loss; higher reduces background traffic.

[rate_limits.verification_submission]
enabled = {} # true limits proof-bundle submissions per creator/client window; false disables this abuse control.
max_requests = {} # Maximum verification submissions allowed per rate-limit window.
window_seconds = {} # Rate-limit window size in seconds.

[content_locks]
max_resource_bytes = {} # Maximum bytes for one guarded resource upload. Raise for larger files; lower to cap memory/storage exposure.
max_resources = {} # Maximum number of resources per content lock. Raise for complex content; lower to keep lock evaluation small.
max_total_resource_bytes = {} # Maximum combined bytes across resources in one content lock. Protects storage/proxy workload.
"#,
        config.bind_addr,
        config.credentials.lock_server_secret_key.display(),
        config.credentials.lock_server_public_key,
        config.credentials.max_ttl_seconds,
        config.database.max_connections,
        config.database.run_migrations_on_startup,
        config.worker.enabled,
        config.worker.poll_interval_ms,
        config.worker.claim_timeout_seconds,
        config.worker.worker_id,
        config
            .paykit
            .as_ref()
            .expect("generated config enables paykit")
            .server_url,
        config
            .paykit
            .as_ref()
            .expect("generated config enables paykit")
            .minimum_confirmations,
        config.creator_authority_acquisition.enabled,
        config
            .creator_authority_acquisition
            .frontend_session_ttl_seconds,
        config
            .creator_authority_acquisition
            .frontend_session_code_ttl_seconds,
        config.secrets.runtime_master_key_env,
        config.deletion.retry_max_attempts,
        config.deletion.retry_initial_backoff_seconds,
        config.deletion.retry_max_backoff_seconds,
        config.deletion.final_credential_issuance_window_seconds,
        config.deletion.final_read_window_seconds,
        config.deletion_worker.enabled,
        config.deletion_worker.poll_interval_ms,
        config.deletion_worker.claim_timeout_seconds,
        config.deletion_worker.shutdown_timeout_seconds,
        config.deletion_worker.worker_id,
        config.logging.level,
        config.pkdns.public_ip,
        config.pkdns.public_pubky_tls_port.unwrap_or(6287),
        config.pkdns.public_icann_http_port.unwrap_or(80),
        config.pkdns.icann_domain.as_deref().unwrap_or("localhost"),
        config.pkdns.key_republisher_interval_seconds,
        config.rate_limits.verification_submission.enabled,
        config.rate_limits.verification_submission.max_requests,
        config.rate_limits.verification_submission.window_seconds,
        config.content_locks.max_resource_bytes,
        config.content_locks.max_resources,
        config.content_locks.max_total_resource_bytes
    );
    std::fs::write(&config_path, config_text).map_err(|source| {
        ConfigError::WriteGeneratedConfig {
            path: config_path,
            source,
        }
    })?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FilesystemLockServerIdentityProvider;
    use tempfile::tempdir;

    #[test]
    fn generated_default_config_comments_every_setting_and_remains_parseable() {
        let home = tempdir().unwrap();
        unsafe {
            std::env::set_var(
                "PUBKY_LOCK_DATABASE_URL",
                "postgres://locks:locks@localhost/locks_test",
            );
        }

        let generated =
            load_or_initialize_config(None, home.path(), &FilesystemLockServerIdentityProvider)
                .unwrap();
        let config_path = home.path().join(".pubky-lock/config.toml");
        let config_text = std::fs::read_to_string(&config_path).unwrap();

        for line in config_text.lines().filter(|line| line.contains(" = ")) {
            assert!(line.contains(" # "), "missing inline comment: {line}");
        }

        let parsed = load_existing_config_from_path(&config_path).unwrap();

        assert_eq!(parsed, generated);
        assert_eq!(
            generated
                .paykit
                .expect("generated paykit config")
                .server_url,
            "http://127.0.0.1:3001/"
        );
        assert!(config_text.contains("[paykit]"));
        assert!(config_text.contains("server_url = \"http://127.0.0.1:3001/\""));
        assert!(config_text.contains("minimum_confirmations = 0"));
    }

    #[test]
    fn paykit_config_requires_signing_seed_secret_on_startup() {
        let home = tempdir().unwrap();
        unsafe {
            std::env::set_var(
                "PUBKY_LOCK_DATABASE_URL",
                "postgres://locks:locks@localhost/locks_test",
            );
        }
        let generated =
            load_or_initialize_config(None, home.path(), &FilesystemLockServerIdentityProvider)
                .unwrap();
        let secret_path = home.path().join(".pubky-lock/secret.sess");
        std::fs::write(
            &secret_path,
            format!(
                "{}:legacy-session-secret",
                generated.credentials.lock_server_public_key
            ),
        )
        .unwrap();

        let error =
            load_or_initialize_config(None, home.path(), &FilesystemLockServerIdentityProvider)
                .unwrap_err();

        assert!(matches!(error, ConfigError::InvalidPaykitSigningSeed));
    }
}
