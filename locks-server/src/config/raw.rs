use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use locks_core::ids::LockServerPubky;
use serde::Deserialize;
use tracing_subscriber::EnvFilter;
use url::Url;

use super::defaults::{
    DEFAULT_DELETION_RETRY_INITIAL_BACKOFF_SECONDS, DEFAULT_DELETION_RETRY_MAX_ATTEMPTS,
    DEFAULT_DELETION_RETRY_MAX_BACKOFF_SECONDS, DEFAULT_DELETION_WORKER_CLAIM_TIMEOUT_SECONDS,
    DEFAULT_DELETION_WORKER_ID, DEFAULT_DELETION_WORKER_POLL_INTERVAL_MS,
    DEFAULT_DELETION_WORKER_SHUTDOWN_TIMEOUT_SECONDS,
    DEFAULT_FINAL_CREDENTIAL_ISSUANCE_WINDOW_SECONDS, DEFAULT_FINAL_READ_WINDOW_SECONDS,
    DEFAULT_RUNTIME_MASTER_KEY_ENV, PUBLIC_KEY_PLACEHOLDER,
};
use super::schema::{
    ConfigError, ContentLocksConfig, CreatorAuthorityAcquisitionConfig,
    CreatorAuthorityAcquisitionMethod, DatabaseConfig, DeletionConfig, DeletionWorkerConfig,
    LegacyConnectAcquisitionConfig, LockServerCredentialsConfig, LockServerRuntimeConfig,
    LoggingConfig, MAX_DELETION_CREDENTIAL_WINDOW_SECONDS, PAYKIT_REQUEST_TIMEOUT_SECONDS,
    PaykitConfig, PkdnsConfig, PubkyConfig, PubkyNetwork, RateLimitsConfig, RuntimeConfig,
    RuntimeEnvironment, SecretsConfig, VerificationSubmissionRateLimitConfig, WorkerConfig,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawConfig {
    bind_addr: SocketAddr,
    credentials: RawCredentialsConfig,
    database: RawDatabaseConfig,
    worker: RawWorkerConfig,
    runtime: RawRuntimeConfig,
    #[serde(default)]
    creator_authority_acquisition: RawCreatorAuthorityAcquisitionConfig,
    #[serde(default)]
    secrets: RawSecretsConfig,
    #[serde(default)]
    logging: RawLoggingConfig,
    #[serde(default)]
    pubky: RawPubkyConfig,
    #[serde(default)]
    pkdns: RawPkdnsConfig,
    #[serde(default)]
    rate_limits: RawRateLimitsConfig,
    #[serde(default)]
    content_locks: RawContentLocksConfig,
    #[serde(default)]
    deletion: RawDeletionConfig,
    #[serde(default)]
    deletion_worker: RawDeletionWorkerConfig,
    #[serde(default)]
    paykit: Option<RawPaykitConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPaykitConfig {
    server_url: String,
    minimum_confirmations: u32,
}

impl RawPaykitConfig {
    fn into_paykit_config(self) -> Result<PaykitConfig, ConfigError> {
        let parsed =
            Url::parse(&self.server_url).map_err(|_| ConfigError::InvalidPaykitServerUrl)?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || parsed.cannot_be_a_base()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.origin().ascii_serialization() != self.server_url
        {
            return Err(ConfigError::InvalidPaykitServerUrl);
        }
        Ok(PaykitConfig {
            server_url: self.server_url,
            minimum_confirmations: self.minimum_confirmations,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPubkyConfig {
    #[serde(default = "default_pubky_network")]
    network: PubkyNetwork,
}

impl Default for RawPubkyConfig {
    fn default() -> Self {
        Self {
            network: default_pubky_network(),
        }
    }
}

impl RawPubkyConfig {
    fn into_pubky_config(self) -> PubkyConfig {
        PubkyConfig {
            network: self.network,
        }
    }
}

fn default_pubky_network() -> PubkyNetwork {
    PubkyNetwork::Testnet
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPkdnsConfig {
    #[serde(default = "default_pkdns_public_ip")]
    public_ip: IpAddr,
    #[serde(default)]
    public_pubky_tls_port: Option<u16>,
    #[serde(default)]
    public_icann_http_port: Option<u16>,
    #[serde(default)]
    icann_domain: Option<String>,
    #[serde(default)]
    pkarr_relays: Vec<String>,
    #[serde(default = "default_key_republisher_interval_seconds")]
    key_republisher_interval_seconds: u64,
}

impl Default for RawPkdnsConfig {
    fn default() -> Self {
        Self {
            public_ip: default_pkdns_public_ip(),
            public_pubky_tls_port: Some(6287),
            public_icann_http_port: Some(80),
            icann_domain: Some("localhost".to_owned()),
            pkarr_relays: Vec::new(),
            key_republisher_interval_seconds: default_key_republisher_interval_seconds(),
        }
    }
}

impl RawPkdnsConfig {
    fn into_pkdns_config(self) -> Result<PkdnsConfig, ConfigError> {
        if self.key_republisher_interval_seconds == 0 {
            return Err(ConfigError::InvalidPkarrRepublisherInterval);
        }
        Ok(PkdnsConfig {
            public_ip: self.public_ip,
            public_pubky_tls_port: self.public_pubky_tls_port,
            public_icann_http_port: self.public_icann_http_port,
            icann_domain: self.icann_domain,
            pkarr_relays: self
                .pkarr_relays
                .into_iter()
                .map(normalize_pkarr_relay_url)
                .collect::<Result<Vec<_>, _>>()?,
            key_republisher_interval_seconds: self.key_republisher_interval_seconds,
        })
    }
}

fn normalize_pkarr_relay_url(value: String) -> Result<String, ConfigError> {
    let parsed =
        Url::parse(&value).map_err(|_| ConfigError::InvalidPkarrRelayUrl(value.clone()))?;
    match parsed.scheme() {
        "http" | "https" => Ok(parsed.to_string()),
        _ => Err(ConfigError::InvalidPkarrRelayUrl(value)),
    }
}

fn default_pkdns_public_ip() -> IpAddr {
    "127.0.0.1".parse().expect("static loopback IP is valid")
}

fn default_key_republisher_interval_seconds() -> u64 {
    3600
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCreatorAuthorityAcquisitionConfig {
    #[serde(default = "default_creator_authority_acquisition_enabled")]
    enabled: bool,
    #[serde(default = "default_creator_authority_acquisition_method")]
    method: CreatorAuthorityAcquisitionMethod,
    #[serde(default)]
    allowed_return_origins: Vec<String>,
    #[serde(default = "default_frontend_session_ttl_seconds")]
    frontend_session_ttl_seconds: u64,
    #[serde(default = "default_frontend_session_code_ttl_seconds")]
    frontend_session_code_ttl_seconds: u64,
    #[serde(default)]
    legacy_connect: RawLegacyConnectAcquisitionConfig,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawLegacyConnectAcquisitionConfig {
    #[serde(default)]
    allowed_return_origins: Vec<String>,
}

impl Default for RawCreatorAuthorityAcquisitionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            method: default_creator_authority_acquisition_method(),
            allowed_return_origins: Vec::new(),
            frontend_session_ttl_seconds: default_frontend_session_ttl_seconds(),
            frontend_session_code_ttl_seconds: default_frontend_session_code_ttl_seconds(),
            legacy_connect: RawLegacyConnectAcquisitionConfig::default(),
        }
    }
}

fn default_creator_authority_acquisition_method() -> CreatorAuthorityAcquisitionMethod {
    CreatorAuthorityAcquisitionMethod::LegacyConnect
}

fn default_creator_authority_acquisition_enabled() -> bool {
    true
}

fn default_frontend_session_ttl_seconds() -> u64 {
    86_400
}

fn default_frontend_session_code_ttl_seconds() -> u64 {
    120
}

fn validate_allowed_return_origins(values: Vec<String>) -> Result<Vec<String>, ConfigError> {
    if values.iter().any(|value| value == "*") {
        if values.len() == 1 {
            return Ok(vec!["*".to_owned()]);
        }
        return Err(ConfigError::InvalidCreatorAuthorityAllowedReturnOrigin(
            "* cannot be mixed with concrete origins".to_owned(),
        ));
    }

    values
        .into_iter()
        .map(validate_allowed_return_origin)
        .collect::<Result<Vec<_>, _>>()
}

fn validate_allowed_return_origin(value: String) -> Result<String, ConfigError> {
    let invalid = || ConfigError::InvalidCreatorAuthorityAllowedReturnOrigin(value.clone());
    if value.contains('#') {
        return Err(invalid());
    }
    let uri: axum::http::Uri = value.parse().map_err(|_| invalid())?;
    let scheme = uri.scheme_str().ok_or_else(invalid)?;
    if scheme != "http" && scheme != "https" {
        return Err(invalid());
    }
    let authority = uri.authority().ok_or_else(invalid)?;
    if uri.query().is_some() {
        return Err(invalid());
    }
    let path = uri.path();
    if path != "/" && !path.is_empty() {
        return Err(invalid());
    }
    Ok(format!("{scheme}://{authority}"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSecretsConfig {
    #[serde(default = "default_runtime_master_key_env")]
    runtime_master_key_env: String,
}

impl Default for RawSecretsConfig {
    fn default() -> Self {
        Self {
            runtime_master_key_env: default_runtime_master_key_env(),
        }
    }
}

fn default_runtime_master_key_env() -> String {
    DEFAULT_RUNTIME_MASTER_KEY_ENV.to_owned()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDeletionConfig {
    #[serde(default = "default_deletion_retry_max_attempts")]
    retry_max_attempts: u32,
    #[serde(default = "default_deletion_retry_initial_backoff_seconds")]
    retry_initial_backoff_seconds: u64,
    #[serde(default = "default_deletion_retry_max_backoff_seconds")]
    retry_max_backoff_seconds: u64,
    #[serde(default = "default_final_credential_issuance_window_seconds")]
    final_credential_issuance_window_seconds: u64,
    #[serde(default = "default_final_read_window_seconds")]
    final_read_window_seconds: u64,
}

impl Default for RawDeletionConfig {
    fn default() -> Self {
        Self {
            retry_max_attempts: default_deletion_retry_max_attempts(),
            retry_initial_backoff_seconds: default_deletion_retry_initial_backoff_seconds(),
            retry_max_backoff_seconds: default_deletion_retry_max_backoff_seconds(),
            final_credential_issuance_window_seconds:
                default_final_credential_issuance_window_seconds(),
            final_read_window_seconds: default_final_read_window_seconds(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDeletionWorkerConfig {
    #[serde(default = "default_deletion_worker_enabled")]
    enabled: bool,
    #[serde(default = "default_deletion_worker_poll_interval_ms")]
    poll_interval_ms: u64,
    #[serde(default = "default_deletion_worker_claim_timeout_seconds")]
    claim_timeout_seconds: u64,
    #[serde(default = "default_deletion_worker_shutdown_timeout_seconds")]
    shutdown_timeout_seconds: u64,
    #[serde(default = "default_deletion_worker_id")]
    worker_id: String,
}

impl Default for RawDeletionWorkerConfig {
    fn default() -> Self {
        Self {
            enabled: default_deletion_worker_enabled(),
            poll_interval_ms: default_deletion_worker_poll_interval_ms(),
            claim_timeout_seconds: default_deletion_worker_claim_timeout_seconds(),
            shutdown_timeout_seconds: default_deletion_worker_shutdown_timeout_seconds(),
            worker_id: default_deletion_worker_id(),
        }
    }
}

impl RawDeletionWorkerConfig {
    fn into_deletion_worker_config(self) -> Result<DeletionWorkerConfig, ConfigError> {
        if self.poll_interval_ms == 0
            || self.claim_timeout_seconds == 0
            || self.shutdown_timeout_seconds == 0
            || self.worker_id.trim().is_empty()
        {
            return Err(ConfigError::InvalidDeletionWorkerConfig);
        }
        Ok(DeletionWorkerConfig {
            enabled: self.enabled,
            poll_interval_ms: self.poll_interval_ms,
            claim_timeout_seconds: self.claim_timeout_seconds,
            shutdown_timeout_seconds: self.shutdown_timeout_seconds,
            worker_id: self.worker_id,
        })
    }
}

fn default_deletion_worker_enabled() -> bool {
    true
}

fn default_deletion_worker_poll_interval_ms() -> u64 {
    DEFAULT_DELETION_WORKER_POLL_INTERVAL_MS
}

fn default_deletion_worker_claim_timeout_seconds() -> u64 {
    DEFAULT_DELETION_WORKER_CLAIM_TIMEOUT_SECONDS
}

fn default_deletion_worker_shutdown_timeout_seconds() -> u64 {
    DEFAULT_DELETION_WORKER_SHUTDOWN_TIMEOUT_SECONDS
}

fn default_deletion_worker_id() -> String {
    DEFAULT_DELETION_WORKER_ID.to_owned()
}

fn default_deletion_retry_max_attempts() -> u32 {
    DEFAULT_DELETION_RETRY_MAX_ATTEMPTS
}
fn default_deletion_retry_initial_backoff_seconds() -> u64 {
    DEFAULT_DELETION_RETRY_INITIAL_BACKOFF_SECONDS
}
fn default_deletion_retry_max_backoff_seconds() -> u64 {
    DEFAULT_DELETION_RETRY_MAX_BACKOFF_SECONDS
}
fn default_final_credential_issuance_window_seconds() -> u64 {
    DEFAULT_FINAL_CREDENTIAL_ISSUANCE_WINDOW_SECONDS
}
fn default_final_read_window_seconds() -> u64 {
    DEFAULT_FINAL_READ_WINDOW_SECONDS
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLoggingConfig {
    #[serde(default = "default_logging_level")]
    level: String,
}

impl Default for RawLoggingConfig {
    fn default() -> Self {
        Self {
            level: default_logging_level(),
        }
    }
}

fn default_logging_level() -> String {
    "info".to_owned()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCredentialsConfig {
    lock_server_secret_key: String,
    lock_server_public_key: String,
    max_ttl_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDatabaseConfig {
    url: Option<String>,
    url_env: Option<String>,
    max_connections: u32,
    run_migrations_on_startup: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWorkerConfig {
    enabled: bool,
    poll_interval_ms: u64,
    claim_timeout_seconds: u64,
    worker_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRuntimeConfig {
    environment: RuntimeEnvironment,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawRateLimitsConfig {
    verification_submission: RawVerificationSubmissionRateLimitConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawVerificationSubmissionRateLimitConfig {
    enabled: bool,
    max_requests: u32,
    window_seconds: u64,
}

impl Default for RawVerificationSubmissionRateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_requests: 60,
            window_seconds: 60,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawContentLocksConfig {
    #[serde(default = "default_max_resource_bytes")]
    max_resource_bytes: usize,
    #[serde(default = "default_max_resources")]
    max_resources: usize,
    #[serde(default = "default_max_total_resource_bytes")]
    max_total_resource_bytes: u64,
}

impl Default for RawContentLocksConfig {
    fn default() -> Self {
        Self {
            max_resource_bytes: default_max_resource_bytes(),
            max_resources: default_max_resources(),
            max_total_resource_bytes: default_max_total_resource_bytes(),
        }
    }
}

fn default_max_resource_bytes() -> usize {
    10_000_000
}

fn default_max_resources() -> usize {
    10
}

fn default_max_total_resource_bytes() -> u64 {
    100_000_000
}

impl RawContentLocksConfig {
    fn into_content_locks_config(self) -> Result<ContentLocksConfig, ConfigError> {
        if self.max_resource_bytes == 0 {
            return Err(ConfigError::InvalidMaxResourceBytes);
        }
        if self.max_resources == 0 {
            return Err(ConfigError::InvalidMaxResources);
        }
        if self.max_total_resource_bytes == 0 {
            return Err(ConfigError::InvalidMaxTotalResourceBytes);
        }
        if self.max_total_resource_bytes < self.max_resource_bytes as u64 {
            return Err(ConfigError::InvalidContentLocksTotalResourceBytes);
        }
        Ok(ContentLocksConfig {
            max_resource_bytes: self.max_resource_bytes,
            max_resources: self.max_resources,
            max_total_resource_bytes: self.max_total_resource_bytes,
        })
    }
}

impl RawDatabaseConfig {
    fn into_database_config(self) -> Result<DatabaseConfig, ConfigError> {
        let url = match (self.url, self.url_env) {
            (Some(_), Some(_)) => return Err(ConfigError::DatabaseUrlConflict),
            (Some(url), None) => url,
            (None, Some(env_name)) => std::env::var(&env_name)
                .map_err(|_| ConfigError::MissingDatabaseUrlEnv(env_name))?,
            (None, None) => return Err(ConfigError::MissingDatabaseUrl),
        };
        if self.max_connections == 0 {
            return Err(ConfigError::InvalidDatabaseMaxConnections);
        }
        Ok(DatabaseConfig {
            url,
            max_connections: self.max_connections,
            run_migrations_on_startup: self.run_migrations_on_startup,
        })
    }
}

impl RawRuntimeConfig {
    pub(super) fn into_runtime_config(self) -> RuntimeConfig {
        RuntimeConfig {
            environment: self.environment,
        }
    }
}

impl RawCreatorAuthorityAcquisitionConfig {
    fn into_creator_authority_acquisition_config(
        self,
        environment: RuntimeEnvironment,
    ) -> Result<CreatorAuthorityAcquisitionConfig, ConfigError> {
        let mut allowed_return_origin_values = self.legacy_connect.allowed_return_origins;
        if allowed_return_origin_values.is_empty() {
            allowed_return_origin_values = self.allowed_return_origins;
        }
        let allowed_return_origins = validate_allowed_return_origins(allowed_return_origin_values)?;
        // `*` allows handing the one-time code to any return origin — acceptable for dev/staging,
        // never for production.
        if environment == RuntimeEnvironment::Production
            && allowed_return_origins.iter().any(|origin| origin == "*")
        {
            return Err(ConfigError::WildcardReturnOriginInProduction);
        }
        Ok(CreatorAuthorityAcquisitionConfig {
            enabled: self.enabled,
            method: self.method,
            frontend_session_ttl_seconds: self.frontend_session_ttl_seconds,
            frontend_session_code_ttl_seconds: self.frontend_session_code_ttl_seconds,
            legacy_connect: LegacyConnectAcquisitionConfig {
                allowed_return_origins,
            },
        })
    }
}

impl RawSecretsConfig {
    fn into_secrets_config(self) -> Result<SecretsConfig, ConfigError> {
        if self.runtime_master_key_env.trim().is_empty() {
            return Err(ConfigError::InvalidRuntimeMasterKeyEnv);
        }
        Ok(SecretsConfig {
            runtime_master_key_env: self.runtime_master_key_env,
        })
    }
}

impl RawDeletionConfig {
    fn into_deletion_config(self) -> Result<DeletionConfig, ConfigError> {
        if self.retry_max_attempts == 0
            || self.retry_initial_backoff_seconds == 0
            || self.retry_max_backoff_seconds == 0
        {
            return Err(ConfigError::InvalidDeletionRetry);
        }
        if self.retry_initial_backoff_seconds > self.retry_max_backoff_seconds {
            return Err(ConfigError::InvalidDeletionRetryBackoffOrder);
        }
        if self.final_credential_issuance_window_seconds == 0
            || self.final_credential_issuance_window_seconds
                > MAX_DELETION_CREDENTIAL_WINDOW_SECONDS
            || self.final_read_window_seconds == 0
            || self.final_read_window_seconds > MAX_DELETION_CREDENTIAL_WINDOW_SECONDS
        {
            return Err(ConfigError::InvalidDeletionCredentialWindow);
        }
        Ok(DeletionConfig {
            retry_max_attempts: self.retry_max_attempts,
            retry_initial_backoff_seconds: self.retry_initial_backoff_seconds,
            retry_max_backoff_seconds: self.retry_max_backoff_seconds,
            final_credential_issuance_window_seconds: self.final_credential_issuance_window_seconds,
            final_read_window_seconds: self.final_read_window_seconds,
        })
    }
}

impl RawLoggingConfig {
    fn into_logging_config(self) -> Result<LoggingConfig, ConfigError> {
        EnvFilter::try_new(&self.level)
            .map_err(|_| ConfigError::InvalidLoggingLevel(self.level.clone()))?;
        Ok(LoggingConfig { level: self.level })
    }
}

impl RawRateLimitsConfig {
    fn into_rate_limits_config(self) -> Result<RateLimitsConfig, ConfigError> {
        Ok(RateLimitsConfig {
            verification_submission: self
                .verification_submission
                .into_verification_submission_rate_limit_config()?,
        })
    }
}

impl RawVerificationSubmissionRateLimitConfig {
    fn into_verification_submission_rate_limit_config(
        self,
    ) -> Result<VerificationSubmissionRateLimitConfig, ConfigError> {
        if self.enabled && self.max_requests == 0 {
            return Err(ConfigError::InvalidVerificationSubmissionRateLimitMaxRequests);
        }
        if self.enabled && self.window_seconds == 0 {
            return Err(ConfigError::InvalidVerificationSubmissionRateLimitWindow);
        }
        Ok(VerificationSubmissionRateLimitConfig {
            enabled: self.enabled,
            max_requests: self.max_requests,
            window_seconds: self.window_seconds,
        })
    }
}

impl RawConfig {
    pub(super) fn into_runtime_config(
        self,
        config_dir: &Path,
    ) -> Result<LockServerRuntimeConfig, ConfigError> {
        let database = self.database.into_database_config()?;
        let runtime = self.runtime.into_runtime_config();
        let creator_authority_acquisition = self
            .creator_authority_acquisition
            .into_creator_authority_acquisition_config(runtime.environment)?;
        let secrets = self.secrets.into_secrets_config()?;
        let logging = self.logging.into_logging_config()?;
        let pubky = self.pubky.into_pubky_config();
        let pkdns = self.pkdns.into_pkdns_config()?;
        let rate_limits = self.rate_limits.into_rate_limits_config()?;
        let content_locks = self.content_locks.into_content_locks_config()?;
        let deletion = self.deletion.into_deletion_config()?;
        let deletion_worker = self.deletion_worker.into_deletion_worker_config()?;
        let paykit = self
            .paykit
            .map(RawPaykitConfig::into_paykit_config)
            .transpose()?;
        if self.worker.poll_interval_ms == 0 {
            return Err(ConfigError::InvalidWorkerPollInterval);
        }
        if paykit.is_some()
            && self.worker.enabled
            && self.worker.claim_timeout_seconds <= PAYKIT_REQUEST_TIMEOUT_SECONDS
        {
            return Err(ConfigError::InvalidPaykitWorkerClaimTimeout {
                request_timeout_seconds: PAYKIT_REQUEST_TIMEOUT_SECONDS,
            });
        }
        Ok(LockServerRuntimeConfig {
            bind_addr: self.bind_addr,
            credentials: LockServerCredentialsConfig {
                lock_server_secret_key: expand_config_path(
                    &self.credentials.lock_server_secret_key,
                    config_dir,
                    "credentials.lock_server_secret_key",
                )?,
                lock_server_public_key: parse_runtime_public_key(
                    &self.credentials.lock_server_public_key,
                )?,
                max_ttl_seconds: self.credentials.max_ttl_seconds,
            },
            database,
            worker: WorkerConfig {
                enabled: self.worker.enabled,
                poll_interval_ms: self.worker.poll_interval_ms,
                claim_timeout_seconds: self.worker.claim_timeout_seconds,
                worker_id: self.worker.worker_id,
            },
            runtime,
            creator_authority_acquisition,
            secrets,
            logging,
            pubky,
            pkdns,
            rate_limits,
            content_locks,
            deletion,
            deletion_worker,
            paykit,
        })
    }
}

fn parse_runtime_public_key(value: &str) -> Result<LockServerPubky, ConfigError> {
    if value == PUBLIC_KEY_PLACEHOLDER {
        return Err(ConfigError::PlaceholderPublicKey);
    }
    Ok(LockServerPubky::from_str(value)?)
}

fn expand_config_path(
    raw_path: &str,
    config_dir: &Path,
    field: &'static str,
) -> Result<PathBuf, ConfigError> {
    if let Some(rest) = raw_path.strip_prefix("~/") {
        let home =
            std::env::var_os("HOME").ok_or_else(|| ConfigError::UnsupportedPathExpansion {
                field,
                value: raw_path.to_owned(),
            })?;
        return Ok(PathBuf::from(home).join(rest));
    }

    if raw_path.starts_with('~') || raw_path.starts_with('$') {
        return Err(ConfigError::UnsupportedPathExpansion {
            field,
            value: raw_path.to_owned(),
        });
    }

    let path = PathBuf::from(raw_path);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(config_dir.join(path))
    }
}
