use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use locks_core::ids::LockServerPubky;
use serde::Deserialize;
use thiserror::Error;

use super::defaults::DEFAULT_CREATOR_AUTHORITY_KEY_ENV;

pub const PAYKIT_CONNECT_TIMEOUT_SECONDS: u64 = 5;
pub const PAYKIT_REQUEST_TIMEOUT_SECONDS: u64 = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockServerRuntimeConfig {
    pub bind_addr: SocketAddr,
    pub credentials: LockServerCredentialsConfig,
    pub database: DatabaseConfig,
    pub worker: WorkerConfig,
    pub runtime: RuntimeConfig,
    pub creator_authority_acquisition: CreatorAuthorityAcquisitionConfig,
    pub secrets: SecretsConfig,
    pub logging: LoggingConfig,
    pub pubky: PubkyConfig,
    pub pkdns: PkdnsConfig,
    pub rate_limits: RateLimitsConfig,
    pub content_locks: ContentLocksConfig,
    pub paykit: Option<PaykitConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaykitConfig {
    pub server_url: String,
    pub minimum_confirmations: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentLocksConfig {
    pub max_resource_bytes: usize,
    pub max_resources: usize,
    pub max_total_resource_bytes: u64,
}

impl Default for ContentLocksConfig {
    fn default() -> Self {
        Self {
            max_resource_bytes: 10_000_000,
            max_resources: 10,
            max_total_resource_bytes: 100_000_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubkyConfig {
    pub network: PubkyNetwork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PubkyNetwork {
    Mainnet,
    Testnet,
}

impl Default for PubkyConfig {
    fn default() -> Self {
        Self {
            network: PubkyNetwork::Testnet,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkdnsConfig {
    pub public_ip: IpAddr,
    pub public_pubky_tls_port: Option<u16>,
    pub public_icann_http_port: Option<u16>,
    pub icann_domain: Option<String>,
    pub pkarr_relays: Vec<String>,
    pub key_republisher_interval_seconds: u64,
}

impl Default for PkdnsConfig {
    fn default() -> Self {
        Self {
            public_ip: "127.0.0.1".parse().expect("static loopback IP is valid"),
            public_pubky_tls_port: Some(6287),
            public_icann_http_port: Some(80),
            icann_domain: Some("localhost".to_owned()),
            pkarr_relays: Vec::new(),
            key_republisher_interval_seconds: 3600,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretsConfig {
    pub creator_authority_key_env: String,
}

impl Default for SecretsConfig {
    fn default() -> Self {
        Self {
            creator_authority_key_env: DEFAULT_CREATOR_AUTHORITY_KEY_ENV.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatorAuthorityAcquisitionConfig {
    pub enabled: bool,
    pub method: CreatorAuthorityAcquisitionMethod,
    pub frontend_session_ttl_seconds: u64,
    pub frontend_session_code_ttl_seconds: u64,
    pub legacy_connect: LegacyConnectAcquisitionConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyConnectAcquisitionConfig {
    pub allowed_return_origins: Vec<String>,
}

impl Default for CreatorAuthorityAcquisitionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            method: CreatorAuthorityAcquisitionMethod::LegacyConnect,
            frontend_session_ttl_seconds: 86_400,
            frontend_session_code_ttl_seconds: 120,
            legacy_connect: LegacyConnectAcquisitionConfig {
                allowed_return_origins: Vec::new(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CreatorAuthorityAcquisitionMethod {
    LegacyConnect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggingConfig {
    pub level: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub run_migrations_on_startup: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    pub enabled: bool,
    pub poll_interval_ms: u64,
    pub claim_timeout_seconds: u64,
    pub worker_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub environment: RuntimeEnvironment,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RateLimitsConfig {
    pub verification_submission: VerificationSubmissionRateLimitConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationSubmissionRateLimitConfig {
    pub enabled: bool,
    pub max_requests: u32,
    pub window_seconds: u64,
}

impl Default for VerificationSubmissionRateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_requests: 60,
            window_seconds: 60,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeEnvironment {
    Development,
    Staging,
    Production,
}

impl RuntimeEnvironment {
    pub fn is_development(self) -> bool {
        self == Self::Development
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockServerCredentialsConfig {
    pub lock_server_secret_key: PathBuf,
    pub lock_server_public_key: LockServerPubky,
    pub max_ttl_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigPathResolution {
    LoadExisting {
        config_path: PathBuf,
    },
    InitializeDefault {
        config_path: PathBuf,
        service_home: PathBuf,
        secret_path: PathBuf,
    },
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("custom config file does not exist: {0}")]
    MissingCustomConfig(PathBuf),
    #[error("config file path has no parent directory: {0}")]
    ConfigPathHasNoParent(PathBuf),
    #[error("failed to read config file {path}: {source}")]
    ReadConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    ParseConfig {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error(
        "credentials.lock_server_public_key must be a valid Lock Server Pubky, not a placeholder"
    )]
    PlaceholderPublicKey,
    #[error("invalid credentials.lock_server_public_key: {0}")]
    InvalidPublicKey(#[from] locks_core::ids::IdParseError),
    #[error("unsupported path expansion in {field}: {value}")]
    UnsupportedPathExpansion { field: &'static str, value: String },
    #[error("failed to create service home {path}: {source}")]
    CreateServiceHome {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write generated config file {path}: {source}")]
    WriteGeneratedConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("configured secret file does not exist: {0}")]
    MissingConfiguredSecret(PathBuf),
    #[error(
        "credentials.lock_server_public_key does not match public key derived from configured secret"
    )]
    PublicKeyMismatch,
    #[error("failed to generate lock server secret {path}: {message}")]
    GenerateSecret { path: PathBuf, message: String },
    #[error("failed to derive lock server public key from {path}: {message}")]
    DerivePublicKey { path: PathBuf, message: String },
    #[error("HOME is not set")]
    MissingHome,
    #[error("invalid command line: {0}")]
    InvalidArgs(String),
    #[error("database.url and database.url_env are mutually exclusive")]
    DatabaseUrlConflict,
    #[error("database config must set exactly one of database.url or database.url_env")]
    MissingDatabaseUrl,
    #[error("database.url_env environment variable is not set: {0}")]
    MissingDatabaseUrlEnv(String),
    #[error("database.max_connections must be greater than zero")]
    InvalidDatabaseMaxConnections,
    #[error("worker.poll_interval_ms must be greater than zero")]
    InvalidWorkerPollInterval,
    #[error(
        "rate_limits.verification_submission.max_requests must be greater than zero when enabled"
    )]
    InvalidVerificationSubmissionRateLimitMaxRequests,
    #[error(
        "rate_limits.verification_submission.window_seconds must be greater than zero when enabled"
    )]
    InvalidVerificationSubmissionRateLimitWindow,
    #[error("content_locks.max_resource_bytes must be greater than zero")]
    InvalidMaxResourceBytes,
    #[error("content_locks.max_resources must be greater than zero")]
    InvalidMaxResources,
    #[error("content_locks.max_total_resource_bytes must be greater than zero")]
    InvalidMaxTotalResourceBytes,
    #[error(
        "content_locks.max_total_resource_bytes must be at least content_locks.max_resource_bytes"
    )]
    InvalidContentLocksTotalResourceBytes,
    #[error("invalid logging.level filter: {0}")]
    InvalidLoggingLevel(String),
    #[error("secrets.creator_authority_key_env must not be empty")]
    InvalidCreatorAuthorityKeyEnv,
    #[error(
        "creator_authority_acquisition.allowed_return_origins must contain http(s) origins without path, query, or fragment: {0}"
    )]
    InvalidCreatorAuthorityAllowedReturnOrigin(String),
    #[error(
        "creator_authority_acquisition.allowed_return_origins must not be \"*\" when runtime.environment is production; list explicit origins"
    )]
    WildcardReturnOriginInProduction,
    #[error("pkdns.pkarr_relays must contain valid http(s) URLs: {0}")]
    InvalidPkarrRelayUrl(String),
    #[error("paykit.server_url must be a valid http(s) URL: {0}")]
    InvalidPaykitServerUrl(String),
    #[error(
        "paykit requires credentials.lock_server_secret_key to contain keypair-seed:<base64url-no-pad-32-byte-seed>"
    )]
    InvalidPaykitSigningSeed,
    #[error(
        "worker.claim_timeout_seconds must exceed the {request_timeout_seconds}-second Paykit request timeout when Paykit and the in-process worker are enabled"
    )]
    InvalidPaykitWorkerClaimTimeout { request_timeout_seconds: u64 },
}
