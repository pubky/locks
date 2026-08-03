use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use axum::Router;
use locks_core::ids::{CreatorPubky, LockServerPubky};
use locks_core::lock_policy::ContentLock;
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::{
    FrontendSessionRecord, FrontendSessionToken, GuardedResourceRecord,
};
use locks_service::application::ports::LegacyCreatorConnectFlowClient;
use locks_service::infrastructure::memory::{
    content_locks::InMemoryContentLockRepository, entitlements::InMemoryEntitlementRepository,
    guarded_resources::InMemoryGuardedResourceRepository,
    lock_service_pointers::InMemoryLockServicePointerRepository,
};
use locks_service::infrastructure::pubky::PubkyHomeserverStorageClient;
use time::OffsetDateTime;

use crate::{
    api::routes::router,
    app_state::AppState,
    config::{
        ContentLocksConfig, DatabaseConfig, LockServerCredentialsConfig, LockServerRuntimeConfig,
        LoggingConfig, PubkyConfig, RateLimitsConfig, RuntimeConfig, RuntimeEnvironment,
        SecretsConfig, WorkerConfig,
    },
};

/// Test-only helper surface for route and future `locks-e2e` tests.
#[derive(Debug, Clone)]
pub struct TestServerApp {
    state: AppState,
}

impl TestServerApp {
    pub fn from_state(state: AppState) -> Self {
        Self { state }
    }

    pub fn new_in_memory(config: LockServerRuntimeConfig) -> Self {
        Self {
            state: AppState::new_empty_in_memory_with_creator_repositories(
                config,
                Arc::new(InMemoryContentLockRepository::new()),
                Arc::new(InMemoryGuardedResourceRepository::new()),
                Arc::new(InMemoryLockServicePointerRepository::new()),
                Arc::new(InMemoryEntitlementRepository::new()),
            ),
        }
    }

    pub fn new_default_in_memory() -> Self {
        Self::new_in_memory(Self::default_in_memory_config())
    }

    pub fn new_in_memory_with_pubky_homeserver_storage<S>(
        config: LockServerRuntimeConfig,
        storage: S,
    ) -> Self
    where
        S: PubkyHomeserverStorageClient + Clone + 'static,
    {
        Self {
            state: AppState::new_empty_in_memory_with_pubky_homeserver_storage(config, storage),
        }
    }

    pub fn default_in_memory_config() -> LockServerRuntimeConfig {
        LockServerRuntimeConfig {
            bind_addr: "127.0.0.1:0".parse().expect("test bind address is valid"),
            credentials: LockServerCredentialsConfig {
                lock_server_secret_key: PathBuf::from("/tmp/lock-server-test-secret.sess"),
                lock_server_public_key: LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .expect("test lock server pubky is valid"),
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
            creator_authority_acquisition:
                crate::config::CreatorAuthorityAcquisitionConfig::default(),
            secrets: SecretsConfig::default(),
            logging: LoggingConfig::default(),
            pubky: PubkyConfig::default(),
            pkdns: crate::config::PkdnsConfig::default(),
            rate_limits: RateLimitsConfig::default(),
            content_locks: ContentLocksConfig::default(),
            paykit: None,
        }
    }

    pub fn state(&self) -> &AppState {
        &self.state
    }

    pub fn with_legacy_creator_connect_flow_client(
        mut self,
        client: Arc<dyn LegacyCreatorConnectFlowClient>,
    ) -> Self {
        self.state = self.state.with_legacy_creator_connect_flow_client(client);
        self
    }

    pub fn router(&self) -> Router {
        router(self.state.clone())
    }

    pub async fn seed_content_lock(
        &self,
        content_lock: ContentLock,
    ) -> Result<(), ApplicationError> {
        self.state
            .content_locks()
            .upsert_content_lock(
                content_lock.creator.clone(),
                content_lock
                    .content_lock_path()
                    .map_err(|error| ApplicationError::Storage {
                        message: format!("invalid content lock path: {error}"),
                    })?,
                content_lock,
            )
            .await
    }

    pub async fn seed_guarded_resource(
        &self,
        content_lock: &ContentLock,
        bytes: Vec<u8>,
    ) -> Result<(), ApplicationError> {
        let guarded_resource = content_lock.primary_resource.as_ref().ok_or_else(|| {
            ApplicationError::InvalidGuardedResource {
                message: "test helper requires a primary guarded resource".to_owned(),
            }
        })?;
        self.state
            .guarded_resources()
            .upsert_guarded_resource(GuardedResourceRecord {
                creator: content_lock.creator.clone(),
                path: guarded_resource.path.clone(),
                hash: guarded_resource.hash,
                content_type: guarded_resource.content_type.clone(),
                size: guarded_resource.size,
                bytes,
            })
            .await
    }

    pub async fn insert_frontend_session_for_test(
        &self,
        token: FrontendSessionToken,
        creator: CreatorPubky,
        expires_at: OffsetDateTime,
    ) -> Result<(), ApplicationError> {
        let created_at = self.state.clock().now();
        self.state
            .frontend_sessions()
            .insert_frontend_session(FrontendSessionRecord {
                token,
                creator,
                created_at,
                expires_at,
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::str::FromStr;

    use locks_core::ids::CreatorPubky;
    use locks_service::application::models::FrontendSessionToken;
    use time::macros::datetime;

    use super::TestServerApp;
    use crate::config::{RateLimitsConfig, VerificationSubmissionRateLimitConfig};
    use crate::rate_limit::VerificationSubmissionRateLimitKey;

    #[test]
    fn test_server_app_can_disable_rate_limiter_through_config_override() {
        let mut config = TestServerApp::default_in_memory_config();
        config.rate_limits = RateLimitsConfig {
            verification_submission: VerificationSubmissionRateLimitConfig {
                enabled: false,
                max_requests: 0,
                window_seconds: 0,
            },
        };
        let app = TestServerApp::new_in_memory(config);
        let key = VerificationSubmissionRateLimitKey {
            client_address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
        };
        let now = datetime!(2026-06-03 12:00:00 UTC);

        for _ in 0..10 {
            assert!(
                app.state()
                    .verification_submission_rate_limiter()
                    .check(&key, now)
                    .allowed
            );
        }
    }

    #[tokio::test]
    async fn test_server_app_can_seed_frontend_sessions_for_route_tests() {
        let app = TestServerApp::new_default_in_memory();
        let token = FrontendSessionToken::new("frontend-session-secret");
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let expires_at = datetime!(2026-06-17 12:00:00 UTC);

        app.insert_frontend_session_for_test(token.clone(), creator.clone(), expires_at)
            .await
            .unwrap();

        let stored = app
            .state()
            .frontend_sessions()
            .get_frontend_session(&token)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.creator, creator);
        assert_eq!(stored.expires_at, expires_at);
    }
}
