mod creator_authority;
mod creator_repositories;
mod generators;
mod private_runtime;
mod pubky_clients;
mod readiness;
#[cfg(test)]
mod test_support;

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use locks_core::ids::CreatorPubky;
use locks_service::{
    application::{
        errors::ApplicationError,
        models::AccessCredentialPolicy,
        ports::{
            AccessCredentialStore, Clock, ContentLockDeletionActionOwnership,
            ContentLockDeletionRepository, ContentLockOwnershipRepository, ContentLockRepository,
            ContentLockTombstoneRepository, CreatorAuthorityManager, CreatorAuthorityStore,
            CreatorConnectFlowStore, EntitlementRepository, FrontendSessionCodeStore,
            FrontendSessionStore, GuardedResourceRepository, LegacyCreatorConnectFlowClient,
            LockServicePointerRepository, PaymentDrainClient, PaymentDrainRepository,
            VerificationTaskClaimer, VerificationTaskRepository,
        },
    },
    infrastructure::{
        final_credentials::FinalCredentialCipher,
        memory::{
            access_credentials::InMemoryAccessCredentialStore,
            content_lock_deletion_action_ownership::InMemoryContentLockDeletionActionOwnership,
            content_lock_deletions::InMemoryContentLockDeletionRepository,
            content_lock_ownership::InMemoryContentLockOwnershipRepository,
            content_lock_tombstones::InMemoryContentLockTombstoneRepository,
            content_locks::InMemoryContentLockRepository,
            public_content_locks::InMemoryPublicContentLockStore,
            verification_task_claims::InMemoryVerificationTaskClaimer,
            verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence,
            verification_tasks::InMemoryVerificationTaskRepository,
        },
        postgres::{
            CreatorAuthoritySecretCipher, PostgresAccessCredentialStore,
            PostgresContentLockDeletionActionOwnership, PostgresContentLockDeletionRepository,
            PostgresContentLockOwnershipRepository, PostgresCreatorAuthorityStore,
            PostgresCreatorConnectFlowStore, PostgresFrontendSessionCodeStore,
            PostgresFrontendSessionStore, PostgresPaymentDrainRepository,
            PostgresVerificationTaskClaimer, PostgresVerificationTaskRepository,
        },
        pubky::{
            AuthorizingPubkyHomeserverStorageClient, LegacyCookieCreatorAuthorityManager,
            PubkyBytesResource, PubkyContentLockRepository, PubkyContentLockTombstoneRepository,
            PubkyEntitlementRepository, PubkyHomeserverStorageClient,
            PubkyLegacyCookieSessionRevalidator, PubkyLegacyCreatorConnectFlowClient,
            PubkyLockServicePointerRepository, PubkyPrivResourceRepository,
        },
        verifiers::dev_static::DevStaticVerifier,
        verifiers::paykit_payment::PaykitPaymentVerifier,
    },
};
use sqlx::PgPool;

use crate::app_state::creator_authority::{
    DisabledLegacyCreatorConnectFlowClient, NoopLegacyCookieSessionRevalidator,
};
use crate::app_state::creator_repositories::CreatorRepositoryAdapters;
pub use crate::app_state::generators::{
    OsRandomAccessCredentialGenerator, OsRandomCreatorConnectFlowIdGenerator,
    OsRandomFrontendSessionCodeGenerator, OsRandomFrontendSessionTokenGenerator,
    OsRandomTaskIdGenerator, SystemClock,
};
use crate::app_state::private_runtime::{
    InMemoryCreatorAuthorityStore, InMemoryCreatorConnectFlowStore,
    InMemoryFrontendSessionCodeStore, InMemoryFrontendSessionStore, PrivateRuntimeAdapters,
};
use crate::app_state::pubky_clients::{
    build_pubky_client, build_pubky_http_client, pubky_auth_relay_for_network,
};
pub use crate::app_state::readiness::{
    ReadinessStatus, RuntimeStorageKind, WorkerKind, WorkerReadiness, WorkerReadinessEvidence,
    WorkerReadinessState,
};
use crate::config::LockServerRuntimeConfig;
use crate::paykit_http_client::{PaykitHttpClient, PaykitSetupStatusProvider};
use crate::rate_limit::InMemoryVerificationSubmissionRateLimiter;

#[async_trait]
pub trait ReaderPubkyResolver: Send + Sync {
    async fn reader_has_homeserver(&self, reader: &CreatorPubky) -> bool;
}

#[derive(Debug, Clone)]
struct PubkyReaderPubkyResolver {
    client: pubky::Pubky,
}

#[async_trait]
impl ReaderPubkyResolver for PubkyReaderPubkyResolver {
    async fn reader_has_homeserver(&self, reader: &CreatorPubky) -> bool {
        let Ok(public_key) = pubky_common::crypto::PublicKey::from_str(&reader.to_string()) else {
            return false;
        };
        self.client
            .get_homeserver_of(&public_key)
            .await
            .is_ok_and(|homeserver| homeserver.is_some())
    }
}

#[derive(Debug, Clone, Copy)]
struct UnavailablePubkyHomeserverStorageClient;

#[async_trait]
impl PubkyHomeserverStorageClient for UnavailablePubkyHomeserverStorageClient {
    async fn put_json_value_as_creator(
        &self,
        _creator: &CreatorPubky,
        _path: &str,
        _body: serde_json::Value,
    ) -> Result<(), ApplicationError> {
        Err(ApplicationError::CreatorAuthorityUnavailable)
    }

    async fn get_json_value_as_creator(
        &self,
        _creator: &CreatorPubky,
        _path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError> {
        Err(ApplicationError::CreatorAuthorityUnavailable)
    }

    async fn put_bytes_as_creator(
        &self,
        _creator: &CreatorPubky,
        _path: &str,
        _bytes: Vec<u8>,
        _content_type: &str,
    ) -> Result<(), ApplicationError> {
        Err(ApplicationError::CreatorAuthorityUnavailable)
    }

    async fn get_bytes_as_creator(
        &self,
        _creator: &CreatorPubky,
        _path: &str,
    ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
        Err(ApplicationError::CreatorAuthorityUnavailable)
    }

    async fn delete_as_creator(
        &self,
        _creator: &CreatorPubky,
        _path: &str,
    ) -> Result<(), ApplicationError> {
        Err(ApplicationError::CreatorAuthorityUnavailable)
    }
}

#[derive(Clone)]
pub struct AppState {
    config: LockServerRuntimeConfig,
    private_runtime_storage_kind: RuntimeStorageKind,
    postgres_pool: Option<PgPool>,
    worker_readiness: WorkerReadiness,
    content_locks: Arc<dyn ContentLockRepository>,
    content_lock_tombstones: Arc<dyn ContentLockTombstoneRepository>,
    guarded_resources: Arc<dyn GuardedResourceRepository>,
    lock_service_pointers: Arc<dyn LockServicePointerRepository>,
    content_lock_ownership: Arc<dyn ContentLockOwnershipRepository>,
    content_lock_deletions: Arc<dyn ContentLockDeletionRepository>,
    content_lock_deletion_action_ownership: Arc<dyn ContentLockDeletionActionOwnership>,
    payment_drains: Option<Arc<dyn PaymentDrainRepository>>,
    verification_tasks: Arc<dyn VerificationTaskRepository>,
    verification_task_claimer: Arc<dyn VerificationTaskClaimer>,
    entitlements: Arc<dyn EntitlementRepository>,
    access_credentials: Arc<dyn AccessCredentialStore>,
    creator_authorities: Arc<dyn CreatorAuthorityStore>,
    creator_connect_flows: Arc<dyn CreatorConnectFlowStore>,
    frontend_session_codes: Arc<dyn FrontendSessionCodeStore>,
    frontend_sessions: Arc<dyn FrontendSessionStore>,
    creator_authority_manager: Arc<dyn CreatorAuthorityManager>,
    legacy_creator_connect_flow_client: Arc<dyn LegacyCreatorConnectFlowClient>,
    dev_static_verifier: Arc<DevStaticVerifier>,
    paykit_payment_verifier: Option<Arc<PaykitPaymentVerifier<Arc<PaykitHttpClient>>>>,
    task_ids: Arc<OsRandomTaskIdGenerator>,
    credential_generator: Arc<OsRandomAccessCredentialGenerator>,
    creator_connect_flow_id_generator: Arc<OsRandomCreatorConnectFlowIdGenerator>,
    frontend_session_code_generator: Arc<OsRandomFrontendSessionCodeGenerator>,
    frontend_session_token_generator: Arc<OsRandomFrontendSessionTokenGenerator>,
    clock: Arc<dyn Clock>,
    access_credential_policy: AccessCredentialPolicy,
    verification_submission_rate_limiter: Arc<InMemoryVerificationSubmissionRateLimiter>,
    reader_pubky_resolver: Arc<dyn ReaderPubkyResolver>,
    paykit_http_client: Option<Arc<PaykitHttpClient>>,
    payment_drain_client: Option<Arc<dyn PaymentDrainClient>>,
    paykit_setup_status_provider: Option<Arc<dyn PaykitSetupStatusProvider>>,
}

/// Purpose-separated runtime ciphers derived from the configured master key.
pub struct RuntimeSecretCiphers {
    creator_authority: CreatorAuthoritySecretCipher,
    final_credential: FinalCredentialCipher,
}

impl RuntimeSecretCiphers {
    pub fn new(
        creator_authority: CreatorAuthoritySecretCipher,
        final_credential: FinalCredentialCipher,
    ) -> Self {
        Self {
            creator_authority,
            final_credential,
        }
    }
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppState")
            .field("config", &self.config)
            .field(
                "private_runtime_storage_kind",
                &self.private_runtime_storage_kind,
            )
            .field("content_locks", &"<content_lock_repository>")
            .field("guarded_resources", &"<guarded_resource_repository>")
            .field(
                "lock_service_pointers",
                &"<lock_service_pointer_repository>",
            )
            .field("entitlements", &"<entitlement_repository>")
            .field("dev_static_verifier", &self.dev_static_verifier)
            .field("task_ids", &self.task_ids)
            .field("credential_generator", &self.credential_generator)
            .field("clock", &"<clock>")
            .field("access_credential_policy", &self.access_credential_policy)
            .field(
                "verification_submission_rate_limiter",
                &self.verification_submission_rate_limiter,
            )
            .finish_non_exhaustive()
    }
}

impl AppState {
    pub fn new_empty_in_memory(config: LockServerRuntimeConfig) -> Self {
        let verification_task_deletion_fence =
            Arc::new(InMemoryVerificationTaskDeletionFence::new());
        let verification_tasks = Arc::new(InMemoryVerificationTaskRepository::with_deletion_fence(
            Arc::clone(&verification_task_deletion_fence),
        ));
        let verification_task_claimer = Arc::new(
            InMemoryVerificationTaskClaimer::with_task_repository_and_deletion_fence(
                vec![],
                verification_tasks.clone(),
                Arc::clone(&verification_task_deletion_fence),
            ),
        );
        let access_credentials = Arc::new(
            InMemoryAccessCredentialStore::with_verification_task_repository_and_deletion_fence(
                verification_tasks.clone(),
                Arc::clone(&verification_task_deletion_fence),
            ),
        );
        let creator_authority_store = InMemoryCreatorAuthorityStore::new();
        let creator_authorities = Arc::new(creator_authority_store.clone());
        let creator_authority_manager = Arc::new(LegacyCookieCreatorAuthorityManager::new(
            creator_authority_store,
            NoopLegacyCookieSessionRevalidator,
        ));
        let unavailable_storage: Arc<dyn PubkyHomeserverStorageClient> =
            Arc::new(UnavailablePubkyHomeserverStorageClient);
        let public_content_locks = InMemoryPublicContentLockStore::new();
        let creator_repositories = CreatorRepositoryAdapters::new(
            Arc::new(InMemoryContentLockRepository::with_public_store(
                public_content_locks.clone(),
            )),
            Arc::new(InMemoryContentLockTombstoneRepository::with_public_store(
                public_content_locks,
            )),
            Arc::new(PubkyPrivResourceRepository::new(
                unavailable_storage.clone(),
            )),
            Arc::new(PubkyLockServicePointerRepository::new(
                unavailable_storage.clone(),
            )),
            Arc::new(PubkyEntitlementRepository::new(unavailable_storage)),
        );

        let content_lock_deletions = Arc::new(
            InMemoryContentLockDeletionRepository::with_access_credentials_and_verification_task_fence(
                Arc::clone(&access_credentials),
                verification_task_deletion_fence,
            ),
        );
        let private_runtime = PrivateRuntimeAdapters {
            content_lock_ownership: Arc::new(InMemoryContentLockOwnershipRepository::new()),
            content_lock_deletions: content_lock_deletions.clone(),
            content_lock_deletion_action_ownership: Arc::new(
                InMemoryContentLockDeletionActionOwnership::new(content_lock_deletions),
            ),
            verification_tasks,
            verification_task_claimer,
            access_credentials,
            creator_authorities,
            creator_connect_flows: Arc::new(InMemoryCreatorConnectFlowStore::new()),
            frontend_session_codes: Arc::new(InMemoryFrontendSessionCodeStore::new()),
            frontend_sessions: Arc::new(InMemoryFrontendSessionStore::new()),
            creator_authority_manager,
            legacy_creator_connect_flow_client: Arc::new(DisabledLegacyCreatorConnectFlowClient),
        };

        Self::new_with_private_runtime_storage(
            config,
            RuntimeStorageKind::InMemory,
            None,
            creator_repositories,
            private_runtime,
        )
    }

    pub fn new_empty_in_memory_with_creator_repositories(
        config: LockServerRuntimeConfig,
        content_locks: Arc<dyn ContentLockRepository>,
        content_lock_tombstones: Arc<dyn ContentLockTombstoneRepository>,
        guarded_resources: Arc<dyn GuardedResourceRepository>,
        lock_service_pointers: Arc<dyn LockServicePointerRepository>,
        entitlements: Arc<dyn EntitlementRepository>,
    ) -> Self {
        let verification_task_deletion_fence =
            Arc::new(InMemoryVerificationTaskDeletionFence::new());
        let verification_tasks = Arc::new(InMemoryVerificationTaskRepository::with_deletion_fence(
            Arc::clone(&verification_task_deletion_fence),
        ));
        let verification_task_claimer = Arc::new(
            InMemoryVerificationTaskClaimer::with_task_repository_and_deletion_fence(
                vec![],
                verification_tasks.clone(),
                Arc::clone(&verification_task_deletion_fence),
            ),
        );
        let access_credentials = Arc::new(
            InMemoryAccessCredentialStore::with_verification_task_repository_and_deletion_fence(
                verification_tasks.clone(),
                Arc::clone(&verification_task_deletion_fence),
            ),
        );
        let creator_authority_store = InMemoryCreatorAuthorityStore::new();
        let creator_authorities = Arc::new(creator_authority_store.clone());
        let creator_authority_manager = Arc::new(LegacyCookieCreatorAuthorityManager::new(
            creator_authority_store,
            NoopLegacyCookieSessionRevalidator,
        ));
        let creator_repositories = CreatorRepositoryAdapters::new(
            content_locks,
            content_lock_tombstones,
            guarded_resources,
            lock_service_pointers,
            entitlements,
        );

        let content_lock_deletions = Arc::new(
            InMemoryContentLockDeletionRepository::with_access_credentials_and_verification_task_fence(
                Arc::clone(&access_credentials),
                verification_task_deletion_fence,
            ),
        );
        let private_runtime = PrivateRuntimeAdapters {
            content_lock_ownership: Arc::new(InMemoryContentLockOwnershipRepository::new()),
            content_lock_deletions: content_lock_deletions.clone(),
            content_lock_deletion_action_ownership: Arc::new(
                InMemoryContentLockDeletionActionOwnership::new(content_lock_deletions),
            ),
            verification_tasks,
            verification_task_claimer,
            access_credentials,
            creator_authorities,
            creator_connect_flows: Arc::new(InMemoryCreatorConnectFlowStore::new()),
            frontend_session_codes: Arc::new(InMemoryFrontendSessionCodeStore::new()),
            frontend_sessions: Arc::new(InMemoryFrontendSessionStore::new()),
            creator_authority_manager,
            legacy_creator_connect_flow_client: Arc::new(DisabledLegacyCreatorConnectFlowClient),
        };

        Self::new_with_private_runtime_storage(
            config,
            RuntimeStorageKind::InMemory,
            None,
            creator_repositories,
            private_runtime,
        )
    }

    pub fn new_empty_in_memory_with_pubky_homeserver_storage<S>(
        config: LockServerRuntimeConfig,
        storage: S,
    ) -> Self
    where
        S: PubkyHomeserverStorageClient + Clone + 'static,
    {
        let verification_task_deletion_fence =
            Arc::new(InMemoryVerificationTaskDeletionFence::new());
        let verification_tasks = Arc::new(InMemoryVerificationTaskRepository::with_deletion_fence(
            Arc::clone(&verification_task_deletion_fence),
        ));
        let verification_task_claimer = Arc::new(
            InMemoryVerificationTaskClaimer::with_task_repository_and_deletion_fence(
                vec![],
                verification_tasks.clone(),
                Arc::clone(&verification_task_deletion_fence),
            ),
        );
        let access_credentials = Arc::new(
            InMemoryAccessCredentialStore::with_verification_task_repository_and_deletion_fence(
                verification_tasks.clone(),
                Arc::clone(&verification_task_deletion_fence),
            ),
        );
        let creator_authority_store = InMemoryCreatorAuthorityStore::new();
        let creator_authorities = Arc::new(creator_authority_store.clone());
        let creator_authority_manager = Arc::new(LegacyCookieCreatorAuthorityManager::new(
            creator_authority_store.clone(),
            NoopLegacyCookieSessionRevalidator,
        ));
        let authorizing_storage: Arc<dyn PubkyHomeserverStorageClient> =
            Arc::new(AuthorizingPubkyHomeserverStorageClient::new(
                storage,
                LegacyCookieCreatorAuthorityManager::new(
                    creator_authority_store.clone(),
                    NoopLegacyCookieSessionRevalidator,
                ),
            ));
        let creator_repositories = CreatorRepositoryAdapters::new(
            Arc::new(PubkyContentLockRepository::new(authorizing_storage.clone())),
            Arc::new(PubkyContentLockTombstoneRepository::new(
                authorizing_storage.clone(),
            )),
            Arc::new(PubkyPrivResourceRepository::new(
                authorizing_storage.clone(),
            )),
            Arc::new(PubkyLockServicePointerRepository::new(
                authorizing_storage.clone(),
            )),
            Arc::new(PubkyEntitlementRepository::new(authorizing_storage)),
        );

        let content_lock_deletions = Arc::new(
            InMemoryContentLockDeletionRepository::with_access_credentials_and_verification_task_fence(
                Arc::clone(&access_credentials),
                verification_task_deletion_fence,
            ),
        );
        let private_runtime = PrivateRuntimeAdapters {
            content_lock_ownership: Arc::new(InMemoryContentLockOwnershipRepository::new()),
            content_lock_deletions: content_lock_deletions.clone(),
            content_lock_deletion_action_ownership: Arc::new(
                InMemoryContentLockDeletionActionOwnership::new(content_lock_deletions),
            ),
            verification_tasks,
            verification_task_claimer,
            access_credentials,
            creator_authorities,
            creator_connect_flows: Arc::new(InMemoryCreatorConnectFlowStore::new()),
            frontend_session_codes: Arc::new(InMemoryFrontendSessionCodeStore::new()),
            frontend_sessions: Arc::new(InMemoryFrontendSessionStore::new()),
            creator_authority_manager,
            legacy_creator_connect_flow_client: Arc::new(DisabledLegacyCreatorConnectFlowClient),
        };

        Self::new_with_private_runtime_storage(
            config,
            RuntimeStorageKind::InMemory,
            None,
            creator_repositories,
            private_runtime,
        )
    }

    pub fn new_with_postgres_runtime(
        config: LockServerRuntimeConfig,
        pool: PgPool,
        creator_authority_cipher: CreatorAuthoritySecretCipher,
        final_credential_cipher: FinalCredentialCipher,
    ) -> Self {
        let verification_tasks = Arc::new(PostgresVerificationTaskRepository::new(pool.clone()));
        let verification_task_claimer =
            Arc::new(PostgresVerificationTaskClaimer::new(pool.clone()));
        let access_credentials =
            Arc::new(PostgresAccessCredentialStore::with_final_credential_cipher(
                pool.clone(),
                final_credential_cipher,
            ));
        let creator_authority_store =
            PostgresCreatorAuthorityStore::new_encrypted(pool.clone(), creator_authority_cipher);
        let creator_authorities = Arc::new(creator_authority_store.clone());
        let pubky_http_client = build_pubky_http_client(config.pubky.network);
        let creator_authority_manager: Arc<dyn CreatorAuthorityManager> =
            Arc::new(LegacyCookieCreatorAuthorityManager::new(
                creator_authority_store.clone(),
                PubkyLegacyCookieSessionRevalidator::new(pubky_http_client.clone()),
            ));
        let creator_repositories =
            CreatorRepositoryAdapters::pubky_homeserver(creator_authority_store, pubky_http_client);
        let legacy_creator_connect_flow_client: Arc<dyn LegacyCreatorConnectFlowClient> =
            if config.creator_authority_acquisition.enabled {
                let pubky = build_pubky_client(config.pubky.network);
                match pubky_auth_relay_for_network(config.pubky.network) {
                    Some(auth_relay) => Arc::new(
                        PubkyLegacyCreatorConnectFlowClient::new_with_auth_relay(pubky, auth_relay),
                    ),
                    None => Arc::new(PubkyLegacyCreatorConnectFlowClient::new(pubky)),
                }
            } else {
                Arc::new(DisabledLegacyCreatorConnectFlowClient)
            };

        let private_runtime = PrivateRuntimeAdapters {
            content_lock_ownership: Arc::new(PostgresContentLockOwnershipRepository::new(
                pool.clone(),
            )),
            content_lock_deletions: Arc::new(PostgresContentLockDeletionRepository::new(
                pool.clone(),
            )),
            content_lock_deletion_action_ownership: Arc::new(
                PostgresContentLockDeletionActionOwnership::new(pool.clone()),
            ),
            verification_tasks,
            verification_task_claimer,
            access_credentials,
            creator_authorities,
            creator_connect_flows: Arc::new(PostgresCreatorConnectFlowStore::new(pool.clone())),
            frontend_session_codes: Arc::new(PostgresFrontendSessionCodeStore::new(pool.clone())),
            frontend_sessions: Arc::new(PostgresFrontendSessionStore::new(pool.clone())),
            creator_authority_manager,
            legacy_creator_connect_flow_client,
        };

        Self::new_with_private_runtime_storage(
            config,
            RuntimeStorageKind::Postgres,
            Some(pool),
            creator_repositories,
            private_runtime,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_postgres_runtime_and_creator_repositories(
        config: LockServerRuntimeConfig,
        pool: PgPool,
        ciphers: RuntimeSecretCiphers,
        content_locks: Arc<dyn ContentLockRepository>,
        content_lock_tombstones: Arc<dyn ContentLockTombstoneRepository>,
        guarded_resources: Arc<dyn GuardedResourceRepository>,
        lock_service_pointers: Arc<dyn LockServicePointerRepository>,
        entitlements: Arc<dyn EntitlementRepository>,
    ) -> Self {
        let verification_tasks = Arc::new(PostgresVerificationTaskRepository::new(pool.clone()));
        let verification_task_claimer =
            Arc::new(PostgresVerificationTaskClaimer::new(pool.clone()));
        let access_credentials =
            Arc::new(PostgresAccessCredentialStore::with_final_credential_cipher(
                pool.clone(),
                ciphers.final_credential,
            ));
        let creator_authority_store =
            PostgresCreatorAuthorityStore::new_encrypted(pool.clone(), ciphers.creator_authority);
        let creator_authorities = Arc::new(creator_authority_store.clone());
        let pubky_http_client = build_pubky_http_client(config.pubky.network);
        let creator_authority_manager: Arc<dyn CreatorAuthorityManager> =
            Arc::new(LegacyCookieCreatorAuthorityManager::new(
                creator_authority_store,
                PubkyLegacyCookieSessionRevalidator::new(pubky_http_client),
            ));
        let legacy_creator_connect_flow_client: Arc<dyn LegacyCreatorConnectFlowClient> =
            if config.creator_authority_acquisition.enabled {
                let pubky = build_pubky_client(config.pubky.network);
                match pubky_auth_relay_for_network(config.pubky.network) {
                    Some(auth_relay) => Arc::new(
                        PubkyLegacyCreatorConnectFlowClient::new_with_auth_relay(pubky, auth_relay),
                    ),
                    None => Arc::new(PubkyLegacyCreatorConnectFlowClient::new(pubky)),
                }
            } else {
                Arc::new(DisabledLegacyCreatorConnectFlowClient)
            };
        let creator_repositories = CreatorRepositoryAdapters::new(
            content_locks,
            content_lock_tombstones,
            guarded_resources,
            lock_service_pointers,
            entitlements,
        );
        let private_runtime = PrivateRuntimeAdapters {
            content_lock_ownership: Arc::new(PostgresContentLockOwnershipRepository::new(
                pool.clone(),
            )),
            content_lock_deletions: Arc::new(PostgresContentLockDeletionRepository::new(
                pool.clone(),
            )),
            content_lock_deletion_action_ownership: Arc::new(
                PostgresContentLockDeletionActionOwnership::new(pool.clone()),
            ),
            verification_tasks,
            verification_task_claimer,
            access_credentials,
            creator_authorities,
            creator_connect_flows: Arc::new(PostgresCreatorConnectFlowStore::new(pool.clone())),
            frontend_session_codes: Arc::new(PostgresFrontendSessionCodeStore::new(pool.clone())),
            frontend_sessions: Arc::new(PostgresFrontendSessionStore::new(pool.clone())),
            creator_authority_manager,
            legacy_creator_connect_flow_client,
        };

        Self::new_with_private_runtime_storage(
            config,
            RuntimeStorageKind::Postgres,
            Some(pool),
            creator_repositories,
            private_runtime,
        )
    }

    fn new_with_private_runtime_storage(
        config: LockServerRuntimeConfig,
        private_runtime_storage_kind: RuntimeStorageKind,
        postgres_pool: Option<PgPool>,
        creator_repositories: CreatorRepositoryAdapters,
        private_runtime: PrivateRuntimeAdapters,
    ) -> Self {
        let worker_readiness =
            WorkerReadiness::new(config.worker.enabled, config.deletion_worker.enabled);
        let access_credential_policy =
            AccessCredentialPolicy::new(config.credentials.max_ttl_seconds);
        let verification_submission_rate_limiter =
            Arc::new(InMemoryVerificationSubmissionRateLimiter::new(
                config.rate_limits.verification_submission.clone(),
            ));
        let reader_pubky_resolver = Arc::new(PubkyReaderPubkyResolver {
            client: build_pubky_client(config.pubky.network),
        });
        let paykit_http_client = config.paykit.as_ref().map(|paykit| {
            Arc::new(
                PaykitHttpClient::new(&paykit.server_url, &config.credentials)
                    .expect("validated paykit config must produce a signed Paykit HTTP client"),
            )
        });
        let paykit_payment_verifier = config.paykit.as_ref().and_then(|paykit| {
            paykit_http_client.as_ref().map(|client| {
                Arc::new(PaykitPaymentVerifier::new(
                    Arc::clone(client),
                    paykit.minimum_confirmations,
                ))
            })
        });
        let payment_drains: Option<Arc<dyn PaymentDrainRepository>> = postgres_pool
            .as_ref()
            .map(|pool| Arc::new(PostgresPaymentDrainRepository::new(pool.clone())) as Arc<_>);
        let payment_drain_client = paykit_http_client
            .as_ref()
            .map(|client| Arc::clone(client) as Arc<dyn PaymentDrainClient>);
        let paykit_setup_status_provider = paykit_http_client
            .as_ref()
            .map(|client| Arc::clone(client) as Arc<dyn PaykitSetupStatusProvider>);

        Self {
            config,
            private_runtime_storage_kind,
            postgres_pool,
            worker_readiness,
            content_locks: creator_repositories.content_locks,
            content_lock_tombstones: creator_repositories.content_lock_tombstones,
            guarded_resources: creator_repositories.guarded_resources,
            lock_service_pointers: creator_repositories.lock_service_pointers,
            content_lock_ownership: private_runtime.content_lock_ownership,
            content_lock_deletions: private_runtime.content_lock_deletions,
            content_lock_deletion_action_ownership: private_runtime
                .content_lock_deletion_action_ownership,
            payment_drains,
            verification_tasks: private_runtime.verification_tasks,
            verification_task_claimer: private_runtime.verification_task_claimer,
            entitlements: creator_repositories.entitlements,
            access_credentials: private_runtime.access_credentials,
            creator_authorities: private_runtime.creator_authorities,
            creator_connect_flows: private_runtime.creator_connect_flows,
            frontend_session_codes: private_runtime.frontend_session_codes,
            frontend_sessions: private_runtime.frontend_sessions,
            creator_authority_manager: private_runtime.creator_authority_manager,
            legacy_creator_connect_flow_client: private_runtime.legacy_creator_connect_flow_client,
            dev_static_verifier: Arc::new(DevStaticVerifier),
            paykit_payment_verifier,
            task_ids: Arc::new(OsRandomTaskIdGenerator),
            credential_generator: Arc::new(OsRandomAccessCredentialGenerator),
            creator_connect_flow_id_generator: Arc::new(OsRandomCreatorConnectFlowIdGenerator),
            frontend_session_code_generator: Arc::new(OsRandomFrontendSessionCodeGenerator),
            frontend_session_token_generator: Arc::new(OsRandomFrontendSessionTokenGenerator),
            clock: Arc::new(SystemClock),
            access_credential_policy,
            verification_submission_rate_limiter,
            reader_pubky_resolver,
            paykit_http_client,
            payment_drain_client,
            paykit_setup_status_provider,
        }
    }

    pub fn config(&self) -> &LockServerRuntimeConfig {
        &self.config
    }

    pub fn private_runtime_storage_kind(&self) -> RuntimeStorageKind {
        self.private_runtime_storage_kind
    }

    pub fn postgres_pool(&self) -> Option<&PgPool> {
        self.postgres_pool.as_ref()
    }

    pub fn worker_readiness(&self) -> &WorkerReadiness {
        &self.worker_readiness
    }

    pub fn worker_readiness_status(&self) -> ReadinessStatus {
        self.worker_readiness.status()
    }

    pub fn record_worker_readiness(&self, worker: WorkerKind, evidence: WorkerReadinessEvidence) {
        self.worker_readiness.record(worker, evidence);
    }

    pub fn content_locks(&self) -> &Arc<dyn ContentLockRepository> {
        &self.content_locks
    }

    pub fn content_lock_tombstones(&self) -> &Arc<dyn ContentLockTombstoneRepository> {
        &self.content_lock_tombstones
    }

    pub fn guarded_resources(&self) -> &Arc<dyn GuardedResourceRepository> {
        &self.guarded_resources
    }

    pub fn content_lock_ownership(&self) -> &Arc<dyn ContentLockOwnershipRepository> {
        &self.content_lock_ownership
    }

    pub fn content_lock_deletions(&self) -> &Arc<dyn ContentLockDeletionRepository> {
        &self.content_lock_deletions
    }

    pub fn content_lock_deletion_action_ownership(
        &self,
    ) -> &Arc<dyn ContentLockDeletionActionOwnership> {
        &self.content_lock_deletion_action_ownership
    }

    pub fn payment_drains(&self) -> Option<&Arc<dyn PaymentDrainRepository>> {
        self.payment_drains.as_ref()
    }

    pub fn payment_drain_client(&self) -> Option<&Arc<dyn PaymentDrainClient>> {
        self.payment_drain_client.as_ref()
    }

    pub fn lock_service_pointers(&self) -> &Arc<dyn LockServicePointerRepository> {
        &self.lock_service_pointers
    }

    pub fn verification_tasks(&self) -> &Arc<dyn VerificationTaskRepository> {
        &self.verification_tasks
    }

    pub fn verification_task_claimer(&self) -> &Arc<dyn VerificationTaskClaimer> {
        &self.verification_task_claimer
    }

    pub fn entitlements(&self) -> &Arc<dyn EntitlementRepository> {
        &self.entitlements
    }

    pub fn access_credentials(&self) -> &Arc<dyn AccessCredentialStore> {
        &self.access_credentials
    }

    pub fn creator_authorities(&self) -> &Arc<dyn CreatorAuthorityStore> {
        &self.creator_authorities
    }

    pub fn creator_connect_flows(&self) -> &Arc<dyn CreatorConnectFlowStore> {
        &self.creator_connect_flows
    }

    pub fn frontend_session_codes(&self) -> &Arc<dyn FrontendSessionCodeStore> {
        &self.frontend_session_codes
    }

    pub fn frontend_sessions(&self) -> &Arc<dyn FrontendSessionStore> {
        &self.frontend_sessions
    }

    pub fn creator_authority_manager(&self) -> &Arc<dyn CreatorAuthorityManager> {
        &self.creator_authority_manager
    }

    pub fn legacy_creator_connect_flow_client(&self) -> &Arc<dyn LegacyCreatorConnectFlowClient> {
        &self.legacy_creator_connect_flow_client
    }

    pub fn dev_static_verifier(&self) -> &Arc<DevStaticVerifier> {
        &self.dev_static_verifier
    }

    pub fn paykit_payment_verifier(
        &self,
    ) -> Option<&Arc<PaykitPaymentVerifier<Arc<PaykitHttpClient>>>> {
        self.paykit_payment_verifier.as_ref()
    }

    pub fn task_ids(&self) -> &Arc<OsRandomTaskIdGenerator> {
        &self.task_ids
    }

    pub fn credential_generator(&self) -> &Arc<OsRandomAccessCredentialGenerator> {
        &self.credential_generator
    }

    pub fn creator_connect_flow_id_generator(&self) -> &Arc<OsRandomCreatorConnectFlowIdGenerator> {
        &self.creator_connect_flow_id_generator
    }

    pub fn frontend_session_code_generator(&self) -> &Arc<OsRandomFrontendSessionCodeGenerator> {
        &self.frontend_session_code_generator
    }

    pub fn frontend_session_token_generator(&self) -> &Arc<OsRandomFrontendSessionTokenGenerator> {
        &self.frontend_session_token_generator
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    #[cfg(test)]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    #[cfg(test)]
    pub fn with_access_credentials(
        mut self,
        access_credentials: Arc<dyn AccessCredentialStore>,
    ) -> Self {
        self.access_credentials = access_credentials;
        self
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn with_legacy_creator_connect_flow_client(
        mut self,
        client: Arc<dyn LegacyCreatorConnectFlowClient>,
    ) -> Self {
        self.legacy_creator_connect_flow_client = client;
        self
    }

    pub fn access_credential_policy(&self) -> AccessCredentialPolicy {
        self.access_credential_policy
    }

    pub fn verification_submission_rate_limiter(
        &self,
    ) -> &Arc<InMemoryVerificationSubmissionRateLimiter> {
        &self.verification_submission_rate_limiter
    }

    pub fn reader_pubky_resolver(&self) -> &Arc<dyn ReaderPubkyResolver> {
        &self.reader_pubky_resolver
    }

    pub fn paykit_http_client(&self) -> Option<&Arc<PaykitHttpClient>> {
        self.paykit_http_client.as_ref()
    }

    pub fn paykit_setup_status_provider(&self) -> Option<&Arc<dyn PaykitSetupStatusProvider>> {
        self.paykit_setup_status_provider.as_ref()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn with_paykit_setup_status_provider(
        mut self,
        provider: Option<Arc<dyn PaykitSetupStatusProvider>>,
    ) -> Self {
        self.paykit_setup_status_provider = provider;
        self
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn with_reader_pubky_resolver(mut self, resolver: Arc<dyn ReaderPubkyResolver>) -> Self {
        self.reader_pubky_resolver = resolver;
        self
    }
}
