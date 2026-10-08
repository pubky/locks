use std::sync::Arc;

use locks_service::{
    application::ports::{
        ContentLockRepository, EntitlementRepository, GuardedResourceRepository,
        LockServicePointerRepository,
    },
    infrastructure::{
        postgres::PostgresCreatorAuthorityStore,
        pubky::{
            LegacyCookieCreatorScopedPubkyStorageProvider, LockServerGrantPopKeys,
            ProviderBackedPubkyHomeserverStorageClient, PubkyContentLockRepository,
            PubkyEntitlementRepository, PubkyHomeserverStorageClient,
            PubkyLegacyCookieSessionImporter, PubkyLockServicePointerRepository,
            PubkyPrivResourceRepository,
        },
    },
};

#[derive(Clone)]
pub(super) struct CreatorRepositoryAdapters {
    pub(super) content_locks: Arc<dyn ContentLockRepository>,
    pub(super) guarded_resources: Arc<dyn GuardedResourceRepository>,
    pub(super) lock_service_pointers: Arc<dyn LockServicePointerRepository>,
    pub(super) entitlements: Arc<dyn EntitlementRepository>,
}

impl CreatorRepositoryAdapters {
    pub(super) fn new(
        content_locks: Arc<dyn ContentLockRepository>,
        guarded_resources: Arc<dyn GuardedResourceRepository>,
        lock_service_pointers: Arc<dyn LockServicePointerRepository>,
        entitlements: Arc<dyn EntitlementRepository>,
    ) -> Self {
        Self {
            content_locks,
            guarded_resources,
            lock_service_pointers,
            entitlements,
        }
    }

    pub(super) fn pubky_homeserver(
        creator_authority_store: PostgresCreatorAuthorityStore,
        pubky_http_client: pubky::PubkyHttpClient,
        grant_pop_keys: Option<LockServerGrantPopKeys>,
        grant_client_id: Option<pubky::ClientId>,
        grant_required_scopes: Vec<String>,
    ) -> Self {
        let importer = match grant_pop_keys.clone() {
            Some(keys) => PubkyLegacyCookieSessionImporter::new(pubky_http_client)
                .with_grant_policy(
                    keys,
                    grant_client_id.expect("grant keys require client id"),
                    grant_required_scopes,
                ),
            None => PubkyLegacyCookieSessionImporter::new(pubky_http_client),
        };
        let provider = match grant_pop_keys {
            Some(_) => LegacyCookieCreatorScopedPubkyStorageProvider::new_grant(
                creator_authority_store,
                importer,
            ),
            None => LegacyCookieCreatorScopedPubkyStorageProvider::new(
                creator_authority_store,
                importer,
            ),
        };
        let client: Arc<dyn PubkyHomeserverStorageClient> =
            Arc::new(ProviderBackedPubkyHomeserverStorageClient::new(provider));

        Self::new(
            Arc::new(PubkyContentLockRepository::new(client.clone())),
            Arc::new(PubkyPrivResourceRepository::new(client.clone())),
            Arc::new(PubkyLockServicePointerRepository::new(client.clone())),
            Arc::new(PubkyEntitlementRepository::new(client)),
        )
    }
}
