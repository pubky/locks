use std::sync::Arc;

use locks_service::{
    application::ports::{
        ContentLockRepository, ContentLockTombstoneRepository, EntitlementRepository,
        GuardedResourceRepository, LockServicePointerRepository,
    },
    infrastructure::{
        postgres::PostgresCreatorAuthorityStore,
        pubky::{
            LegacyCookieCreatorScopedPubkyStorageProvider,
            ProviderBackedPubkyHomeserverStorageClient, PubkyContentLockRepository,
            PubkyContentLockTombstoneRepository, PubkyEntitlementRepository,
            PubkyHomeserverStorageClient, PubkyLegacyCookieSessionImporter,
            PubkyLockServicePointerRepository, PubkyPrivResourceRepository,
        },
    },
};

#[derive(Clone)]
pub(super) struct CreatorRepositoryAdapters {
    pub(super) content_locks: Arc<dyn ContentLockRepository>,
    pub(super) content_lock_tombstones: Arc<dyn ContentLockTombstoneRepository>,
    pub(super) guarded_resources: Arc<dyn GuardedResourceRepository>,
    pub(super) lock_service_pointers: Arc<dyn LockServicePointerRepository>,
    pub(super) entitlements: Arc<dyn EntitlementRepository>,
}

impl CreatorRepositoryAdapters {
    pub(super) fn new(
        content_locks: Arc<dyn ContentLockRepository>,
        content_lock_tombstones: Arc<dyn ContentLockTombstoneRepository>,
        guarded_resources: Arc<dyn GuardedResourceRepository>,
        lock_service_pointers: Arc<dyn LockServicePointerRepository>,
        entitlements: Arc<dyn EntitlementRepository>,
    ) -> Self {
        Self {
            content_locks,
            content_lock_tombstones,
            guarded_resources,
            lock_service_pointers,
            entitlements,
        }
    }

    pub(super) fn pubky_homeserver(
        creator_authority_store: PostgresCreatorAuthorityStore,
        pubky_http_client: pubky::PubkyHttpClient,
    ) -> Self {
        let importer = PubkyLegacyCookieSessionImporter::new(pubky_http_client);
        let provider =
            LegacyCookieCreatorScopedPubkyStorageProvider::new(creator_authority_store, importer);
        let client: Arc<dyn PubkyHomeserverStorageClient> =
            Arc::new(ProviderBackedPubkyHomeserverStorageClient::new(provider));

        Self::new(
            Arc::new(PubkyContentLockRepository::new(client.clone())),
            Arc::new(PubkyContentLockTombstoneRepository::new(client.clone())),
            Arc::new(PubkyPrivResourceRepository::new(client.clone())),
            Arc::new(PubkyLockServicePointerRepository::new(client.clone())),
            Arc::new(PubkyEntitlementRepository::new(client)),
        )
    }
}
