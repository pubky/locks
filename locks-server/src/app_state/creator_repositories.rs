use std::sync::Arc;

use locks_service::{
    application::ports::{
        ContentLockRepository, ContentLockTombstoneRepository, EntitlementRepository,
        GuardedResourceRepository, LockServicePointerRepository,
    },
    infrastructure::{
        postgres::PostgresCreatorAuthorityStore,
        pubky::{
            GrantCreatorScopedPubkyStorageProvider, GrantSessionCache,
            ProviderBackedPubkyHomeserverStorageClient, PubkyContentLockRepository,
            PubkyContentLockTombstoneRepository, PubkyEntitlementRepository,
            PubkyGrantSessionImporter, PubkyHomeserverStorageClient,
            PubkyLockServicePointerRepository, PubkyPrivResourceRepository,
        },
    },
};

use super::UnavailablePubkyHomeserverStorageClient;

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

    /// Homeserver-backed repositories over the grant session cache. Without one (acquisition
    /// disabled and no grant-connect table) every creator homeserver operation fails closed.
    pub(super) fn pubky_homeserver(
        grant_session_cache: Option<
            Arc<GrantSessionCache<PostgresCreatorAuthorityStore, PubkyGrantSessionImporter>>,
        >,
    ) -> Self {
        let client: Arc<dyn PubkyHomeserverStorageClient> = match grant_session_cache {
            Some(cache) => Arc::new(ProviderBackedPubkyHomeserverStorageClient::new(
                GrantCreatorScopedPubkyStorageProvider::new(cache),
            )),
            None => Arc::new(UnavailablePubkyHomeserverStorageClient),
        };

        Self::new(
            Arc::new(PubkyContentLockRepository::new(client.clone())),
            Arc::new(PubkyContentLockTombstoneRepository::new(client.clone())),
            Arc::new(PubkyPrivResourceRepository::new(client.clone())),
            Arc::new(PubkyLockServicePointerRepository::new(client.clone())),
            Arc::new(PubkyEntitlementRepository::new(client)),
        )
    }
}
