use async_trait::async_trait;
use locks_core::content_lock_deletion::ContentLockDeletionTombstone;
use locks_core::ids::{ContentLockPath, CreatorPubky};
use locks_core::lock_policy::ContentLock;

use crate::application::errors::ApplicationError;
use crate::application::ports::content_lock_tombstone::{
    canonical_tombstone_bytes, classify_tombstone_bytes,
};
use crate::application::ports::{ContentLockTombstoneRepository, TombstoneReadback};
use crate::infrastructure::memory::public_content_locks::InMemoryPublicContentLockStore;

/// In-memory exact-byte public tombstone adapter.
#[derive(Debug, Default)]
pub struct InMemoryContentLockTombstoneRepository {
    public_store: InMemoryPublicContentLockStore,
}

impl InMemoryContentLockTombstoneRepository {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a raw tombstone adapter over shared canonical public-path storage.
    pub fn with_public_store(public_store: InMemoryPublicContentLockStore) -> Self {
        Self { public_store }
    }
}

#[async_trait]
impl ContentLockTombstoneRepository for InMemoryContentLockTombstoneRepository {
    async fn withdraw_content_lock(
        &self,
        creator: CreatorPubky,
        content_lock_path: ContentLockPath,
        frozen_original: &ContentLock,
        tombstone: &ContentLockDeletionTombstone,
    ) -> Result<TombstoneReadback, ApplicationError> {
        let expected = canonical_tombstone_bytes(tombstone)?;
        let original =
            frozen_original
                .canonical_json_bytes()
                .map_err(|error| ApplicationError::Storage {
                    message: format!("failed to serialize frozen content lock: {error}"),
                })?;
        match self
            .public_store
            .get(&creator, &content_lock_path)
            .await
            .as_deref()
        {
            Some(actual) if actual == expected => return Ok(TombstoneReadback::Exact),
            Some(actual) if actual == original => {}
            None => return Ok(TombstoneReadback::Missing),
            Some(_) => return Ok(TombstoneReadback::Replaced),
        }
        self.public_store
            .put(creator.clone(), content_lock_path.clone(), expected)
            .await;
        self.read_tombstone(&creator, &content_lock_path, tombstone)
            .await
    }

    async fn read_tombstone(
        &self,
        creator: &CreatorPubky,
        content_lock_path: &ContentLockPath,
        expected: &ContentLockDeletionTombstone,
    ) -> Result<TombstoneReadback, ApplicationError> {
        let expected = canonical_tombstone_bytes(expected)?;
        Ok(classify_tombstone_bytes(
            self.public_store
                .get(creator, content_lock_path)
                .await
                .as_deref(),
            &expected,
        ))
    }

    async fn force_delete_content_lock_and_verify_absent(
        &self,
        creator: &CreatorPubky,
        content_lock_path: &ContentLockPath,
    ) -> Result<(), ApplicationError> {
        self.public_store.remove(creator, content_lock_path).await;
        if self
            .public_store
            .get(creator, content_lock_path)
            .await
            .is_some()
        {
            return Err(ApplicationError::Storage {
                message: "forced public content lock deletion did not reach absence".to_owned(),
            });
        }
        Ok(())
    }
}
