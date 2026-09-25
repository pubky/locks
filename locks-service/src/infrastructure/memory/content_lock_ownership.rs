use std::collections::HashMap;

use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, LockId};
use tokio::sync::RwLock;

use crate::application::errors::ApplicationError;
use crate::application::models::{ContentLockOwnership, ContentLockOwnershipStatus};
use crate::application::ports::ContentLockOwnershipRepository;

type OwnershipKey = (CreatorPubky, String);

/// In-memory exclusive guarded-path ownership repository for tests and ephemeral runtime.
#[derive(Debug, Default)]
pub struct InMemoryContentLockOwnershipRepository {
    records: RwLock<HashMap<OwnershipKey, ContentLockOwnership>>,
}

impl InMemoryContentLockOwnershipRepository {
    /// Creates an empty repository.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ContentLockOwnershipRepository for InMemoryContentLockOwnershipRepository {
    async fn reserve_paths(
        &self,
        creator: &CreatorPubky,
        guarded_paths: &[String],
        lock_id: &LockId,
    ) -> Result<(), ApplicationError> {
        let mut records = self.records.write().await;
        for guarded_path in guarded_paths {
            if let Some(existing) = records.get(&(creator.clone(), guarded_path.clone()))
                && (existing.lock_id != *lock_id
                    || existing.status == ContentLockOwnershipStatus::Reserved)
            {
                return Err(ApplicationError::ContentLockPathConflict {
                    guarded_path: guarded_path.clone(),
                });
            }
        }

        for guarded_path in guarded_paths {
            records
                .entry((creator.clone(), guarded_path.clone()))
                .or_insert_with(|| ContentLockOwnership {
                    creator: creator.clone(),
                    guarded_path: guarded_path.clone(),
                    lock_id: lock_id.clone(),
                    status: ContentLockOwnershipStatus::Reserved,
                });
        }
        Ok(())
    }

    async fn mark_paths_published(
        &self,
        creator: &CreatorPubky,
        guarded_paths: &[String],
        lock_id: &LockId,
    ) -> Result<(), ApplicationError> {
        let mut records = self.records.write().await;
        for guarded_path in guarded_paths {
            let Some(ownership) = records.get(&(creator.clone(), guarded_path.clone())) else {
                return Err(ApplicationError::MissingRecord {
                    record: "content_lock_ownership",
                });
            };
            if ownership.lock_id != *lock_id {
                return Err(ApplicationError::ContentLockPathConflict {
                    guarded_path: guarded_path.clone(),
                });
            }
        }
        for guarded_path in guarded_paths {
            let ownership = records
                .get_mut(&(creator.clone(), guarded_path.clone()))
                .expect("ownership set was validated while holding the write lock");
            ownership.status = ContentLockOwnershipStatus::Published;
        }
        Ok(())
    }

    async fn compensate_reserved_paths(
        &self,
        creator: &CreatorPubky,
        guarded_paths: &[String],
        lock_id: &LockId,
    ) -> Result<(), ApplicationError> {
        let mut records = self.records.write().await;
        for guarded_path in guarded_paths {
            let key = (creator.clone(), guarded_path.clone());
            let remove = records.get(&key).is_some_and(|ownership| {
                ownership.lock_id == *lock_id
                    && ownership.status == ContentLockOwnershipStatus::Reserved
            });
            if remove {
                records.remove(&key);
            }
        }
        Ok(())
    }

    async fn get_path_ownership(
        &self,
        creator: &CreatorPubky,
        guarded_path: &str,
    ) -> Result<Option<ContentLockOwnership>, ApplicationError> {
        Ok(self
            .records
            .read()
            .await
            .get(&(creator.clone(), guarded_path.to_owned()))
            .cloned())
    }
}
