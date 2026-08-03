use async_trait::async_trait;
use locks_core::ids::{ContentLockPath, CreatorPubky};
use locks_core::lock_policy::ContentLock;
use locks_core::lock_service_pointer::LockServicePointer;

use crate::application::errors::ApplicationError;

/// Repository for public content lock payloads.
#[async_trait]
pub trait ContentLockRepository: Send + Sync {
    /// Creates or replaces a content lock for a creator and canonical content lock path.
    async fn upsert_content_lock(
        &self,
        creator: CreatorPubky,
        content_lock_path: ContentLockPath,
        content_lock: ContentLock,
    ) -> Result<(), ApplicationError>;

    /// Loads a content lock by creator and canonical content lock path.
    ///
    /// Returns `Ok(None)` when the content lock is absent. Use cases decide how
    /// to interpret absence.
    async fn get_content_lock(
        &self,
        creator: &CreatorPubky,
        content_lock_path: &ContentLockPath,
    ) -> Result<Option<ContentLock>, ApplicationError>;
}

/// Repository for creator-owned Lock Service Pointer config objects.
#[async_trait]
pub trait LockServicePointerRepository: Send + Sync {
    /// Creates or replaces the current Lock Service Pointer for a creator.
    async fn upsert_lock_service_pointer(
        &self,
        creator: CreatorPubky,
        pointer: LockServicePointer,
    ) -> Result<(), ApplicationError>;

    /// Loads the current Lock Service Pointer for a creator.
    async fn get_lock_service_pointer(
        &self,
        creator: &CreatorPubky,
    ) -> Result<Option<LockServicePointer>, ApplicationError>;
}
