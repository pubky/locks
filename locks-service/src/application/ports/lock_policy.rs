use async_trait::async_trait;
use locks_core::ids::{ContentLockPath, CreatorPubky};
use locks_core::lock_policy::ContentLock;
use locks_core::lock_service_pointer::LockServicePointer;

use crate::application::errors::ApplicationError;

/// Classifies whether a content-lock write may have reached external storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentLockWriteError {
    /// Failure occurred before external write dispatch, so local admission can be compensated.
    NotStarted(ApplicationError),
    /// External write dispatch may have occurred, so publication must remain fenced.
    Ambiguous(ApplicationError),
}

impl ContentLockWriteError {
    pub fn into_inner(self) -> ApplicationError {
        match self {
            Self::NotStarted(error) | Self::Ambiguous(error) => error,
        }
    }
}

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

    /// Writes a content lock while preserving whether failure preceded external dispatch.
    async fn upsert_content_lock_classified(
        &self,
        creator: CreatorPubky,
        content_lock_path: ContentLockPath,
        content_lock: ContentLock,
    ) -> Result<(), ContentLockWriteError> {
        self.upsert_content_lock(creator, content_lock_path, content_lock)
            .await
            .map_err(ContentLockWriteError::Ambiguous)
    }

    /// Loads a content lock by creator and canonical content lock path.
    ///
    /// Returns `Ok(None)` when the content lock is absent. Use cases decide how
    /// to interpret absence.
    async fn get_content_lock(
        &self,
        creator: &CreatorPubky,
        content_lock_path: &ContentLockPath,
    ) -> Result<Option<ContentLock>, ApplicationError>;

    /// Deletes the public content lock at the canonical creator-owned path.
    /// Returns true when a record existed and false when already absent.
    async fn delete_content_lock(
        &self,
        creator: &CreatorPubky,
        content_lock_path: &ContentLockPath,
    ) -> Result<bool, ApplicationError>;
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
