use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, LockId};

use crate::application::errors::ApplicationError;
use crate::application::models::ContentLockOwnership;

/// Repository for exclusive creator-scoped guarded-path ownership.
#[async_trait]
pub trait ContentLockOwnershipRepository: Send + Sync {
    /// Atomically reserves every path for the intended lock.
    ///
    /// Exact retry for the same published Lock ID is idempotent. An existing
    /// reservation, or a path owned by a different Lock ID, returns
    /// `ContentLockPathConflict` and reserves none of the previously unowned
    /// paths in the request. Blocking in-flight reservations prevents one
    /// publisher from compensating another publisher's ownership.
    async fn reserve_paths(
        &self,
        creator: &CreatorPubky,
        guarded_paths: &[String],
        lock_id: &LockId,
    ) -> Result<(), ApplicationError>;

    /// Marks the intended lock's complete path set as successfully published.
    async fn mark_paths_published(
        &self,
        creator: &CreatorPubky,
        guarded_paths: &[String],
        lock_id: &LockId,
    ) -> Result<(), ApplicationError>;

    /// Best-effort publication-failure compensation for matching reserved rows.
    ///
    /// Published ownership is deliberately retained.
    async fn compensate_reserved_paths(
        &self,
        creator: &CreatorPubky,
        guarded_paths: &[String],
        lock_id: &LockId,
    ) -> Result<(), ApplicationError>;

    /// Reads current ownership for a creator-scoped guarded path.
    async fn get_path_ownership(
        &self,
        creator: &CreatorPubky,
        guarded_path: &str,
    ) -> Result<Option<ContentLockOwnership>, ApplicationError>;
}
