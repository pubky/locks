use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, GuardedResourceHash};

use crate::application::errors::ApplicationError;
use crate::application::models::GuardedResourceRecord;

/// Repository for guarded resource bytes used by the first retrieval/access slice.
#[async_trait]
pub trait GuardedResourceRepository: Send + Sync {
    /// Creates or replaces a guarded resource record.
    async fn upsert_guarded_resource(
        &self,
        guarded_resource: GuardedResourceRecord,
    ) -> Result<(), ApplicationError>;

    /// Loads a guarded resource by creator, path, and expected hash.
    ///
    /// Returns `Ok(None)` when the guarded resource is absent or its current
    /// stored hash differs from the expected hash.
    async fn get_guarded_resource(
        &self,
        creator: &CreatorPubky,
        path: &str,
        hash: &GuardedResourceHash,
    ) -> Result<Option<GuardedResourceRecord>, ApplicationError>;

    /// Loads the current guarded resource descriptor and bytes by creator/path.
    ///
    /// Returns `Ok(None)` when the guarded resource is absent.
    async fn get_current_guarded_resource(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<GuardedResourceRecord>, ApplicationError>;

    /// Deletes the current guarded resource by creator/path.
    ///
    /// Returns `true` when an existing resource was deleted and `false` when
    /// the resource was already missing.
    async fn delete_guarded_resource(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<bool, ApplicationError>;
}
