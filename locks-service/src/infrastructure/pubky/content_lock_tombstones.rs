use async_trait::async_trait;
use locks_core::content_lock_deletion::ContentLockDeletionTombstone;
use locks_core::ids::{ContentLockPath, CreatorPubky};
use locks_core::lock_policy::ContentLock;

use crate::application::errors::ApplicationError;
use crate::application::ports::content_lock_tombstone::{
    canonical_tombstone_bytes, classify_tombstone_bytes,
};
use crate::application::ports::{ContentLockTombstoneRepository, TombstoneReadback};
use crate::infrastructure::pubky::storage_client::PubkyHomeserverStorageClient;

/// Pubky homeserver adapter for exact public content-lock tombstone bytes.
///
/// Pubky 0.9.3 cannot condition a PUT on the bytes read below. The pre-write comparison protects
/// already-visible replacements and crash/reclaim replay, but an out-of-band replacement can race
/// between GET and PUT and be overwritten. This accepted limitation is documented in the active
/// graceful-deletion plan and public API reference; this adapter must not be described as CAS.
#[derive(Debug)]
pub struct PubkyContentLockTombstoneRepository<C> {
    client: C,
}

impl<C> PubkyContentLockTombstoneRepository<C> {
    pub fn new(client: C) -> Self {
        Self { client }
    }

    pub fn client(&self) -> &C {
        &self.client
    }
}

#[async_trait]
impl<C> ContentLockTombstoneRepository for PubkyContentLockTombstoneRepository<C>
where
    C: PubkyHomeserverStorageClient,
{
    async fn withdraw_content_lock(
        &self,
        creator: CreatorPubky,
        content_lock_path: ContentLockPath,
        frozen_original: &ContentLock,
        tombstone: &ContentLockDeletionTombstone,
    ) -> Result<TombstoneReadback, ApplicationError> {
        let tombstone_bytes = canonical_tombstone_bytes(tombstone)?;
        let original_bytes =
            frozen_original
                .canonical_json_bytes()
                .map_err(|error| ApplicationError::Storage {
                    message: format!("failed to serialize frozen content lock: {error}"),
                })?;
        let actual = self
            .client
            .get_bytes_as_creator(&creator, &content_lock_path.to_string())
            .await?;
        match actual.as_ref().map(|resource| resource.bytes.as_slice()) {
            Some(actual) if actual == tombstone_bytes => return Ok(TombstoneReadback::Exact),
            Some(actual) if actual == original_bytes => {}
            None => return Ok(TombstoneReadback::Missing),
            Some(_) => return Ok(TombstoneReadback::Replaced),
        }
        self.client
            .put_bytes_as_creator(
                &creator,
                &content_lock_path.to_string(),
                tombstone_bytes,
                "application/json",
            )
            .await?;
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
        let actual = self
            .client
            .get_bytes_as_creator(creator, &content_lock_path.to_string())
            .await?;
        Ok(classify_tombstone_bytes(
            actual.as_ref().map(|resource| resource.bytes.as_slice()),
            &expected,
        ))
    }

    async fn force_delete_content_lock_and_verify_absent(
        &self,
        creator: &CreatorPubky,
        content_lock_path: &ContentLockPath,
    ) -> Result<(), ApplicationError> {
        let path = content_lock_path.to_string();
        self.client.delete_as_creator(creator, &path).await?;
        if self
            .client
            .get_bytes_as_creator(creator, &path)
            .await?
            .is_some()
        {
            return Err(ApplicationError::Storage {
                message: "forced public content lock deletion did not reach absence".to_owned(),
            });
        }
        Ok(())
    }
}
