use async_trait::async_trait;
use locks_core::content_lock_deletion::ContentLockDeletionTombstone;
use locks_core::ids::{ContentLockPath, CreatorPubky};
use locks_core::lock_policy::ContentLock;

use crate::application::errors::ApplicationError;

/// Byte-for-byte state of a canonical public deletion tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TombstoneReadback {
    /// The public bytes exactly match the canonical expected tombstone bytes.
    Exact,
    /// The canonical public lock path is absent.
    Missing,
    /// The path contains bytes other than the canonical expected tombstone.
    Replaced,
}

/// Exact public tombstone publication and readback boundary.
#[async_trait]
pub trait ContentLockTombstoneRepository: Send + Sync {
    /// Reconciles withdrawal from the current canonical public bytes.
    ///
    /// An exact tombstone is replay success without a write. The implementation compares current
    /// bytes with the frozen original before publishing and fails closed on bytes already observed
    /// as missing or replaced. Pubky 0.9.3 has no conditional write, so this is deliberately not a
    /// CAS guarantee: an out-of-band replacement racing between the comparison and unconditional
    /// tombstone PUT can be overwritten under the product's documented TOCTOU exception.
    async fn withdraw_content_lock(
        &self,
        creator: CreatorPubky,
        content_lock_path: ContentLockPath,
        frozen_original: &ContentLock,
        tombstone: &ContentLockDeletionTombstone,
    ) -> Result<TombstoneReadback, ApplicationError>;

    /// Classifies raw bytes at the canonical public lock path without parsing them as a lock.
    async fn read_tombstone(
        &self,
        creator: &CreatorPubky,
        content_lock_path: &ContentLockPath,
        expected: &ContentLockDeletionTombstone,
    ) -> Result<TombstoneReadback, ApplicationError>;

    /// Force-only operation that unconditionally deletes whatever bytes occupy the canonical
    /// public lock path and succeeds only after a raw read verifies that the path is absent.
    ///
    /// This deliberately does not compare or parse the current bytes: an original Content Lock,
    /// the expected tombstone, or any replacement is deleted.
    async fn force_delete_content_lock_and_verify_absent(
        &self,
        creator: &CreatorPubky,
        content_lock_path: &ContentLockPath,
    ) -> Result<(), ApplicationError>;
}

pub(crate) fn canonical_tombstone_bytes(
    tombstone: &ContentLockDeletionTombstone,
) -> Result<Vec<u8>, ApplicationError> {
    serde_json::to_vec(tombstone).map_err(|error| ApplicationError::Storage {
        message: format!("failed to serialize content lock deletion tombstone: {error}"),
    })
}

pub(crate) fn classify_tombstone_bytes(
    actual: Option<&[u8]>,
    expected: &[u8],
) -> TombstoneReadback {
    match actual {
        None => TombstoneReadback::Missing,
        Some(actual) if actual == expected => TombstoneReadback::Exact,
        Some(_) => TombstoneReadback::Replaced,
    }
}
