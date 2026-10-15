pub mod access_credentials;
pub mod content_lock_deletions;
pub mod content_lock_ownership;
pub mod content_locks;
pub mod entitlements;
pub mod guarded_resources;
pub mod lock_service_pointers;
pub mod verification_task_claims;
pub mod verification_task_deletion_fence;
pub mod verification_tasks;

use content_lock_deletions::InMemoryContentLockDeletionRepository;
use content_lock_ownership::InMemoryContentLockOwnershipRepository;
use std::sync::Arc;
use verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence;

/// Creates publication repositories sharing ownership state for atomic compensation.
pub fn content_lock_publication_repositories() -> (
    InMemoryContentLockDeletionRepository,
    InMemoryContentLockOwnershipRepository,
) {
    let ownership = InMemoryContentLockOwnershipRepository::new();
    let deletions =
        InMemoryContentLockDeletionRepository::with_ownership_records(ownership.records());
    (deletions, ownership)
}

#[cfg(test)]
pub(crate) fn content_lock_publication_repositories_with_bookkeeping_failures(
    mark: usize,
    compensate: usize,
    abandon: usize,
) -> (
    InMemoryContentLockDeletionRepository,
    InMemoryContentLockOwnershipRepository,
) {
    let ownership = InMemoryContentLockOwnershipRepository::new();
    let deletions =
        InMemoryContentLockDeletionRepository::with_ownership_records(ownership.records())
            .with_publication_bookkeeping_failures(mark, compensate, abandon);
    (deletions, ownership)
}

/// Creates publication repositories sharing ownership and verification-deletion fences.
pub fn content_lock_publication_repositories_with_verification_task_fence(
    verification_task_fence: Arc<InMemoryVerificationTaskDeletionFence>,
) -> (
    InMemoryContentLockDeletionRepository,
    InMemoryContentLockOwnershipRepository,
) {
    let ownership = InMemoryContentLockOwnershipRepository::new();
    let deletions = InMemoryContentLockDeletionRepository::with_shared_fences(
        ownership.records(),
        verification_task_fence,
    );
    (deletions, ownership)
}
