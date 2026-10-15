pub mod access_credentials;
pub mod content_lock_deletions;
pub mod content_lock_ownership;
pub mod content_locks;
pub mod entitlements;
pub mod guarded_resources;
pub mod lock_service_pointers;
pub mod verification_task_claims;
pub mod verification_tasks;

use content_lock_deletions::InMemoryContentLockDeletionRepository;
use content_lock_ownership::InMemoryContentLockOwnershipRepository;

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
