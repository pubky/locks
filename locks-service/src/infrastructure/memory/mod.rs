pub mod access_credentials;
pub mod content_lock_deletion_action_ownership;
pub mod content_lock_deletions;
pub mod content_lock_ownership;
pub mod content_lock_tombstones;
pub mod content_locks;
pub mod entitlements;
pub mod guarded_resources;
pub mod lock_service_pointers;
pub mod public_content_locks;
pub mod verification_task_claims;
pub mod verification_task_deletion_fence;
pub mod verification_tasks;

use content_lock_deletions::InMemoryContentLockDeletionRepository;
use content_lock_ownership::InMemoryContentLockOwnershipRepository;
use std::sync::Arc;

use access_credentials::InMemoryAccessCredentialStore;
use verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence;

/// Creates publication repositories sharing ownership state for atomic compensation.
pub fn content_lock_publication_repositories() -> (
    InMemoryContentLockDeletionRepository,
    InMemoryContentLockOwnershipRepository,
) {
    content_lock_publication_repositories_with_dependencies(
        Arc::new(InMemoryAccessCredentialStore::new()),
        Arc::new(InMemoryVerificationTaskDeletionFence::new()),
    )
}

/// Creates publication repositories sharing ownership and deletion runtime dependencies.
pub fn content_lock_publication_repositories_with_dependencies(
    access_credentials: Arc<InMemoryAccessCredentialStore>,
    verification_task_fence: Arc<InMemoryVerificationTaskDeletionFence>,
) -> (
    InMemoryContentLockDeletionRepository,
    InMemoryContentLockOwnershipRepository,
) {
    let ownership = InMemoryContentLockOwnershipRepository::new();
    let deletions = InMemoryContentLockDeletionRepository::with_dependencies(
        access_credentials,
        verification_task_fence,
        ownership.records(),
    );
    (deletions, ownership)
}
