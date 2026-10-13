use locks_core::ids::{BundleId, ContentLockPath, CreatorPubky, LockId};
use locks_core::lock_policy::ContentLock;
use locks_core::verification::VerifiedProofBundle;

use crate::application::entitlement_evaluator::evaluate_entitlement;
use crate::application::errors::ApplicationError;
use crate::application::ports::{ContentLockRepository, EntitlementRepository};

/// Current entitlement state after loading, hash/path validation, and logic evaluation.
pub(super) struct ValidEntitlement {
    /// Current hash-verified content lock referenced by the entitlement.
    pub content_lock: ContentLock,
    /// Canonical Lock ID verified against the entitlement path.
    pub lock_id: LockId,
}

/// Loads and validates current entitlement state for credential issuance/validation.
pub(super) async fn load_valid_entitlement(
    entitlements: &dyn EntitlementRepository,
    content_locks: &dyn ContentLockRepository,
    creator: &CreatorPubky,
    bundle_id: &BundleId,
) -> Result<ValidEntitlement, ApplicationError> {
    let verified_proof_bundle = entitlements
        .get_verified_proof_bundle(creator, bundle_id)
        .await?
        .ok_or(ApplicationError::EntitlementNotFound)?;

    let content_lock = load_current_content_lock(content_locks, &verified_proof_bundle).await?;
    let lock_id = verify_content_lock_identity(
        &content_lock,
        verified_proof_bundle
            .pubky_lock_resource
            .content_lock_path(),
    )?;

    if !evaluate_entitlement(&content_lock, &verified_proof_bundle.verification_result)? {
        return Err(ApplicationError::EntitlementNotSatisfied);
    }

    Ok(ValidEntitlement {
        content_lock,
        lock_id,
    })
}

async fn load_current_content_lock(
    content_locks: &dyn ContentLockRepository,
    verified_proof_bundle: &VerifiedProofBundle,
) -> Result<ContentLock, ApplicationError> {
    content_locks
        .get_content_lock(
            verified_proof_bundle.pubky_lock_resource.creator(),
            verified_proof_bundle
                .pubky_lock_resource
                .content_lock_path(),
        )
        .await?
        .ok_or(ApplicationError::ContentLockUnavailable)
}

pub(super) fn verify_content_lock_identity(
    content_lock: &ContentLock,
    content_lock_path: &ContentLockPath,
) -> Result<LockId, ApplicationError> {
    let actual =
        content_lock
            .lock_id()
            .map_err(|error| ApplicationError::ContentLockCanonicalization {
                message: error.to_string(),
            })?;
    let expected = content_lock_path.lock_id().clone();

    if actual == expected {
        Ok(actual)
    } else {
        Err(ApplicationError::ContentLockHashMismatch { expected, actual })
    }
}
