use locks_core::ids::{BundleId, ContentLockPath, CreatorPubky, LockId};
use locks_core::lock_policy::{ContentLock, ContentLockPayload, ContentLockV2};
use locks_core::verification::{
    VerifiedProofBundle, VerifiedProofBundlePayload, validate_verified_content_lock_publication,
};

use crate::application::entitlement_evaluator::evaluate_entitlement;
use crate::application::errors::ApplicationError;
use crate::application::ports::{ContentLockRepository, EntitlementRepository};

/// Current entitlement state after loading, hash/path validation, and logic evaluation.
pub(super) struct ValidEntitlement {
    /// Current hash-verified content lock referenced by the entitlement.
    content_lock: ContentLockPayload,
    /// Canonical Lock ID verified against the entitlement path.
    pub lock_id: LockId,
}

impl ValidEntitlement {
    pub fn requested_credential_ttl_seconds(&self) -> u64 {
        match &self.content_lock {
            ContentLockPayload::V1(lock) => lock.access_policy.requested_credential_ttl_seconds,
            ContentLockPayload::V2(lock) => lock.access_policy.requested_credential_ttl_seconds,
        }
    }

    pub fn resource_for_path(
        &self,
        path: &str,
    ) -> Option<locks_core::lock_policy::GuardedResource> {
        match &self.content_lock {
            ContentLockPayload::V1(lock) => lock.resource_for_path(path),
            ContentLockPayload::V2(lock) => {
                if let Some(primary) = lock
                    .primary_resource
                    .as_ref()
                    .filter(|resource| resource.path == path)
                {
                    return Some(primary.clone());
                }
                lock.secondary_resources.get(path).map(|resource| {
                    locks_core::lock_policy::GuardedResource {
                        path: path.to_owned(),
                        hash: resource.hash,
                        content_type: resource.content_type.clone(),
                        size: resource.size,
                    }
                })
            }
        }
    }
}

/// Loads and validates current entitlement state for credential issuance/validation.
pub(super) async fn load_valid_entitlement(
    entitlements: &dyn EntitlementRepository,
    content_locks: &dyn ContentLockRepository,
    creator: &CreatorPubky,
    bundle_id: &BundleId,
) -> Result<ValidEntitlement, ApplicationError> {
    let entitlement = entitlements
        .get_verified_proof_bundle_payload(creator, bundle_id)
        .await?
        .ok_or(ApplicationError::EntitlementNotFound)?;

    match entitlement {
        VerifiedProofBundlePayload::V1(bundle) => {
            load_valid_v1_entitlement(content_locks, bundle).await
        }
        VerifiedProofBundlePayload::V2(bundle) => {
            bundle
                .validate()
                .map_err(|_| ApplicationError::EntitlementNotSatisfied)?;
            if &bundle.creator != creator
                || &bundle.bundle_id != bundle_id
                || bundle.pubky_lock_resource.creator() != creator
            {
                return Err(ApplicationError::EntitlementNotSatisfied);
            }
            let content_lock = content_locks
                .get_content_lock_payload(
                    bundle.pubky_lock_resource.creator(),
                    bundle.pubky_lock_resource.content_lock_path(),
                )
                .await?
                .ok_or(ApplicationError::ContentLockUnavailable)?;
            let ContentLockPayload::V2(content_lock) = content_lock else {
                return Err(ApplicationError::EntitlementNotSatisfied);
            };
            let lock_id = verify_content_lock_v2_identity(
                &content_lock,
                bundle.pubky_lock_resource.content_lock_path(),
            )?;
            validate_verified_content_lock_publication(&content_lock, &bundle)
                .map_err(|_| ApplicationError::EntitlementNotSatisfied)?;
            Ok(ValidEntitlement {
                content_lock: ContentLockPayload::V2(content_lock),
                lock_id,
            })
        }
    }
}

async fn load_valid_v1_entitlement(
    content_locks: &dyn ContentLockRepository,
    verified_proof_bundle: VerifiedProofBundle,
) -> Result<ValidEntitlement, ApplicationError> {
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
        content_lock: ContentLockPayload::V1(content_lock),
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
    verify_lock_id(actual, content_lock_path)
}

fn verify_content_lock_v2_identity(
    content_lock: &ContentLockV2,
    content_lock_path: &ContentLockPath,
) -> Result<LockId, ApplicationError> {
    let actual =
        content_lock
            .lock_id()
            .map_err(|error| ApplicationError::ContentLockCanonicalization {
                message: error.to_string(),
            })?;
    verify_lock_id(actual, content_lock_path)
}

fn verify_lock_id(
    actual: LockId,
    content_lock_path: &ContentLockPath,
) -> Result<LockId, ApplicationError> {
    let expected = content_lock_path.lock_id().clone();
    if actual == expected {
        Ok(actual)
    } else {
        Err(ApplicationError::ContentLockHashMismatch { expected, actual })
    }
}
