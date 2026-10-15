use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky};
use locks_core::verification::VerifiedProofBundle;

use crate::application::errors::ApplicationError;

/// Repository for verified proof bundles stored as entitlement records.
#[async_trait]
pub trait EntitlementRepository: Send + Sync {
    /// Inserts a verified proof bundle.
    ///
    /// Returns `DuplicateRecord` if an entitlement already exists for the same
    /// creator and bundle ID.
    async fn insert_verified_proof_bundle(
        &self,
        verified_proof_bundle: VerifiedProofBundle,
    ) -> Result<(), ApplicationError>;

    /// Loads a verified proof bundle by creator and bundle ID.
    ///
    /// Returns `Ok(None)` when no entitlement exists.
    async fn get_verified_proof_bundle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<VerifiedProofBundle>, ApplicationError>;

    /// Ensures a verified proof bundle is absent.
    ///
    /// Deleting a missing entitlement is successful; callers needing must-exist
    /// semantics should read first and interpret `None` themselves.
    async fn delete_verified_proof_bundle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<(), ApplicationError>;
}

pub(crate) fn same_entitlement_decision(
    existing: &VerifiedProofBundle,
    candidate: &VerifiedProofBundle,
) -> bool {
    if existing.verification_result.criteria.len() != candidate.verification_result.criteria.len() {
        return false;
    }
    let mut normalized_candidate = candidate.clone();
    for (candidate_result, existing_result) in normalized_candidate
        .verification_result
        .criteria
        .iter_mut()
        .zip(&existing.verification_result.criteria)
    {
        candidate_result.verified_at = existing_result.verified_at;
    }
    existing == &normalized_candidate
}
