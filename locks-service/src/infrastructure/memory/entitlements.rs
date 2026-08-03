use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

use locks_core::ids::{BundleId, CreatorPubky};
use locks_core::verification::VerifiedProofBundle;

use crate::application::errors::ApplicationError;
use crate::application::ports::EntitlementRepository;

type EntitlementKey = (CreatorPubky, BundleId);

/// In-memory entitlement repository for verified proof bundles.
#[derive(Debug, Default)]
pub struct InMemoryEntitlementRepository {
    records: RwLock<HashMap<EntitlementKey, VerifiedProofBundle>>,
}

impl InMemoryEntitlementRepository {
    /// Creates an empty repository.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl EntitlementRepository for InMemoryEntitlementRepository {
    async fn insert_verified_proof_bundle(
        &self,
        verified_proof_bundle: VerifiedProofBundle,
    ) -> Result<(), ApplicationError> {
        let key = (
            verified_proof_bundle.pubky_lock_resource.creator().clone(),
            verified_proof_bundle.bundle_id.clone(),
        );
        let mut records = self.records.write().await;
        if records.contains_key(&key) {
            return Err(ApplicationError::DuplicateRecord {
                record: "verified_proof_bundle",
            });
        }
        records.insert(key, verified_proof_bundle);
        Ok(())
    }

    async fn get_verified_proof_bundle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<VerifiedProofBundle>, ApplicationError> {
        Ok(self
            .records
            .read()
            .await
            .get(&(creator.clone(), bundle_id.clone()))
            .cloned())
    }

    async fn delete_verified_proof_bundle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<(), ApplicationError> {
        self.records
            .write()
            .await
            .remove(&(creator.clone(), bundle_id.clone()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use time::macros::datetime;

    use locks_core::ids::{BundleId, LockServerPubky, PubkyLockResource};
    use locks_core::lock_policy::VerifierType;
    use locks_core::verification::{
        CriterionVerificationResult, EntitlementLifetime, VERIFIED_PROOF_BUNDLE_VERSION,
        VerificationResult,
    };

    use super::*;

    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    #[tokio::test]
    async fn insert_rejects_duplicate_read_miss_is_none_delete_is_ensure_absent() {
        let repo = InMemoryEntitlementRepository::new();
        let bundle = verified_bundle();
        let creator = bundle.pubky_lock_resource.creator().clone();
        let bundle_id = bundle.bundle_id.clone();

        assert_eq!(
            repo.get_verified_proof_bundle(&creator, &bundle_id)
                .await
                .unwrap(),
            None
        );

        repo.insert_verified_proof_bundle(bundle.clone())
            .await
            .unwrap();
        assert_eq!(
            repo.get_verified_proof_bundle(&creator, &bundle_id)
                .await
                .unwrap(),
            Some(bundle.clone())
        );
        assert_eq!(
            repo.insert_verified_proof_bundle(bundle).await,
            Err(ApplicationError::DuplicateRecord {
                record: "verified_proof_bundle",
            })
        );

        repo.delete_verified_proof_bundle(&creator, &bundle_id)
            .await
            .unwrap();
        repo.delete_verified_proof_bundle(&creator, &bundle_id)
            .await
            .unwrap();
        assert_eq!(
            repo.get_verified_proof_bundle(&creator, &bundle_id)
                .await
                .unwrap(),
            None
        );
    }

    fn verified_bundle() -> VerifiedProofBundle {
        VerifiedProofBundle {
            version: VERIFIED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(&format!(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/{LOCK_ID}.json"
            ))
            .unwrap(),
            verification_result: VerificationResult {
                criteria: vec![CriterionVerificationResult {
                    criterion_id: "criterion-1".to_owned(),
                    satisfied: true,
                    verified_at: datetime!(2026-05-29 12:00:00 UTC),
                    verified_by: LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo").unwrap(),
                    verifier_type: VerifierType::DevStatic,
                }],
            },
            entitlement_lifetime: EntitlementLifetime::Unbounded,
        }
    }
}
