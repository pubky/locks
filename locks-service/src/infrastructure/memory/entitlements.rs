use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

use locks_core::ids::{BundleId, CreatorPubky};
use locks_core::verification::{
    VerifiedProofBundle, VerifiedProofBundlePayload, VerifiedProofBundleV2,
};

use crate::application::errors::ApplicationError;
use crate::application::ports::EntitlementRepository;

type EntitlementKey = (CreatorPubky, BundleId);

#[derive(Debug, Clone)]
enum StoredEntitlement {
    V1(VerifiedProofBundle),
    V2(VerifiedProofBundleV2),
}

/// In-memory entitlement repository for verified proof bundles.
#[derive(Debug, Default)]
pub struct InMemoryEntitlementRepository {
    records: RwLock<HashMap<EntitlementKey, StoredEntitlement>>,
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
        records.insert(key, StoredEntitlement::V1(verified_proof_bundle));
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
            .and_then(|record| match record {
                StoredEntitlement::V1(bundle) => Some(bundle.clone()),
                StoredEntitlement::V2(_) => None,
            }))
    }

    async fn get_verified_proof_bundle_payload(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<VerifiedProofBundlePayload>, ApplicationError> {
        let key = (creator.clone(), bundle_id.clone());
        Ok(self
            .records
            .read()
            .await
            .get(&key)
            .map(|record| match record {
                StoredEntitlement::V1(bundle) => VerifiedProofBundlePayload::V1(bundle.clone()),
                StoredEntitlement::V2(bundle) => VerifiedProofBundlePayload::V2(bundle.clone()),
            }))
    }

    async fn delete_verified_proof_bundle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<(), ApplicationError> {
        let key = (creator.clone(), bundle_id.clone());
        self.records.write().await.remove(&key);
        Ok(())
    }

    async fn insert_verified_proof_bundle_v2(
        &self,
        verified_proof_bundle: VerifiedProofBundleV2,
    ) -> Result<(), ApplicationError> {
        verified_proof_bundle
            .validate()
            .map_err(|error| ApplicationError::Storage {
                message: error.to_string(),
            })?;
        let key = (
            verified_proof_bundle.creator.clone(),
            verified_proof_bundle.bundle_id.clone(),
        );
        let mut records = self.records.write().await;
        if let Some(existing) = records.get(&key) {
            return if matches!(existing, StoredEntitlement::V2(bundle) if bundle == &verified_proof_bundle)
            {
                Ok(())
            } else {
                Err(ApplicationError::DuplicateRecord {
                    record: "verified_proof_bundle_v2",
                })
            };
        }
        records.insert(key, StoredEntitlement::V2(verified_proof_bundle));
        Ok(())
    }

    async fn get_verified_proof_bundle_v2(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<VerifiedProofBundleV2>, ApplicationError> {
        Ok(self
            .records
            .read()
            .await
            .get(&(creator.clone(), bundle_id.clone()))
            .and_then(|record| match record {
                StoredEntitlement::V1(_) => None,
                StoredEntitlement::V2(bundle) => Some(bundle.clone()),
            }))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, LockServerPubky, PubkyLockResource};
    use locks_core::lock_policy::VerifierType;
    use locks_core::verification::{
        CriterionVerificationResult, EntitlementLifetime, SatisfiedByV2,
        VERIFIED_PROOF_BUNDLE_V2_VERSION, VERIFIED_PROOF_BUNDLE_VERSION, VerificationResult,
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

    #[tokio::test]
    async fn delete_removes_v2_entitlement_at_same_canonical_bundle_path() {
        let repo = InMemoryEntitlementRepository::new();
        let bundle = verified_bundle_v2();
        let creator = bundle.creator.clone();
        let bundle_id = bundle.bundle_id.clone();

        repo.insert_verified_proof_bundle_v2(bundle).await.unwrap();
        repo.delete_verified_proof_bundle(&creator, &bundle_id)
            .await
            .unwrap();

        assert_eq!(
            repo.get_verified_proof_bundle_v2(&creator, &bundle_id)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn cross_version_inserts_have_one_atomic_winner_and_delete_clears_the_path() {
        use std::sync::Arc;

        use tokio::sync::Barrier;

        let repo = Arc::new(InMemoryEntitlementRepository::new());
        let v1 = verified_bundle();
        let v2 = verified_bundle_v2();
        let creator = v2.creator.clone();
        let bundle_id = v2.bundle_id.clone();
        let barrier = Arc::new(Barrier::new(3));

        let insert_v1 = {
            let repo = Arc::clone(&repo);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                repo.insert_verified_proof_bundle(v1).await
            })
        };
        let insert_v2 = {
            let repo = Arc::clone(&repo);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                repo.insert_verified_proof_bundle_v2(v2).await
            })
        };
        barrier.wait().await;

        let results = [insert_v1.await.unwrap(), insert_v2.await.unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(ApplicationError::DuplicateRecord { .. })))
                .count(),
            1
        );
        assert!(
            repo.get_verified_proof_bundle_payload(&creator, &bundle_id)
                .await
                .unwrap()
                .is_some()
        );

        repo.delete_verified_proof_bundle(&creator, &bundle_id)
            .await
            .unwrap();
        assert_eq!(
            repo.get_verified_proof_bundle_payload(&creator, &bundle_id)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repo.get_verified_proof_bundle(&creator, &bundle_id)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repo.get_verified_proof_bundle_v2(&creator, &bundle_id)
                .await
                .unwrap(),
            None
        );
    }

    fn verified_bundle_v2() -> VerifiedProofBundleV2 {
        VerifiedProofBundleV2 {
            version: VERIFIED_PROOF_BUNDLE_V2_VERSION,
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            reader: CreatorPubky::from_str(
                "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky",
            )
            .unwrap(),
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(&format!(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/{LOCK_ID}.json"
            ))
            .unwrap(),
            verified_at: datetime!(2026-05-29 12:00:00 UTC),
            satisfied_by: vec![SatisfiedByV2::Graph],
        }
    }

    fn verified_bundle() -> VerifiedProofBundle {
        VerifiedProofBundle {
            version: VERIFIED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(&format!(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/{LOCK_ID}.json"
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
