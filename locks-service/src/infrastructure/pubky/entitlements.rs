use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky};
use locks_core::lock_policy::verified_proof_bundle_path;
use locks_core::verification::VerifiedProofBundle;

use crate::application::errors::ApplicationError;
use crate::application::ports::EntitlementRepository;
use crate::infrastructure::pubky::storage_client::PubkyHomeserverStorageClient;

/// Pubky homeserver-backed repository for verified proof bundle entitlements.
#[derive(Debug)]
pub struct PubkyEntitlementRepository<C> {
    client: C,
}

impl<C> PubkyEntitlementRepository<C> {
    /// Creates a repository backed by a Pubky homeserver storage client.
    pub fn new(client: C) -> Self {
        Self { client }
    }

    /// Returns the storage client. Exposed for tests and composition.
    pub fn client(&self) -> &C {
        &self.client
    }
}

#[async_trait]
impl<C> EntitlementRepository for PubkyEntitlementRepository<C>
where
    C: PubkyHomeserverStorageClient,
{
    async fn insert_verified_proof_bundle(
        &self,
        verified_proof_bundle: VerifiedProofBundle,
    ) -> Result<(), ApplicationError> {
        let creator = verified_proof_bundle.pubky_lock_resource.creator().clone();
        let path = verified_proof_bundle_path(&verified_proof_bundle.bundle_id);
        if self
            .client
            .get_json_value_as_creator(&creator, &path)
            .await?
            .is_some()
        {
            return Err(ApplicationError::DuplicateRecord {
                record: "verified_proof_bundle",
            });
        }
        let body = serde_json::to_value(verified_proof_bundle).map_err(|error| {
            ApplicationError::Storage {
                message: format!(
                    "failed to serialize verified proof bundle for Pubky storage: {error}"
                ),
            }
        })?;
        self.client
            .put_json_value_as_creator(&creator, &path, body)
            .await
    }

    async fn get_verified_proof_bundle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<VerifiedProofBundle>, ApplicationError> {
        let path = verified_proof_bundle_path(bundle_id);
        self.client
            .get_json_value_as_creator(creator, &path)
            .await?
            .map(|value| {
                serde_json::from_value(value).map_err(|error| ApplicationError::Storage {
                    message: format!(
                        "failed to deserialize verified proof bundle from Pubky storage: {error}"
                    ),
                })
            })
            .transpose()
    }

    async fn delete_verified_proof_bundle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<(), ApplicationError> {
        let path = verified_proof_bundle_path(bundle_id);
        self.client.delete_as_creator(creator, &path).await
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use locks_core::ids::{BundleId, CreatorPubky, LockServerPubky, PubkyLockResource};
    use locks_core::lock_policy::{VerifierType, verified_proof_bundle_path};
    use locks_core::verification::{
        CriterionVerificationResult, EntitlementLifetime, VERIFIED_PROOF_BUNDLE_VERSION,
        VerificationResult, VerifiedProofBundle,
    };
    use time::macros::datetime;

    use super::PubkyEntitlementRepository;
    use crate::application::errors::ApplicationError;
    use crate::application::ports::EntitlementRepository;
    use crate::infrastructure::pubky::storage_client::{
        PubkyBytesResource, PubkyHomeserverStorageClient,
    };

    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    #[tokio::test]
    async fn insert_verified_proof_bundle_writes_json_to_bundle_id_private_proof_path() {
        let client = FakeStorageClient::default();
        let repository = PubkyEntitlementRepository::new(client);
        let bundle = verified_bundle();
        let expected_path = verified_proof_bundle_path(&bundle.bundle_id);

        repository
            .insert_verified_proof_bundle(bundle.clone())
            .await
            .unwrap();

        assert_eq!(
            repository.client().operations(),
            vec![
                format!(
                    "get_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy {expected_path}"
                ),
                format!(
                    "put_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy {expected_path}"
                ),
            ]
        );
        assert_eq!(
            repository.client().last_json_body(),
            serde_json::to_value(bundle).unwrap()
        );
    }

    #[tokio::test]
    async fn insert_verified_proof_bundle_returns_duplicate_when_bundle_already_exists() {
        let existing = serde_json::to_value(verified_bundle()).unwrap();
        let client = FakeStorageClient::default().with_json_read(Some(existing));
        let repository = PubkyEntitlementRepository::new(client);

        assert_eq!(
            repository
                .insert_verified_proof_bundle(verified_bundle())
                .await
                .unwrap_err(),
            ApplicationError::DuplicateRecord {
                record: "verified_proof_bundle",
            }
        );
        assert_eq!(
            repository.client().operations(),
            vec![format!(
                "get_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy {}",
                verified_proof_bundle_path(&bundle_id())
            )]
        );
    }

    #[tokio::test]
    async fn get_verified_proof_bundle_reads_json_from_bundle_id_private_proof_path() {
        let bundle = verified_bundle();
        let client = FakeStorageClient::default()
            .with_json_read(Some(serde_json::to_value(bundle.clone()).unwrap()));
        let repository = PubkyEntitlementRepository::new(client);

        let loaded = repository
            .get_verified_proof_bundle(&creator(), &bundle.bundle_id)
            .await
            .unwrap();

        assert_eq!(loaded, Some(bundle));
        assert_eq!(
            repository.client().operations(),
            vec![format!(
                "get_json pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy {}",
                verified_proof_bundle_path(&bundle_id())
            )]
        );
    }

    #[tokio::test]
    async fn get_verified_proof_bundle_returns_none_when_homeserver_resource_is_missing() {
        let repository = PubkyEntitlementRepository::new(FakeStorageClient::default());

        let loaded = repository
            .get_verified_proof_bundle(&creator(), &bundle_id())
            .await
            .unwrap();

        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn delete_verified_proof_bundle_deletes_bundle_id_private_proof_path() {
        let repository = PubkyEntitlementRepository::new(FakeStorageClient::default());

        repository
            .delete_verified_proof_bundle(&creator(), &bundle_id())
            .await
            .unwrap();

        assert_eq!(
            repository.client().operations(),
            vec![format!(
                "delete pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy {}",
                verified_proof_bundle_path(&bundle_id())
            )]
        );
    }

    #[derive(Debug, Default)]
    struct FakeStorageClient {
        json_read: Mutex<Option<serde_json::Value>>,
        last_json_body: Mutex<Option<serde_json::Value>>,
        operations: Mutex<Vec<String>>,
    }

    impl FakeStorageClient {
        fn with_json_read(self, value: Option<serde_json::Value>) -> Self {
            *self.json_read.lock().unwrap() = value;
            self
        }

        fn operations(&self) -> Vec<String> {
            self.operations.lock().unwrap().clone()
        }

        fn last_json_body(&self) -> serde_json::Value {
            self.last_json_body
                .lock()
                .unwrap()
                .clone()
                .expect("json body recorded")
        }
    }

    #[async_trait]
    impl PubkyHomeserverStorageClient for FakeStorageClient {
        async fn put_json_value_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
            body: serde_json::Value,
        ) -> Result<(), ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("put_json {creator} {path}"));
            *self.last_json_body.lock().unwrap() = Some(body);
            Ok(())
        }

        async fn get_json_value_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
        ) -> Result<Option<serde_json::Value>, ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("get_json {creator} {path}"));
            Ok(self.json_read.lock().unwrap().clone())
        }

        async fn put_bytes_as_creator(
            &self,
            _creator: &CreatorPubky,
            _path: &str,
            _bytes: Vec<u8>,
            _content_type: &str,
        ) -> Result<(), ApplicationError> {
            unimplemented!("not needed by entitlement repository tests")
        }

        async fn get_bytes_as_creator(
            &self,
            _creator: &CreatorPubky,
            _path: &str,
        ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
            unimplemented!("not needed by entitlement repository tests")
        }

        async fn delete_as_creator(
            &self,
            creator: &CreatorPubky,
            path: &str,
        ) -> Result<(), ApplicationError> {
            self.operations
                .lock()
                .unwrap()
                .push(format!("delete {creator} {path}"));
            Ok(())
        }
    }

    fn verified_bundle() -> VerifiedProofBundle {
        VerifiedProofBundle {
            version: VERIFIED_PROOF_BUNDLE_VERSION,
            bundle_id: bundle_id(),
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

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn bundle_id() -> BundleId {
        BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap()
    }
}
