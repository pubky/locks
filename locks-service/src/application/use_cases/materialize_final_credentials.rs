use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    ports::{
        AccessCredentialGenerator, AccessCredentialStore, Clock, FinalCredentialWorkerIssueRequest,
    },
};

/// Exact worker claim and bounded batch used to materialize final deletion credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializeFinalCredentialsRequest<'a> {
    pub deletion_job_id: Uuid,
    pub worker_id: &'a str,
    pub claim_token: Uuid,
    pub now: OffsetDateTime,
    pub batch_limit: usize,
}

/// Secret-free result of one bounded materialization pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializeFinalCredentialsOutcome {
    pub materialized_count: usize,
}

/// Materializes final credentials selected under a live deletion-worker claim.
pub struct MaterializeFinalCredentialsUseCase<'a> {
    store: &'a dyn AccessCredentialStore,
    generator: &'a dyn AccessCredentialGenerator,
    clock: &'a dyn Clock,
}

impl<'a> MaterializeFinalCredentialsUseCase<'a> {
    pub fn new(
        store: &'a dyn AccessCredentialStore,
        generator: &'a dyn AccessCredentialGenerator,
        clock: &'a dyn Clock,
    ) -> Self {
        Self {
            store,
            generator,
            clock,
        }
    }

    pub async fn execute(
        &self,
        request: MaterializeFinalCredentialsRequest<'_>,
    ) -> Result<MaterializeFinalCredentialsOutcome, ApplicationError> {
        let pending = self
            .store
            .final_credentials_to_materialize(
                request.deletion_job_id,
                request.worker_id,
                request.claim_token,
                request.batch_limit,
            )
            .await?;
        let mut materialized_count = 0;
        for item in pending {
            let candidate = self.generator.generate_access_credential().await?;
            let fresh_now = self.clock.now();
            if self
                .store
                .issue_or_replay_final_credential_for_worker(FinalCredentialWorkerIssueRequest {
                    deletion_job_id: request.deletion_job_id,
                    worker_id: request.worker_id,
                    claim_token: request.claim_token,
                    creator: &item.creator,
                    bundle_id: &item.bundle_id,
                    now: fresh_now,
                    candidate,
                })
                .await?
                .is_some()
            {
                materialized_count += 1;
            }
        }
        Ok(MaterializeFinalCredentialsOutcome { materialized_count })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr, sync::Mutex};

    use async_trait::async_trait;
    use locks_core::ids::{BundleId, CreatorPubky, LockId};
    use time::macros::datetime;
    use uuid::Uuid;

    use super::{MaterializeFinalCredentialsRequest, MaterializeFinalCredentialsUseCase};
    use crate::application::{
        errors::ApplicationError,
        models::{
            AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord,
            FinalCredentialMaterialization, IssuedDeletionCredential,
        },
        ports::{
            AccessCredentialGenerator, AccessCredentialStore, Clock,
            FinalCredentialWorkerIssueRequest,
        },
    };

    const NOW: time::OffsetDateTime = datetime!(2026-08-17 12:00:00 UTC);

    #[tokio::test]
    async fn materializes_every_eligible_snapshot_in_deterministic_store_order() {
        let creator = creator();
        let first_bundle = BundleId::from_str("000G40R40M30E209185GR38E1V").unwrap();
        let second_bundle = BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap();
        let store = FakeStore::with_pending(vec![
            FinalCredentialMaterialization {
                creator: creator.clone(),
                bundle_id: first_bundle.clone(),
            },
            FinalCredentialMaterialization {
                creator: creator.clone(),
                bundle_id: second_bundle.clone(),
            },
        ]);
        let generator = SequenceGenerator::new(["first-secret", "second-secret"]);
        let request = request(10);

        let clock = FixedClock(NOW + time::Duration::seconds(1));
        let outcome = MaterializeFinalCredentialsUseCase::new(&store, &generator, &clock)
            .execute(request)
            .await
            .unwrap();

        assert_eq!(outcome.materialized_count, 2);
        assert_eq!(
            store.calls(),
            vec![
                (first_bundle, "first-secret".to_owned()),
                (second_bundle, "second-secret".to_owned()),
            ]
        );
        assert_eq!(generator.generated_count(), 2);
        assert!(!format!("{outcome:?}").contains("secret"));
    }

    #[tokio::test]
    async fn retry_after_materialization_does_not_generate_or_persist_a_second_bearer() {
        let creator = creator();
        let bundle_id = BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap();
        let store = FakeStore::with_pending(vec![FinalCredentialMaterialization {
            creator,
            bundle_id: bundle_id.clone(),
        }]);
        let generator = SequenceGenerator::new(["persisted-secret", "must-not-be-generated"]);

        let clock = FixedClock(NOW + time::Duration::seconds(1));
        let first = MaterializeFinalCredentialsUseCase::new(&store, &generator, &clock)
            .execute(request(1))
            .await
            .unwrap();
        let retry = MaterializeFinalCredentialsUseCase::new(&store, &generator, &clock)
            .execute(request(1))
            .await
            .unwrap();

        assert_eq!(first.materialized_count, 1);
        assert_eq!(retry.materialized_count, 0);
        assert_eq!(generator.generated_count(), 1);
        assert_eq!(
            store.calls(),
            vec![(bundle_id, "persisted-secret".to_owned())]
        );
    }

    fn request(batch_limit: usize) -> MaterializeFinalCredentialsRequest<'static> {
        MaterializeFinalCredentialsRequest {
            deletion_job_id: Uuid::from_u128(7),
            worker_id: "worker-final",
            claim_token: Uuid::from_u128(8),
            now: NOW,
            batch_limit,
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    #[derive(Debug, Clone, Copy)]
    struct FixedClock(time::OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> time::OffsetDateTime {
            self.0
        }
    }

    #[derive(Debug)]
    struct SequenceGenerator {
        values: Mutex<Vec<AccessCredential>>,
        generated: Mutex<usize>,
    }

    impl SequenceGenerator {
        fn new<const N: usize>(values: [&str; N]) -> Self {
            Self {
                values: Mutex::new(
                    values
                        .into_iter()
                        .rev()
                        .map(AccessCredential::new)
                        .collect(),
                ),
                generated: Mutex::new(0),
            }
        }

        fn generated_count(&self) -> usize {
            *self.generated.lock().unwrap()
        }
    }

    #[async_trait]
    impl AccessCredentialGenerator for SequenceGenerator {
        async fn generate_access_credential(&self) -> Result<AccessCredential, ApplicationError> {
            *self.generated.lock().unwrap() += 1;
            self.values.lock().unwrap().pop().ok_or_else(|| {
                ApplicationError::CredentialGeneration {
                    message: "test generator exhausted".to_owned(),
                }
            })
        }
    }

    #[derive(Debug)]
    struct FakeStore {
        pending: Mutex<Vec<FinalCredentialMaterialization>>,
        winners: Mutex<HashMap<BundleId, AccessCredential>>,
        calls: Mutex<Vec<(BundleId, String)>>,
    }

    impl FakeStore {
        fn with_pending(pending: Vec<FinalCredentialMaterialization>) -> Self {
            Self {
                pending: Mutex::new(pending),
                winners: Mutex::new(HashMap::new()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<(BundleId, String)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl AccessCredentialStore for FakeStore {
        async fn insert_access_credential(
            &self,
            _lock_id: &LockId,
            _lookup_key: AccessCredentialLookupKey,
            _record: AccessCredentialRecord,
        ) -> Result<(), ApplicationError> {
            unreachable!()
        }

        async fn get_access_credential(
            &self,
            _lookup_key: &AccessCredentialLookupKey,
        ) -> Result<Option<AccessCredentialRecord>, ApplicationError> {
            unreachable!()
        }

        async fn delete_access_credential(
            &self,
            _lookup_key: &AccessCredentialLookupKey,
        ) -> Result<(), ApplicationError> {
            unreachable!()
        }

        async fn final_credentials_to_materialize(
            &self,
            _deletion_job_id: Uuid,
            _worker_id: &str,
            _claim_token: Uuid,
            limit: usize,
        ) -> Result<Vec<FinalCredentialMaterialization>, ApplicationError> {
            let mut pending = self.pending.lock().unwrap();
            let take = pending.len().min(limit);
            Ok(pending.drain(..take).collect())
        }

        async fn issue_or_replay_final_credential_for_worker(
            &self,
            request: FinalCredentialWorkerIssueRequest<'_>,
        ) -> Result<Option<IssuedDeletionCredential>, ApplicationError> {
            let FinalCredentialWorkerIssueRequest {
                deletion_job_id,
                worker_id,
                claim_token,
                bundle_id,
                now,
                candidate,
                ..
            } = request;
            assert_eq!(deletion_job_id, Uuid::from_u128(7));
            assert_eq!(worker_id, "worker-final");
            assert_eq!(claim_token, Uuid::from_u128(8));
            assert_eq!(now, NOW + time::Duration::seconds(1));
            self.calls
                .lock()
                .unwrap()
                .push((bundle_id.clone(), candidate.as_str().to_owned()));
            let winner = self
                .winners
                .lock()
                .unwrap()
                .entry(bundle_id.clone())
                .or_insert(candidate)
                .clone();
            Ok(Some(IssuedDeletionCredential {
                credential: winner,
                expires_at: NOW + time::Duration::minutes(30),
            }))
        }
    }
}
