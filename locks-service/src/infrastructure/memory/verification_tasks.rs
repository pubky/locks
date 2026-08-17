use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;

use locks_core::ids::{BundleId, CreatorPubky, TaskId};

use crate::application::errors::ApplicationError;
use crate::application::models::VerificationTaskRecord;
use crate::application::ports::VerificationTaskRepository;
use crate::infrastructure::memory::verification_task_deletion_fence::{
    InMemoryVerificationTaskDeletionFence, InMemoryVerificationTaskFenceRecord,
};

/// In-memory verification task repository.
#[derive(Debug)]
pub struct InMemoryVerificationTaskRepository {
    records: RwLock<HashMap<TaskId, VerificationTaskRecord>>,
    deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>,
}

impl Default for InMemoryVerificationTaskRepository {
    fn default() -> Self {
        Self::with_deletion_fence(Arc::new(InMemoryVerificationTaskDeletionFence::new()))
    }
}

impl InMemoryVerificationTaskRepository {
    /// Creates an empty repository.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_deletion_fence(deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>) -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            deletion_fence,
        }
    }
}

#[async_trait]
impl VerificationTaskRepository for InMemoryVerificationTaskRepository {
    async fn insert_verification_task(
        &self,
        task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError> {
        let mut fence_records = self.deletion_fence.records.write().await;
        let mut records = self.records.write().await;
        if records.contains_key(&task.task_id)
            || records.values().any(|existing| {
                existing.creator == task.creator
                    && existing.submitted_proof_bundle.bundle_id
                        == task.submitted_proof_bundle.bundle_id
            })
        {
            return Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            });
        }
        fence_records.insert(
            task.task_id,
            InMemoryVerificationTaskFenceRecord {
                creator: task.creator.clone(),
                lock_id: task
                    .submitted_proof_bundle
                    .pubky_lock_resource
                    .lock_id()
                    .clone(),
                entitlement_publication_claim_token: None,
                deletion_job_id: None,
            },
        );
        records.insert(task.task_id, task);
        Ok(())
    }

    async fn update_verification_task(
        &self,
        task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError> {
        let mut records = self.records.write().await;
        if !records.contains_key(&task.task_id) {
            return Err(ApplicationError::MissingRecord {
                record: "verification_task",
            });
        }
        records.insert(task.task_id, task);
        Ok(())
    }

    async fn get_verification_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        Ok(self.records.read().await.get(task_id).cloned())
    }

    async fn get_verification_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        Ok(self
            .records
            .read()
            .await
            .values()
            .find(|task| {
                &task.creator == creator && &task.submitted_proof_bundle.bundle_id == bundle_id
            })
            .cloned())
    }

    async fn delete_verification_task(&self, task_id: &TaskId) -> Result<(), ApplicationError> {
        let mut fence_records = self.deletion_fence.records.write().await;
        self.records.write().await.remove(task_id);
        fence_records.remove(task_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource};
    use locks_core::lock_policy::VerifierType;
    use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};

    use super::*;
    use crate::application::models::VerificationTaskStatus;

    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
    const OTHER_TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d11";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

    #[tokio::test]
    async fn insert_update_read_and_delete_use_explicit_semantics() {
        let repo = InMemoryVerificationTaskRepository::new();
        let task_id = TaskId::from_str(TASK_ID).unwrap();
        let pending = task(VerificationTaskStatus::Pending);
        let in_progress = pending
            .transition_to(
                VerificationTaskStatus::InProgress,
                datetime!(2026-05-29 12:01:00 UTC),
                None,
            )
            .unwrap();

        assert_eq!(repo.get_verification_task(&task_id).await.unwrap(), None);
        assert_eq!(
            repo.update_verification_task(pending.clone()).await,
            Err(ApplicationError::MissingRecord {
                record: "verification_task",
            })
        );

        repo.insert_verification_task(pending.clone())
            .await
            .unwrap();
        assert_eq!(
            repo.insert_verification_task(pending).await,
            Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            })
        );

        repo.update_verification_task(in_progress.clone())
            .await
            .unwrap();
        assert_eq!(
            repo.get_verification_task(&task_id).await.unwrap(),
            Some(in_progress)
        );

        repo.delete_verification_task(&task_id).await.unwrap();
        repo.delete_verification_task(&task_id).await.unwrap();
        assert_eq!(repo.get_verification_task(&task_id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn lookup_by_handle_matches_creator_and_bundle_id() {
        let repo = InMemoryVerificationTaskRepository::new();
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let other_creator =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        let bundle_id = BundleId::from_str(BUNDLE_ID).unwrap();
        let task = task_with(
            TASK_ID,
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            BUNDLE_ID,
            VerificationTaskStatus::Pending,
        );
        let other_creator_task = task_with(
            OTHER_TASK_ID,
            "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky",
            BUNDLE_ID,
            VerificationTaskStatus::Pending,
        );

        repo.insert_verification_task(other_creator_task.clone())
            .await
            .unwrap();
        repo.insert_verification_task(task.clone()).await.unwrap();

        assert_eq!(
            repo.get_verification_task_by_handle(&creator, &bundle_id)
                .await
                .unwrap(),
            Some(task)
        );
        assert_eq!(
            repo.get_verification_task_by_handle(&other_creator, &bundle_id)
                .await
                .unwrap(),
            Some(other_creator_task)
        );
        assert_eq!(
            repo.get_verification_task_by_handle(
                &CreatorPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
                )
                .unwrap(),
                &bundle_id,
            )
            .await
            .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn insert_rejects_duplicate_public_handle_even_with_distinct_task_id() {
        let repo = InMemoryVerificationTaskRepository::new();
        let original = task_with(
            TASK_ID,
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            BUNDLE_ID,
            VerificationTaskStatus::Pending,
        );
        let duplicate_handle = task_with(
            OTHER_TASK_ID,
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            BUNDLE_ID,
            VerificationTaskStatus::Pending,
        );

        repo.insert_verification_task(original.clone())
            .await
            .unwrap();

        assert_eq!(
            repo.insert_verification_task(duplicate_handle).await,
            Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            })
        );
        assert_eq!(
            repo.get_verification_task(&TaskId::from_str(TASK_ID).unwrap())
                .await
                .unwrap(),
            Some(original)
        );
        assert_eq!(
            repo.get_verification_task(&TaskId::from_str(OTHER_TASK_ID).unwrap())
                .await
                .unwrap(),
            None
        );
    }

    fn task(status: VerificationTaskStatus) -> VerificationTaskRecord {
        task_with(
            TASK_ID,
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            BUNDLE_ID,
            status,
        )
    }

    fn task_with(
        task_id: &str,
        creator: &str,
        bundle_id: &str,
        status: VerificationTaskStatus,
    ) -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: TaskId::from_str(task_id).unwrap(),
            creator: CreatorPubky::from_str(creator).unwrap(),
            submitted_proof_bundle: SubmittedProofBundle {
                version: SUBMITTED_PROOF_BUNDLE_VERSION,
                bundle_id: BundleId::from_str(bundle_id).unwrap(),
                pubky_lock_resource: PubkyLockResource::from_str(&format!(
                    "{creator}/pub/locks.app/{LOCK_ID}.json"
                ))
                .unwrap(),
                reader_public_key: None,
                proofs: vec![Proof {
                    criterion_id: "criterion-1".to_owned(),
                    verifier_type: VerifierType::DevStatic,
                    payload: json!({}),
                }],
            },
            status,
            submitted_at: datetime!(2026-05-29 12:00:00 UTC),
            started_at: None,
            completed_at: None,
            failure_message: None,
        }
    }
}
