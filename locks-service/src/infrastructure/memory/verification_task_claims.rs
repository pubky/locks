use std::sync::Arc;

use async_trait::async_trait;
use locks_core::ids::TaskId;
use tokio::sync::RwLock;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedVerificationTask, VerificationTaskRecord, VerificationTaskStatus,
};
use crate::application::ports::{VerificationTaskClaimer, VerificationTaskRepository};
use crate::infrastructure::memory::verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence;

/// In-memory verification task claimer used to model worker lease semantics.
#[derive(Default)]
pub struct InMemoryVerificationTaskClaimer {
    records: RwLock<Vec<ClaimableVerificationTask>>,
    task_repository: Option<Arc<dyn VerificationTaskRepository>>,
    deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>,
}

#[derive(Debug, Clone)]
struct ClaimableVerificationTask {
    task: VerificationTaskRecord,
    claimed_by: Option<String>,
    claim_token: Option<uuid::Uuid>,
    claim_expires_at: Option<time::OffsetDateTime>,
    next_attempt_at: Option<time::OffsetDateTime>,
}

impl InMemoryVerificationTaskClaimer {
    /// Creates a claimer seeded with unclaimed task records.
    pub fn new(records: Vec<VerificationTaskRecord>) -> Self {
        let deletion_fence = Arc::new(InMemoryVerificationTaskDeletionFence::from_tasks(&records));
        Self::with_deletion_fence(records, deletion_fence)
    }

    pub fn with_deletion_fence(
        records: Vec<VerificationTaskRecord>,
        deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>,
    ) -> Self {
        Self {
            records: RwLock::new(
                records
                    .into_iter()
                    .map(|task| ClaimableVerificationTask {
                        task,
                        claimed_by: None,
                        claim_token: None,
                        claim_expires_at: None,
                        next_attempt_at: None,
                    })
                    .collect(),
            ),
            task_repository: None,
            deletion_fence,
        }
    }

    /// Creates a claimer whose lifecycle transitions are mirrored to the task repository.
    pub fn with_task_repository(
        records: Vec<VerificationTaskRecord>,
        task_repository: Arc<dyn VerificationTaskRepository>,
    ) -> Self {
        let mut claimer = Self::new(records);
        claimer.task_repository = Some(task_repository);
        claimer
    }

    pub fn with_task_repository_and_deletion_fence(
        records: Vec<VerificationTaskRecord>,
        task_repository: Arc<dyn VerificationTaskRepository>,
        deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>,
    ) -> Self {
        let mut claimer = Self::with_deletion_fence(records, deletion_fence);
        claimer.task_repository = Some(task_repository);
        claimer
    }

    /// Creates a claimer seeded with already-claimed task records.
    pub fn with_claimed_tasks(
        records: Vec<(VerificationTaskRecord, String, time::OffsetDateTime)>,
    ) -> Self {
        let tasks = records
            .iter()
            .map(|(task, _, _)| task.clone())
            .collect::<Vec<_>>();
        Self {
            records: RwLock::new(
                records
                    .into_iter()
                    .map(
                        |(task, claimed_by, claim_expires_at)| ClaimableVerificationTask {
                            task,
                            claimed_by: Some(claimed_by),
                            claim_token: Some(uuid::Uuid::new_v4()),
                            claim_expires_at: Some(claim_expires_at),
                            next_attempt_at: None,
                        },
                    )
                    .collect(),
            ),
            task_repository: None,
            deletion_fence: Arc::new(InMemoryVerificationTaskDeletionFence::from_tasks(&tasks)),
        }
    }
}

#[async_trait]
impl VerificationTaskClaimer for InMemoryVerificationTaskClaimer {
    async fn begin_claimed_entitlement_publication(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        let mut fence_records = self.deletion_fence.records.write().await;
        let records = self.records.read().await;
        let owned = records.iter().any(|record| {
            record.task.task_id == *task_id
                && record.task.status == VerificationTaskStatus::InProgress
                && record.claimed_by.as_deref() == Some(worker_id)
                && record.claim_token.as_ref() == Some(claim_token)
                && record
                    .claim_expires_at
                    .is_some_and(|expires| expires >= now)
        });
        let Some(fence) = fence_records.get_mut(task_id) else {
            return Ok(false);
        };
        if !owned || fence.deletion_job_id.is_some() {
            return Ok(false);
        }
        fence.entitlement_publication_claim_token = Some(*claim_token);
        Ok(true)
    }

    async fn claim_next_verification_task(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedVerificationTask>, ApplicationError> {
        let fence_records = self.deletion_fence.records.read().await;
        let mut records = self.records.write().await;
        let Some(index) = records.iter().position(|record| {
            record.is_claimable_at(now)
                && fence_records
                    .get(&record.task.task_id)
                    .is_some_and(|fence| fence.deletion_job_id.is_none())
        }) else {
            return Ok(None);
        };

        let mut claimed = records[index].clone();
        claimed.claimed_by = Some(worker_id.to_owned());
        let claim_token = uuid::Uuid::new_v4();
        claimed.claim_token = Some(claim_token);
        claimed.claim_expires_at = Some(claim_expires_at);
        claimed.next_attempt_at = None;
        if claimed.task.status == VerificationTaskStatus::Pending {
            claimed.task =
                claimed
                    .task
                    .transition_to(VerificationTaskStatus::InProgress, now, None)?;
        }
        if let Some(task_repository) = &self.task_repository {
            task_repository
                .update_verification_task(claimed.task.clone())
                .await?;
        }
        records[index] = claimed.clone();

        Ok(Some(ClaimedVerificationTask {
            task: claimed.task,
            claim_token,
        }))
    }

    async fn schedule_verification_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        next_attempt_at: time::OffsetDateTime,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        let fence_records = self.deletion_fence.records.read().await;
        if fence_records
            .get(task_id)
            .is_none_or(|fence| fence.deletion_job_id.is_some())
        {
            return Ok(None);
        }
        let mut records = self.records.write().await;
        let Some(record) = records.iter_mut().find(|record| {
            record.task.task_id == *task_id
                && record.task.status == VerificationTaskStatus::InProgress
                && record.claimed_by.as_deref() == Some(worker_id)
                && record.claim_token.as_ref() == Some(claim_token)
                && record
                    .claim_expires_at
                    .is_some_and(|claim_expires_at| claim_expires_at >= now)
        }) else {
            return Ok(None);
        };

        let pending = record
            .task
            .transition_to(VerificationTaskStatus::Pending, now, None)?;
        if let Some(task_repository) = &self.task_repository {
            task_repository
                .update_verification_task(pending.clone())
                .await?;
        }
        record.task = pending;
        record.claimed_by = None;
        record.claim_token = None;
        record.claim_expires_at = None;
        record.next_attempt_at = Some(next_attempt_at);
        Ok(Some(record.task.clone()))
    }

    async fn persist_claimed_verification_task_transition(
        &self,
        task: VerificationTaskRecord,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        if !matches!(
            task.status,
            VerificationTaskStatus::Completed
                | VerificationTaskStatus::Failed
                | VerificationTaskStatus::Expired
        ) {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "claimed task transition must be terminal".to_owned(),
            });
        }
        let mut fence_records = self.deletion_fence.records.write().await;
        if fence_records
            .get(&task.task_id)
            .is_none_or(|fence| fence.deletion_job_id.is_some())
        {
            return Ok(None);
        }
        let mut records = self.records.write().await;
        let Some(record) = records.iter_mut().find(|record| {
            record.task.task_id == task.task_id
                && record.task.status == VerificationTaskStatus::InProgress
                && record.claimed_by.as_deref() == Some(worker_id)
                && record.claim_token.as_ref() == Some(claim_token)
                && record
                    .claim_expires_at
                    .is_some_and(|claim_expires_at| claim_expires_at >= now)
        }) else {
            return Ok(None);
        };
        if let Some(task_repository) = &self.task_repository {
            task_repository
                .update_verification_task(task.clone())
                .await?;
        }
        record.task = task;
        record.claimed_by = None;
        record.claim_token = None;
        record.claim_expires_at = None;
        record.next_attempt_at = None;
        if let Some(fence) = fence_records.get_mut(&record.task.task_id) {
            fence.entitlement_publication_claim_token = None;
        }
        Ok(Some(record.task.clone()))
    }
}

impl ClaimableVerificationTask {
    fn is_claimable_at(&self, now: time::OffsetDateTime) -> bool {
        match self.task.status {
            VerificationTaskStatus::Pending => self
                .next_attempt_at
                .is_none_or(|next_attempt_at| next_attempt_at <= now),
            VerificationTaskStatus::InProgress => self
                .claim_expires_at
                .is_some_and(|claim_expires_at| claim_expires_at < now),
            VerificationTaskStatus::Completed
            | VerificationTaskStatus::Failed
            | VerificationTaskStatus::Expired => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr, sync::Arc};

    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{
        BundleId, CreatorPubky, GuardedResourceHash, LockId, PubkyLockResource, TaskId,
    };
    use locks_core::lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GuardedResource, LockLogic,
        LockServerConfig, VerifierType,
    };
    use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};

    use super::InMemoryVerificationTaskClaimer;
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        ContentLockDeletionJob, PrepareForceDeletionResult, VerificationTaskRecord,
        VerificationTaskStatus,
    };
    use crate::application::ports::{
        ContentLockDeletionRepository, VerificationTaskClaimer, VerificationTaskRepository,
    };
    use crate::infrastructure::memory::content_lock_deletions::InMemoryContentLockDeletionRepository;
    use crate::infrastructure::memory::verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence;
    use crate::infrastructure::memory::verification_tasks::InMemoryVerificationTaskRepository;

    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";
    const NOW: time::OffsetDateTime = datetime!(2026-05-29 12:10:00 UTC);
    const CLAIM_EXPIRES_AT: time::OffsetDateTime = datetime!(2026-05-29 12:15:00 UTC);

    #[tokio::test]
    async fn no_pending_or_expired_in_progress_task_returns_none() {
        let claimer = InMemoryVerificationTaskClaimer::new(vec![]);

        assert_eq!(
            claimer
                .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn publication_first_blocks_in_memory_deletion_admission() {
        let job = deletion_job();
        let task = task_for_lock(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d20",
            VerificationTaskStatus::Pending,
            &job.lock_id,
        );
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::from_tasks(
            std::slice::from_ref(&task),
        ));
        let claimer =
            InMemoryVerificationTaskClaimer::with_deletion_fence(vec![task], Arc::clone(&fence));
        let deletions = InMemoryContentLockDeletionRepository::with_verification_task_fence(fence);
        let claimed = claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .unwrap();
        assert!(
            claimer
                .begin_claimed_entitlement_publication(
                    &claimed.task.task_id,
                    "worker-a",
                    &claimed.claim_token,
                    NOW,
                )
                .await
                .unwrap()
        );

        assert_eq!(
            deletions.insert_job(job).await,
            Err(ApplicationError::ContentLockDeletionInProgress)
        );
    }

    #[tokio::test]
    async fn deletion_first_blocks_in_memory_publication_and_terminal_transition() {
        let job = deletion_job();
        let task = task_for_lock(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d21",
            VerificationTaskStatus::Pending,
            &job.lock_id,
        );
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::from_tasks(
            std::slice::from_ref(&task),
        ));
        let claimer =
            InMemoryVerificationTaskClaimer::with_deletion_fence(vec![task], Arc::clone(&fence));
        let deletions = InMemoryContentLockDeletionRepository::with_verification_task_fence(fence);
        let claimed = claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .unwrap();
        deletions.insert_job(job).await.unwrap();

        assert!(
            !claimer
                .begin_claimed_entitlement_publication(
                    &claimed.task.task_id,
                    "worker-a",
                    &claimed.claim_token,
                    NOW,
                )
                .await
                .unwrap()
        );
        let completed = claimed
            .task
            .transition_to(VerificationTaskStatus::Completed, NOW, None)
            .unwrap();
        assert_eq!(
            claimer
                .persist_claimed_verification_task_transition(
                    completed,
                    "worker-a",
                    &claimed.claim_token,
                    NOW,
                )
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn deletion_owned_publication_marker_blocks_in_memory_force_escalation() {
        let job = deletion_job();
        let task = task_for_lock(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d24",
            VerificationTaskStatus::Pending,
            &job.lock_id,
        );
        let task_id = task.task_id;
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::from_tasks(
            std::slice::from_ref(&task),
        ));
        let deletions =
            InMemoryContentLockDeletionRepository::with_verification_task_fence(Arc::clone(&fence));
        deletions.insert_job(job.clone()).await.unwrap();
        {
            let mut records = fence.records.write().await;
            let record = records.get_mut(&task_id).unwrap();
            assert_eq!(record.deletion_job_id, Some(job.job_id));
            record.entitlement_publication_claim_token = Some(uuid::Uuid::new_v4());
        }

        assert_eq!(
            deletions
                .prepare_force_deletion(&job.creator, &job.lock_id, NOW)
                .await
                .unwrap(),
            PrepareForceDeletionResult::PublicationInProgress
        );
        assert!(
            deletions
                .get_job(&job.creator, &job.lock_id)
                .await
                .unwrap()
                .unwrap()
                .force_requested_at
                .is_none()
        );
    }

    #[tokio::test]
    async fn retry_retains_publication_fence_until_reconciled_terminal_transition() {
        let job = deletion_job();
        let task = task_for_lock(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d22",
            VerificationTaskStatus::Pending,
            &job.lock_id,
        );
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::from_tasks(
            std::slice::from_ref(&task),
        ));
        let claimer =
            InMemoryVerificationTaskClaimer::with_deletion_fence(vec![task], Arc::clone(&fence));
        let deletions = InMemoryContentLockDeletionRepository::with_verification_task_fence(fence);
        let first = claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .unwrap();
        assert!(
            claimer
                .begin_claimed_entitlement_publication(
                    &first.task.task_id,
                    "worker-a",
                    &first.claim_token,
                    NOW,
                )
                .await
                .unwrap()
        );
        let retry_at = NOW + time::Duration::seconds(10);
        claimer
            .schedule_verification_task_retry(
                &first.task.task_id,
                "worker-a",
                &first.claim_token,
                NOW,
                retry_at,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            deletions.insert_job(job.clone()).await,
            Err(ApplicationError::ContentLockDeletionInProgress)
        );

        let second = claimer
            .claim_next_verification_task(
                "worker-b",
                retry_at,
                retry_at + time::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            claimer
                .begin_claimed_entitlement_publication(
                    &second.task.task_id,
                    "worker-b",
                    &second.claim_token,
                    retry_at,
                )
                .await
                .unwrap()
        );
        let completed = second
            .task
            .transition_to(VerificationTaskStatus::Completed, retry_at, None)
            .unwrap();
        claimer
            .persist_claimed_verification_task_transition(
                completed,
                "worker-b",
                &second.claim_token,
                retry_at,
            )
            .await
            .unwrap()
            .unwrap();

        deletions.insert_job(job).await.unwrap();
    }

    #[tokio::test]
    async fn pending_task_can_be_claimed_and_transitions_to_in_progress() {
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
        );
        let claimer = InMemoryVerificationTaskClaimer::new(vec![pending.clone()]);

        let claimed = claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .expect("pending task is claimed");

        assert_eq!(claimed.task.task_id, pending.task_id);
        assert_eq!(claimed.task.status, VerificationTaskStatus::InProgress);
        assert_eq!(claimed.task.started_at, Some(NOW));
        assert_eq!(claimed.task.completed_at, None);
        assert_eq!(claimed.task.failure_message, None);
        assert_eq!(
            claimer
                .claim_next_verification_task("worker-b", NOW, CLAIM_EXPIRES_AT)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn claim_transition_is_mirrored_to_configured_task_repository() {
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
        );
        let repository = Arc::new(InMemoryVerificationTaskRepository::new());
        repository
            .insert_verification_task(pending.clone())
            .await
            .unwrap();
        let claimer = InMemoryVerificationTaskClaimer::with_task_repository(
            vec![pending.clone()],
            repository.clone(),
        );

        claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            repository
                .get_verification_task(&pending.task_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            VerificationTaskStatus::InProgress
        );
    }

    #[tokio::test]
    async fn failed_repository_mirror_leaves_task_claimable() {
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
        );
        let repository = Arc::new(InMemoryVerificationTaskRepository::new());
        let claimer = InMemoryVerificationTaskClaimer::with_task_repository(
            vec![pending.clone()],
            repository.clone(),
        );

        assert!(
            claimer
                .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
                .await
                .is_err()
        );
        repository.insert_verification_task(pending).await.unwrap();

        assert!(
            claimer
                .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn stale_claim_token_cannot_reschedule_after_same_worker_id_reclaims() {
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
        );
        let task_id = pending.task_id;
        let claimer = InMemoryVerificationTaskClaimer::new(vec![pending]);
        let first = claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .unwrap();
        let reclaimed_at = CLAIM_EXPIRES_AT + time::Duration::milliseconds(1);
        let second = claimer
            .claim_next_verification_task(
                "worker-a",
                reclaimed_at,
                reclaimed_at + time::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();

        assert_ne!(first.claim_token, second.claim_token);
        assert_eq!(
            claimer
                .schedule_verification_task_retry(
                    &task_id,
                    "worker-a",
                    &first.claim_token,
                    reclaimed_at,
                    reclaimed_at + time::Duration::seconds(10),
                )
                .await
                .unwrap(),
            None
        );
        assert!(
            claimer
                .schedule_verification_task_retry(
                    &task_id,
                    "worker-a",
                    &second.claim_token,
                    reclaimed_at,
                    reclaimed_at + time::Duration::seconds(10),
                )
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn stale_claim_token_cannot_persist_terminal_state_after_same_worker_id_reclaims() {
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
        );
        let claimer = InMemoryVerificationTaskClaimer::new(vec![pending]);
        let first = claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .unwrap();
        let reclaimed_at = CLAIM_EXPIRES_AT + time::Duration::milliseconds(1);
        let second = claimer
            .claim_next_verification_task(
                "worker-a",
                reclaimed_at,
                reclaimed_at + time::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let completed = second
            .task
            .clone()
            .transition_to(
                VerificationTaskStatus::Completed,
                reclaimed_at + time::Duration::seconds(1),
                None,
            )
            .unwrap();
        let failed = second
            .task
            .clone()
            .transition_to(
                VerificationTaskStatus::Failed,
                reclaimed_at + time::Duration::seconds(1),
                Some("stale failure".to_owned()),
            )
            .unwrap();

        for terminal in [completed.clone(), failed] {
            assert_eq!(
                claimer
                    .persist_claimed_verification_task_transition(
                        terminal,
                        "worker-a",
                        &first.claim_token,
                        reclaimed_at,
                    )
                    .await
                    .unwrap(),
                None
            );
        }
        assert_eq!(
            claimer
                .persist_claimed_verification_task_transition(
                    completed.clone(),
                    "worker-a",
                    &second.claim_token,
                    reclaimed_at,
                )
                .await
                .unwrap(),
            Some(completed)
        );
    }

    #[tokio::test]
    async fn retry_schedule_is_owner_fenced_and_not_claimable_before_due_time() {
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
        );
        let task_id = pending.task_id;
        let claimer = InMemoryVerificationTaskClaimer::new(vec![pending]);
        let claim = claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .unwrap();
        let next_attempt_at = NOW + time::Duration::seconds(10);

        assert_eq!(
            claimer
                .schedule_verification_task_retry(
                    &task_id,
                    "worker-b",
                    &claim.claim_token,
                    NOW,
                    next_attempt_at,
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            claimer
                .schedule_verification_task_retry(
                    &task_id,
                    "worker-a",
                    &claim.claim_token,
                    CLAIM_EXPIRES_AT + time::Duration::nanoseconds(1),
                    next_attempt_at,
                )
                .await
                .unwrap(),
            None
        );
        let scheduled = claimer
            .schedule_verification_task_retry(
                &task_id,
                "worker-a",
                &claim.claim_token,
                NOW,
                next_attempt_at,
            )
            .await
            .unwrap()
            .expect("owning worker schedules retry");
        assert_eq!(scheduled.status, VerificationTaskStatus::Pending);
        assert_eq!(scheduled.started_at, None);
        assert_eq!(scheduled.completed_at, None);
        assert_eq!(scheduled.failure_message, None);

        assert_eq!(
            claimer
                .claim_next_verification_task(
                    "worker-b",
                    NOW + time::Duration::seconds(9),
                    CLAIM_EXPIRES_AT,
                )
                .await
                .unwrap(),
            None
        );
        assert!(
            claimer
                .claim_next_verification_task(
                    "worker-b",
                    next_attempt_at,
                    CLAIM_EXPIRES_AT + time::Duration::seconds(10),
                )
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn terminal_tasks_are_not_claimed() {
        let completed = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
        )
        .transition_to(VerificationTaskStatus::InProgress, NOW, None)
        .unwrap()
        .transition_to(
            VerificationTaskStatus::Completed,
            datetime!(2026-05-29 12:11:00 UTC),
            None,
        )
        .unwrap();
        let failed = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d11",
            VerificationTaskStatus::Pending,
        )
        .transition_to(VerificationTaskStatus::InProgress, NOW, None)
        .unwrap()
        .transition_to(
            VerificationTaskStatus::Failed,
            datetime!(2026-05-29 12:11:00 UTC),
            Some("failure".to_owned()),
        )
        .unwrap();
        let expired = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d12",
            VerificationTaskStatus::Pending,
        )
        .transition_to(VerificationTaskStatus::Expired, NOW, None)
        .unwrap();
        let claimer = InMemoryVerificationTaskClaimer::new(vec![completed, failed, expired]);

        assert_eq!(
            claimer
                .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn expired_in_progress_task_can_be_reclaimed() {
        let in_progress = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
        )
        .transition_to(
            VerificationTaskStatus::InProgress,
            datetime!(2026-05-29 12:00:00 UTC),
            None,
        )
        .unwrap();
        let claimer = InMemoryVerificationTaskClaimer::with_claimed_tasks(vec![(
            in_progress.clone(),
            "worker-a".to_owned(),
            datetime!(2026-05-29 12:05:00 UTC),
        )]);

        let reclaimed = claimer
            .claim_next_verification_task("worker-b", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .expect("expired in-progress claim is reclaimed");

        assert_eq!(reclaimed.task.task_id, in_progress.task_id);
        assert_eq!(reclaimed.task.status, VerificationTaskStatus::InProgress);
        assert_eq!(reclaimed.task.started_at, in_progress.started_at);
    }

    #[tokio::test]
    async fn non_expired_in_progress_task_is_not_claimed() {
        let in_progress = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
        )
        .transition_to(
            VerificationTaskStatus::InProgress,
            datetime!(2026-05-29 12:00:00 UTC),
            None,
        )
        .unwrap();
        let claimer = InMemoryVerificationTaskClaimer::with_claimed_tasks(vec![(
            in_progress,
            "worker-a".to_owned(),
            datetime!(2026-05-29 12:11:00 UTC),
        )]);

        assert_eq!(
            claimer
                .claim_next_verification_task("worker-b", NOW, CLAIM_EXPIRES_AT)
                .await
                .unwrap(),
            None
        );
    }

    fn task(task_id: &str, status: VerificationTaskStatus) -> VerificationTaskRecord {
        task_for_lock(task_id, status, &LockId::from_str(LOCK_ID).unwrap())
    }

    fn task_for_lock(
        task_id: &str,
        status: VerificationTaskStatus,
        lock_id: &LockId,
    ) -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: TaskId::from_str(task_id).unwrap(),
            creator: CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap(),
            submitted_proof_bundle: SubmittedProofBundle {
                version: SUBMITTED_PROOF_BUNDLE_VERSION,
                bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
                pubky_lock_resource: PubkyLockResource::from_str(&format!(
                    "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/{lock_id}.json"
                ))
                .unwrap(),
                reader_public_key: None,
                proofs: vec![Proof {
                    criterion_id: "criterion-1".to_owned(),
                    verifier_type: VerifierType::DevStatic,
                    payload: json!({ "satisfied": true }),
                }],
            },
            status,
            submitted_at: datetime!(2026-05-29 12:00:00 UTC),
            started_at: None,
            completed_at: None,
            failure_message: None,
        }
    }

    fn deletion_job() -> ContentLockDeletionJob {
        ContentLockDeletionJob::new(
            uuid::Uuid::new_v4(),
            ContentLock {
                version: CONTENT_LOCK_VERSION,
                creator: CreatorPubky::from_str(
                    "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                )
                .unwrap(),
                primary_resource: Some(
                    GuardedResource::new(
                        "/priv/locks.app/content/post.json".to_owned(),
                        GuardedResourceHash::from_bytes([7; 32]),
                        "application/json".to_owned(),
                        42,
                    )
                    .unwrap(),
                ),
                secondary_resources: BTreeMap::new(),
                criteria: vec![],
                lock_logic: LockLogic::All { criteria: vec![] },
                access_policy: AccessPolicy {
                    requested_credential_ttl_seconds: 900,
                },
                lock_server: LockServerConfig { override_: None },
                created_at: datetime!(2026-08-12 04:00:00 UTC),
            },
            datetime!(2026-08-12 05:00:00 UTC),
        )
        .unwrap()
    }
}
