use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, LockId};
use locks_core::lock_policy::ContentLock;
use time::OffsetDateTime;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    models::{
        ClaimedContentLockDeletionJob, ContentLockDeletionFailureCode, ContentLockDeletionJob,
        ContentLockDeletionPhase, ContentLockDeletionState, ForceDeletionReceipt,
        PrepareForceDeletionResult,
    },
    ports::{
        ContentLockDeletionRepository, PublicationRecovery, publication_intent_matches_request,
        publication_reconciliation_matches,
    },
};
use crate::infrastructure::memory::verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence;

type JobKey = (CreatorPubky, LockId);

#[derive(Debug, Clone)]
struct StoredJob {
    job: ContentLockDeletionJob,
    claimed_by: Option<String>,
    claim_token: Option<Uuid>,
    claim_expires_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone)]
struct PublicationIntent {
    token: Uuid,
    write_completed: bool,
    original_content_lock: ContentLock,
}

/// In-memory deletion repository with the same lease-fencing semantics as PostgreSQL.
#[derive(Debug)]
pub struct InMemoryContentLockDeletionRepository {
    jobs: RwLock<HashMap<JobKey, StoredJob>>,
    force_receipts: RwLock<HashMap<JobKey, ForceDeletionReceipt>>,
    publication_intents: RwLock<HashMap<JobKey, PublicationIntent>>,
    verification_task_fence: Arc<InMemoryVerificationTaskDeletionFence>,
}

impl Default for InMemoryContentLockDeletionRepository {
    fn default() -> Self {
        Self::with_verification_task_fence(Arc::new(InMemoryVerificationTaskDeletionFence::new()))
    }
}

impl InMemoryContentLockDeletionRepository {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_verification_task_fence(
        verification_task_fence: Arc<InMemoryVerificationTaskDeletionFence>,
    ) -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            force_receipts: RwLock::new(HashMap::new()),
            publication_intents: RwLock::new(HashMap::new()),
            verification_task_fence,
        }
    }
}

#[async_trait]
impl ContentLockDeletionRepository for InMemoryContentLockDeletionRepository {
    async fn begin_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
        original_content_lock: &ContentLock,
    ) -> Result<(), ApplicationError> {
        if !publication_intent_matches_request(
            creator,
            lock_id,
            original_content_lock,
            original_content_lock,
        ) {
            return Err(ApplicationError::Storage {
                message: "content lock publication intent identity mismatch".to_owned(),
            });
        }
        let key = (creator.clone(), lock_id.clone());
        let mut intents = self.publication_intents.write().await;
        let jobs = self.jobs.read().await;
        let receipts = self.force_receipts.read().await;
        if jobs.contains_key(&key) || receipts.contains_key(&key) {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        if intents.contains_key(&key) {
            return Err(ApplicationError::ContentLockPathConflict {
                guarded_path: "content lock publication in progress".to_owned(),
            });
        }
        intents.insert(
            key,
            PublicationIntent {
                token: publication_token,
                write_completed: false,
                original_content_lock: original_content_lock.clone(),
            },
        );
        Ok(())
    }

    async fn mark_publication_written(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        let mut intents = self.publication_intents.write().await;
        let Some(intent) = intents.get_mut(&(creator.clone(), lock_id.clone())) else {
            return Ok(false);
        };
        if intent.token != publication_token {
            return Ok(false);
        }
        intent.write_completed = true;
        Ok(true)
    }

    async fn abandon_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        remove_publication_intent(
            &self.publication_intents,
            creator,
            lock_id,
            publication_token,
        )
        .await
    }

    async fn reconcile_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        published_content_lock: &ContentLock,
        published_guarded_paths: &[String],
    ) -> Result<bool, ApplicationError> {
        if !publication_reconciliation_matches(
            creator,
            lock_id,
            published_content_lock,
            published_guarded_paths,
        ) {
            return Ok(false);
        }
        let key = (creator.clone(), lock_id.clone());
        let mut intents = self.publication_intents.write().await;
        if !intents.get(&key).is_some_and(|intent| {
            intent.write_completed && intent.original_content_lock == *published_content_lock
        }) {
            return Ok(false);
        }
        intents.remove(&key);
        Ok(true)
    }

    async fn publication_in_progress(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<bool, ApplicationError> {
        let key = (creator.clone(), lock_id.clone());
        let intents = self.publication_intents.read().await;
        let jobs = self.jobs.read().await;
        let receipts = self.force_receipts.read().await;
        if jobs.contains_key(&key) || receipts.contains_key(&key) {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        Ok(intents.contains_key(&key))
    }

    async fn recover_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        replacement_token: Uuid,
        requested_content_lock: &ContentLock,
    ) -> Result<PublicationRecovery, ApplicationError> {
        let key = (creator.clone(), lock_id.clone());
        let mut intents = self.publication_intents.write().await;
        let jobs = self.jobs.read().await;
        let receipts = self.force_receipts.read().await;
        if jobs.contains_key(&key) || receipts.contains_key(&key) {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        let Some(intent) = intents.get_mut(&key) else {
            return Ok(PublicationRecovery::None);
        };
        if !publication_intent_matches_request(
            creator,
            lock_id,
            &intent.original_content_lock,
            requested_content_lock,
        ) {
            return Ok(PublicationRecovery::RequestMismatch);
        }
        if intent.write_completed {
            return Ok(PublicationRecovery::WriteCompleted {
                publication_token: intent.token,
                content_lock: intent.original_content_lock.clone(),
            });
        }
        intent.token = replacement_token;
        Ok(PublicationRecovery::RetryWithToken {
            publication_token: replacement_token,
            content_lock: intent.original_content_lock.clone(),
        })
    }

    async fn insert_job(&self, job: ContentLockDeletionJob) -> Result<(), ApplicationError> {
        job.validate_frozen_identity()?;
        job.validate_state(false)?;
        let key = (job.creator.clone(), job.lock_id.clone());
        let mut verification_tasks = self.verification_task_fence.records.write().await;
        let intents = self.publication_intents.read().await;
        let mut jobs = self.jobs.write().await;
        let receipts = self.force_receipts.read().await;
        if intents.contains_key(&key) {
            return Err(ApplicationError::ContentLockPublicationInProgress);
        }
        if receipts.contains_key(&key) {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        if jobs.contains_key(&key) || jobs.values().any(|stored| stored.job.job_id == job.job_id) {
            return Err(ApplicationError::DuplicateRecord {
                record: "content_lock_deletion_job",
            });
        }
        let matching_task_ids = verification_tasks
            .iter()
            .filter_map(|(task_id, task)| {
                (task.creator == job.creator && task.lock_id == job.lock_id).then_some(*task_id)
            })
            .collect::<Vec<_>>();
        if matching_task_ids.iter().any(|task_id| {
            verification_tasks
                .get(task_id)
                .is_some_and(|task| task.entitlement_publication_claim_token.is_some())
        }) {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        for task_id in matching_task_ids {
            if let Some(task) = verification_tasks.get_mut(&task_id) {
                task.deletion_job_id = Some(job.job_id);
            }
        }
        jobs.insert(
            key,
            StoredJob {
                job,
                claimed_by: None,
                claim_token: None,
                claim_expires_at: None,
            },
        );
        Ok(())
    }

    async fn get_job(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let stored = self
            .jobs
            .read()
            .await
            .get(&(creator.clone(), lock_id.clone()))
            .cloned();
        if let Some(stored) = stored {
            stored.job.validate_frozen_identity()?;
            let has_active_lease = stored.claimed_by.is_some()
                && stored.claim_token.is_some()
                && stored.claim_expires_at.is_some();
            stored.job.validate_state(has_active_lease)?;
            Ok(Some(stored.job))
        } else {
            Ok(None)
        }
    }

    async fn claim_next(
        &self,
        worker_id: &str,
        now: OffsetDateTime,
        claim_expires_at: OffsetDateTime,
    ) -> Result<Option<ClaimedContentLockDeletionJob>, ApplicationError> {
        let mut jobs = self.jobs.write().await;
        let Some(stored) = jobs
            .values_mut()
            .filter(|stored| is_claimable(stored, now))
            .min_by_key(|stored| stored.job.deletion_started_at)
        else {
            return Ok(None);
        };
        let claim_token = Uuid::new_v4();
        stored.job.state = ContentLockDeletionState::Running;
        stored.job.attempt_count = stored.job.attempt_count.saturating_add(1);
        stored.job.next_attempt_at = None;
        stored.claimed_by = Some(worker_id.to_owned());
        stored.claim_token = Some(claim_token);
        stored.claim_expires_at = Some(claim_expires_at);
        Ok(Some(ClaimedContentLockDeletionJob {
            job: stored.job.clone(),
            claim_token,
        }))
    }

    async fn schedule_retry(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        next_attempt_at: OffsetDateTime,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let mut jobs = self.jobs.write().await;
        let Some(stored) = jobs
            .values_mut()
            .find(|stored| owns_claim(stored, job_id, worker_id, claim_token, now))
        else {
            return Ok(None);
        };
        stored.job.state = ContentLockDeletionState::Queued;
        stored.job.next_attempt_at = Some(next_attempt_at);
        clear_claim(stored);
        Ok(Some(stored.job.clone()))
    }

    async fn advance_phase(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        next_phase: ContentLockDeletionPhase,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let mut jobs = self.jobs.write().await;
        let Some(stored) = jobs
            .values_mut()
            .find(|stored| owns_claim(stored, job_id, worker_id, claim_token, now))
        else {
            return Ok(None);
        };
        if !stored.job.phase.permits(next_phase) {
            return Err(ApplicationError::InvalidContentLockDeletionState {
                message: "deletion phase must advance to its immediate successor".to_owned(),
            });
        }
        stored.job.phase = next_phase;
        stored.job.state = ContentLockDeletionState::Queued;
        stored.job.attempt_count = 0;
        stored.job.next_attempt_at = None;
        stored.job.failure_code = None;
        clear_claim(stored);
        Ok(Some(stored.job.clone()))
    }

    async fn finish(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        failure_code: Option<ContentLockDeletionFailureCode>,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let mut jobs = self.jobs.write().await;
        let Some(stored) = jobs
            .values_mut()
            .find(|stored| owns_claim(stored, job_id, worker_id, claim_token, now))
        else {
            return Ok(None);
        };
        stored.job.state = if failure_code.is_some() {
            ContentLockDeletionState::Failed
        } else {
            ContentLockDeletionState::Completed
        };
        stored.job.failure_code = failure_code;
        stored.job.next_attempt_at = None;
        clear_claim(stored);
        Ok(Some(stored.job.clone()))
    }

    async fn resume_failed_job(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        _resumed_at: OffsetDateTime,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let mut jobs = self.jobs.write().await;
        let receipts = self.force_receipts.read().await;
        if receipts.contains_key(&(creator.clone(), lock_id.clone())) {
            return Ok(None);
        }
        let Some(stored) = jobs.get_mut(&(creator.clone(), lock_id.clone())) else {
            return Ok(None);
        };
        if stored.job.state == ContentLockDeletionState::Failed {
            stored.job.state = ContentLockDeletionState::Queued;
            stored.job.attempt_count = 0;
            stored.job.next_attempt_at = None;
            stored.job.failure_code = None;
            clear_claim(stored);
        }
        Ok(Some(stored.job.clone()))
    }

    async fn prepare_force_deletion(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        frozen_content_lock: Option<&ContentLock>,
        forced_at: OffsetDateTime,
    ) -> Result<PrepareForceDeletionResult, ApplicationError> {
        let key = (creator.clone(), lock_id.clone());
        let mut verification_tasks = self.verification_task_fence.records.write().await;
        let intents = self.publication_intents.read().await;
        if intents.contains_key(&key) {
            return Ok(PrepareForceDeletionResult::PublicationInProgress);
        }
        if verification_tasks.values().any(|task| {
            task.creator == *creator
                && task.lock_id == *lock_id
                && task.deletion_job_id.is_some()
                && task.entitlement_publication_claim_token.is_some()
        }) {
            return Ok(PrepareForceDeletionResult::PublicationInProgress);
        }
        let mut jobs = self.jobs.write().await;
        let mut receipts = self.force_receipts.write().await;
        if let Some(receipt) = receipts.get(&key) {
            return Ok(PrepareForceDeletionResult::Synchronous(receipt.clone()));
        }
        if let Some(stored) = jobs.get_mut(&key) {
            if matches!(
                stored.job.state,
                ContentLockDeletionState::Queued | ContentLockDeletionState::Running
            ) {
                stored.job.force_requested_at.get_or_insert(forced_at);
                stored.job.state = ContentLockDeletionState::Queued;
                stored.job.next_attempt_at = None;
                clear_claim(stored);
                return Ok(PrepareForceDeletionResult::Active(stored.job.clone()));
            }
            let receipt = ForceDeletionReceipt {
                creator: creator.clone(),
                lock_id: lock_id.clone(),
                frozen_content_lock: Some(stored.job.frozen_content_lock.clone()),
                forced_at,
                completed_at: None,
            };
            receipts.insert(key, receipt.clone());
            let terminal_job_id = stored.job.job_id;
            jobs.remove(&(creator.clone(), lock_id.clone()));
            for task in verification_tasks.values_mut() {
                if task.deletion_job_id == Some(terminal_job_id) {
                    task.deletion_job_id = None;
                }
            }
            return Ok(PrepareForceDeletionResult::Synchronous(receipt));
        }
        let receipt = ForceDeletionReceipt {
            creator: creator.clone(),
            lock_id: lock_id.clone(),
            frozen_content_lock: frozen_content_lock.cloned(),
            forced_at,
            completed_at: None,
        };
        receipts.insert(key, receipt.clone());
        Ok(PrepareForceDeletionResult::Synchronous(receipt))
    }

    async fn get_force_receipt(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<Option<ForceDeletionReceipt>, ApplicationError> {
        Ok(self
            .force_receipts
            .read()
            .await
            .get(&(creator.clone(), lock_id.clone()))
            .cloned())
    }

    async fn complete_force_deletion(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        completed_at: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        let mut receipts = self.force_receipts.write().await;
        let Some(receipt) = receipts.get_mut(&(creator.clone(), lock_id.clone())) else {
            return Ok(false);
        };
        receipt.completed_at.get_or_insert(completed_at);
        Ok(true)
    }

    async fn has_force_receipt(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<bool, ApplicationError> {
        Ok(self
            .force_receipts
            .read()
            .await
            .contains_key(&(creator.clone(), lock_id.clone())))
    }
}

async fn remove_publication_intent(
    intents: &RwLock<HashMap<JobKey, PublicationIntent>>,
    creator: &CreatorPubky,
    lock_id: &LockId,
    publication_token: Uuid,
) -> Result<bool, ApplicationError> {
    let key = (creator.clone(), lock_id.clone());
    let mut intents = intents.write().await;
    if intents
        .get(&key)
        .is_none_or(|intent| intent.token != publication_token || intent.write_completed)
    {
        return Ok(false);
    }
    intents.remove(&key);
    Ok(true)
}

fn is_claimable(stored: &StoredJob, now: OffsetDateTime) -> bool {
    match stored.job.state {
        ContentLockDeletionState::Queued => stored
            .job
            .next_attempt_at
            .is_none_or(|next_attempt_at| next_attempt_at <= now),
        ContentLockDeletionState::Running => stored
            .claim_expires_at
            .is_some_and(|claim_expires_at| claim_expires_at <= now),
        ContentLockDeletionState::Completed | ContentLockDeletionState::Failed => false,
    }
}

fn owns_claim(
    stored: &StoredJob,
    job_id: Uuid,
    worker_id: &str,
    claim_token: Uuid,
    now: OffsetDateTime,
) -> bool {
    stored.job.job_id == job_id
        && stored.job.state == ContentLockDeletionState::Running
        && stored.claimed_by.as_deref() == Some(worker_id)
        && stored.claim_token == Some(claim_token)
        && stored
            .claim_expires_at
            .is_some_and(|claim_expires_at| now < claim_expires_at)
}

fn clear_claim(stored: &mut StoredJob) {
    stored.claimed_by = None;
    stored.claim_token = None;
    stored.claim_expires_at = None;
}
