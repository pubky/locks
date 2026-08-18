use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, LockId};
use time::OffsetDateTime;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    models::{
        ClaimedContentLockDeletionJob, ContentLockDeletionFailureCode, ContentLockDeletionJob,
        ContentLockDeletionPhase, ContentLockDeletionState, PrepareForceDeletionResult,
    },
    ports::ContentLockDeletionRepository,
};

type JobKey = (CreatorPubky, LockId);

#[derive(Debug, Clone)]
struct StoredJob {
    job: ContentLockDeletionJob,
    claimed_by: Option<String>,
    claim_token: Option<Uuid>,
    claim_expires_at: Option<OffsetDateTime>,
}

/// In-memory deletion repository with the same lease-fencing semantics as PostgreSQL.
#[derive(Debug, Default)]
pub struct InMemoryContentLockDeletionRepository {
    jobs: RwLock<HashMap<JobKey, StoredJob>>,
    force_receipts: RwLock<HashSet<JobKey>>,
    publication_intents: RwLock<HashMap<JobKey, Uuid>>,
}

impl InMemoryContentLockDeletionRepository {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ContentLockDeletionRepository for InMemoryContentLockDeletionRepository {
    async fn begin_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<(), ApplicationError> {
        let key = (creator.clone(), lock_id.clone());
        let mut intents = self.publication_intents.write().await;
        let jobs = self.jobs.read().await;
        let receipts = self.force_receipts.read().await;
        if jobs.contains_key(&key) || receipts.contains(&key) {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        if intents.contains_key(&key) {
            return Err(ApplicationError::ContentLockPathConflict {
                guarded_path: "content lock publication in progress".to_owned(),
            });
        }
        intents.insert(key, publication_token);
        Ok(())
    }

    async fn finish_publication(
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

    async fn publication_in_progress(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<bool, ApplicationError> {
        Ok(self
            .publication_intents
            .read()
            .await
            .contains_key(&(creator.clone(), lock_id.clone())))
    }

    async fn insert_job(&self, job: ContentLockDeletionJob) -> Result<(), ApplicationError> {
        job.validate_frozen_identity()?;
        job.validate_state(false)?;
        let key = (job.creator.clone(), job.lock_id.clone());
        let intents = self.publication_intents.read().await;
        let mut jobs = self.jobs.write().await;
        let receipts = self.force_receipts.read().await;
        if intents.contains_key(&key) || receipts.contains(&key) {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        if jobs.contains_key(&key) || jobs.values().any(|stored| stored.job.job_id == job.job_id) {
            return Err(ApplicationError::DuplicateRecord {
                record: "content_lock_deletion_job",
            });
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
        if receipts.contains(&(creator.clone(), lock_id.clone())) {
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
        forced_at: OffsetDateTime,
    ) -> Result<PrepareForceDeletionResult, ApplicationError> {
        let key = (creator.clone(), lock_id.clone());
        let intents = self.publication_intents.read().await;
        if intents.contains_key(&key) {
            return Ok(PrepareForceDeletionResult::PublicationInProgress);
        }
        let mut jobs = self.jobs.write().await;
        let mut receipts = self.force_receipts.write().await;
        if receipts.contains(&key) {
            return Ok(PrepareForceDeletionResult::Synchronous(
                jobs.get(&key).map(|stored| stored.job.clone()),
            ));
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
            let job = stored.job.clone();
            jobs.remove(&key);
            receipts.insert(key);
            return Ok(PrepareForceDeletionResult::Synchronous(Some(job)));
        }
        receipts.insert(key);
        Ok(PrepareForceDeletionResult::Synchronous(None))
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
            .contains(&(creator.clone(), lock_id.clone())))
    }
}

async fn remove_publication_intent(
    intents: &RwLock<HashMap<JobKey, Uuid>>,
    creator: &CreatorPubky,
    lock_id: &LockId,
    publication_token: Uuid,
) -> Result<bool, ApplicationError> {
    let key = (creator.clone(), lock_id.clone());
    let mut intents = intents.write().await;
    if intents.get(&key) != Some(&publication_token) {
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
            .is_some_and(|claim_expires_at| claim_expires_at < now),
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
            .is_some_and(|claim_expires_at| claim_expires_at >= now)
}

fn clear_claim(stored: &mut StoredJob) {
    stored.claimed_by = None;
    stored.claim_token = None;
    stored.claim_expires_at = None;
}
