use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, LockId};
use time::OffsetDateTime;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    models::{
        AdvanceContentLockDeletionPhaseResult, ClaimedContentLockDeletionJob,
        ContentLockDeletionFailureCode, ContentLockDeletionJob, ContentLockDeletionPhase,
        ContentLockDeletionState, PrepareForceDeletionResult,
    },
    ports::{ContentLockDeletionActionClaim, ContentLockDeletionRepository},
};
use crate::infrastructure::memory::{
    access_credentials::{AccessPhaseAdvanceStatus, InMemoryAccessCredentialStore},
    verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence,
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
#[derive(Debug)]
pub struct InMemoryContentLockDeletionRepository {
    jobs: RwLock<HashMap<JobKey, StoredJob>>,
    force_receipts: RwLock<HashMap<JobKey, OffsetDateTime>>,
    publication_intents: RwLock<HashMap<JobKey, Uuid>>,
    claim_transition_gate: Mutex<()>,
    verification_task_fence: Arc<InMemoryVerificationTaskDeletionFence>,
    access_credentials: Arc<InMemoryAccessCredentialStore>,
}

impl Default for InMemoryContentLockDeletionRepository {
    fn default() -> Self {
        Self::with_access_credentials_and_verification_task_fence(
            Arc::new(InMemoryAccessCredentialStore::new()),
            Arc::new(InMemoryVerificationTaskDeletionFence::new()),
        )
    }
}

impl InMemoryContentLockDeletionRepository {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_verification_task_fence(
        verification_task_fence: Arc<InMemoryVerificationTaskDeletionFence>,
    ) -> Self {
        Self::with_access_credentials_and_verification_task_fence(
            Arc::new(InMemoryAccessCredentialStore::new()),
            verification_task_fence,
        )
    }

    pub fn with_access_credentials_and_verification_task_fence(
        access_credentials: Arc<InMemoryAccessCredentialStore>,
        verification_task_fence: Arc<InMemoryVerificationTaskDeletionFence>,
    ) -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            force_receipts: RwLock::new(HashMap::new()),
            publication_intents: RwLock::new(HashMap::new()),
            claim_transition_gate: Mutex::new(()),
            verification_task_fence,
            access_credentials,
        }
    }

    pub(super) async fn action_claim_is_live(
        &self,
        claim: ContentLockDeletionActionClaim<'_>,
    ) -> bool {
        let now = self.verification_task_fence.authoritative_cutoff();
        self.jobs.read().await.values().any(|stored| {
            stored.job.job_id == claim.job_id
                && stored.job.state == ContentLockDeletionState::Running
                && stored.job.phase == claim.expected_phase
                && stored.claimed_by.as_deref() == Some(claim.worker_id)
                && stored.claim_token == Some(claim.claim_token)
                && stored
                    .claim_expires_at
                    .is_some_and(|expires_at| expires_at > now)
                && (stored.job.force_requested_at.is_some() == claim.force)
        })
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
        if jobs.contains_key(&key) || receipts.contains_key(&key) {
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

    async fn insert_job(&self, mut job: ContentLockDeletionJob) -> Result<(), ApplicationError> {
        job.validate_frozen_identity()?;
        job.validate_state(false)?;
        let key = (job.creator.clone(), job.lock_id.clone());
        let _admission = self
            .verification_task_fence
            .acquire_lock_admission(&job.creator, &job.lock_id)
            .await;
        job.deletion_started_at = self.verification_task_fence.authoritative_cutoff();
        let mut verification_tasks = self.verification_task_fence.records.write().await;
        let intents = self.publication_intents.read().await;
        let mut jobs = self.jobs.write().await;
        let receipts = self.force_receipts.read().await;
        if intents.contains_key(&key) || receipts.contains_key(&key) {
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
        let snapshot_bundles = matching_task_ids
            .iter()
            .filter_map(|task_id| {
                verification_tasks.get(task_id).map(|task| {
                    (
                        task.bundle_id.clone(),
                        (*task_id, task.paykit_admission_required, task.status),
                    )
                })
            })
            .collect::<HashMap<_, _>>();
        if matching_task_ids.iter().any(|task_id| {
            verification_tasks
                .get(task_id)
                .is_some_and(|task| task.entitlement_publication_claim_token.is_some())
        }) {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        self.access_credentials
            .register_deletion(&job, &snapshot_bundles)
            .await?;
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
        claim_ttl: time::Duration,
    ) -> Result<Option<ClaimedContentLockDeletionJob>, ApplicationError> {
        let _claim_transition = self.claim_transition_gate.lock().await;
        let mut jobs = self.jobs.write().await;
        let now = self.verification_task_fence.authoritative_cutoff();
        let claim_expires_at = now + claim_ttl;
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
        self.access_credentials
            .synchronize_job(
                &stored.job,
                stored.claimed_by.as_deref(),
                stored.claim_token,
                stored.claim_expires_at,
            )
            .await;
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
        retry_after: time::Duration,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let _claim_transition = self.claim_transition_gate.lock().await;
        let mut jobs = self.jobs.write().await;
        let now = self.verification_task_fence.authoritative_cutoff();
        let next_attempt_at = now + retry_after;
        let Some(stored) = jobs
            .values_mut()
            .find(|stored| owns_claim(stored, job_id, worker_id, claim_token, now))
        else {
            return Ok(None);
        };
        stored.job.state = ContentLockDeletionState::Queued;
        stored.job.next_attempt_at = Some(next_attempt_at);
        clear_claim(stored);
        self.access_credentials
            .synchronize_job(&stored.job, None, None, None)
            .await;
        Ok(Some(stored.job.clone()))
    }

    async fn defer(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        defer_for: time::Duration,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let _claim_transition = self.claim_transition_gate.lock().await;
        let mut jobs = self.jobs.write().await;
        let now = self.verification_task_fence.authoritative_cutoff();
        let next_attempt_at = now + defer_for;
        let Some(stored) = jobs
            .values_mut()
            .find(|stored| owns_claim(stored, job_id, worker_id, claim_token, now))
        else {
            return Ok(None);
        };
        stored.job.attempt_count = stored.job.attempt_count.saturating_sub(1);
        stored.job.state = ContentLockDeletionState::Queued;
        stored.job.next_attempt_at = Some(next_attempt_at);
        clear_claim(stored);
        self.access_credentials
            .synchronize_job(&stored.job, None, None, None)
            .await;
        Ok(Some(stored.job.clone()))
    }

    async fn advance_phase(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        next_phase: ContentLockDeletionPhase,
    ) -> Result<AdvanceContentLockDeletionPhaseResult, ApplicationError> {
        let _claim_transition = self.claim_transition_gate.lock().await;
        let mut jobs = self.jobs.write().await;
        let now = self.verification_task_fence.authoritative_cutoff();
        let Some(stored) = jobs
            .values_mut()
            .find(|stored| owns_claim(stored, job_id, worker_id, claim_token, now))
        else {
            return Ok(AdvanceContentLockDeletionPhaseResult::ClaimLost);
        };
        if !stored.job.phase.permits(next_phase) {
            return Err(ApplicationError::InvalidContentLockDeletionState {
                message: "deletion phase must advance to its immediate successor".to_owned(),
            });
        }
        let access_status = self
            .access_credentials
            .check_phase_advance(job_id, stored.job.phase, next_phase, now)
            .await?;
        match access_status {
            AccessPhaseAdvanceStatus::Ready => {}
            AccessPhaseAdvanceStatus::ObligationsPending => {
                return Ok(AdvanceContentLockDeletionPhaseResult::ObligationsPending);
            }
            AccessPhaseAdvanceStatus::FinalCredentialIssuanceMissed => {
                return Ok(AdvanceContentLockDeletionPhaseResult::TerminalFailure(
                    ContentLockDeletionFailureCode::StateCorrupt,
                ));
            }
        }
        stored.job.phase = next_phase;
        stored.job.state = ContentLockDeletionState::Queued;
        stored.job.attempt_count = 0;
        stored.job.next_attempt_at = None;
        stored.job.failure_code = None;
        clear_claim(stored);
        self.access_credentials
            .synchronize_job(&stored.job, None, None, None)
            .await;
        Ok(AdvanceContentLockDeletionPhaseResult::Advanced(Box::new(
            stored.job.clone(),
        )))
    }

    async fn expire_unresolved_non_paykit_tasks(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        let _claim_transition = self.claim_transition_gate.lock().await;
        let now = self.verification_task_fence.authoritative_cutoff();
        self.access_credentials
            .expire_unresolved_non_paykit_tasks(job_id, worker_id, claim_token, now)
            .await
    }

    async fn finish(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        failure_code: Option<ContentLockDeletionFailureCode>,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let _claim_transition = self.claim_transition_gate.lock().await;
        let mut jobs = self.jobs.write().await;
        let now = self.verification_task_fence.authoritative_cutoff();
        let Some(stored) = jobs
            .values_mut()
            .find(|stored| owns_claim(stored, job_id, worker_id, claim_token, now))
        else {
            return Ok(None);
        };
        if failure_code.is_none() {
            self.access_credentials
                .check_successful_finish(job_id, stored.job.phase, now)
                .await?;
        }
        stored.job.state = if failure_code.is_some() {
            ContentLockDeletionState::Failed
        } else {
            ContentLockDeletionState::Completed
        };
        stored.job.failure_code = failure_code;
        stored.job.next_attempt_at = None;
        clear_claim(stored);
        self.access_credentials
            .synchronize_job(&stored.job, None, None, None)
            .await;
        Ok(Some(stored.job.clone()))
    }

    async fn resume_failed_job(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        _resumed_at: OffsetDateTime,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let _claim_transition = self.claim_transition_gate.lock().await;
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
        self.access_credentials
            .synchronize_job(&stored.job, None, None, None)
            .await;
        Ok(Some(stored.job.clone()))
    }

    async fn prepare_force_deletion(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<PrepareForceDeletionResult, ApplicationError> {
        let _claim_transition = self.claim_transition_gate.lock().await;
        let forced_at = self.verification_task_fence.authoritative_cutoff();
        let key = (creator.clone(), lock_id.clone());
        let intents = self.publication_intents.read().await;
        if intents.contains_key(&key) {
            return Ok(PrepareForceDeletionResult::PublicationInProgress);
        }
        let verification_tasks = self.verification_task_fence.records.read().await;
        if verification_tasks.values().any(|task| {
            task.creator == *creator
                && task.lock_id == *lock_id
                && task.deletion_job_id.is_some()
                && task.entitlement_publication_claim_token.is_some()
        }) {
            return Ok(PrepareForceDeletionResult::PublicationInProgress);
        }
        drop(verification_tasks);
        let mut jobs = self.jobs.write().await;
        let mut receipts = self.force_receipts.write().await;
        if receipts.contains_key(&key) {
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
                self.access_credentials
                    .synchronize_job(&stored.job, None, None, None)
                    .await;
                return Ok(PrepareForceDeletionResult::Active(stored.job.clone()));
            }
            let job = stored.job.clone();
            self.access_credentials
                .block_key_and_disable_job(creator, lock_id, Some(job.job_id))
                .await;
            jobs.remove(&key);
            receipts.insert(key, forced_at);
            return Ok(PrepareForceDeletionResult::Synchronous(Some(job)));
        }
        self.access_credentials
            .block_key_and_disable_job(creator, lock_id, None)
            .await;
        receipts.insert(key, forced_at);
        Ok(PrepareForceDeletionResult::Synchronous(None))
    }

    async fn complete_force_deletion(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        let _claim_transition = self.claim_transition_gate.lock().await;
        let now = self.verification_task_fence.authoritative_cutoff();
        let key = self
            .jobs
            .read()
            .await
            .iter()
            .find_map(|(key, stored)| (stored.job.job_id == job_id).then(|| key.clone()));
        let Some(key) = key else {
            return Ok(false);
        };
        let _admission = self
            .verification_task_fence
            .acquire_lock_admission(&key.0, &key.1)
            .await;
        let mut jobs = self.jobs.write().await;
        let mut receipts = self.force_receipts.write().await;
        let Some(stored) = jobs.get(&key) else {
            return Ok(false);
        };
        if !owns_claim(stored, job_id, worker_id, claim_token, now) {
            return Ok(false);
        }
        let Some(forced_at) = stored.job.force_requested_at else {
            return Ok(false);
        };
        self.access_credentials
            .block_key_and_disable_job(&key.0, &key.1, Some(job_id))
            .await;
        receipts.insert(key.clone(), forced_at);
        jobs.remove(&key);
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
