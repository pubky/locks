use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, LockId};
use locks_core::lock_policy::{ContentLock, ContentLockPayload};
use time::OffsetDateTime;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    models::{
        AdvanceContentLockDeletionPhaseResult, ClaimedContentLockDeletionJob,
        ContentLockDeletionFailureCode, ContentLockDeletionJob, ContentLockDeletionPhase,
        ContentLockDeletionState, ForceDeletionReceipt, PrepareForceDeletionResult,
        VerificationTaskStatus,
    },
    ports::{
        ContentLockDeletionActionClaim, ContentLockDeletionRepository, PublicationRecovery,
        publication_guarded_paths, publication_intent_matches_request,
        publication_reconciliation_matches,
    },
};
use crate::infrastructure::memory::{
    access_credentials::{AccessPhaseAdvanceStatus, InMemoryAccessCredentialStore},
    content_lock_ownership::OwnershipRecords,
    verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence,
};

type JobKey = (CreatorPubky, LockId);

#[cfg(test)]
fn consume_injected_failure(counter: &AtomicUsize) -> bool {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
            value.checked_sub(1)
        })
        .is_ok()
}

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
    ownership_records: OwnershipRecords,
    claim_transition_gate: Mutex<()>,
    verification_task_fence: Arc<InMemoryVerificationTaskDeletionFence>,
    access_credentials: Arc<InMemoryAccessCredentialStore>,
    #[cfg(test)]
    mark_publication_failures: AtomicUsize,
    #[cfg(test)]
    compensate_publication_failures: AtomicUsize,
    #[cfg(test)]
    abandon_publication_failures: AtomicUsize,
    #[cfg(test)]
    compensation_barrier: Option<(
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    )>,
}

impl Default for InMemoryContentLockDeletionRepository {
    fn default() -> Self {
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::new());
        Self::with_dependencies(
            Arc::new(InMemoryAccessCredentialStore::with_deletion_fence(
                Arc::clone(&fence),
            )),
            fence,
            OwnershipRecords::default(),
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
            Arc::new(InMemoryAccessCredentialStore::with_deletion_fence(
                Arc::clone(&verification_task_fence),
            )),
            verification_task_fence,
        )
    }

    pub fn with_access_credentials_and_verification_task_fence(
        access_credentials: Arc<InMemoryAccessCredentialStore>,
        verification_task_fence: Arc<InMemoryVerificationTaskDeletionFence>,
    ) -> Self {
        Self::with_dependencies(
            access_credentials,
            verification_task_fence,
            OwnershipRecords::default(),
        )
    }

    pub(super) fn with_dependencies(
        access_credentials: Arc<InMemoryAccessCredentialStore>,
        verification_task_fence: Arc<InMemoryVerificationTaskDeletionFence>,
        ownership_records: OwnershipRecords,
    ) -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            force_receipts: RwLock::new(HashMap::new()),
            publication_intents: RwLock::new(HashMap::new()),
            ownership_records,
            claim_transition_gate: Mutex::new(()),
            verification_task_fence,
            access_credentials,
            #[cfg(test)]
            mark_publication_failures: AtomicUsize::new(0),
            #[cfg(test)]
            compensate_publication_failures: AtomicUsize::new(0),
            #[cfg(test)]
            abandon_publication_failures: AtomicUsize::new(0),
            #[cfg(test)]
            compensation_barrier: None,
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

    pub(super) async fn synchronous_force_action_is_live(&self, job_id: Uuid) -> bool {
        let jobs = self.jobs.read().await;
        let Some((key, stored)) = jobs.iter().find(|(_, stored)| stored.job.job_id == job_id)
        else {
            return false;
        };
        if !matches!(
            stored.job.state,
            ContentLockDeletionState::Failed | ContentLockDeletionState::Completed
        ) {
            return false;
        }
        self.force_receipts
            .read()
            .await
            .get(key)
            .is_some_and(|receipt| receipt.completed_at.is_none())
    }

    pub(super) async fn graceful_withdrawal_action_is_live(&self, job_id: Uuid) -> bool {
        self.jobs.read().await.values().any(|stored| {
            stored.job.job_id == job_id
                && matches!(
                    stored.job.state,
                    ContentLockDeletionState::Queued | ContentLockDeletionState::Running
                )
                && stored.job.phase == ContentLockDeletionPhase::Withdraw
                && stored.job.force_requested_at.is_none()
        })
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn with_compensation_barrier(
        mut self,
        started: std::sync::Arc<tokio::sync::Notify>,
        release: std::sync::Arc<tokio::sync::Notify>,
    ) -> Self {
        self.compensation_barrier = Some((started, release));
        self
    }

    #[cfg(test)]
    pub(crate) fn with_publication_bookkeeping_failures(
        self,
        mark: usize,
        compensate: usize,
        abandon: usize,
    ) -> Self {
        self.mark_publication_failures.store(mark, Ordering::SeqCst);
        self.compensate_publication_failures
            .store(compensate, Ordering::SeqCst);
        self.abandon_publication_failures
            .store(abandon, Ordering::SeqCst);
        self
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
        let _admission = self
            .verification_task_fence
            .acquire_lock_admission(creator, lock_id)
            .await;
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
        #[cfg(test)]
        if consume_injected_failure(&self.mark_publication_failures) {
            return Err(ApplicationError::Storage {
                message: "injected mark publication failure".to_owned(),
            });
        }
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
        #[cfg(test)]
        if consume_injected_failure(&self.abandon_publication_failures) {
            return Err(ApplicationError::Storage {
                message: "injected abandon publication failure".to_owned(),
            });
        }
        remove_publication_intent(
            &self.publication_intents,
            creator,
            lock_id,
            publication_token,
        )
        .await
    }

    async fn compensate_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        #[cfg(test)]
        if consume_injected_failure(&self.compensate_publication_failures) {
            return Err(ApplicationError::Storage {
                message: "injected compensate publication failure".to_owned(),
            });
        }
        let key = (creator.clone(), lock_id.clone());
        let mut intents = self.publication_intents.write().await;
        let Some(intent) = intents.get(&key) else {
            return Ok(false);
        };
        if intent.token != publication_token || intent.write_completed {
            return Ok(false);
        }
        let guarded_paths = publication_guarded_paths(&intent.original_content_lock);
        let mut ownership = self.ownership_records.write().await;
        #[cfg(test)]
        if let Some((started, release)) = &self.compensation_barrier {
            started.notify_one();
            release.notified().await;
        }
        for guarded_path in guarded_paths {
            let ownership_key = (creator.clone(), guarded_path);
            if ownership.get(&ownership_key).is_some_and(|record| {
                record.lock_id == *lock_id
                    && record.status
                        == crate::application::models::ContentLockOwnershipStatus::Reserved
            }) {
                ownership.remove(&ownership_key);
            }
        }
        intents.remove(&key);
        Ok(true)
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
        Ok(PublicationRecovery::Pending {
            publication_token: intent.token,
            content_lock: intent.original_content_lock.clone(),
        })
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
        if next_phase == ContentLockDeletionPhase::StartPaymentDrain
            && self
                .verification_task_fence
                .records
                .read()
                .await
                .values()
                .any(|task| {
                    task.deletion_job_id == Some(job_id)
                        && task.paykit_admission_required
                        && !task.invoice_admission_ready
                        && task.status != VerificationTaskStatus::Failed
                })
        {
            return Ok(AdvanceContentLockDeletionPhaseResult::ObligationsPending);
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

    async fn has_paykit_snapshots(&self, job_id: Uuid) -> Result<bool, ApplicationError> {
        Ok(self
            .verification_task_fence
            .records
            .read()
            .await
            .values()
            .any(|task| task.deletion_job_id == Some(job_id) && task.paykit_admission_required))
    }

    async fn finish(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        failure_code: Option<ContentLockDeletionFailureCode>,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        if failure_code.is_none() {
            let key = {
                let jobs = self.jobs.read().await;
                let Some(stored) = jobs.values().find(|stored| stored.job.job_id == job_id) else {
                    return Ok(None);
                };
                (stored.job.creator.clone(), stored.job.lock_id.clone())
            };
            let _admission = self
                .verification_task_fence
                .acquire_lock_admission(&key.0, &key.1)
                .await;
            let _claim_transition = self.claim_transition_gate.lock().await;
            let mut folded_auth_reservations = self
                .verification_task_fence
                .folded_auth_reservations
                .write()
                .await;
            let mut fence_records = self.verification_task_fence.records.write().await;
            let task_ids = fence_records
                .iter()
                .filter_map(|(task_id, record)| {
                    (record.creator == key.0 && record.lock_id == key.1).then_some(*task_id)
                })
                .collect::<HashSet<_>>();
            let task_handles = fence_records
                .iter()
                .filter_map(|(task_id, record)| {
                    task_ids
                        .contains(task_id)
                        .then_some((record.creator.clone(), record.bundle_id.clone()))
                })
                .collect::<HashSet<_>>();
            let mut payment_records = self.verification_task_fence.payment_records.write().await;
            let mut current_statuses = self.verification_task_fence.current_statuses.write().await;
            let mut graph_records = self.verification_task_fence.graph_records.write().await;
            let mut deletion_cutoffs = self.verification_task_fence.deletion_cutoffs.write().await;
            let mut intents = self.publication_intents.write().await;
            let mut ownership = self.ownership_records.write().await;
            let mut jobs = self.jobs.write().await;
            let now = self.verification_task_fence.authoritative_cutoff();
            let Some(stored) = jobs
                .values()
                .find(|stored| owns_claim(stored, job_id, worker_id, claim_token, now))
            else {
                return Ok(None);
            };
            self.access_credentials
                .check_successful_finish(job_id, stored.job.phase, now)
                .await?;
            if let Some(verification_tasks) = self.access_credentials.verification_task_repository()
            {
                verification_tasks
                    .purge_verification_tasks(&task_ids.iter().copied().collect::<Vec<_>>())
                    .await?;
            }
            let mut completed = stored.job.clone();
            completed.state = ContentLockDeletionState::Completed;
            completed.failure_code = None;
            completed.next_attempt_at = None;
            self.access_credentials.purge_deletion(job_id).await;
            fence_records.retain(|task_id, _| !task_ids.contains(task_id));
            graph_records.retain(|stored| !task_ids.contains(&stored.task.task_id));
            payment_records.retain(|stored| !task_ids.contains(&stored.task.task_id));
            current_statuses.retain(|task_id, _| !task_ids.contains(task_id));
            folded_auth_reservations.retain(|handle| !task_handles.contains(handle));
            intents.remove(&key);
            ownership.retain(|_, record| record.creator != key.0 || record.lock_id != key.1);
            deletion_cutoffs.remove(&key);
            jobs.remove(&key);
            return Ok(Some(completed));
        }

        let _claim_transition = self.claim_transition_gate.lock().await;
        let mut jobs = self.jobs.write().await;
        let now = self.verification_task_fence.authoritative_cutoff();
        let Some(stored) = jobs
            .values_mut()
            .find(|stored| owns_claim(stored, job_id, worker_id, claim_token, now))
        else {
            return Ok(None);
        };
        stored.job.state = ContentLockDeletionState::Failed;
        stored.job.failure_code = failure_code;
        stored.job.next_attempt_at = None;
        clear_claim(stored);
        self.access_credentials
            .synchronize_job(&stored.job, None, None, None)
            .await;
        Ok(Some(stored.job.clone()))
    }

    async fn fail_graceful_withdrawal(
        &self,
        job_id: Uuid,
        failure_code: ContentLockDeletionFailureCode,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let _claim_transition = self.claim_transition_gate.lock().await;
        let mut jobs = self.jobs.write().await;
        let Some(stored) = jobs.values_mut().find(|stored| {
            stored.job.job_id == job_id
                && matches!(
                    stored.job.state,
                    ContentLockDeletionState::Queued | ContentLockDeletionState::Running
                )
                && stored.job.phase == ContentLockDeletionPhase::Withdraw
                && stored.job.force_requested_at.is_none()
        }) else {
            return Ok(None);
        };
        stored.job.state = ContentLockDeletionState::Failed;
        stored.job.failure_code = Some(failure_code);
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
        self.prepare_force_deletion_with_manifest(creator, lock_id, None)
            .await
    }

    async fn prepare_force_deletion_with_manifest(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        frozen_content_lock: Option<&ContentLockPayload>,
    ) -> Result<PrepareForceDeletionResult, ApplicationError> {
        let _admission = self
            .verification_task_fence
            .acquire_lock_admission(creator, lock_id)
            .await;
        let _claim_transition = self.claim_transition_gate.lock().await;
        let forced_at = self.verification_task_fence.authoritative_cutoff();
        let key = (creator.clone(), lock_id.clone());
        let verification_tasks = self.verification_task_fence.records.read().await;
        let entitlement_publication_in_progress = verification_tasks.values().any(|task| {
            task.creator == *creator
                && task.lock_id == *lock_id
                && task.entitlement_publication_claim_token.is_some()
        });
        drop(verification_tasks);
        let intents = self.publication_intents.read().await;
        if intents.contains_key(&key) {
            return Ok(PrepareForceDeletionResult::PublicationInProgress);
        }
        drop(intents);
        let requires_terminal_abandonment = {
            let jobs = self.jobs.read().await;
            let receipts = self.force_receipts.read().await;
            !receipts.contains_key(&key)
                && match jobs.get(&key) {
                    None => true,
                    Some(stored) => {
                        !(matches!(
                            stored.job.state,
                            ContentLockDeletionState::Queued | ContentLockDeletionState::Running
                        ) || (stored.job.state == ContentLockDeletionState::Failed
                            && entitlement_publication_in_progress))
                    }
                }
        };
        if requires_terminal_abandonment {
            self.verification_task_fence
                .deletion_cutoffs
                .write()
                .await
                .insert(key.clone());
            self.verification_task_fence
                .abandon_provider_v2_tasks(creator, lock_id, forced_at, &HashSet::new())
                .await;
        }
        let mut jobs = self.jobs.write().await;
        let mut receipts = self.force_receipts.write().await;
        if let Some(receipt) = receipts.get_mut(&key) {
            if receipt.completed_at.is_none()
                && receipt.frozen_content_lock.is_none()
                && frozen_content_lock.is_some()
            {
                receipt.frozen_content_lock = frozen_content_lock.cloned();
            }
            return Ok(PrepareForceDeletionResult::Synchronous(
                jobs.get(&key).map(|stored| stored.job.clone()),
            ));
        }
        if let Some(stored) = jobs.get_mut(&key) {
            if entitlement_publication_in_progress {
                if stored.job.state != ContentLockDeletionState::Failed {
                    return Ok(PrepareForceDeletionResult::PublicationInProgress);
                }
                stored.job.state = ContentLockDeletionState::Queued;
                stored.job.attempt_count = 0;
                stored.job.next_attempt_at = None;
                stored.job.failure_code = None;
                clear_claim(stored);
                self.access_credentials
                    .synchronize_job(&stored.job, None, None, None)
                    .await;
                return Ok(PrepareForceDeletionResult::Active(stored.job.clone()));
            }
            if matches!(
                stored.job.state,
                ContentLockDeletionState::Queued | ContentLockDeletionState::Running
            ) {
                stored.job.force_requested_at.get_or_insert(forced_at);
                stored.job.state = ContentLockDeletionState::Queued;
                stored.job.attempt_count = 0;
                stored.job.next_attempt_at = None;
                stored.job.failure_code = None;
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
            receipts.insert(
                key,
                ForceDeletionReceipt {
                    creator: creator.clone(),
                    lock_id: lock_id.clone(),
                    frozen_content_lock: Some(job.frozen_content_lock.clone()),
                    forced_at,
                    completed_at: None,
                },
            );
            return Ok(PrepareForceDeletionResult::Synchronous(Some(job)));
        }
        self.access_credentials
            .block_key_and_disable_job(creator, lock_id, None)
            .await;
        receipts.insert(
            key,
            ForceDeletionReceipt {
                creator: creator.clone(),
                lock_id: lock_id.clone(),
                frozen_content_lock: frozen_content_lock.cloned(),
                forced_at,
                completed_at: None,
            },
        );
        Ok(PrepareForceDeletionResult::Synchronous(None))
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

    async fn complete_synchronous_force_deletion(
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

    async fn complete_force_deletion(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
    ) -> Result<bool, ApplicationError> {
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
        let _claim_transition = self.claim_transition_gate.lock().await;
        let preserve_terminal = self
            .access_credentials
            .reconciled_provider_v2_terminal_task_ids(job_id)
            .await;
        let mut fence_records = self.verification_task_fence.records.write().await;
        let mut payment_records = self.verification_task_fence.payment_records.write().await;
        let mut current_statuses = self.verification_task_fence.current_statuses.write().await;
        let mut jobs = self.jobs.write().await;
        let mut receipts = self.force_receipts.write().await;
        let now = self.verification_task_fence.authoritative_cutoff();
        let Some(stored) = jobs.get(&key) else {
            return Ok(false);
        };
        if !owns_claim(stored, job_id, worker_id, claim_token, now) {
            return Ok(false);
        }
        let Some(forced_at) = stored.job.force_requested_at else {
            return Ok(false);
        };
        InMemoryVerificationTaskDeletionFence::abandon_provider_v2_tasks_under_locks(
            &mut fence_records,
            &mut payment_records,
            &mut current_statuses,
            &key.0,
            &key.1,
            now,
            &preserve_terminal,
        );
        self.access_credentials
            .block_key_and_disable_job(&key.0, &key.1, Some(job_id))
            .await;
        receipts.insert(
            key.clone(),
            ForceDeletionReceipt {
                creator: key.0.clone(),
                lock_id: key.1.clone(),
                frozen_content_lock: Some(stored.job.frozen_content_lock.clone()),
                forced_at,
                completed_at: Some(now),
            },
        );
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
    intents: &RwLock<HashMap<JobKey, PublicationIntent>>,
    creator: &CreatorPubky,
    lock_id: &LockId,
    publication_token: Uuid,
) -> Result<bool, ApplicationError> {
    let key = (creator.clone(), lock_id.clone());
    let mut intents = intents.write().await;
    if intents
        .get(&key)
        .is_none_or(|intent| intent.token != publication_token)
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

#[cfg(test)]
mod lock_order_tests {
    use std::{
        collections::BTreeMap,
        future::{Future, poll_fn},
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicI64, Ordering},
        },
        task::Poll,
        time::Duration as StdDuration,
    };

    use locks_core::{
        ids::{BundleId, CreatorPubky, GuardedResourceHash, PubkyLockResource, TaskId},
        lock_policy::{
            AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GraphParticipantV2, GraphPolicyV2,
            GraphPredicateV2, GuardedResource, LockLogic, LockServerConfig,
        },
    };
    use time::macros::datetime;

    use super::*;
    use crate::{
        application::{
            models::{GraphTaskRecord, GraphTaskSnapshot, GraphTaskSnapshotV1},
            ports::{Clock, GraphTaskClaimer, GraphTaskRepository},
        },
        infrastructure::memory::graph_tasks::InMemoryGraphTaskRepository,
    };

    const NOW: OffsetDateTime = datetime!(2026-11-10 12:00:00 UTC);
    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";

    #[derive(Debug)]
    struct FixedClock;

    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            NOW
        }
    }

    #[derive(Debug)]
    struct MutableClock(AtomicI64);

    impl MutableClock {
        fn new(now: OffsetDateTime) -> Self {
            Self(AtomicI64::new(now.unix_timestamp()))
        }

        fn set(&self, now: OffsetDateTime) {
            self.0.store(now.unix_timestamp(), Ordering::SeqCst);
        }
    }

    impl Clock for MutableClock {
        fn now(&self) -> OffsetDateTime {
            OffsetDateTime::from_unix_timestamp(self.0.load(Ordering::SeqCst)).unwrap()
        }
    }

    #[tokio::test]
    async fn successful_finish_rejects_claim_that_expires_while_waiting_for_fence_locks() {
        let clock = Arc::new(MutableClock::new(NOW));
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(
            clock.clone(),
        ));
        let repository = Arc::new(
            InMemoryContentLockDeletionRepository::with_verification_task_fence(Arc::clone(&fence)),
        );
        let claim = ready_to_finish(&repository, content_lock(0)).await;
        let claim_key = (claim.job.creator.clone(), claim.job.lock_id.clone());

        let records_guard = fence.records.write().await;
        let finishing = {
            let repository = Arc::clone(&repository);
            tokio::spawn(async move {
                repository
                    .finish(claim.job.job_id, "finish-worker", claim.claim_token, None)
                    .await
            })
        };
        wait_until_locked(|| fence.folded_auth_reservations.try_read().is_err()).await;
        clock.set(NOW + time::Duration::minutes(2));
        drop(records_guard);

        assert!(
            finishing.await.unwrap().unwrap().is_none(),
            "finish must revalidate the exact live claim at post-fence authoritative time"
        );
        assert!(
            repository
                .get_job(&claim_key.0, &claim_key.1)
                .await
                .unwrap()
                .is_some(),
            "expired finisher must not purge the deletion job"
        );
    }

    #[tokio::test]
    async fn force_completion_rejects_claim_that_expires_while_waiting_for_fence_locks() {
        let clock = Arc::new(MutableClock::new(NOW));
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(
            clock.clone(),
        ));
        let repository = Arc::new(
            InMemoryContentLockDeletionRepository::with_verification_task_fence(Arc::clone(&fence)),
        );
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(1), NOW).unwrap();
        repository.insert_job(job.clone()).await.unwrap();
        assert!(matches!(
            repository
                .prepare_force_deletion(&job.creator, &job.lock_id)
                .await
                .unwrap(),
            PrepareForceDeletionResult::Active(_)
        ));
        let claim = repository
            .claim_next("force-worker", time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        let claim_key = (claim.job.creator.clone(), claim.job.lock_id.clone());

        let records_guard = fence.records.write().await;
        let completing = {
            let repository = Arc::clone(&repository);
            tokio::spawn(async move {
                repository
                    .complete_force_deletion(job.job_id, "force-worker", claim.claim_token)
                    .await
            })
        };
        wait_until_locked(|| repository.claim_transition_gate.try_lock().is_err()).await;
        clock.set(NOW + time::Duration::minutes(2));
        drop(records_guard);

        assert!(
            !completing.await.unwrap().unwrap(),
            "force completion must revalidate the exact live claim at post-fence authoritative time"
        );
        assert!(
            !repository
                .has_force_receipt(&claim_key.0, &claim_key.1)
                .await
                .unwrap(),
            "expired force worker must not create a completion receipt"
        );
    }

    #[tokio::test]
    async fn force_completion_does_not_deadlock_with_other_lock_admission() {
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(Arc::new(
            FixedClock,
        )));
        let repository = Arc::new(
            InMemoryContentLockDeletionRepository::with_verification_task_fence(Arc::clone(&fence)),
        );
        let force_job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(1), NOW).unwrap();
        repository.insert_job(force_job.clone()).await.unwrap();
        assert!(matches!(
            repository
                .prepare_force_deletion(&force_job.creator, &force_job.lock_id)
                .await
                .unwrap(),
            PrepareForceDeletionResult::Active(_)
        ));
        let claim = repository
            .claim_next("force-worker", time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        let other_job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(2), NOW).unwrap();

        let records_guard = fence.records.write().await;
        let receipts_guard = repository.force_receipts.write().await;
        let completing = {
            let repository = Arc::clone(&repository);
            tokio::spawn(async move {
                repository
                    .complete_force_deletion(force_job.job_id, "force-worker", claim.claim_token)
                    .await
            })
        };
        wait_until_locked(|| repository.claim_transition_gate.try_lock().is_err()).await;
        drop(records_guard);
        wait_until_locked(|| fence.records.try_read().is_err()).await;

        let mut inserting = Box::pin(repository.insert_job(other_job));
        poll_fn(|context| match inserting.as_mut().poll(context) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(result) => {
                panic!(
                    "intake unexpectedly completed before force receipt lock release: {result:?}"
                )
            }
        })
        .await;
        drop(receipts_guard);

        let (completed, inserted) = tokio::time::timeout(StdDuration::from_secs(1), async {
            (completing.await.unwrap(), inserting.await)
        })
        .await
        .expect("force completion and admission lock cycle must not deadlock");
        assert!(completed.unwrap());
        inserted.unwrap();
        assert!(
            repository
                .has_force_receipt(&claim.job.creator, &claim.job.lock_id)
                .await
                .unwrap()
        );
        assert!(
            repository
                .get_job(&claim.job.creator, &claim.job.lock_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn successful_finish_does_not_deadlock_with_graph_claim_release() {
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(Arc::new(
            FixedClock,
        )));
        let repository = Arc::new(
            InMemoryContentLockDeletionRepository::with_verification_task_fence(Arc::clone(&fence)),
        );
        let finish_claim = ready_to_finish(&repository, content_lock(3)).await;
        let graph_repository = Arc::new(InMemoryGraphTaskRepository::with_deletion_fence(
            Arc::clone(&fence),
        ));
        let graph_task = graph_task(&content_lock(4));
        graph_repository
            .insert_graph_task(graph_task.clone())
            .await
            .unwrap();
        let graph_claim = graph_repository
            .claim_next_graph_task("graph-worker", NOW, NOW + time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();

        let graph_guard = fence.graph_records.write().await;
        let finishing = {
            let repository = Arc::clone(&repository);
            tokio::spawn(async move {
                repository
                    .finish(
                        finish_claim.job.job_id,
                        "finish-worker",
                        finish_claim.claim_token,
                        None,
                    )
                    .await
            })
        };
        wait_until_locked(|| fence.records.try_read().is_err()).await;
        let releasing = {
            let graph_repository = Arc::clone(&graph_repository);
            let task_id = graph_task.task_id;
            tokio::spawn(async move {
                graph_repository
                    .release_claimed_graph_task(&task_id, "graph-worker", &graph_claim.claim_token)
                    .await
            })
        };
        wait_until_locked(|| fence.current_statuses.try_read().is_err()).await;
        drop(graph_guard);

        let (finished, released) = tokio::time::timeout(StdDuration::from_secs(1), async {
            (finishing.await.unwrap(), releasing.await.unwrap())
        })
        .await
        .expect("finish and graph release lock cycle must not deadlock");
        assert!(finished.unwrap().is_some());
        assert!(released.unwrap());
        let reclaimed = graph_repository
            .claim_next_graph_task("next-graph-worker", NOW, NOW + time::Duration::minutes(1))
            .await
            .unwrap()
            .expect("released graph task remains claimable");
        assert_eq!(reclaimed.task.task_id, graph_task.task_id);
        assert_ne!(reclaimed.claim_token, graph_claim.claim_token);
    }

    #[tokio::test]
    async fn successful_finish_does_not_deadlock_with_graph_admission() {
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(Arc::new(
            FixedClock,
        )));
        let repository = Arc::new(
            InMemoryContentLockDeletionRepository::with_verification_task_fence(Arc::clone(&fence)),
        );
        let finish_claim = ready_to_finish(&repository, content_lock(5)).await;
        let graph_repository = Arc::new(InMemoryGraphTaskRepository::with_deletion_fence(
            Arc::clone(&fence),
        ));
        let graph_task = graph_task(&content_lock(6));

        let records_guard = fence.records.write().await;
        let finishing = {
            let repository = Arc::clone(&repository);
            tokio::spawn(async move {
                repository
                    .finish(
                        finish_claim.job.job_id,
                        "finish-worker",
                        finish_claim.claim_token,
                        None,
                    )
                    .await
            })
        };
        tokio::task::yield_now().await;
        let inserting = {
            let graph_repository = Arc::clone(&graph_repository);
            tokio::spawn(async move { graph_repository.insert_graph_task(graph_task).await })
        };
        wait_until_locked(|| fence.folded_auth_reservations.try_read().is_err()).await;
        drop(records_guard);

        let (finished, inserted) = tokio::time::timeout(StdDuration::from_secs(1), async {
            (finishing.await.unwrap(), inserting.await.unwrap())
        })
        .await
        .expect("finish and graph admission lock cycle must not deadlock");
        assert!(finished.unwrap().is_some());
        inserted.unwrap();
        assert!(
            graph_repository
                .claim_next_graph_task("graph-worker", NOW, NOW + time::Duration::minutes(1),)
                .await
                .unwrap()
                .is_some()
        );
    }

    async fn ready_to_finish(
        repository: &InMemoryContentLockDeletionRepository,
        lock: ContentLock,
    ) -> ClaimedContentLockDeletionJob {
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();
        repository.insert_job(job.clone()).await.unwrap();
        for phase in [
            ContentLockDeletionPhase::StartPaymentDrain,
            ContentLockDeletionPhase::DrainPayments,
            ContentLockDeletionPhase::DrainExistingCredentials,
            ContentLockDeletionPhase::IssueFinalCredentials,
            ContentLockDeletionPhase::DrainFinalReads,
            ContentLockDeletionPhase::DeleteContent,
            ContentLockDeletionPhase::DeleteTombstone,
            ContentLockDeletionPhase::PurgeOperationalState,
        ] {
            let claim = repository
                .claim_next("finish-worker", time::Duration::minutes(1))
                .await
                .unwrap()
                .unwrap();
            repository
                .advance_phase(job.job_id, "finish-worker", claim.claim_token, phase)
                .await
                .unwrap()
                .advanced()
                .expect("test deletion phase advances");
        }
        repository
            .claim_next("finish-worker", time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap()
    }

    async fn wait_until_locked(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(StdDuration::from_secs(1), async {
            while !predicate() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("contender reached decisive lock");
    }

    fn graph_task(lock: &ContentLock) -> GraphTaskRecord {
        let lock_id = lock.lock_id().unwrap();
        GraphTaskRecord {
            task_id: TaskId::from_str(&Uuid::new_v4().to_string()).unwrap(),
            snapshot: GraphTaskSnapshot::V1(
                GraphTaskSnapshotV1::new(
                    lock.creator.clone(),
                    BundleId::new_random(),
                    PubkyLockResource::from_str(&format!(
                        "{}/pub/app.locks/{lock_id}.json",
                        lock.creator
                    ))
                    .unwrap(),
                    *lock.lock_hash().unwrap().as_bytes(),
                    lock.creator.clone(),
                    NOW,
                    60,
                    GraphPolicyV2 {
                        predicates: vec![GraphPredicateV2::Follows {
                            follower: GraphParticipantV2::Reader,
                            followed: GraphParticipantV2::Creator,
                        }],
                        operator: None,
                    },
                )
                .unwrap(),
            ),
            evaluator_revision: 0,
            status: VerificationTaskStatus::Pending,
            entitlement_to_publish: None,
            entitlement_publication_token: None,
            completed_at: None,
        }
    }

    fn content_lock(seed: u8) -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: CreatorPubky::from_str(CREATOR).unwrap(),
            primary_resource: Some(
                GuardedResource::new(
                    format!("/priv/app.locks/content/{seed}.json"),
                    GuardedResourceHash::from_bytes([seed; 32]),
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
            created_at: NOW - time::Duration::hours(i64::from(seed)),
        }
    }
}
