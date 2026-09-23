use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use async_trait::async_trait;
use locks_core::{
    ids::{BundleId, CreatorPubky, LockId, TaskId},
    lock_policy::ContentLock,
};
use rand::{RngCore, rngs::OsRng};
use time::{Duration, OffsetDateTime};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    models::{
        AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord,
        ContentLockDeletionJob, ContentLockDeletionPhase, ContentLockDeletionState,
        DeletionReadAuthorization, EncryptedFinalCredential, FinalAccessWindows,
        FinalCredentialContext, FinalCredentialMaterialization, InitializeFinalAccessWindowsResult,
        IssuedDeletionCredential, VerificationTaskStatus,
    },
    ports::{AccessCredentialStore, FinalCredentialWorkerIssueRequest, VerificationTaskRepository},
};
use crate::infrastructure::{
    final_credentials::FinalCredentialCipher,
    memory::verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence,
};

type JobKey = (CreatorPubky, LockId);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccessPhaseAdvanceStatus {
    Ready,
    ObligationsPending,
    FinalCredentialIssuanceMissed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainCredentialKind {
    Ordinary,
    Final,
}

#[derive(Debug, Clone)]
struct StoredCredential {
    record: AccessCredentialRecord,
    lock_id: LockId,
    deletion: Option<(Uuid, DrainCredentialKind)>,
}

#[derive(Debug, Clone)]
struct DeletionAccessState {
    creator: CreatorPubky,
    lock_id: LockId,
    frozen_content_lock: ContentLock,
    state: ContentLockDeletionState,
    phase: ContentLockDeletionPhase,
    force_requested: bool,
    claimed_by: Option<String>,
    claim_token: Option<Uuid>,
    claim_expires_at: Option<OffsetDateTime>,
    issuance_started_at: Option<OffsetDateTime>,
    issuance_deadline: Option<OffsetDateTime>,
    read_deadline: Option<OffsetDateTime>,
    payment_aggregate: Option<DeletionPaymentAggregate>,
    bundle_snapshots: HashMap<BundleId, DeletionBundleSnapshot>,
}

#[derive(Debug, Clone, Copy)]
struct DeletionPaymentAggregate {
    completed: bool,
    accepted_count: u64,
}

#[derive(Debug, Clone, Copy)]
struct DeletionBundleSnapshot {
    task_id: TaskId,
    paykit_admission_required: bool,
    had_active_credential_at_cutoff: bool,
    status_at_cutoff: VerificationTaskStatus,
    resolved_status: Option<VerificationTaskStatus>,
    resolved_at: Option<OffsetDateTime>,
    final_credential_eligible_at: Option<OffsetDateTime>,
    final_credential_issued: bool,
}

impl DeletionBundleSnapshot {
    fn permits_final_credential(self) -> bool {
        self.paykit_admission_required
            && !self.had_active_credential_at_cutoff
            && self.resolved_status.unwrap_or(self.status_at_cutoff)
                == VerificationTaskStatus::Completed
            && self.final_credential_eligible_at.is_some()
    }
}

#[derive(Debug, Clone)]
struct FinalCredentialRecord {
    lookup_key: AccessCredentialLookupKey,
    encrypted_bearer: EncryptedFinalCredential,
    expires_at: OffsetDateTime,
    reads: HashMap<String, FinalReadState>,
}

#[derive(Debug, Clone, Default)]
struct FinalReadState {
    claim_token: Option<Uuid>,
    claim_expires_at: Option<OffsetDateTime>,
    consumed_at: Option<OffsetDateTime>,
}

#[derive(Debug, Default)]
struct StoreState {
    records: HashMap<AccessCredentialLookupKey, StoredCredential>,
    deletions: HashMap<Uuid, DeletionAccessState>,
    deletion_jobs_by_key: HashMap<JobKey, Uuid>,
    blocked_keys: HashSet<JobKey>,
    final_credentials: HashMap<(Uuid, BundleId), FinalCredentialRecord>,
}

/// In-memory access credential store with deletion-drain parity.
pub struct InMemoryAccessCredentialStore {
    state: RwLock<StoreState>,
    final_credential_cipher: FinalCredentialCipher,
    verification_tasks: Option<Arc<dyn VerificationTaskRepository>>,
    verification_task_deletion_fence: Option<Arc<InMemoryVerificationTaskDeletionFence>>,
}

impl fmt::Debug for InMemoryAccessCredentialStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryAccessCredentialStore")
            .field("state", &self.state)
            .field("final_credential_cipher", &self.final_credential_cipher)
            .field(
                "verification_tasks",
                &self.verification_tasks.as_ref().map(|_| "<repository>"),
            )
            .field(
                "verification_task_deletion_fence",
                &self
                    .verification_task_deletion_fence
                    .as_ref()
                    .map(|_| "<fence>"),
            )
            .finish()
    }
}

impl Default for InMemoryAccessCredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryAccessCredentialStore {
    /// Creates an empty standalone store. Final credentials are unavailable until
    /// verification dependencies are supplied.
    pub fn new() -> Self {
        Self::build(None, None)
    }

    pub fn with_verification_task_repository_and_deletion_fence(
        verification_tasks: Arc<dyn VerificationTaskRepository>,
        verification_task_deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>,
    ) -> Self {
        Self::build(
            Some(verification_tasks),
            Some(verification_task_deletion_fence),
        )
    }

    fn build(
        verification_tasks: Option<Arc<dyn VerificationTaskRepository>>,
        verification_task_deletion_fence: Option<Arc<InMemoryVerificationTaskDeletionFence>>,
    ) -> Self {
        let mut key = [0_u8; 32];
        OsRng.fill_bytes(&mut key);
        Self {
            state: RwLock::new(StoreState::default()),
            final_credential_cipher: FinalCredentialCipher::new(key),
            verification_tasks,
            verification_task_deletion_fence,
        }
    }

    fn authoritative_winner_time(&self) -> OffsetDateTime {
        self.verification_task_deletion_fence
            .as_ref()
            .map_or_else(OffsetDateTime::now_utc, |fence| {
                fence.authoritative_cutoff()
            })
    }

    pub(crate) async fn register_deletion(
        &self,
        job: &ContentLockDeletionJob,
        snapshot_bundles: &HashMap<BundleId, (TaskId, bool, VerificationTaskStatus)>,
    ) -> Result<(), ApplicationError> {
        let key = (job.creator.clone(), job.lock_id.clone());
        let mut state = self.state.write().await;
        if state.blocked_keys.contains(&key) || state.deletion_jobs_by_key.contains_key(&key) {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }

        let bundle_snapshots = snapshot_bundles
            .iter()
            .map(
                |(bundle_id, (task_id, paykit_admission_required, status_at_cutoff))| {
                    let had_active_credential_at_cutoff = state.records.values().any(|stored| {
                        stored.record.creator == job.creator
                            && stored.lock_id == job.lock_id
                            && stored.record.bundle_id == *bundle_id
                            && stored.record.expires_at > job.deletion_started_at
                    });
                    (
                        bundle_id.clone(),
                        DeletionBundleSnapshot {
                            task_id: *task_id,
                            paykit_admission_required: *paykit_admission_required,
                            had_active_credential_at_cutoff,
                            status_at_cutoff: *status_at_cutoff,
                            resolved_status: matches!(
                                status_at_cutoff,
                                VerificationTaskStatus::Completed
                                    | VerificationTaskStatus::Failed
                                    | VerificationTaskStatus::Expired
                            )
                            .then_some(*status_at_cutoff),
                            resolved_at: matches!(
                                status_at_cutoff,
                                VerificationTaskStatus::Completed
                                    | VerificationTaskStatus::Failed
                                    | VerificationTaskStatus::Expired
                            )
                            .then_some(job.deletion_started_at),
                            final_credential_eligible_at: (*paykit_admission_required
                                && *status_at_cutoff == VerificationTaskStatus::Completed
                                && !had_active_credential_at_cutoff)
                                .then_some(job.deletion_started_at),
                            final_credential_issued: false,
                        },
                    )
                },
            )
            .collect();
        state.deletions.insert(
            job.job_id,
            DeletionAccessState {
                creator: job.creator.clone(),
                lock_id: job.lock_id.clone(),
                frozen_content_lock: job.frozen_content_lock.clone(),
                state: job.state,
                phase: job.phase,
                force_requested: job.force_requested_at.is_some(),
                claimed_by: None,
                claim_token: None,
                claim_expires_at: None,
                issuance_started_at: None,
                issuance_deadline: None,
                read_deadline: None,
                payment_aggregate: None,
                bundle_snapshots,
            },
        );
        state.deletion_jobs_by_key.insert(key.clone(), job.job_id);
        state.blocked_keys.insert(key);

        for stored in state.records.values_mut() {
            if stored.record.creator == job.creator
                && stored.lock_id == job.lock_id
                && snapshot_bundles.contains_key(&stored.record.bundle_id)
                && stored.record.expires_at > job.deletion_started_at
            {
                stored.deletion = Some((job.job_id, DrainCredentialKind::Ordinary));
            }
        }
        Ok(())
    }

    pub(crate) async fn synchronize_job(
        &self,
        job: &ContentLockDeletionJob,
        claimed_by: Option<&str>,
        claim_token: Option<Uuid>,
        claim_expires_at: Option<OffsetDateTime>,
    ) {
        let mut state = self.state.write().await;
        if let Some(deletion) = state.deletions.get_mut(&job.job_id) {
            deletion.state = job.state;
            deletion.phase = job.phase;
            deletion.force_requested = job.force_requested_at.is_some();
            deletion.claimed_by = claimed_by.map(str::to_owned);
            deletion.claim_token = claim_token;
            deletion.claim_expires_at = claim_expires_at;
        }
        if job.force_requested_at.is_some()
            || matches!(
                job.state,
                ContentLockDeletionState::Completed | ContentLockDeletionState::Failed
            )
        {
            disable_job_access(&mut state, job.job_id);
        } else if job.phase == ContentLockDeletionPhase::DeleteContent {
            disable_final_access(&mut state, job.job_id);
        }
    }

    pub(crate) async fn block_key_and_disable_job(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        job_id: Option<Uuid>,
    ) {
        let mut state = self.state.write().await;
        state
            .blocked_keys
            .insert((creator.clone(), lock_id.clone()));
        if let Some(job_id) = job_id {
            disable_job_access(&mut state, job_id);
        }
    }

    /// Records a terminal payment result only while deletion owns a live drain claim.
    pub async fn resolve_deletion_payment(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        task_id: &TaskId,
        status: VerificationTaskStatus,
    ) -> Result<bool, ApplicationError> {
        if !matches!(
            status,
            VerificationTaskStatus::Completed | VerificationTaskStatus::Expired
        ) {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "payment drain transition must be completed or expired".to_owned(),
            });
        }
        let mut state = self.state.write().await;
        let Some(deletion) = state.deletions.get_mut(&deletion_job_id) else {
            return Ok(false);
        };
        let owns_live_claim = deletion.state == ContentLockDeletionState::Running
            && deletion.phase == ContentLockDeletionPhase::DrainPayments
            && !deletion.force_requested
            && deletion.claimed_by.as_deref() == Some(worker_id)
            && deletion.claim_token == Some(claim_token)
            && deletion
                .claim_expires_at
                .is_some_and(|claim_expires_at| claim_expires_at > now);
        if !owns_live_claim {
            return Ok(false);
        }
        let Some(snapshot) = deletion
            .bundle_snapshots
            .values_mut()
            .find(|snapshot| snapshot.task_id == *task_id)
        else {
            return Ok(false);
        };
        if !snapshot.paykit_admission_required || snapshot.resolved_status.is_some() {
            return Ok(false);
        }
        snapshot.resolved_status = Some(status);
        snapshot.resolved_at = Some(now);
        snapshot.final_credential_eligible_at = (status == VerificationTaskStatus::Completed
            && !snapshot.had_active_credential_at_cutoff)
            .then_some(now);
        Ok(true)
    }

    /// Marks the deletion-owned payment aggregate terminal only under the exact live drain claim.
    pub async fn complete_deletion_payment_aggregate(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        let mut state = self.state.write().await;
        let Some(deletion) = state.deletions.get_mut(&deletion_job_id) else {
            return Ok(false);
        };
        if !owns_live_payment_drain_claim(deletion, worker_id, claim_token, now) {
            return Ok(false);
        }
        deletion.payment_aggregate = Some(DeletionPaymentAggregate {
            completed: true,
            accepted_count: 0,
        });
        Ok(true)
    }

    pub(crate) async fn expire_unresolved_non_paykit_tasks(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        let task_ids = {
            let state = self.state.read().await;
            let Some(deletion) = state.deletions.get(&deletion_job_id) else {
                return Ok(false);
            };
            if !owns_live_no_paykit_claim(deletion, worker_id, claim_token, now) {
                return Ok(false);
            }
            if deletion
                .bundle_snapshots
                .values()
                .any(|snapshot| snapshot.paykit_admission_required)
            {
                return Err(invalid_deletion_state(
                    "non-Paykit deletion drain cannot process a Paykit snapshot",
                ));
            }
            deletion
                .bundle_snapshots
                .values()
                .filter(|snapshot| snapshot.resolved_status.is_none())
                .map(|snapshot| snapshot.task_id)
                .collect::<Vec<_>>()
        };

        if let Some(tasks) = &self.verification_tasks {
            let mut updates = Vec::with_capacity(task_ids.len());
            for task_id in &task_ids {
                let Some(task) = tasks.get_verification_task(task_id).await? else {
                    return Err(invalid_deletion_state(
                        "frozen non-Paykit verification task is missing",
                    ));
                };
                if matches!(
                    task.status,
                    VerificationTaskStatus::Pending | VerificationTaskStatus::InProgress
                ) {
                    updates.push(task.transition_to(VerificationTaskStatus::Expired, now, None)?);
                }
            }
            tasks.update_verification_tasks_atomically(updates).await?;
        }

        let mut state = self.state.write().await;
        let Some(deletion) = state.deletions.get_mut(&deletion_job_id) else {
            return Ok(false);
        };
        if !owns_live_no_paykit_claim(deletion, worker_id, claim_token, now) {
            return Ok(false);
        }
        for snapshot in deletion.bundle_snapshots.values_mut() {
            if snapshot.resolved_status.is_none() {
                snapshot.resolved_status = Some(VerificationTaskStatus::Expired);
                snapshot.resolved_at = Some(now);
            }
        }
        Ok(true)
    }

    pub(crate) async fn check_phase_advance(
        &self,
        deletion_job_id: Uuid,
        current_phase: ContentLockDeletionPhase,
        next_phase: ContentLockDeletionPhase,
        now: OffsetDateTime,
    ) -> Result<AccessPhaseAdvanceStatus, ApplicationError> {
        let state = self.state.read().await;
        let Some(deletion) = state.deletions.get(&deletion_job_id) else {
            return Ok(AccessPhaseAdvanceStatus::Ready);
        };
        if current_phase == ContentLockDeletionPhase::DrainPayments
            && next_phase == ContentLockDeletionPhase::DrainExistingCredentials
        {
            if deletion
                .bundle_snapshots
                .values()
                .any(|snapshot| snapshot.resolved_status.is_none())
            {
                return Err(invalid_deletion_state(
                    "every frozen deletion obligation must be terminal before credential draining",
                ));
            }
            if !payment_aggregate_completed(deletion) {
                return Err(invalid_deletion_state(
                    "payment drain aggregate must be durably completed before credential draining",
                ));
            }
        }
        if current_phase == ContentLockDeletionPhase::DrainExistingCredentials
            && next_phase == ContentLockDeletionPhase::IssueFinalCredentials
            && state.records.values().any(|stored| {
                stored.deletion == Some((deletion_job_id, DrainCredentialKind::Ordinary))
                    && stored.record.expires_at > now
            })
        {
            return Ok(AccessPhaseAdvanceStatus::ObligationsPending);
        }
        if current_phase == ContentLockDeletionPhase::IssueFinalCredentials
            && next_phase == ContentLockDeletionPhase::DrainFinalReads
            && deletion.bundle_snapshots.values().any(|snapshot| {
                snapshot.permits_final_credential() && !snapshot.final_credential_issued
            })
        {
            return Ok(
                if deletion
                    .issuance_deadline
                    .is_some_and(|deadline| now >= deadline)
                {
                    AccessPhaseAdvanceStatus::FinalCredentialIssuanceMissed
                } else {
                    AccessPhaseAdvanceStatus::ObligationsPending
                },
            );
        }
        if current_phase == ContentLockDeletionPhase::DrainFinalReads
            && next_phase == ContentLockDeletionPhase::DeleteContent
            && has_live_access_obligation(&state, deletion_job_id, now)
        {
            return Ok(AccessPhaseAdvanceStatus::ObligationsPending);
        }
        Ok(AccessPhaseAdvanceStatus::Ready)
    }

    pub(crate) async fn check_successful_finish(
        &self,
        deletion_job_id: Uuid,
        phase: ContentLockDeletionPhase,
        now: OffsetDateTime,
    ) -> Result<(), ApplicationError> {
        let state = self.state.read().await;
        let Some(deletion) = state.deletions.get(&deletion_job_id) else {
            return Ok(());
        };
        if phase != ContentLockDeletionPhase::PurgeOperationalState {
            return Err(invalid_deletion_state(
                "successful completion requires the final operational-cleanup phase",
            ));
        }
        if deletion
            .bundle_snapshots
            .values()
            .any(|snapshot| snapshot.resolved_status.is_none())
        {
            return Err(invalid_deletion_state(
                "every frozen deletion obligation must be terminal before credential draining",
            ));
        }
        if !payment_aggregate_completed(deletion) {
            return Err(invalid_deletion_state(
                "payment drain aggregate must be durably completed before credential draining",
            ));
        }
        if has_live_access_obligation(&state, deletion_job_id, now)
            || deletion.bundle_snapshots.values().any(|snapshot| {
                snapshot.permits_final_credential() && !snapshot.final_credential_issued
            })
        {
            return Err(invalid_deletion_state(
                "successful completion cannot bypass deletion access obligations",
            ));
        }
        Ok(())
    }
}

fn owns_live_payment_drain_claim(
    deletion: &DeletionAccessState,
    worker_id: &str,
    claim_token: Uuid,
    now: OffsetDateTime,
) -> bool {
    deletion.state == ContentLockDeletionState::Running
        && deletion.phase == ContentLockDeletionPhase::DrainPayments
        && !deletion.force_requested
        && deletion.claimed_by.as_deref() == Some(worker_id)
        && deletion.claim_token == Some(claim_token)
        && deletion
            .claim_expires_at
            .is_some_and(|claim_expires_at| claim_expires_at > now)
}

fn owns_live_no_paykit_claim(
    deletion: &DeletionAccessState,
    worker_id: &str,
    claim_token: Uuid,
    now: OffsetDateTime,
) -> bool {
    deletion.state == ContentLockDeletionState::Running
        && matches!(
            deletion.phase,
            ContentLockDeletionPhase::StartPaymentDrain | ContentLockDeletionPhase::DrainPayments
        )
        && !deletion.force_requested
        && deletion.claimed_by.as_deref() == Some(worker_id)
        && deletion.claim_token == Some(claim_token)
        && deletion
            .claim_expires_at
            .is_some_and(|claim_expires_at| claim_expires_at > now)
}

fn payment_aggregate_completed(deletion: &DeletionAccessState) -> bool {
    deletion.payment_aggregate.map_or_else(
        || {
            !deletion
                .bundle_snapshots
                .values()
                .any(|snapshot| snapshot.paykit_admission_required)
        },
        |aggregate| aggregate.completed && aggregate.accepted_count == 0,
    )
}

#[async_trait]
impl AccessCredentialStore for InMemoryAccessCredentialStore {
    async fn insert_access_credential(
        &self,
        lock_id: &LockId,
        lookup_key: AccessCredentialLookupKey,
        record: AccessCredentialRecord,
    ) -> Result<(), ApplicationError> {
        let _admission = if let Some(fence) = &self.verification_task_deletion_fence {
            Some(fence.acquire_lock_admission(&record.creator, lock_id).await)
        } else {
            None
        };
        let mut state = self.state.write().await;
        let key = (record.creator.clone(), lock_id.clone());
        if state.blocked_keys.contains(&key) || state.deletion_jobs_by_key.contains_key(&key) {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        if state.records.contains_key(&lookup_key) {
            return Err(ApplicationError::DuplicateRecord {
                record: "access_credential",
            });
        }
        state.records.insert(
            lookup_key,
            StoredCredential {
                record,
                lock_id: lock_id.clone(),
                deletion: None,
            },
        );
        Ok(())
    }

    async fn get_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<Option<AccessCredentialRecord>, ApplicationError> {
        Ok(self
            .state
            .read()
            .await
            .records
            .get(lookup_key)
            .map(|stored| stored.record.clone()))
    }

    async fn delete_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<(), ApplicationError> {
        let mut state = self.state.write().await;
        state.records.remove(lookup_key);
        state
            .final_credentials
            .retain(|_, credential| &credential.lookup_key != lookup_key);
        Ok(())
    }

    async fn initialize_final_access_windows(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        issuance_window: Duration,
        read_window: Duration,
    ) -> Result<InitializeFinalAccessWindowsResult, ApplicationError> {
        if issuance_window <= Duration::ZERO || read_window <= Duration::ZERO {
            return Err(ApplicationError::Storage {
                message: "final access window durations must be positive".to_owned(),
            });
        }
        let mut state = self.state.write().await;
        let now = self.authoritative_winner_time();
        let Some(deletion) = state.deletions.get_mut(&deletion_job_id) else {
            return Ok(InitializeFinalAccessWindowsResult::ClaimLost);
        };
        let owns_live_claim = deletion.state == ContentLockDeletionState::Running
            && deletion.phase == ContentLockDeletionPhase::IssueFinalCredentials
            && !deletion.force_requested
            && deletion.claimed_by.as_deref() == Some(worker_id)
            && deletion.claim_token == Some(claim_token)
            && deletion
                .claim_expires_at
                .is_some_and(|claim_expires_at| claim_expires_at > now);
        if !owns_live_claim {
            return Ok(InitializeFinalAccessWindowsResult::ClaimLost);
        }
        match (
            deletion.issuance_started_at,
            deletion.issuance_deadline,
            deletion.read_deadline,
        ) {
            (
                Some(issuance_started_at),
                Some(credential_issuance_deadline),
                Some(read_deadline),
            ) => {
                return Ok(InitializeFinalAccessWindowsResult::Initialized(
                    FinalAccessWindows {
                        issuance_started_at,
                        credential_issuance_deadline,
                        read_deadline,
                    },
                ));
            }
            (None, None, None) => {}
            _ => {
                return Err(ApplicationError::Storage {
                    message: "incomplete final access windows in memory".to_owned(),
                });
            }
        }
        let credential_issuance_deadline =
            now.checked_add(issuance_window)
                .ok_or_else(|| ApplicationError::Storage {
                    message: "final credential issuance deadline overflow".to_owned(),
                })?;
        let read_deadline = credential_issuance_deadline
            .checked_add(read_window)
            .ok_or_else(|| ApplicationError::Storage {
                message: "final read deadline overflow".to_owned(),
            })?;
        deletion.issuance_started_at = Some(now);
        deletion.issuance_deadline = Some(credential_issuance_deadline);
        deletion.read_deadline = Some(read_deadline);
        Ok(InitializeFinalAccessWindowsResult::Initialized(
            FinalAccessWindows {
                issuance_started_at: now,
                credential_issuance_deadline,
                read_deadline,
            },
        ))
    }

    async fn final_credentials_to_materialize(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        limit: usize,
    ) -> Result<Vec<FinalCredentialMaterialization>, ApplicationError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let state = self.state.read().await;
        let now = self.authoritative_winner_time();
        let Some(deletion) = state.deletions.get(&deletion_job_id) else {
            return Ok(Vec::new());
        };
        let owns_live_issue_claim = deletion.state == ContentLockDeletionState::Running
            && deletion.phase == ContentLockDeletionPhase::IssueFinalCredentials
            && !deletion.force_requested
            && deletion.claimed_by.as_deref() == Some(worker_id)
            && deletion.claim_token == Some(claim_token)
            && deletion
                .claim_expires_at
                .is_some_and(|claim_expires_at| claim_expires_at > now)
            && deletion
                .issuance_deadline
                .is_some_and(|issuance_deadline| issuance_deadline > now);
        if !owns_live_issue_claim {
            return Ok(Vec::new());
        }
        let mut pending: Vec<_> = deletion
            .bundle_snapshots
            .iter()
            .filter(|(_, snapshot)| {
                snapshot.permits_final_credential() && !snapshot.final_credential_issued
            })
            .map(|(bundle_id, _)| FinalCredentialMaterialization {
                creator: deletion.creator.clone(),
                bundle_id: bundle_id.clone(),
            })
            .collect();
        pending.sort_by(|left, right| left.bundle_id.as_str().cmp(right.bundle_id.as_str()));
        pending.truncate(limit);
        Ok(pending)
    }

    async fn issue_or_replay_final_credential(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
        _caller_now: OffsetDateTime,
        candidate: AccessCredential,
    ) -> Result<Option<IssuedDeletionCredential>, ApplicationError> {
        let mut state = self.state.write().await;
        let now = self.authoritative_winner_time();
        let Some((deletion_job_id, _)) = state.deletions.iter().find(|(_, deletion)| {
            deletion.creator == *creator && deletion.bundle_snapshots.contains_key(bundle_id)
        }) else {
            return Ok(None);
        };
        let deletion_job_id = *deletion_job_id;
        let Some(deletion) = state.deletions.get(&deletion_job_id) else {
            return Ok(None);
        };
        if deletion.creator != *creator
            || deletion.force_requested
            || !matches!(
                deletion.state,
                ContentLockDeletionState::Queued | ContentLockDeletionState::Running
            )
            || !matches!(
                deletion.phase,
                ContentLockDeletionPhase::IssueFinalCredentials
                    | ContentLockDeletionPhase::DrainFinalReads
            )
            || deletion
                .read_deadline
                .is_none_or(|deadline| deadline <= now)
        {
            return Ok(None);
        }

        let context = FinalCredentialContext {
            deletion_job_id,
            creator: creator.clone(),
            bundle_id: bundle_id.clone(),
        };
        if let Some(existing) = state
            .final_credentials
            .get(&(deletion_job_id, bundle_id.clone()))
        {
            let credential = self
                .final_credential_cipher
                .decrypt(&context, &existing.encrypted_bearer)?;
            return Ok(Some(IssuedDeletionCredential {
                credential,
                expires_at: existing.expires_at,
            }));
        }

        if deletion.phase != ContentLockDeletionPhase::IssueFinalCredentials
            || deletion
                .issuance_deadline
                .is_none_or(|deadline| now >= deadline)
            || !deletion
                .bundle_snapshots
                .get(bundle_id)
                .copied()
                .is_some_and(DeletionBundleSnapshot::permits_final_credential)
        {
            return Ok(None);
        }

        let expires_at = deletion
            .read_deadline
            .expect("final issuance requires an initialized read deadline");
        let frozen_content_lock = deletion.frozen_content_lock.clone();
        let lock_id = deletion.lock_id.clone();
        let encrypted_bearer = self.final_credential_cipher.encrypt(&context, &candidate)?;
        let lookup_key = AccessCredentialLookupKey::derive(&candidate);
        if state.records.contains_key(&lookup_key) {
            return Err(ApplicationError::DuplicateRecord {
                record: "access_credential",
            });
        }
        let mut reads = HashMap::new();
        if let Some(resource) = frozen_content_lock.primary_resource {
            reads.insert(resource.path, FinalReadState::default());
        }
        for path in frozen_content_lock.secondary_resources.keys() {
            reads.insert(path.clone(), FinalReadState::default());
        }
        state.records.insert(
            lookup_key.clone(),
            StoredCredential {
                record: AccessCredentialRecord {
                    creator: creator.clone(),
                    bundle_id: bundle_id.clone(),
                    expires_at,
                },
                lock_id,
                deletion: Some((deletion_job_id, DrainCredentialKind::Final)),
            },
        );
        state.final_credentials.insert(
            (deletion_job_id, bundle_id.clone()),
            FinalCredentialRecord {
                lookup_key,
                encrypted_bearer,
                expires_at,
                reads,
            },
        );
        state
            .deletions
            .get_mut(&deletion_job_id)
            .and_then(|deletion| deletion.bundle_snapshots.get_mut(bundle_id))
            .expect("issued final credential must retain its immutable snapshot")
            .final_credential_issued = true;
        Ok(Some(IssuedDeletionCredential {
            credential: candidate,
            expires_at,
        }))
    }

    async fn issue_or_replay_final_credential_for_worker(
        &self,
        request: FinalCredentialWorkerIssueRequest<'_>,
    ) -> Result<Option<IssuedDeletionCredential>, ApplicationError> {
        let FinalCredentialWorkerIssueRequest {
            deletion_job_id,
            worker_id,
            claim_token,
            creator,
            bundle_id,
            now: _caller_now,
            candidate,
        } = request;
        let mut state = self.state.write().await;
        let now = self.authoritative_winner_time();
        let Some(deletion) = state.deletions.get(&deletion_job_id) else {
            return Ok(None);
        };
        let owns_live_issue_claim = deletion.creator == *creator
            && deletion.state == ContentLockDeletionState::Running
            && deletion.phase == ContentLockDeletionPhase::IssueFinalCredentials
            && !deletion.force_requested
            && deletion.claimed_by.as_deref() == Some(worker_id)
            && deletion.claim_token == Some(claim_token)
            && deletion
                .claim_expires_at
                .is_some_and(|claim_expires_at| claim_expires_at > now)
            && deletion
                .issuance_deadline
                .is_some_and(|issuance_deadline| issuance_deadline > now)
            && deletion
                .read_deadline
                .is_some_and(|read_deadline| read_deadline > now)
            && deletion
                .bundle_snapshots
                .get(bundle_id)
                .copied()
                .is_some_and(DeletionBundleSnapshot::permits_final_credential);
        if !owns_live_issue_claim {
            return Ok(None);
        }

        let context = FinalCredentialContext {
            deletion_job_id,
            creator: creator.clone(),
            bundle_id: bundle_id.clone(),
        };
        if let Some(existing) = state
            .final_credentials
            .get(&(deletion_job_id, bundle_id.clone()))
        {
            let credential = self
                .final_credential_cipher
                .decrypt(&context, &existing.encrypted_bearer)?;
            return Ok(Some(IssuedDeletionCredential {
                credential,
                expires_at: existing.expires_at,
            }));
        }

        let deletion = state
            .deletions
            .get(&deletion_job_id)
            .expect("worker-fenced deletion was validated under the write lock");
        let expires_at = deletion
            .read_deadline
            .expect("worker-fenced final issuance requires an initialized read deadline");
        let frozen_content_lock = deletion.frozen_content_lock.clone();
        let lock_id = deletion.lock_id.clone();
        let encrypted_bearer = self.final_credential_cipher.encrypt(&context, &candidate)?;
        let lookup_key = AccessCredentialLookupKey::derive(&candidate);
        if state.records.contains_key(&lookup_key) {
            return Err(ApplicationError::DuplicateRecord {
                record: "access_credential",
            });
        }
        let mut reads = HashMap::new();
        if let Some(resource) = frozen_content_lock.primary_resource {
            reads.insert(resource.path, FinalReadState::default());
        }
        for path in frozen_content_lock.secondary_resources.keys() {
            reads.insert(path.clone(), FinalReadState::default());
        }
        state.records.insert(
            lookup_key.clone(),
            StoredCredential {
                record: AccessCredentialRecord {
                    creator: creator.clone(),
                    bundle_id: bundle_id.clone(),
                    expires_at,
                },
                lock_id,
                deletion: Some((deletion_job_id, DrainCredentialKind::Final)),
            },
        );
        state.final_credentials.insert(
            (deletion_job_id, bundle_id.clone()),
            FinalCredentialRecord {
                lookup_key,
                encrypted_bearer,
                expires_at,
                reads,
            },
        );
        state
            .deletions
            .get_mut(&deletion_job_id)
            .and_then(|deletion| deletion.bundle_snapshots.get_mut(bundle_id))
            .expect("issued final credential must retain its immutable snapshot")
            .final_credential_issued = true;
        Ok(Some(IssuedDeletionCredential {
            credential: candidate,
            expires_at,
        }))
    }

    async fn final_credential_available(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
        now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        let state = self.state.read().await;
        let Some((deletion_job_id, deletion)) = state.deletions.iter().find(|(_, deletion)| {
            deletion.creator == *creator && deletion.bundle_snapshots.contains_key(bundle_id)
        }) else {
            return Ok(false);
        };
        let lifecycle_allows_access = !deletion.force_requested
            && matches!(
                deletion.state,
                ContentLockDeletionState::Queued | ContentLockDeletionState::Running
            )
            && matches!(
                deletion.phase,
                ContentLockDeletionPhase::IssueFinalCredentials
                    | ContentLockDeletionPhase::DrainFinalReads
            )
            && deletion
                .read_deadline
                .is_some_and(|deadline| deadline > now);
        if !lifecycle_allows_access {
            return Ok(false);
        }
        if state
            .final_credentials
            .contains_key(&(*deletion_job_id, bundle_id.clone()))
        {
            return Ok(true);
        }
        Ok(
            deletion.phase == ContentLockDeletionPhase::IssueFinalCredentials
                && deletion
                    .issuance_deadline
                    .is_some_and(|deadline| now < deadline)
                && deletion
                    .bundle_snapshots
                    .get(bundle_id)
                    .copied()
                    .is_some_and(DeletionBundleSnapshot::permits_final_credential),
        )
    }

    async fn prepare_deletion_read(
        &self,
        lookup_key: &AccessCredentialLookupKey,
        path: &str,
        claim_duration: Duration,
    ) -> Result<Option<DeletionReadAuthorization>, ApplicationError> {
        let mut state = self.state.write().await;
        let now = self.authoritative_winner_time();
        let Some(stored) = state.records.get(lookup_key).cloned() else {
            return Ok(None);
        };
        let Some((deletion_job_id, kind)) = stored.deletion else {
            return Ok(None);
        };
        let Some(deletion) = state.deletions.get(&deletion_job_id) else {
            return Ok(None);
        };
        if stored.record.expires_at <= now
            || deletion.force_requested
            || !matches!(
                deletion.state,
                ContentLockDeletionState::Queued | ContentLockDeletionState::Running
            )
            || !matches!(
                deletion.phase,
                ContentLockDeletionPhase::Withdraw
                    | ContentLockDeletionPhase::StartPaymentDrain
                    | ContentLockDeletionPhase::DrainPayments
                    | ContentLockDeletionPhase::DrainExistingCredentials
                    | ContentLockDeletionPhase::IssueFinalCredentials
                    | ContentLockDeletionPhase::DrainFinalReads
            )
        {
            return Ok(None);
        }
        let Some(resource) = deletion.frozen_content_lock.resource_for_path(path) else {
            return Ok(None);
        };
        let creator = deletion.creator.clone();
        if kind == DrainCredentialKind::Ordinary {
            return Ok(Some(DeletionReadAuthorization {
                claim_token: None,
                creator,
                resource,
            }));
        }
        if !matches!(
            deletion.phase,
            ContentLockDeletionPhase::IssueFinalCredentials
                | ContentLockDeletionPhase::DrainFinalReads
        ) || deletion
            .read_deadline
            .is_none_or(|deadline| deadline <= now)
        {
            return Ok(None);
        }
        let read_deadline = deletion
            .read_deadline
            .expect("final credential requires a read deadline");
        let Some(final_credential) = state
            .final_credentials
            .get_mut(&(deletion_job_id, stored.record.bundle_id.clone()))
        else {
            return Ok(None);
        };
        let Some(read) = final_credential.reads.get_mut(path) else {
            return Ok(None);
        };
        if read.consumed_at.is_some()
            || (read.claim_token.is_some()
                && read
                    .claim_expires_at
                    .is_some_and(|claim_expires_at| claim_expires_at > now))
        {
            return Ok(None);
        }
        let bounded_expiry = now
            .checked_add(claim_duration)
            .ok_or_else(|| ApplicationError::Storage {
                message: "final read claim expiry overflow".to_owned(),
            })?
            .min(now + Duration::seconds(30))
            .min(final_credential.expires_at)
            .min(read_deadline);
        if bounded_expiry <= now {
            return Ok(None);
        }
        let claim_token = Uuid::new_v4();
        read.claim_token = Some(claim_token);
        read.claim_expires_at = Some(bounded_expiry);
        Ok(Some(DeletionReadAuthorization {
            claim_token: Some(claim_token),
            creator,
            resource,
        }))
    }

    async fn deletion_credential_enrolled(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<bool, ApplicationError> {
        let state = self.state.read().await;
        Ok(state
            .records
            .get(lookup_key)
            .is_some_and(|stored| stored.deletion.is_some()))
    }

    async fn release_deletion_read(
        &self,
        lookup_key: &AccessCredentialLookupKey,
        path: &str,
        claim_token: Uuid,
        _now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        let mut state = self.state.write().await;
        let Some((deletion_job_id, bundle_id)) = final_credential_identity(&state, lookup_key)
        else {
            return Ok(false);
        };
        let Some(read) = state
            .final_credentials
            .get_mut(&(deletion_job_id, bundle_id))
            .and_then(|credential| credential.reads.get_mut(path))
        else {
            return Ok(false);
        };
        if read.consumed_at.is_some() || read.claim_token != Some(claim_token) {
            return Ok(false);
        }
        read.claim_token = None;
        read.claim_expires_at = None;
        Ok(true)
    }

    async fn consume_deletion_read(
        &self,
        lookup_key: &AccessCredentialLookupKey,
        path: &str,
        claim_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        let mut state = self.state.write().await;
        let now = self.authoritative_winner_time();
        let Some((deletion_job_id, bundle_id)) = final_credential_identity(&state, lookup_key)
        else {
            return Ok(false);
        };
        let Some(read) = state
            .final_credentials
            .get_mut(&(deletion_job_id, bundle_id))
            .and_then(|credential| credential.reads.get_mut(path))
        else {
            return Ok(false);
        };
        if read.consumed_at.is_some()
            || read.claim_token != Some(claim_token)
            || read
                .claim_expires_at
                .is_none_or(|claim_expires_at| claim_expires_at <= now)
        {
            return Ok(false);
        }
        read.claim_token = None;
        read.claim_expires_at = None;
        read.consumed_at = Some(now);
        Ok(true)
    }
}

fn has_live_access_obligation(
    state: &StoreState,
    deletion_job_id: Uuid,
    now: OffsetDateTime,
) -> bool {
    state.records.values().any(|stored| {
        stored.deletion == Some((deletion_job_id, DrainCredentialKind::Ordinary))
            && stored.record.expires_at > now
    }) || state
        .final_credentials
        .iter()
        .any(|((job_id, _), credential)| {
            *job_id == deletion_job_id
                && credential.expires_at > now
                && credential
                    .reads
                    .values()
                    .any(|read| read.consumed_at.is_none())
        })
}

fn invalid_deletion_state(message: &str) -> ApplicationError {
    ApplicationError::InvalidContentLockDeletionState {
        message: message.to_owned(),
    }
}

fn final_credential_identity(
    state: &StoreState,
    lookup_key: &AccessCredentialLookupKey,
) -> Option<(Uuid, BundleId)> {
    let stored = state.records.get(lookup_key)?;
    let (job_id, kind) = stored.deletion?;
    (kind == DrainCredentialKind::Final).then(|| (job_id, stored.record.bundle_id.clone()))
}

fn disable_final_access(state: &mut StoreState, job_id: Uuid) {
    revoke_job_read_claims(state, job_id);
}

fn disable_job_access(state: &mut StoreState, job_id: Uuid) {
    revoke_job_read_claims(state, job_id);
}

fn revoke_job_read_claims(state: &mut StoreState, job_id: Uuid) {
    for ((deletion_job_id, _), credential) in &mut state.final_credentials {
        if *deletion_job_id == job_id {
            for read in credential.reads.values_mut() {
                read.claim_token = None;
                read.claim_expires_at = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr, sync::Mutex as StdMutex};

    use serde_json::json;
    use time::macros::datetime;
    use tokio::sync::RwLock;

    use locks_core::{
        ids::{BundleId, CreatorPubky, GuardedResourceHash, LockId, PubkyLockResource, TaskId},
        lock_policy::{
            AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GuardedResource, LockLogic,
            LockServerConfig, VerifierType,
        },
        verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle},
    };

    use super::*;
    use crate::application::models::VerificationTaskRecord;

    #[derive(Debug, Default)]
    struct FailingVerificationTaskRepository {
        records: RwLock<HashMap<TaskId, VerificationTaskRecord>>,
        fail_update_for: RwLock<Option<TaskId>>,
    }

    #[async_trait]
    impl VerificationTaskRepository for FailingVerificationTaskRepository {
        async fn insert_verification_task(
            &self,
            task: VerificationTaskRecord,
        ) -> Result<(), ApplicationError> {
            self.records.write().await.insert(task.task_id, task);
            Ok(())
        }

        async fn update_verification_task(
            &self,
            task: VerificationTaskRecord,
        ) -> Result<(), ApplicationError> {
            if *self.fail_update_for.read().await == Some(task.task_id) {
                return Err(ApplicationError::Storage {
                    message: "injected verification task update failure".to_owned(),
                });
            }
            let mut records = self.records.write().await;
            if !records.contains_key(&task.task_id) {
                return Err(ApplicationError::MissingRecord {
                    record: "verification_task",
                });
            }
            records.insert(task.task_id, task);
            Ok(())
        }

        async fn update_verification_tasks_atomically(
            &self,
            tasks: Vec<VerificationTaskRecord>,
        ) -> Result<(), ApplicationError> {
            let mut records = self.records.write().await;
            let fail_update_for = *self.fail_update_for.read().await;
            if tasks
                .iter()
                .any(|task| !records.contains_key(&task.task_id))
            {
                return Err(ApplicationError::MissingRecord {
                    record: "verification_task",
                });
            }
            if tasks
                .iter()
                .any(|task| fail_update_for == Some(task.task_id))
            {
                return Err(ApplicationError::Storage {
                    message: "injected verification task update failure".to_owned(),
                });
            }
            for task in tasks {
                records.insert(task.task_id, task);
            }
            Ok(())
        }

        async fn get_verification_task(
            &self,
            task_id: &TaskId,
        ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
            Ok(self.records.read().await.get(task_id).cloned())
        }

        async fn delete_verification_task(&self, task_id: &TaskId) -> Result<(), ApplicationError> {
            self.records.write().await.remove(task_id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn insert_rejects_duplicate_read_miss_is_none_delete_is_ensure_absent() {
        let store = InMemoryAccessCredentialStore::new();
        let credential = AccessCredential::new("raw-bearer-credential");
        let lookup_key = AccessCredentialLookupKey::derive(&credential);
        let record = record();
        let lock_id =
            LockId::from_str("000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG").unwrap();

        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            None
        );
        store
            .insert_access_credential(&lock_id, lookup_key.clone(), record.clone())
            .await
            .unwrap();
        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            Some(record.clone())
        );
        assert_eq!(
            store
                .insert_access_credential(&lock_id, lookup_key.clone(), record)
                .await,
            Err(ApplicationError::DuplicateRecord {
                record: "access_credential",
            })
        );

        store.delete_access_credential(&lookup_key).await.unwrap();
        store.delete_access_credential(&lookup_key).await.unwrap();
        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn non_paykit_expiry_missing_later_task_mutates_neither_tasks_nor_snapshots() {
        let tasks = Arc::new(FailingVerificationTaskRepository::default());
        let store = non_paykit_expiry_store(tasks.clone()).await;
        let task_ids = unresolved_task_ids(&store).await;
        tasks
            .insert_verification_task(non_paykit_task(task_ids[0]))
            .await
            .unwrap();

        assert!(expire_non_paykit(&store).await.is_err());
        assert_eq!(
            tasks
                .get_verification_task(&task_ids[0])
                .await
                .unwrap()
                .unwrap()
                .status,
            VerificationTaskStatus::Pending
        );
        assert_snapshots_unresolved(&store).await;
    }

    #[tokio::test]
    async fn non_paykit_expiry_later_update_failure_mutates_neither_tasks_nor_snapshots() {
        let tasks = Arc::new(FailingVerificationTaskRepository::default());
        let store = non_paykit_expiry_store(tasks.clone()).await;
        let task_ids = unresolved_task_ids(&store).await;
        for task_id in task_ids.iter().copied() {
            tasks
                .insert_verification_task(non_paykit_task(task_id))
                .await
                .unwrap();
        }
        *tasks.fail_update_for.write().await = Some(task_ids[1]);

        assert!(expire_non_paykit(&store).await.is_err());
        for task_id in task_ids {
            assert_eq!(
                tasks
                    .get_verification_task(&task_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                VerificationTaskStatus::Pending
            );
        }
        assert_snapshots_unresolved(&store).await;
    }

    async fn expire_non_paykit(
        store: &InMemoryAccessCredentialStore,
    ) -> Result<bool, ApplicationError> {
        store
            .expire_unresolved_non_paykit_tasks(
                Uuid::from_u128(7),
                "worker-drain",
                Uuid::from_u128(8),
                datetime!(2026-08-17 12:00:00 UTC),
            )
            .await
    }

    async fn non_paykit_expiry_store(
        tasks: Arc<FailingVerificationTaskRepository>,
    ) -> InMemoryAccessCredentialStore {
        let store = InMemoryAccessCredentialStore::build(Some(tasks), None);
        let snapshots = [
            (
                "000G40R40M30E209185GR38E1W",
                "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            ),
            (
                "000G40R40M30E209185GR38E1V",
                "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d11",
            ),
        ]
        .into_iter()
        .map(|(bundle_id, task_id)| {
            (
                BundleId::from_str(bundle_id).unwrap(),
                DeletionBundleSnapshot {
                    task_id: TaskId::from_str(task_id).unwrap(),
                    paykit_admission_required: false,
                    had_active_credential_at_cutoff: false,
                    status_at_cutoff: VerificationTaskStatus::Pending,
                    resolved_status: None,
                    resolved_at: None,
                    final_credential_eligible_at: None,
                    final_credential_issued: false,
                },
            )
        })
        .collect();
        store.state.write().await.deletions.insert(
            Uuid::from_u128(7),
            DeletionAccessState {
                creator: record().creator,
                lock_id: LockId::from_str("000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG")
                    .unwrap(),
                frozen_content_lock: test_content_lock(),
                state: ContentLockDeletionState::Running,
                phase: ContentLockDeletionPhase::StartPaymentDrain,
                force_requested: false,
                claimed_by: Some("worker-drain".to_owned()),
                claim_token: Some(Uuid::from_u128(8)),
                claim_expires_at: Some(datetime!(2026-08-17 12:05:00 UTC)),
                issuance_started_at: None,
                issuance_deadline: None,
                read_deadline: None,
                payment_aggregate: None,
                bundle_snapshots: snapshots,
            },
        );
        store
    }

    async fn unresolved_task_ids(store: &InMemoryAccessCredentialStore) -> Vec<TaskId> {
        store
            .state
            .read()
            .await
            .deletions
            .get(&Uuid::from_u128(7))
            .unwrap()
            .bundle_snapshots
            .values()
            .map(|snapshot| snapshot.task_id)
            .collect()
    }

    async fn assert_snapshots_unresolved(store: &InMemoryAccessCredentialStore) {
        assert!(
            store
                .state
                .read()
                .await
                .deletions
                .get(&Uuid::from_u128(7))
                .unwrap()
                .bundle_snapshots
                .values()
                .all(
                    |snapshot| snapshot.resolved_status.is_none() && snapshot.resolved_at.is_none()
                )
        );
    }

    fn non_paykit_task(task_id: TaskId) -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id,
            creator: record().creator,
            submitted_proof_bundle: SubmittedProofBundle {
                version: SUBMITTED_PROOF_BUNDLE_VERSION,
                bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
                pubky_lock_resource: PubkyLockResource::from_str(
                    "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json",
                )
                .unwrap(),
                reader_public_key: None,
                proofs: vec![Proof {
                    criterion_id: "criterion-1".to_owned(),
                    verifier_type: VerifierType::DevStatic,
                    payload: json!({}),
                }],
            },
            status: VerificationTaskStatus::Pending,
            submitted_at: datetime!(2026-08-17 11:00:00 UTC),
            started_at: None,
            completed_at: None,
            failure_message: None,
        }
    }

    #[tokio::test]
    async fn final_materialization_enumeration_is_ordered_eligible_bounded_and_claim_fenced() {
        let store = final_materialization_store().await;
        let job_id = Uuid::from_u128(7);
        let claim_token = Uuid::from_u128(8);

        let selected = store
            .final_credentials_to_materialize(job_id, "worker-final", claim_token, 1)
            .await
            .unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].bundle_id.as_str(), "000G40R40M30E209185GR38E1R");

        for (worker, token) in [
            ("wrong-worker", claim_token),
            ("worker-final", Uuid::from_u128(9)),
        ] {
            assert!(
                store
                    .final_credentials_to_materialize(job_id, worker, token, 10)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        {
            let mut state = store.state.write().await;
            state.deletions.get_mut(&job_id).unwrap().force_requested = true;
        }
        assert!(
            store
                .final_credentials_to_materialize(job_id, "worker-final", claim_token, 10)
                .await
                .unwrap()
                .is_empty()
        );
        {
            let mut state = store.state.write().await;
            let deletion = state.deletions.get_mut(&job_id).unwrap();
            deletion.force_requested = false;
            deletion.phase = ContentLockDeletionPhase::DrainFinalReads;
        }
        assert!(
            store
                .final_credentials_to_materialize(job_id, "worker-final", claim_token, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn final_materialization_enumeration_samples_time_after_state_fence() {
        let initial = datetime!(2026-08-17 12:00:00 UTC);
        let claim_expiry = datetime!(2026-08-17 12:05:00 UTC);
        let clock = Arc::new(MutableClock::new(initial));
        let store = Arc::new(final_materialization_store_with_clock(clock.clone()).await);
        let state_guard = store.state.write().await;
        let waiting_store = Arc::clone(&store);

        let enumeration = tokio::spawn(async move {
            waiting_store
                .final_credentials_to_materialize(
                    Uuid::from_u128(7),
                    "worker-final",
                    Uuid::from_u128(8),
                    10,
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!enumeration.is_finished());
        clock.set(claim_expiry);
        drop(state_guard);

        assert!(enumeration.await.unwrap().unwrap().is_empty());
    }

    #[tokio::test]
    async fn worker_final_issuance_revalidates_exact_live_claim_and_fresh_deadlines() {
        let job_id = Uuid::from_u128(7);
        let claim_token = Uuid::from_u128(8);
        let now = datetime!(2026-08-17 12:00:00 UTC);
        let bundle_id = BundleId::from_str("000G40R40M30E209185GR38E1R").unwrap();
        let creator = record().creator;

        for (worker, token, at) in [
            ("wrong-worker", claim_token, now),
            ("worker-final", Uuid::from_u128(9), now),
            (
                "worker-final",
                claim_token,
                datetime!(2026-08-17 12:05:00 UTC),
            ),
            (
                "worker-final",
                claim_token,
                datetime!(2026-08-17 12:15:00 UTC),
            ),
        ] {
            let store = final_materialization_store_at(at).await;
            let candidate = AccessCredential::new(format!("denied-{worker}-{at}"));
            let lookup_key = AccessCredentialLookupKey::derive(&candidate);
            assert!(
                store
                    .issue_or_replay_final_credential_for_worker(
                        FinalCredentialWorkerIssueRequest {
                            deletion_job_id: job_id,
                            worker_id: worker,
                            claim_token: token,
                            creator: &creator,
                            bundle_id: &bundle_id,
                            now: at,
                            candidate,
                        },
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                store
                    .get_access_credential(&lookup_key)
                    .await
                    .unwrap()
                    .is_none()
            );
        }

        let forced = final_materialization_store().await;
        forced
            .state
            .write()
            .await
            .deletions
            .get_mut(&job_id)
            .unwrap()
            .force_requested = true;
        assert!(
            forced
                .issue_or_replay_final_credential_for_worker(FinalCredentialWorkerIssueRequest {
                    deletion_job_id: job_id,
                    worker_id: "worker-final",
                    claim_token,
                    creator: &creator,
                    bundle_id: &bundle_id,
                    now,
                    candidate: AccessCredential::new("force-loser"),
                },)
                .await
                .unwrap()
                .is_none()
        );

        let store = final_materialization_store().await;
        let winner = AccessCredential::new("worker-winner");
        let issued = store
            .issue_or_replay_final_credential_for_worker(FinalCredentialWorkerIssueRequest {
                deletion_job_id: job_id,
                worker_id: "worker-final",
                claim_token,
                creator: &creator,
                bundle_id: &bundle_id,
                now,
                candidate: winner.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        let replay = store
            .issue_or_replay_final_credential_for_worker(FinalCredentialWorkerIssueRequest {
                deletion_job_id: job_id,
                worker_id: "worker-final",
                claim_token,
                creator: &creator,
                bundle_id: &bundle_id,
                now,
                candidate: AccessCredential::new("worker-loser"),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(issued.credential, winner);
        assert_eq!(replay, issued);
        assert_eq!(store.state.read().await.final_credentials.len(), 1);
    }

    #[tokio::test]
    async fn final_winner_paths_sample_time_after_waiting_for_state_fence() {
        let initial = datetime!(2026-08-17 12:00:00 UTC);
        let clock = Arc::new(MutableClock::new(initial));
        let job_id = Uuid::from_u128(7);
        let claim_token = Uuid::from_u128(8);
        let bundle_id = BundleId::from_str("000G40R40M30E209185GR38E1R").unwrap();
        let creator = record().creator;

        let public_store = Arc::new(final_materialization_store_with_clock(clock.clone()).await);
        let public_guard = public_store.state.write().await;
        let issuing_store = Arc::clone(&public_store);
        let issuing_creator = creator.clone();
        let issuing_bundle = bundle_id.clone();
        let public_issue = tokio::spawn(async move {
            issuing_store
                .issue_or_replay_final_credential(
                    &issuing_creator,
                    &issuing_bundle,
                    initial,
                    AccessCredential::new("stale-public-candidate"),
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!public_issue.is_finished());
        clock.set(datetime!(2026-08-17 12:15:00 UTC));
        drop(public_guard);
        assert!(public_issue.await.unwrap().unwrap().is_none());
        assert!(public_store.state.read().await.final_credentials.is_empty());

        clock.set(initial);
        let worker_store = Arc::new(final_materialization_store_with_clock(clock.clone()).await);
        let worker_guard = worker_store.state.write().await;
        let issuing_store = Arc::clone(&worker_store);
        let worker_issue = tokio::spawn(async move {
            issuing_store
                .issue_or_replay_final_credential_for_worker(FinalCredentialWorkerIssueRequest {
                    deletion_job_id: job_id,
                    worker_id: "worker-final",
                    claim_token,
                    creator: &creator,
                    bundle_id: &bundle_id,
                    now: initial,
                    candidate: AccessCredential::new("stale-worker-candidate"),
                })
                .await
        });
        tokio::task::yield_now().await;
        assert!(!worker_issue.is_finished());
        clock.set(datetime!(2026-08-17 12:05:00 UTC));
        drop(worker_guard);
        assert!(worker_issue.await.unwrap().unwrap().is_none());
        assert!(worker_store.state.read().await.final_credentials.is_empty());
    }

    #[tokio::test]
    async fn final_access_window_initialization_samples_time_after_waiting_for_state_fence() {
        let initial = datetime!(2026-08-17 12:00:00 UTC);
        let claim_expiry = datetime!(2026-08-17 12:05:00 UTC);
        let clock = Arc::new(MutableClock::new(initial));
        let store = Arc::new(final_materialization_store_with_clock(clock.clone()).await);
        {
            let mut state = store.state.write().await;
            let deletion = state.deletions.get_mut(&Uuid::from_u128(7)).unwrap();
            deletion.issuance_deadline = None;
            deletion.read_deadline = None;
        }

        let state_guard = store.state.write().await;
        let initializing_store = Arc::clone(&store);
        let initialization = tokio::spawn(async move {
            initializing_store
                .initialize_final_access_windows(
                    Uuid::from_u128(7),
                    "worker-final",
                    Uuid::from_u128(8),
                    Duration::minutes(15),
                    Duration::minutes(15),
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!initialization.is_finished());
        clock.set(claim_expiry);
        drop(state_guard);

        assert_eq!(
            initialization.await.unwrap().unwrap(),
            InitializeFinalAccessWindowsResult::ClaimLost
        );
        let state = store.state.read().await;
        let deletion = state.deletions.get(&Uuid::from_u128(7)).unwrap();
        assert!(deletion.issuance_deadline.is_none());
        assert!(deletion.read_deadline.is_none());
    }

    #[tokio::test]
    async fn final_read_prepare_samples_time_after_state_fence_and_rejects_deadline_equality() {
        let initial = datetime!(2026-08-17 12:00:00 UTC);
        let deadline = datetime!(2026-08-17 12:30:00 UTC);
        let clock = Arc::new(MutableClock::new(initial));
        let store = Arc::new(final_materialization_store_with_clock(clock.clone()).await);
        let credential = AccessCredential::new("blocked-final-read");
        let lookup = AccessCredentialLookupKey::derive(&credential);
        let creator = record().creator;
        let bundle = BundleId::from_str("000G40R40M30E209185GR38E1R").unwrap();
        store
            .issue_or_replay_final_credential(&creator, &bundle, initial, credential)
            .await
            .unwrap()
            .unwrap();

        let state_guard = store.state.write().await;
        let waiting_store = Arc::clone(&store);
        let waiting_lookup = lookup.clone();
        let prepare = tokio::spawn(async move {
            waiting_store
                .prepare_deletion_read(
                    &waiting_lookup,
                    "/priv/locks.app/content/post.json",
                    Duration::seconds(30),
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!prepare.is_finished());
        clock.set(deadline);
        drop(state_guard);

        assert!(prepare.await.unwrap().unwrap().is_none());
        let state = store.state.read().await;
        let read = &state
            .final_credentials
            .get(&(Uuid::from_u128(7), bundle))
            .unwrap()
            .reads["/priv/locks.app/content/post.json"];
        assert!(read.claim_token.is_none());
        assert!(read.consumed_at.is_none());
    }

    #[tokio::test]
    async fn final_read_consume_samples_time_after_state_fence_and_rejects_claim_expiry_equality() {
        let initial = datetime!(2026-08-17 12:00:00 UTC);
        let claim_expiry = initial + Duration::seconds(30);
        let clock = Arc::new(MutableClock::new(initial));
        let store = Arc::new(final_materialization_store_with_clock(clock.clone()).await);
        let credential = AccessCredential::new("blocked-final-consume");
        let lookup = AccessCredentialLookupKey::derive(&credential);
        let creator = record().creator;
        let bundle = BundleId::from_str("000G40R40M30E209185GR38E1R").unwrap();
        store
            .issue_or_replay_final_credential(&creator, &bundle, initial, credential)
            .await
            .unwrap()
            .unwrap();
        let claim_token = store
            .prepare_deletion_read(
                &lookup,
                "/priv/locks.app/content/post.json",
                Duration::seconds(30),
            )
            .await
            .unwrap()
            .unwrap()
            .claim_token
            .unwrap();

        let state_guard = store.state.write().await;
        let waiting_store = Arc::clone(&store);
        let waiting_lookup = lookup.clone();
        let consume = tokio::spawn(async move {
            waiting_store
                .consume_deletion_read(
                    &waiting_lookup,
                    "/priv/locks.app/content/post.json",
                    claim_token,
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!consume.is_finished());
        clock.set(claim_expiry);
        drop(state_guard);

        assert!(!consume.await.unwrap().unwrap());
        let state = store.state.read().await;
        let read = &state
            .final_credentials
            .get(&(Uuid::from_u128(7), bundle))
            .unwrap()
            .reads["/priv/locks.app/content/post.json"];
        assert!(read.consumed_at.is_none());
    }

    #[tokio::test]
    async fn final_materialization_enumeration_does_not_create_eligibility() {
        let store = final_materialization_store().await;
        let job_id = Uuid::from_u128(7);
        let ineligible = BundleId::from_str("000G40R40M30E209185GR38E1X").unwrap();
        {
            let mut state = store.state.write().await;
            let snapshot = state
                .deletions
                .get_mut(&job_id)
                .unwrap()
                .bundle_snapshots
                .get_mut(&ineligible)
                .unwrap();
            snapshot.resolved_status = Some(VerificationTaskStatus::Completed);
            assert!(snapshot.final_credential_eligible_at.is_none());
        }

        let selected = store
            .final_credentials_to_materialize(job_id, "worker-final", Uuid::from_u128(8), 10)
            .await
            .unwrap();
        assert!(!selected.iter().any(|item| item.bundle_id == ineligible));
        assert!(
            store
                .state
                .read()
                .await
                .deletions
                .get(&job_id)
                .unwrap()
                .bundle_snapshots
                .get(&ineligible)
                .unwrap()
                .final_credential_eligible_at
                .is_none()
        );
    }

    struct MutableClock(StdMutex<OffsetDateTime>);

    impl MutableClock {
        fn new(now: OffsetDateTime) -> Self {
            Self(StdMutex::new(now))
        }

        fn set(&self, now: OffsetDateTime) {
            *self.0.lock().unwrap() = now;
        }
    }

    impl crate::application::ports::Clock for MutableClock {
        fn now(&self) -> OffsetDateTime {
            *self.0.lock().unwrap()
        }
    }

    async fn final_materialization_store() -> InMemoryAccessCredentialStore {
        final_materialization_store_at(datetime!(2026-08-17 12:00:00 UTC)).await
    }

    async fn final_materialization_store_at(now: OffsetDateTime) -> InMemoryAccessCredentialStore {
        final_materialization_store_with_clock(Arc::new(MutableClock::new(now))).await
    }

    async fn final_materialization_store_with_clock(
        clock: Arc<dyn crate::application::ports::Clock>,
    ) -> InMemoryAccessCredentialStore {
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(clock));
        let store = InMemoryAccessCredentialStore::build(None, Some(fence));
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let eligible_at = datetime!(2026-08-17 11:00:00 UTC);
        let snapshots = [
            ("000G40R40M30E209185GR38E1W", true),
            ("000G40R40M30E209185GR38E1V", true),
            ("000G40R40M30E209185GR38E1X", false),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (bundle, eligible))| {
            (
                BundleId::from_str(bundle).unwrap(),
                DeletionBundleSnapshot {
                    task_id: TaskId::from_str(&format!(
                        "550e8400-e29b-41d4-a716-44665544000{index}"
                    ))
                    .unwrap(),
                    paykit_admission_required: true,
                    had_active_credential_at_cutoff: false,
                    status_at_cutoff: VerificationTaskStatus::Completed,
                    resolved_status: Some(VerificationTaskStatus::Completed),
                    resolved_at: Some(eligible_at),
                    final_credential_eligible_at: eligible.then_some(eligible_at),
                    final_credential_issued: false,
                },
            )
        })
        .collect();
        store.state.write().await.deletions.insert(
            Uuid::from_u128(7),
            DeletionAccessState {
                creator,
                lock_id: LockId::from_str("000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG")
                    .unwrap(),
                frozen_content_lock: test_content_lock(),
                state: ContentLockDeletionState::Running,
                phase: ContentLockDeletionPhase::IssueFinalCredentials,
                force_requested: false,
                claimed_by: Some("worker-final".to_owned()),
                claim_token: Some(Uuid::from_u128(8)),
                claim_expires_at: Some(datetime!(2026-08-17 12:05:00 UTC)),
                issuance_started_at: Some(datetime!(2026-08-17 12:00:00 UTC)),
                issuance_deadline: Some(datetime!(2026-08-17 12:15:00 UTC)),
                read_deadline: Some(datetime!(2026-08-17 12:30:00 UTC)),
                payment_aggregate: None,
                bundle_snapshots: snapshots,
            },
        );
        store
    }

    fn test_content_lock() -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: record().creator,
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
            created_at: datetime!(2026-08-17 11:00:00 UTC),
        }
    }

    fn record() -> AccessCredentialRecord {
        AccessCredentialRecord {
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            expires_at: datetime!(2026-05-29 12:15:00 UTC),
        }
    }
}
