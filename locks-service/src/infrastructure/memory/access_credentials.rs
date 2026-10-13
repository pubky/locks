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
        DeletionReadAuthorization, EncryptedFinalCredential, FinalCredentialContext,
        IssuedDeletionCredential, VerificationTaskStatus,
    },
    ports::{AccessCredentialStore, VerificationTaskRepository},
};
use crate::infrastructure::{
    final_credentials::FinalCredentialCipher,
    memory::verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence,
};

type JobKey = (CreatorPubky, LockId);

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
                .is_some_and(|claim_expires_at| claim_expires_at >= now);
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

    pub(crate) async fn check_phase_advance(
        &self,
        deletion_job_id: Uuid,
        current_phase: ContentLockDeletionPhase,
        next_phase: ContentLockDeletionPhase,
        now: OffsetDateTime,
    ) -> Result<(), ApplicationError> {
        let state = self.state.read().await;
        let Some(deletion) = state.deletions.get(&deletion_job_id) else {
            return Ok(());
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
            return Err(invalid_deletion_state(
                "existing credentials must reach their original expiry before final issuance",
            ));
        }
        if current_phase == ContentLockDeletionPhase::IssueFinalCredentials
            && next_phase == ContentLockDeletionPhase::DrainFinalReads
            && deletion.bundle_snapshots.values().any(|snapshot| {
                snapshot.permits_final_credential() && !snapshot.final_credential_issued
            })
        {
            return Err(invalid_deletion_state(
                "final credential issuance must complete before final-read draining",
            ));
        }
        if current_phase == ContentLockDeletionPhase::DrainFinalReads
            && next_phase == ContentLockDeletionPhase::DeleteContent
            && has_live_access_obligation(&state, deletion_job_id, now)
        {
            return Err(invalid_deletion_state(
                "credential expiry and final-read obligations must drain before destructive deletion",
            ));
        }
        Ok(())
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
            .is_some_and(|claim_expires_at| claim_expires_at >= now)
}

fn payment_aggregate_completed(deletion: &DeletionAccessState) -> bool {
    deletion
        .payment_aggregate
        .is_some_and(|aggregate| aggregate.completed && aggregate.accepted_count == 0)
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
        now: OffsetDateTime,
        issuance_deadline: OffsetDateTime,
        read_deadline: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        if issuance_deadline <= now || read_deadline <= issuance_deadline {
            return Err(ApplicationError::Storage {
                message: "invalid final access window ordering".to_owned(),
            });
        }
        let mut state = self.state.write().await;
        let Some(deletion) = state.deletions.get_mut(&deletion_job_id) else {
            return Ok(false);
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
            return Ok(false);
        }
        deletion.issuance_deadline.get_or_insert(issuance_deadline);
        deletion.read_deadline.get_or_insert(read_deadline);
        Ok(true)
    }

    async fn issue_or_replay_final_credential(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
        now: OffsetDateTime,
        candidate: AccessCredential,
    ) -> Result<Option<IssuedDeletionCredential>, ApplicationError> {
        let mut state = self.state.write().await;
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
        now: OffsetDateTime,
        claim_expires_at: OffsetDateTime,
    ) -> Result<Option<DeletionReadAuthorization>, ApplicationError> {
        let mut state = self.state.write().await;
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
        let bounded_expiry = claim_expires_at
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
        now: OffsetDateTime,
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
    use std::str::FromStr;

    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, LockId};

    use super::*;

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
