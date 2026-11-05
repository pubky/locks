use std::sync::Arc;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use locks_core::verification::{
    SatisfiedByV2, VERIFIED_PROOF_BUNDLE_V2_VERSION, VerifiedProofBundleV2,
};
use tokio::sync::{OwnedMutexGuard, RwLock};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedPaymentTask, PaymentRootState, PaymentTaskRecord, PaymentTaskSnapshot,
    PaymentTaskSnapshotV1, VerificationTaskStatus,
};
use crate::application::ports::{
    AdmitPaymentTask, PaymentPublicationActionGuard, PaymentTaskClaimer, PaymentTaskRepository,
};

use super::verification_task_deletion_fence::{
    InMemoryVerificationTaskDeletionFence, InMemoryVerificationTaskFenceRecord,
};

#[derive(Debug, Clone)]
struct StoredPaymentTask {
    task: PaymentTaskRecord,
    claimed_by: Option<String>,
    claim_token: Option<uuid::Uuid>,
    claim_expires_at: Option<time::OffsetDateTime>,
    next_attempt_at: Option<time::OffsetDateTime>,
}

#[derive(Debug, Clone)]
pub struct InMemoryPaymentTaskRepository {
    deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>,
    records: Arc<RwLock<Vec<StoredPaymentTask>>>,
}

struct InMemoryPaymentPublicationActionGuard {
    _guard: Option<OwnedMutexGuard<()>>,
    repository: InMemoryPaymentTaskRepository,
    task_id: TaskId,
    worker_id: String,
    claim_token: uuid::Uuid,
    expected_revision: u64,
    publication_token: uuid::Uuid,
}

impl Default for InMemoryPaymentTaskRepository {
    fn default() -> Self {
        Self::with_deletion_fence(Arc::new(InMemoryVerificationTaskDeletionFence::new()))
    }
}

impl InMemoryPaymentTaskRepository {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_deletion_fence(deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>) -> Self {
        Self {
            deletion_fence,
            records: Arc::new(RwLock::new(Vec::new())),
        }
    }
    fn now(&self) -> time::OffsetDateTime {
        self.deletion_fence.authoritative_cutoff()
    }
}

#[async_trait]
impl PaymentTaskRepository for InMemoryPaymentTaskRepository {
    async fn admit_payment_task(
        &self,
        request: AdmitPaymentTask,
    ) -> Result<PaymentTaskRecord, ApplicationError> {
        let creator = request
            .submitted_proof_bundle
            .pubky_lock_resource
            .creator()
            .clone();
        let bundle_id = request.submitted_proof_bundle.bundle_id.clone();
        let lock_id = request
            .submitted_proof_bundle
            .pubky_lock_resource
            .lock_id()
            .clone();
        let _handle = self
            .deletion_fence
            .acquire_handle_admission(&creator, &bundle_id)
            .await;
        let _lock = self
            .deletion_fence
            .acquire_lock_admission(&creator, &lock_id)
            .await;

        if let Some(existing) = self.records.read().await.iter().find(|stored| {
            let snapshot = stored.task.snapshot.as_v1();
            snapshot.creator == creator && snapshot.bundle_id == bundle_id
        }) {
            let candidate = PaymentTaskSnapshotV1::new(
                request.submitted_proof_bundle,
                request.lock_hash,
                request.payment,
                existing.task.snapshot.as_v1().accepted_at,
                request.verification_window_seconds,
            )
            .map_err(invalid_state)?;
            if existing
                .task
                .snapshot
                .as_v1()
                .has_same_admission_binding(&candidate)
            {
                return Ok(existing.task.clone());
            }
            return Err(ApplicationError::VerificationTaskConflict);
        }

        if self
            .deletion_fence
            .records
            .read()
            .await
            .values()
            .any(|record| record.creator == creator && record.bundle_id == bundle_id)
        {
            return Err(ApplicationError::VerificationTaskConflict);
        }
        if self
            .deletion_fence
            .deletion_cutoffs
            .read()
            .await
            .contains(&(creator.clone(), lock_id.clone()))
        {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        let snapshot = PaymentTaskSnapshotV1::new(
            request.submitted_proof_bundle,
            request.lock_hash,
            request.payment,
            self.now(),
            request.verification_window_seconds,
        )
        .map_err(invalid_state)?;
        let task = PaymentTaskRecord {
            task_id: request.task_id,
            snapshot: PaymentTaskSnapshot::V1(snapshot),
            evaluator_revision: 0,
            status: VerificationTaskStatus::Pending,
            entitlement_to_publish: None,
            entitlement_publication_token: None,
            completed_at: None,
        };
        let identity = task.snapshot.as_v1();
        self.deletion_fence.records.write().await.insert(
            task.task_id,
            InMemoryVerificationTaskFenceRecord {
                creator: identity.creator.clone(),
                lock_id,
                bundle_id: identity.bundle_id.clone(),
                paykit_admission_required: true,
                invoice_admission_ready: true,
                status: task.status,
                entitlement_publication_claim_token: None,
                deletion_job_id: None,
            },
        );
        self.deletion_fence
            .current_statuses
            .write()
            .await
            .insert(task.task_id, task.status);
        self.records.write().await.push(StoredPaymentTask {
            task: task.clone(),
            claimed_by: None,
            claim_token: None,
            claim_expires_at: None,
            next_attempt_at: None,
        });
        Ok(task)
    }

    async fn get_payment_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError> {
        Ok(self
            .records
            .read()
            .await
            .iter()
            .find(|stored| {
                let snapshot = stored.task.snapshot.as_v1();
                &snapshot.creator == creator && &snapshot.bundle_id == bundle_id
            })
            .map(|stored| stored.task.clone()))
    }
}

#[async_trait]
impl PaymentTaskClaimer for InMemoryPaymentTaskRepository {
    async fn claim_next_payment_task(
        &self,
        worker_id: &str,
        caller_now: time::OffsetDateTime,
        requested_claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedPaymentTask>, ApplicationError> {
        let ttl = requested_claim_expires_at - caller_now;
        if !ttl.is_positive() {
            return Err(invalid_state("payment claim expiry must follow claim time"));
        }
        let now = self.now();
        let fence = self.deletion_fence.records.read().await;
        let mut records = self.records.write().await;
        let Some(stored) = records.iter_mut().find(|stored| {
            ((matches!(
                stored.task.status,
                VerificationTaskStatus::Pending | VerificationTaskStatus::InProgress
            ) && stored.task.snapshot.as_v1().root == PaymentRootState::Pending)
                || (stored.task.status == VerificationTaskStatus::PublishingEntitlement
                    && stored.task.snapshot.as_v1().root == PaymentRootState::Satisfied))
                && stored.next_attempt_at.is_none_or(|due| due <= now)
                && stored.claim_expires_at.is_none_or(|expiry| expiry <= now)
                && fence
                    .get(&stored.task.task_id)
                    .is_none_or(|record| record.deletion_job_id.is_none())
        }) else {
            return Ok(None);
        };
        if stored.task.status != VerificationTaskStatus::PublishingEntitlement {
            stored.task.status = VerificationTaskStatus::InProgress;
        }
        let token = uuid::Uuid::new_v4();
        stored.claimed_by = Some(worker_id.to_owned());
        stored.claim_token = Some(token);
        stored.claim_expires_at = Some(now + ttl);
        stored.next_attempt_at = None;
        Ok(Some(ClaimedPaymentTask {
            task: stored.task.clone(),
            claim_token: token,
        }))
    }

    async fn persist_claimed_payment_snapshot(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _caller_now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: PaymentTaskSnapshot,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError> {
        let now = self.now();
        let mut fence = self.deletion_fence.records.write().await;
        let mut records = self.records.write().await;
        let Some(stored) = records.iter_mut().find(|stored| {
            live(
                stored,
                task_id,
                worker_id,
                claim_token,
                expected_revision,
                now,
            )
        }) else {
            return Ok(None);
        };
        if fence
            .get(task_id)
            .is_some_and(|record| record.deletion_job_id.is_some())
        {
            return Ok(None);
        }
        snapshot
            .as_v1()
            .validate_progress_from(stored.task.snapshot.as_v1())
            .map_err(invalid_state)?;
        let root = snapshot.as_v1().root;
        stored.task.snapshot = snapshot;
        stored.task.evaluator_revision = stored
            .task
            .evaluator_revision
            .checked_add(1)
            .ok_or_else(|| invalid_state("payment evaluator revision overflow"))?;
        match root {
            PaymentRootState::Pending => {}
            PaymentRootState::Satisfied => {
                let identity = stored.task.snapshot.as_v1();
                let entitlement = VerifiedProofBundleV2 {
                    version: VERIFIED_PROOF_BUNDLE_V2_VERSION,
                    creator: identity.creator.clone(),
                    reader: identity.reader.clone(),
                    bundle_id: identity.bundle_id.clone(),
                    pubky_lock_resource: identity.pubky_lock_resource.clone(),
                    verified_at: now,
                    satisfied_by: vec![SatisfiedByV2::Payment],
                };
                entitlement.validate().map_err(|error| {
                    invalid_state(format!("invalid payment entitlement: {error}"))
                })?;
                stored.task.status = VerificationTaskStatus::PublishingEntitlement;
                stored.task.entitlement_to_publish = Some(entitlement);
                let publication_token = uuid::Uuid::new_v4();
                stored.task.entitlement_publication_token = Some(publication_token);
                if let Some(record) = fence.get_mut(task_id) {
                    record.status = stored.task.status;
                    record.entitlement_publication_claim_token = Some(publication_token);
                }
            }
            PaymentRootState::Expired(_) => {
                terminalize(stored, VerificationTaskStatus::Expired, now)
            }
            PaymentRootState::Failed(_) => terminalize(stored, VerificationTaskStatus::Failed, now),
        }
        if let Some(record) = fence.get_mut(task_id) {
            record.status = stored.task.status;
            if stored.task.status != VerificationTaskStatus::PublishingEntitlement {
                record.entitlement_publication_claim_token = None;
            }
        }
        self.deletion_fence
            .current_statuses
            .write()
            .await
            .insert(*task_id, stored.task.status);
        Ok(Some(stored.task.clone()))
    }

    async fn schedule_payment_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        caller_now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: PaymentTaskSnapshot,
        requested_next: time::OffsetDateTime,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError> {
        let delay = requested_next - caller_now;
        if !delay.is_positive() {
            return Err(invalid_state("payment retry delay must be positive"));
        }
        let now = self.now();
        let mut records = self.records.write().await;
        let Some(stored) = records.iter_mut().find(|stored| {
            live(
                stored,
                task_id,
                worker_id,
                claim_token,
                expected_revision,
                now,
            )
        }) else {
            return Ok(None);
        };
        snapshot
            .as_v1()
            .validate_progress_from(stored.task.snapshot.as_v1())
            .map_err(invalid_state)?;
        if snapshot.as_v1().root != PaymentRootState::Pending {
            return Err(invalid_state("payment retry requires pending root"));
        }
        stored.task.snapshot = snapshot;
        stored.task.evaluator_revision += 1;
        stored.task.status = VerificationTaskStatus::Pending;
        stored.claimed_by = None;
        stored.claim_token = None;
        stored.claim_expires_at = None;
        stored.next_attempt_at = Some((now + delay).min(stored.task.snapshot.as_v1().deadline_at));
        self.deletion_fence
            .current_statuses
            .write()
            .await
            .insert(*task_id, stored.task.status);
        Ok(Some(stored.task.clone()))
    }

    async fn release_claimed_payment_task(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError> {
        let mut records = self.records.write().await;
        let Some(stored) = records.iter_mut().find(|stored| {
            stored.task.task_id == *task_id
                && stored.claimed_by.as_deref() == Some(worker_id)
                && stored.claim_token.as_ref() == Some(claim_token)
        }) else {
            return Ok(false);
        };
        if stored.task.status == VerificationTaskStatus::InProgress
            && stored.task.snapshot.as_v1().root == PaymentRootState::Pending
        {
            stored.task.status = VerificationTaskStatus::Pending;
        }
        stored.claimed_by = None;
        stored.claim_token = None;
        stored.claim_expires_at = None;
        Ok(true)
    }

    async fn acquire_payment_publication_action(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<Box<dyn PaymentPublicationActionGuard>>, ApplicationError> {
        let identity = self
            .records
            .read()
            .await
            .iter()
            .find(|stored| stored.task.task_id == *task_id)
            .map(|stored| {
                let snapshot = stored.task.snapshot.as_v1();
                (
                    snapshot.creator.clone(),
                    snapshot.pubky_lock_resource.lock_id().clone(),
                )
            });
        let Some((creator, lock_id)) = identity else {
            return Ok(None);
        };
        let guard = self
            .deletion_fence
            .acquire_lock_admission(&creator, &lock_id)
            .await;
        let now = self.now();
        let live = self.records.read().await.iter().any(|stored| {
            live(
                stored,
                task_id,
                worker_id,
                claim_token,
                expected_revision,
                now,
            ) && stored.task.status == VerificationTaskStatus::PublishingEntitlement
                && stored.task.entitlement_publication_token == Some(publication_token)
        });
        if !live {
            return Ok(None);
        }
        Ok(Some(Box::new(InMemoryPaymentPublicationActionGuard {
            _guard: Some(guard),
            repository: self.clone(),
            task_id: *task_id,
            worker_id: worker_id.to_owned(),
            claim_token: *claim_token,
            expected_revision,
            publication_token,
        })))
    }

    async fn complete_claimed_payment_publication(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError> {
        complete_publication(
            self,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            publication_token,
        )
        .await
    }
}

#[async_trait]
impl PaymentPublicationActionGuard for InMemoryPaymentPublicationActionGuard {
    async fn complete(&mut self) -> Result<Option<PaymentTaskRecord>, ApplicationError> {
        complete_publication(
            &self.repository,
            &self.task_id,
            &self.worker_id,
            &self.claim_token,
            self.expected_revision,
            self.publication_token,
        )
        .await
    }
    async fn release(mut self: Box<Self>) -> Result<(), ApplicationError> {
        self._guard.take();
        Ok(())
    }
}

async fn complete_publication(
    repository: &InMemoryPaymentTaskRepository,
    task_id: &TaskId,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    expected_revision: u64,
    publication_token: uuid::Uuid,
) -> Result<Option<PaymentTaskRecord>, ApplicationError> {
    let now = repository.now();
    let mut records = repository.records.write().await;
    let Some(stored) = records.iter_mut().find(|stored| {
        live(
            stored,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            now,
        ) && stored.task.status == VerificationTaskStatus::PublishingEntitlement
            && stored.task.entitlement_publication_token == Some(publication_token)
    }) else {
        return Ok(None);
    };
    stored.task.status = VerificationTaskStatus::Completed;
    stored.task.completed_at = Some(now);
    stored.claimed_by = None;
    stored.claim_token = None;
    stored.claim_expires_at = None;
    stored.next_attempt_at = None;
    if let Some(record) = repository
        .deletion_fence
        .records
        .write()
        .await
        .get_mut(task_id)
    {
        record.status = VerificationTaskStatus::Completed;
        record.entitlement_publication_claim_token = None;
    }
    repository
        .deletion_fence
        .current_statuses
        .write()
        .await
        .insert(*task_id, VerificationTaskStatus::Completed);
    Ok(Some(stored.task.clone()))
}

fn live(
    stored: &StoredPaymentTask,
    task_id: &TaskId,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    revision: u64,
    now: time::OffsetDateTime,
) -> bool {
    stored.task.task_id == *task_id
        && stored.task.evaluator_revision == revision
        && stored.claimed_by.as_deref() == Some(worker_id)
        && stored.claim_token.as_ref() == Some(claim_token)
        && stored.claim_expires_at.is_some_and(|expiry| expiry > now)
}

fn terminalize(
    stored: &mut StoredPaymentTask,
    status: VerificationTaskStatus,
    now: time::OffsetDateTime,
) {
    stored.task.status = status;
    stored.task.completed_at = Some(now);
    stored.claimed_by = None;
    stored.claim_token = None;
    stored.claim_expires_at = None;
    stored.next_attempt_at = None;
}

fn invalid_state(message: impl Into<String>) -> ApplicationError {
    ApplicationError::InvalidVerificationTaskState {
        message: message.into(),
    }
}
