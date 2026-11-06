use std::sync::Arc;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use tokio::sync::{OwnedMutexGuard, RwLock};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedPaymentTask, PaymentRootState, PaymentTaskRecord, PaymentTaskSnapshot,
    PaymentTaskSnapshotV1, VerificationTaskStatus,
};
use crate::application::ports::{
    AdmitPaymentTask, EvaluatorMode, EvaluatorTaskKernel, PaymentPublicationActionGuard,
    PaymentTaskClaimer, PaymentTaskRepository, build_evaluator_publication,
};

use super::evaluator_tasks::{
    MemoryEvaluatorTransition, StoredEvaluatorTask, claim_next, claimed_task_index,
    complete_publication as complete_evaluator_publication, persist_claimed, publication_is_live,
    release_claim, retry_claimed,
};
use super::verification_task_deletion_fence::{
    InMemoryVerificationTaskDeletionFence, InMemoryVerificationTaskFenceRecord,
};

type StoredPaymentTask = StoredEvaluatorTask<PaymentTaskRecord>;

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
        self.records
            .write()
            .await
            .push(StoredEvaluatorTask::new(task.clone()));
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
        let ttl = EvaluatorTaskKernel::claim_ttl(caller_now, requested_claim_expires_at)
            .map_err(|_| invalid_state("payment claim expiry must follow claim time"))?;
        let now = self.now();
        let fence = self.deletion_fence.records.read().await;
        let mut records = self.records.write().await;
        let stored = claim_next(&mut records, worker_id, now, ttl, |task_id| {
            fence
                .get(&task_id)
                .is_some_and(|record| record.deletion_job_id.is_some())
        });
        Ok(stored.map(|stored| ClaimedPaymentTask {
            task: stored.task,
            claim_token: stored.claim_token.expect("shared claim kernel sets token"),
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
        let Some(index) = claimed_task_index(
            &records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            now,
            |id| {
                fence
                    .get(&id)
                    .is_some_and(|record| record.deletion_job_id.is_some())
            },
        ) else {
            return Ok(None);
        };
        let current = &records[index].task;
        snapshot
            .as_v1()
            .validate_progress_from(current.snapshot.as_v1())
            .map_err(invalid_state)?;
        let root = snapshot.as_v1().root;
        let (status, publication, publication_token, completed_at, retain_claim) = match root {
            PaymentRootState::Pending => {
                (VerificationTaskStatus::InProgress, None, None, None, true)
            }
            PaymentRootState::Satisfied => {
                let publication = build_evaluator_publication(
                    &snapshot.as_v1().identity(),
                    EvaluatorMode::Payment,
                    now,
                )
                .map_err(|error| invalid_state(format!("invalid payment entitlement: {error}")))?;
                (
                    VerificationTaskStatus::PublishingEntitlement,
                    Some(publication),
                    Some(uuid::Uuid::new_v4()),
                    None,
                    true,
                )
            }
            PaymentRootState::Expired(_) => (
                VerificationTaskStatus::Expired,
                None,
                None,
                Some(now),
                false,
            ),
            PaymentRootState::Failed(_) => {
                (VerificationTaskStatus::Failed, None, None, Some(now), false)
            }
        };
        let updated = persist_claimed(
            &mut records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            now,
            snapshot,
            MemoryEvaluatorTransition {
                status,
                publication,
                publication_token,
                completed_at,
                retain_claim,
            },
            |id| {
                fence
                    .get(&id)
                    .is_some_and(|record| record.deletion_job_id.is_some())
            },
        )?;
        if let Some(updated) = &updated {
            if let Some(record) = fence.get_mut(task_id) {
                record.status = updated.status;
                record.entitlement_publication_claim_token = updated.entitlement_publication_token;
            }
            self.deletion_fence
                .current_statuses
                .write()
                .await
                .insert(*task_id, updated.status);
        }
        Ok(updated)
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
        let now = self.now();
        let mut fence = self.deletion_fence.records.write().await;
        let mut records = self.records.write().await;
        let Some(index) = claimed_task_index(
            &records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            now,
            |id| {
                fence
                    .get(&id)
                    .is_some_and(|record| record.deletion_job_id.is_some())
            },
        ) else {
            return Ok(None);
        };
        let current = &records[index].task;
        snapshot
            .as_v1()
            .validate_progress_from(current.snapshot.as_v1())
            .map_err(invalid_state)?;
        if snapshot.as_v1().root != PaymentRootState::Pending {
            return Err(invalid_state("payment retry requires pending root"));
        }
        let due = EvaluatorTaskKernel::retry_due(
            caller_now,
            requested_next,
            now,
            snapshot.as_v1().deadline_at,
        )?;
        let updated = retry_claimed(
            &mut records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            now,
            due,
            snapshot.as_v1().deadline_at,
            snapshot,
            |id| {
                fence
                    .get(&id)
                    .is_some_and(|record| record.deletion_job_id.is_some())
            },
        )?;
        if updated.is_some() {
            if let Some(record) = fence.get_mut(task_id) {
                record.status = VerificationTaskStatus::Pending;
            }
            self.deletion_fence
                .current_statuses
                .write()
                .await
                .insert(*task_id, VerificationTaskStatus::Pending);
        }
        Ok(updated)
    }

    async fn release_claimed_payment_task(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError> {
        let fence = self.deletion_fence.records.read().await;
        let mut records = self.records.write().await;
        Ok(release_claim(
            &mut records,
            task_id,
            worker_id,
            claim_token,
            |id| {
                fence
                    .get(&id)
                    .is_some_and(|record| record.deletion_job_id.is_some())
            },
        ))
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
        let fence = self.deletion_fence.records.read().await;
        if fence
            .get(task_id)
            .is_some_and(|record| record.deletion_job_id.is_some())
        {
            return Ok(None);
        }
        let now = self.now();
        let records = self.records.read().await;
        let live = publication_is_live(
            &records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            publication_token,
            now,
            |id| {
                fence
                    .get(&id)
                    .is_some_and(|record| record.deletion_job_id.is_some())
            },
        );
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
    let mut fence = repository.deletion_fence.records.write().await;
    let mut records = repository.records.write().await;
    let updated = complete_evaluator_publication(
        &mut records,
        task_id,
        worker_id,
        claim_token,
        expected_revision,
        publication_token,
        now,
        |id| {
            fence
                .get(&id)
                .is_some_and(|record| record.deletion_job_id.is_some())
        },
    );
    if updated.is_some() {
        if let Some(record) = fence.get_mut(task_id) {
            record.status = VerificationTaskStatus::Completed;
            record.entitlement_publication_claim_token = None;
        }
        repository
            .deletion_fence
            .current_statuses
            .write()
            .await
            .insert(*task_id, VerificationTaskStatus::Completed);
    }
    Ok(updated)
}

fn invalid_state(message: impl Into<String>) -> ApplicationError {
    ApplicationError::InvalidVerificationTaskState {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::{LockHash, LockId, PubkyLockResource};
    use locks_core::lock_policy::PaymentPolicyV2;
    use locks_core::verification::{SUBMITTED_PROOF_BUNDLE_V2_VERSION, SubmittedProofBundleV2};

    use super::*;
    use crate::application::models::{
        BitcoinOutpointV2, PaymentAssetV2, PaymentProviderStatus, PaymentRequestStateV2,
        PaymentSampleStatusV2, PaymentSampleV2, PaymentStateV2,
    };
    use crate::application::payment_v2_reducer::reduce_payment_snapshot;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
    const BUNDLE: &str = "000G40R40M30E209185GR38E1W";

    fn request() -> AdmitPaymentTask {
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let hash = LockHash::from_bytes([7; 32]);
        let resource = PubkyLockResource::from_str(&format!(
            "{CREATOR}/pub/app.locks/{}.json",
            LockId::from_hash(hash)
        ))
        .unwrap();
        AdmitPaymentTask {
            task_id: TaskId::from_str("123e4567-e89b-42d3-a456-426614174021").unwrap(),
            submitted_proof_bundle: SubmittedProofBundleV2 {
                version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
                bundle_id: BundleId::from_str(BUNDLE).unwrap(),
                pubky_lock_resource: resource,
                reader_pubky: Some(CreatorPubky::from_str(READER).unwrap()),
            },
            lock_hash: [7; 32],
            payment: PaymentPolicyV2 {
                recipient_pubky: creator,
                amount: "50000".to_owned(),
                asset: "BTC".to_owned(),
                minimum_confirmations: 1,
            },
            verification_window_seconds: 300,
        }
    }

    #[tokio::test]
    async fn deletion_snapshot_fences_retry_before_it_can_mutate_frozen_state() {
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::new());
        let repository = InMemoryPaymentTaskRepository::with_deletion_fence(fence.clone());
        let admitted = repository.admit_payment_task(request()).await.unwrap();
        let now = repository.now();
        let claimed = repository
            .claim_next_payment_task("worker", now, now + time::Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
        fence
            .records
            .write()
            .await
            .get_mut(&admitted.task_id)
            .unwrap()
            .deletion_job_id = Some(uuid::Uuid::new_v4());

        let mut progressed = claimed.task.snapshot;
        progressed.as_v1_mut().next_due_at = Some(now + time::Duration::seconds(5));
        assert!(
            repository
                .schedule_payment_task_retry(
                    &admitted.task_id,
                    "worker",
                    &claimed.claim_token,
                    now,
                    0,
                    progressed,
                    now + time::Duration::seconds(5),
                )
                .await
                .unwrap()
                .is_none()
        );
        let stored = repository
            .get_payment_task_by_handle(
                &CreatorPubky::from_str(CREATOR).unwrap(),
                &BundleId::from_str(BUNDLE).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::InProgress);
        assert_eq!(stored.evaluator_revision, 0);
    }

    #[tokio::test]
    async fn deletion_snapshot_fences_publication_before_external_action() {
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::new());
        let repository = InMemoryPaymentTaskRepository::with_deletion_fence(fence.clone());
        let admitted = repository.admit_payment_task(request()).await.unwrap();
        let now = repository.now();
        let claimed = repository
            .claim_next_payment_task("worker", now, now + time::Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
        let mut snapshot = claimed.task.snapshot;
        let state = snapshot.as_v1_mut();
        let accepted = state.accepted_at;
        let deadline = state.deadline_at;
        state
            .apply_create_response(
                accepted + time::Duration::seconds(1),
                accepted + time::Duration::seconds(60),
                deadline,
            )
            .unwrap();
        let sample = PaymentSampleV2 {
            observed_at: accepted + time::Duration::seconds(2),
            status: PaymentSampleStatusV2::Confirmed,
            outpoint: Some(BitcoinOutpointV2 {
                txid: "ab".repeat(32),
                vout: 1,
            }),
            confirmations: 1,
            amount_matched: true,
        };
        state
            .apply_status(PaymentProviderStatus {
                creator: state.creator.clone(),
                bundle_id: state.bundle_id.clone(),
                request_state: PaymentRequestStateV2::Accepted,
                payment_state: PaymentStateV2::Detected,
                invoice_created_at: accepted + time::Duration::seconds(1),
                proposal_expires_at: accepted + time::Duration::seconds(60),
                deadline,
                asset: PaymentAssetV2::Btc,
                minimum_confirmations: 1,
                current_sample: Some(sample.clone()),
                first_threshold_satisfied: Some(sample.clone()),
            })
            .unwrap();
        reduce_payment_snapshot(state, sample.observed_at).unwrap();
        let publishing = repository
            .persist_claimed_payment_snapshot(
                &admitted.task_id,
                "worker",
                &claimed.claim_token,
                now,
                0,
                snapshot,
            )
            .await
            .unwrap()
            .unwrap();
        let publication_token = publishing.entitlement_publication_token.unwrap();
        fence
            .records
            .write()
            .await
            .get_mut(&admitted.task_id)
            .unwrap()
            .deletion_job_id = Some(uuid::Uuid::new_v4());

        assert!(
            repository
                .acquire_payment_publication_action(
                    &admitted.task_id,
                    "worker",
                    &claimed.claim_token,
                    1,
                    publication_token,
                )
                .await
                .unwrap()
                .is_none()
        );
    }
}
