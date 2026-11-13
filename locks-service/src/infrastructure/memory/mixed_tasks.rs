use std::sync::Arc;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use tokio::sync::OwnedMutexGuard;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedMixedTask, MixedRootState, MixedTaskRecord, MixedTaskSnapshot, VerificationTaskStatus,
};
use crate::application::ports::{
    EvaluatorTaskKernel, MixedPublicationActionGuard, MixedTaskClaimer, MixedTaskRepository,
    build_evaluator_publication_with_path,
};

use super::evaluator_tasks::{
    MemoryEvaluatorTransition, StoredEvaluatorTask, claim_next, claimed_task_index,
    complete_publication as complete_evaluator_publication, persist_claimed, publication_is_live,
    release_claim, retry_claimed,
};
use super::verification_task_deletion_fence::{
    InMemoryVerificationTaskDeletionFence, InMemoryVerificationTaskFenceRecord,
};

#[derive(Debug, Clone)]
pub struct InMemoryMixedTaskRepository {
    deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>,
}

struct InMemoryMixedPublicationActionGuard {
    _guard: Option<OwnedMutexGuard<()>>,
    repository: InMemoryMixedTaskRepository,
    task_id: TaskId,
    worker_id: String,
    claim_token: uuid::Uuid,
    expected_revision: u64,
    publication_token: uuid::Uuid,
}

impl Default for InMemoryMixedTaskRepository {
    fn default() -> Self {
        Self::with_deletion_fence(Arc::new(InMemoryVerificationTaskDeletionFence::new()))
    }
}

impl InMemoryMixedTaskRepository {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_deletion_fence(deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>) -> Self {
        Self { deletion_fence }
    }
    fn now(&self) -> time::OffsetDateTime {
        self.deletion_fence.authoritative_cutoff()
    }

    pub(crate) async fn insert_mixed_task_before_publish_under_admission<F>(
        &self,
        task: MixedTaskRecord,
        before_publish: F,
    ) -> Result<MixedTaskRecord, ApplicationError>
    where
        F: FnOnce(&MixedTaskRecord) -> Result<(), ApplicationError> + Send,
    {
        task.snapshot
            .as_v1()
            .validate_pristine_admission()
            .map_err(invalid_state)?;
        if task.status != VerificationTaskStatus::Pending || task.evaluator_revision != 0 {
            return Err(invalid_state("new mixed task lifecycle is not pristine"));
        }
        let identity = task.snapshot.as_v1();
        let lock_id = identity.pubky_lock_resource.lock_id().clone();
        let reservation_key = (identity.creator.clone(), identity.bundle_id.clone());
        let mut reservations = self.deletion_fence.folded_auth_reservations.write().await;
        if !reservations.contains(&reservation_key) {
            return Err(ApplicationError::VerificationTaskConflict);
        }
        if self
            .deletion_fence
            .deletion_cutoffs
            .read()
            .await
            .contains(&(identity.creator.clone(), lock_id.clone()))
        {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        let mut fence_records = self.deletion_fence.records.write().await;
        let mut records = self.deletion_fence.mixed_records.write().await;
        let mut current_statuses = self.deletion_fence.current_statuses.write().await;
        if fence_records.values().any(|record| {
            record.creator == identity.creator && record.bundle_id == identity.bundle_id
        }) {
            return Err(ApplicationError::VerificationTaskConflict);
        }
        if records
            .iter()
            .any(|stored| stored.task.task_id == task.task_id)
            || fence_records.contains_key(&task.task_id)
        {
            return Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            });
        }
        before_publish(&task)?;
        fence_records.insert(
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
        current_statuses.insert(task.task_id, task.status);
        records.push(StoredEvaluatorTask::new(task.clone()));
        reservations.remove(&reservation_key);
        Ok(task)
    }
}

#[async_trait]
impl MixedTaskRepository for InMemoryMixedTaskRepository {
    async fn insert_mixed_task(
        &self,
        task: MixedTaskRecord,
    ) -> Result<MixedTaskRecord, ApplicationError> {
        task.snapshot
            .as_v1()
            .validate_pristine_admission()
            .map_err(invalid_state)?;
        if task.status != VerificationTaskStatus::Pending || task.evaluator_revision != 0 {
            return Err(invalid_state("new mixed task lifecycle is not pristine"));
        }
        let identity = task.snapshot.as_v1();
        let creator = identity.creator.clone();
        let bundle_id = identity.bundle_id.clone();
        let lock_id = identity.pubky_lock_resource.lock_id().clone();
        let _handle = self
            .deletion_fence
            .acquire_handle_admission(&creator, &bundle_id)
            .await;
        let _lock = self
            .deletion_fence
            .acquire_lock_admission(&creator, &lock_id)
            .await;

        if let Some(existing) =
            self.deletion_fence
                .mixed_records
                .read()
                .await
                .iter()
                .find(|stored| {
                    let value = stored.task.snapshot.as_v1();
                    value.creator == creator && value.bundle_id == bundle_id
                })
        {
            if existing.task.snapshot.as_v1().submitted_proof_bundle
                == identity.submitted_proof_bundle
                && existing.task.snapshot.as_v1().lock_hash == identity.lock_hash
                && existing.task.snapshot.as_v1().reader == identity.reader
                && existing.task.snapshot.as_v1().graph.operator == identity.graph.operator
                && existing
                    .task
                    .snapshot
                    .as_v1()
                    .graph
                    .predicates
                    .iter()
                    .map(|v| &v.predicate)
                    .eq(identity.graph.predicates.iter().map(|v| &v.predicate))
                && existing.task.snapshot.as_v1().payment.payment == identity.payment.payment
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
        self.deletion_fence.records.write().await.insert(
            task.task_id,
            InMemoryVerificationTaskFenceRecord {
                creator,
                lock_id,
                bundle_id,
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
        self.deletion_fence
            .mixed_records
            .write()
            .await
            .push(StoredEvaluatorTask::new(task.clone()));
        Ok(task)
    }

    async fn get_mixed_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError> {
        Ok(self
            .deletion_fence
            .mixed_records
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
impl MixedTaskClaimer for InMemoryMixedTaskRepository {
    async fn claim_next_mixed_task(
        &self,
        worker_id: &str,
        caller_now: time::OffsetDateTime,
        requested_expiry: time::OffsetDateTime,
    ) -> Result<Option<ClaimedMixedTask>, ApplicationError> {
        let ttl = EvaluatorTaskKernel::claim_ttl(caller_now, requested_expiry)?;
        let fence = self.deletion_fence.records.read().await;
        let mut records = self.deletion_fence.mixed_records.write().await;
        let now = self.now();
        Ok(claim_next(&mut records, worker_id, now, ttl, |id| {
            fence.get(&id).is_some_and(|v| v.deletion_job_id.is_some())
        })
        .map(|stored| ClaimedMixedTask {
            task: stored.task,
            claim_token: stored.claim_token.expect("shared claim kernel sets token"),
        }))
    }

    async fn persist_claimed_mixed_snapshot(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _caller_now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: MixedTaskSnapshot,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError> {
        let mut fence = self.deletion_fence.records.write().await;
        let mut records = self.deletion_fence.mixed_records.write().await;
        let now = self.now();
        let Some(index) = claimed_task_index(
            &records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            now,
            |id| fence.get(&id).is_some_and(|v| v.deletion_job_id.is_some()),
        ) else {
            return Ok(None);
        };
        snapshot
            .as_v1()
            .validate_progress_from(records[index].task.snapshot.as_v1())
            .map_err(invalid_state)?;
        let root = snapshot.as_v1().root;
        let reason = root.terminal_reason();
        let (status, publication, token, completed_at, retain_claim) = match root {
            MixedRootState::Pending => (VerificationTaskStatus::InProgress, None, None, None, true),
            MixedRootState::Satisfied => {
                let publication = build_evaluator_publication_with_path(
                    &snapshot.as_v1().identity(),
                    snapshot.as_v1().satisfied_by.clone(),
                    now,
                )?;
                (
                    VerificationTaskStatus::PublishingEntitlement,
                    Some(publication),
                    Some(uuid::Uuid::new_v4()),
                    None,
                    true,
                )
            }
            MixedRootState::Expired(_) => (
                VerificationTaskStatus::Expired,
                None,
                None,
                Some(now),
                false,
            ),
            MixedRootState::Failed(_) => {
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
                publication_token: token,
                completed_at,
                retain_claim,
            },
            |id| fence.get(&id).is_some_and(|v| v.deletion_job_id.is_some()),
        )?;
        if let Some(stored) = records.iter_mut().find(|v| v.task.task_id == *task_id) {
            stored.task.terminal_reason = reason;
        }
        if let Some(updated) = updated {
            let mut updated = updated;
            updated.terminal_reason = reason;
            if let Some(record) = fence.get_mut(task_id) {
                record.status = updated.status;
                record.entitlement_publication_claim_token = updated.entitlement_publication_token;
            }
            self.deletion_fence
                .current_statuses
                .write()
                .await
                .insert(*task_id, updated.status);
            return Ok(Some(updated));
        }
        Ok(None)
    }

    async fn schedule_mixed_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        caller_now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: MixedTaskSnapshot,
        requested_next: time::OffsetDateTime,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError> {
        let mut fence = self.deletion_fence.records.write().await;
        let mut records = self.deletion_fence.mixed_records.write().await;
        let now = self.now();
        let Some(index) = claimed_task_index(
            &records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            now,
            |id| fence.get(&id).is_some_and(|v| v.deletion_job_id.is_some()),
        ) else {
            return Ok(None);
        };
        snapshot
            .as_v1()
            .validate_progress_from(records[index].task.snapshot.as_v1())
            .map_err(invalid_state)?;
        if snapshot.as_v1().root != MixedRootState::Pending {
            return Err(invalid_state("mixed retry requires pending root"));
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
            snapshot,
            |id| fence.get(&id).is_some_and(|v| v.deletion_job_id.is_some()),
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

    async fn release_claimed_mixed_task(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError> {
        let fence = self.deletion_fence.records.read().await;
        let mut records = self.deletion_fence.mixed_records.write().await;
        Ok(release_claim(
            &mut records,
            task_id,
            worker_id,
            claim_token,
            |id| fence.get(&id).is_some_and(|v| v.deletion_job_id.is_some()),
        ))
    }

    async fn acquire_mixed_publication_action(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<Box<dyn MixedPublicationActionGuard>>, ApplicationError> {
        let identity = self
            .deletion_fence
            .mixed_records
            .read()
            .await
            .iter()
            .find(|v| v.task.task_id == *task_id)
            .map(|v| {
                let s = v.task.snapshot.as_v1();
                (s.creator.clone(), s.pubky_lock_resource.lock_id().clone())
            });
        let Some((creator, lock_id)) = identity else {
            return Ok(None);
        };
        let guard = self
            .deletion_fence
            .acquire_lock_admission(&creator, &lock_id)
            .await;
        let fence = self.deletion_fence.records.read().await;
        let records = self.deletion_fence.mixed_records.read().await;
        let now = self.now();
        if !publication_is_live(
            &records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            publication_token,
            now,
            |id| fence.get(&id).is_some_and(|v| v.deletion_job_id.is_some()),
        ) {
            return Ok(None);
        }
        Ok(Some(Box::new(InMemoryMixedPublicationActionGuard {
            _guard: Some(guard),
            repository: self.clone(),
            task_id: *task_id,
            worker_id: worker_id.to_owned(),
            claim_token: *claim_token,
            expected_revision,
            publication_token,
        })))
    }

    async fn complete_claimed_mixed_publication(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError> {
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
impl MixedPublicationActionGuard for InMemoryMixedPublicationActionGuard {
    async fn complete(&mut self) -> Result<Option<MixedTaskRecord>, ApplicationError> {
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
    repository: &InMemoryMixedTaskRepository,
    task_id: &TaskId,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    expected_revision: u64,
    publication_token: uuid::Uuid,
) -> Result<Option<MixedTaskRecord>, ApplicationError> {
    let mut fence = repository.deletion_fence.records.write().await;
    let mut records = repository.deletion_fence.mixed_records.write().await;
    let now = repository.now();
    let updated = complete_evaluator_publication(
        &mut records,
        task_id,
        worker_id,
        claim_token,
        expected_revision,
        publication_token,
        now,
        |id| fence.get(&id).is_some_and(|v| v.deletion_job_id.is_some()),
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
