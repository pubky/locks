use std::sync::Arc;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, LockId, TaskId};
use time::OffsetDateTime;
use tokio::sync::OwnedMutexGuard;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedGraphTask, GraphRootState, GraphTaskRecord, GraphTaskSnapshot, VerificationTaskStatus,
};
use crate::application::ports::{GraphTaskClaimer, GraphTaskRepository};

use super::verification_task_deletion_fence::{
    InMemoryStoredGraphTask, InMemoryVerificationTaskDeletionFence,
    InMemoryVerificationTaskFenceRecord,
};

#[derive(Debug)]
pub struct InMemoryGraphTaskRepository {
    deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>,
}

impl Default for InMemoryGraphTaskRepository {
    fn default() -> Self {
        Self::with_deletion_fence(Arc::new(InMemoryVerificationTaskDeletionFence::new()))
    }
}

impl InMemoryGraphTaskRepository {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_deletion_fence(deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>) -> Self {
        Self { deletion_fence }
    }

    /// Graph-only records never own invoice-admission/payment intent state.
    pub async fn has_payment_intent(&self, _task_id: &TaskId) -> bool {
        false
    }

    pub(crate) async fn acquire_admission(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> OwnedMutexGuard<()> {
        self.deletion_fence
            .acquire_lock_admission(creator, lock_id)
            .await
    }

    pub(crate) fn authoritative_time(&self) -> OffsetDateTime {
        self.deletion_fence.authoritative_cutoff()
    }

    pub(crate) async fn acquire_handle_admission(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> OwnedMutexGuard<()> {
        self.deletion_fence
            .acquire_handle_admission(creator, bundle_id)
            .await
    }

    pub(crate) async fn has_graph_task_by_handle_under_admission(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> bool {
        self.deletion_fence
            .graph_records
            .read()
            .await
            .iter()
            .any(|stored| {
                let snapshot = stored.task.snapshot.as_v1();
                &snapshot.creator == creator && &snapshot.bundle_id == bundle_id
            })
    }

    pub(crate) async fn reserve_folded_auth_under_admission(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) {
        self.deletion_fence
            .folded_auth_reservations
            .write()
            .await
            .insert((creator.clone(), bundle_id.clone()));
    }

    pub(crate) async fn release_folded_auth_reservation(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) {
        self.deletion_fence
            .folded_auth_reservations
            .write()
            .await
            .remove(&(creator.clone(), bundle_id.clone()));
    }

    pub(crate) async fn insert_graph_task_before_publish<F>(
        &self,
        task: GraphTaskRecord,
        before_publish: F,
    ) -> Result<GraphTaskRecord, ApplicationError>
    where
        F: FnOnce(&GraphTaskRecord) -> Result<(), ApplicationError> + Send,
    {
        let identity = task.snapshot.as_v1();
        let lock_id = identity.pubky_lock_resource.lock_id().clone();
        let _guard = self
            .deletion_fence
            .acquire_lock_admission(&identity.creator, &lock_id)
            .await;
        self.insert_graph_task_before_publish_under_admission(task, before_publish, false)
            .await
    }

    pub(crate) async fn insert_graph_task_before_publish_under_admission<F>(
        &self,
        task: GraphTaskRecord,
        before_publish: F,
        folded_auth_source: bool,
    ) -> Result<GraphTaskRecord, ApplicationError>
    where
        F: FnOnce(&GraphTaskRecord) -> Result<(), ApplicationError> + Send,
    {
        validate_new_task(&task)?;
        let identity = task.snapshot.as_v1();
        let lock_id = identity.pubky_lock_resource.lock_id().clone();
        let reservation_key = (identity.creator.clone(), identity.bundle_id.clone());
        let mut reservations = self.deletion_fence.folded_auth_reservations.write().await;
        if reservations.contains(&reservation_key) != folded_auth_source {
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
        let mut current_statuses = self.deletion_fence.current_statuses.write().await;
        let mut records = self.deletion_fence.graph_records.write().await;
        if fence_records.contains_key(&task.task_id)
            && !records
                .iter()
                .any(|stored| stored.task.task_id == task.task_id)
        {
            return Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            });
        }
        if fence_records.iter().any(|(task_id, record)| {
            record.creator == identity.creator
                && record.bundle_id == identity.bundle_id
                && !records.iter().any(|stored| stored.task.task_id == *task_id)
        }) {
            return Err(ApplicationError::VerificationTaskConflict);
        }
        if let Some(existing) = records.iter().find(|stored| {
            let stored = stored.task.snapshot.as_v1();
            stored.creator == identity.creator && stored.bundle_id == identity.bundle_id
        }) {
            return if existing.task.task_id == task.task_id
                && ensure_same_binding(&existing.task.snapshot, &task.snapshot).is_ok()
            {
                Ok(existing.task.clone())
            } else {
                Err(ApplicationError::VerificationTaskConflict)
            };
        }
        if records
            .iter()
            .any(|stored| stored.task.task_id == task.task_id)
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
                paykit_admission_required: false,
                invoice_admission_ready: true,
                status: task.status,
                entitlement_publication_claim_token: None,
                deletion_job_id: None,
            },
        );
        current_statuses.insert(task.task_id, task.status);
        records.push(InMemoryStoredGraphTask {
            task: task.clone(),
            claimed_by: None,
            claim_token: None,
            claim_expires_at: None,
            next_attempt_at: None,
        });
        if folded_auth_source {
            reservations.remove(&reservation_key);
        }
        Ok(task)
    }
}

#[async_trait]
impl GraphTaskRepository for InMemoryGraphTaskRepository {
    async fn insert_graph_task(
        &self,
        task: GraphTaskRecord,
    ) -> Result<GraphTaskRecord, ApplicationError> {
        self.insert_graph_task_before_publish(task, |_| Ok(()))
            .await
    }

    async fn get_graph_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        Ok(self
            .deletion_fence
            .graph_records
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
impl GraphTaskClaimer for InMemoryGraphTaskRepository {
    async fn claim_next_graph_task(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedGraphTask>, ApplicationError> {
        if claim_expires_at <= now {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "graph task claim expiry must follow claim time".to_owned(),
            });
        }
        let fence = self.deletion_fence.records.read().await;
        let mut records = self.deletion_fence.graph_records.write().await;
        let Some(stored) = records.iter_mut().find(|stored| {
            matches!(
                stored.task.status,
                VerificationTaskStatus::Pending | VerificationTaskStatus::InProgress
            ) && stored.task.snapshot.as_v1().root == GraphRootState::Pending
                && stored.next_attempt_at.is_none_or(|due| due <= now)
                && stored
                    .claim_expires_at
                    .is_none_or(|claim_expires_at| claim_expires_at <= now)
                && fence
                    .get(&stored.task.task_id)
                    .is_none_or(|record| record.deletion_job_id.is_none())
        }) else {
            return Ok(None);
        };
        stored.task.status = VerificationTaskStatus::InProgress;
        stored.claimed_by = Some(worker_id.to_owned());
        let claim_token = uuid::Uuid::new_v4();
        stored.claim_token = Some(claim_token);
        stored.claim_expires_at = Some(claim_expires_at);
        stored.next_attempt_at = None;
        Ok(Some(ClaimedGraphTask {
            task: stored.task.clone(),
            claim_token,
        }))
    }

    async fn persist_claimed_graph_snapshot(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: GraphTaskSnapshot,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let fence = self.deletion_fence.records.read().await;
        let mut records = self.deletion_fence.graph_records.write().await;
        let Some(stored) = records.iter_mut().find(|stored| {
            stored.task.task_id == *task_id
                && stored.task.status == VerificationTaskStatus::InProgress
                && stored.task.evaluator_revision == expected_revision
                && stored.claimed_by.as_deref() == Some(worker_id)
                && stored.claim_token.as_ref() == Some(claim_token)
                && stored
                    .claim_expires_at
                    .is_some_and(|claim_expires_at| claim_expires_at > now)
                && fence
                    .get(task_id)
                    .is_none_or(|record| record.deletion_job_id.is_none())
        }) else {
            return Ok(None);
        };
        snapshot.as_v1().validate().map_err(invalid_snapshot)?;
        snapshot
            .as_v1()
            .validate_observations_not_after(now)
            .map_err(invalid_snapshot)?;
        ensure_same_binding(&stored.task.snapshot, &snapshot)?;
        snapshot
            .as_v1()
            .validate_progress_from(stored.task.snapshot.as_v1())
            .map_err(invalid_snapshot)?;
        if matches!(snapshot.as_v1().root, GraphRootState::Expired(_))
            && now < snapshot.as_v1().deadline_at
        {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "graph task cannot expire before its deadline".to_owned(),
            });
        }
        let next_revision =
            expected_revision
                .checked_add(1)
                .ok_or_else(|| ApplicationError::Storage {
                    message: "graph evaluator revision overflow".to_owned(),
                })?;
        let terminal = snapshot.as_v1().root != GraphRootState::Pending;
        stored.task.snapshot = snapshot;
        stored.task.evaluator_revision = next_revision;
        if terminal {
            stored.claimed_by = None;
            stored.claim_token = None;
            stored.claim_expires_at = None;
        }
        Ok(Some(stored.task.clone()))
    }

    async fn schedule_graph_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: GraphTaskSnapshot,
        next_attempt_at: time::OffsetDateTime,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let fence = self.deletion_fence.records.read().await;
        let mut records = self.deletion_fence.graph_records.write().await;
        let Some(stored) = records.iter_mut().find(|stored| {
            stored.task.task_id == *task_id
                && stored.task.status == VerificationTaskStatus::InProgress
                && stored.task.evaluator_revision == expected_revision
                && stored.claimed_by.as_deref() == Some(worker_id)
                && stored.claim_token.as_ref() == Some(claim_token)
                && stored
                    .claim_expires_at
                    .is_some_and(|claim_expires_at| claim_expires_at > now)
                && fence
                    .get(task_id)
                    .is_none_or(|record| record.deletion_job_id.is_none())
        }) else {
            return Ok(None);
        };
        if next_attempt_at <= now || next_attempt_at > snapshot.as_v1().deadline_at {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "graph retry must be after now and no later than deadline".to_owned(),
            });
        }
        snapshot.as_v1().validate().map_err(invalid_snapshot)?;
        snapshot
            .as_v1()
            .validate_observations_not_after(now)
            .map_err(invalid_snapshot)?;
        ensure_same_binding(&stored.task.snapshot, &snapshot)?;
        snapshot
            .as_v1()
            .validate_progress_from(stored.task.snapshot.as_v1())
            .map_err(invalid_snapshot)?;
        if snapshot.as_v1().root != GraphRootState::Pending {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "graph retry requires pending snapshot".to_owned(),
            });
        }
        let next_revision =
            expected_revision
                .checked_add(1)
                .ok_or_else(|| ApplicationError::Storage {
                    message: "graph evaluator revision overflow".to_owned(),
                })?;
        stored.task.snapshot = snapshot;
        stored.task.evaluator_revision = next_revision;
        stored.task.status = VerificationTaskStatus::Pending;
        stored.claimed_by = None;
        stored.claim_token = None;
        stored.claim_expires_at = None;
        stored.next_attempt_at = Some(next_attempt_at);
        Ok(Some(stored.task.clone()))
    }
}

fn validate_new_task(task: &GraphTaskRecord) -> Result<(), ApplicationError> {
    if task.status != VerificationTaskStatus::Pending || task.evaluator_revision != 0 {
        return Err(ApplicationError::InvalidVerificationTaskState {
            message: "new graph task must be pending at evaluator revision zero".to_owned(),
        });
    }
    task.snapshot
        .as_v1()
        .validate_pristine_admission()
        .map_err(invalid_snapshot)
}

fn ensure_same_binding(
    current: &GraphTaskSnapshot,
    next: &GraphTaskSnapshot,
) -> Result<(), ApplicationError> {
    let current = current.as_v1();
    let next = next.as_v1();
    if current.creator != next.creator
        || current.bundle_id != next.bundle_id
        || current.pubky_lock_resource != next.pubky_lock_resource
        || current.lock_hash != next.lock_hash
        || current.reader != next.reader
        || current.accepted_at != next.accepted_at
        || current.deadline_at != next.deadline_at
        || current.operator != next.operator
        || current
            .predicates
            .iter()
            .map(|state| &state.predicate)
            .ne(next.predicates.iter().map(|state| &state.predicate))
    {
        return Err(ApplicationError::InvalidVerificationTaskState {
            message: "graph snapshot replacement changed immutable task binding".to_owned(),
        });
    }
    Ok(())
}

fn invalid_snapshot(message: String) -> ApplicationError {
    ApplicationError::Storage {
        message: format!("invalid graph task snapshot: {message}"),
    }
}
