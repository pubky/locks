use std::sync::Arc;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, LockId, TaskId};
use time::OffsetDateTime;
use tokio::sync::OwnedMutexGuard;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedGraphTask, GraphRootState, GraphTaskRecord, GraphTaskSnapshot, VerificationTaskStatus,
};
use crate::application::ports::{
    EvaluatorMode, EvaluatorTaskKernel, GraphPublicationActionGuard, GraphTaskClaimer,
    GraphTaskRepository, build_evaluator_publication,
};

use super::evaluator_tasks::{
    MemoryEvaluatorTransition, StoredEvaluatorTask, claim_next, claimed_task_index,
    complete_publication, persist_claimed, publication_is_live, release_claim, retry_claimed,
};
use super::verification_task_deletion_fence::{
    InMemoryVerificationTaskDeletionFence, InMemoryVerificationTaskFenceRecord,
};

#[derive(Debug, Clone)]
pub struct InMemoryGraphTaskRepository {
    deletion_fence: Arc<InMemoryVerificationTaskDeletionFence>,
}

struct InMemoryGraphPublicationActionGuard {
    guard: Option<OwnedMutexGuard<()>>,
    repository: InMemoryGraphTaskRepository,
    task_id: TaskId,
    worker_id: String,
    claim_token: uuid::Uuid,
    expected_revision: u64,
    publication_token: uuid::Uuid,
}

#[async_trait]
impl GraphPublicationActionGuard for InMemoryGraphPublicationActionGuard {
    async fn complete(&mut self) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        self.repository
            .complete_claimed_graph_publication(
                &self.task_id,
                &self.worker_id,
                &self.claim_token,
                self.repository.authoritative_time(),
                self.expected_revision,
                self.publication_token,
            )
            .await
    }

    async fn release(mut self: Box<Self>) -> Result<(), ApplicationError> {
        self.guard.take();
        Ok(())
    }
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

    pub(crate) fn shared_deletion_fence(&self) -> Arc<InMemoryVerificationTaskDeletionFence> {
        self.deletion_fence.clone()
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
            .records
            .read()
            .await
            .values()
            .any(|record| &record.creator == creator && &record.bundle_id == bundle_id)
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
        records.push(StoredEvaluatorTask::new(task.clone()));
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
        caller_now: time::OffsetDateTime,
        requested_claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedGraphTask>, ApplicationError> {
        let claim_ttl = EvaluatorTaskKernel::claim_ttl(caller_now, requested_claim_expires_at)
            .map_err(|_| ApplicationError::InvalidVerificationTaskState {
                message: "graph task claim expiry must follow claim time".to_owned(),
            })?;
        let fence = self.deletion_fence.records.read().await;
        let mut records = self.deletion_fence.graph_records.write().await;
        let stored = claim_next(
            &mut records,
            worker_id,
            self.authoritative_time(),
            claim_ttl,
            |task_id| {
                fence
                    .get(&task_id)
                    .is_some_and(|record| record.deletion_job_id.is_some())
            },
        );
        Ok(stored.map(|stored| ClaimedGraphTask {
            task: stored.task,
            claim_token: stored.claim_token.expect("shared claim kernel sets token"),
        }))
    }

    async fn persist_claimed_graph_snapshot(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _caller_now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: GraphTaskSnapshot,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let mut fence = self.deletion_fence.records.write().await;
        let mut current_statuses = self.deletion_fence.current_statuses.write().await;
        let mut records = self.deletion_fence.graph_records.write().await;
        let now = self.authoritative_time();
        let fenced = |id: TaskId| {
            fence
                .get(&id)
                .is_some_and(|record| record.deletion_job_id.is_some())
        };
        let Some(index) = claimed_task_index(
            &records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            now,
            fenced,
        ) else {
            return Ok(None);
        };
        let current = &records[index].task;
        snapshot.as_v1().validate().map_err(invalid_snapshot)?;
        snapshot
            .as_v1()
            .validate_observations_not_after(now)
            .map_err(invalid_snapshot)?;
        ensure_same_binding(&current.snapshot, &snapshot)?;
        snapshot
            .as_v1()
            .validate_progress_from(current.snapshot.as_v1())
            .map_err(invalid_snapshot)?;
        let root = snapshot.as_v1().root;
        if matches!(root, GraphRootState::Expired(_)) && now < snapshot.as_v1().deadline_at {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "graph task cannot expire before its deadline".to_owned(),
            });
        }
        let (status, publication, publication_token, completed_at, retain_claim) = match root {
            GraphRootState::Pending => (VerificationTaskStatus::InProgress, None, None, None, true),
            GraphRootState::Satisfied => {
                let publication = build_evaluator_publication(
                    &snapshot.as_v1().identity(),
                    EvaluatorMode::Graph,
                    now,
                )
                .map_err(|error| ApplicationError::Storage {
                    message: format!("invalid graph entitlement publication: {error}"),
                })?;
                (
                    VerificationTaskStatus::PublishingEntitlement,
                    Some(publication),
                    Some(uuid::Uuid::new_v4()),
                    None,
                    true,
                )
            }
            GraphRootState::Expired(_) | GraphRootState::FailedInvalidResponse => (
                root.terminal_status().expect("terminal root has status"),
                None,
                None,
                Some(now),
                false,
            ),
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
            current_statuses.insert(*task_id, updated.status);
            if let Some(record) = fence.get_mut(task_id) {
                record.status = updated.status;
                record.entitlement_publication_claim_token = updated.entitlement_publication_token;
            }
        }
        Ok(updated)
    }

    async fn schedule_graph_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        caller_now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: GraphTaskSnapshot,
        requested_next_attempt_at: time::OffsetDateTime,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let retry_delay = requested_next_attempt_at - caller_now;
        let fence = self.deletion_fence.records.read().await;
        let mut records = self.deletion_fence.graph_records.write().await;
        let now = self.authoritative_time();
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
        let next_attempt_at = now + retry_delay;
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
        ensure_same_binding(&current.snapshot, &snapshot)?;
        snapshot
            .as_v1()
            .validate_progress_from(current.snapshot.as_v1())
            .map_err(invalid_snapshot)?;
        if snapshot.as_v1().root != GraphRootState::Pending {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "graph retry requires pending snapshot".to_owned(),
            });
        }
        retry_claimed(
            &mut records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            now,
            next_attempt_at,
            snapshot,
            |id| {
                fence
                    .get(&id)
                    .is_some_and(|record| record.deletion_job_id.is_some())
            },
        )
    }

    async fn release_claimed_graph_task(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError> {
        let mut current_statuses = self.deletion_fence.current_statuses.write().await;
        let mut records = self.deletion_fence.graph_records.write().await;
        let released = release_claim(&mut records, task_id, worker_id, claim_token, |_| false);
        if released {
            let status = records
                .iter()
                .find(|stored| stored.task.task_id == *task_id)
                .expect("released task remains stored")
                .task
                .status;
            current_statuses.insert(*task_id, status);
        }
        Ok(released)
    }

    async fn acquire_graph_publication_action(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<Box<dyn GraphPublicationActionGuard>>, ApplicationError> {
        let identity = {
            let records = self.deletion_fence.graph_records.read().await;
            let Some(stored) = records
                .iter()
                .find(|stored| stored.task.task_id == *task_id)
            else {
                return Ok(None);
            };
            let snapshot = stored.task.snapshot.as_v1();
            (
                snapshot.creator.clone(),
                snapshot.pubky_lock_resource.lock_id().clone(),
            )
        };
        let guard = self
            .deletion_fence
            .acquire_lock_admission(&identity.0, &identity.1)
            .await;
        if self
            .deletion_fence
            .deletion_cutoffs
            .read()
            .await
            .contains(&identity)
        {
            return Ok(None);
        }
        let now = self.authoritative_time();
        let fence = self.deletion_fence.records.read().await;
        let records = self.deletion_fence.graph_records.read().await;
        let live = publication_is_live(
            &records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            publication_token,
            now,
            |id| {
                fence.get(&id).is_none_or(|record| {
                    record.deletion_job_id.is_some()
                        || record.entitlement_publication_claim_token != Some(publication_token)
                })
            },
        );
        if !live {
            drop(guard);
            return Ok(None);
        }
        Ok(Some(Box::new(InMemoryGraphPublicationActionGuard {
            guard: Some(guard),
            repository: self.clone(),
            task_id: *task_id,
            worker_id: worker_id.to_owned(),
            claim_token: *claim_token,
            expected_revision,
            publication_token,
        })))
    }

    async fn complete_claimed_graph_publication(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _caller_now: time::OffsetDateTime,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let mut fence = self.deletion_fence.records.write().await;
        let mut current_statuses = self.deletion_fence.current_statuses.write().await;
        let mut records = self.deletion_fence.graph_records.write().await;
        let now = self.authoritative_time();
        let updated = complete_publication(
            &mut records,
            task_id,
            worker_id,
            claim_token,
            expected_revision,
            publication_token,
            now,
            |id| {
                fence.get(&id).is_none_or(|record| {
                    record.deletion_job_id.is_some()
                        || record.entitlement_publication_claim_token != Some(publication_token)
                })
            },
        );
        if updated.is_some() {
            current_statuses.insert(*task_id, VerificationTaskStatus::Completed);
            if let Some(record) = fence.get_mut(task_id) {
                record.status = VerificationTaskStatus::Completed;
                record.entitlement_publication_claim_token = None;
            }
        }
        Ok(updated)
    }
}

fn validate_new_task(task: &GraphTaskRecord) -> Result<(), ApplicationError> {
    if task.status != VerificationTaskStatus::Pending
        || task.evaluator_revision != 0
        || task.entitlement_to_publish.is_some()
        || task.entitlement_publication_token.is_some()
        || task.completed_at.is_some()
    {
        return Err(ApplicationError::InvalidVerificationTaskState {
            message: "new graph task must be pristine and pending at evaluator revision zero"
                .to_owned(),
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

#[cfg(test)]
mod post_lock_time_tests {
    use std::{
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
    use locks_core::lock_policy::{GraphParticipantV2, GraphPolicyV2, GraphPredicateV2};
    use time::macros::datetime;

    use super::*;
    use crate::application::ports::Clock;

    const NOW: OffsetDateTime = datetime!(2026-10-01 12:00:00 UTC);

    #[derive(Debug)]
    struct MutableClock(Mutex<OffsetDateTime>);

    impl MutableClock {
        fn set(&self, now: OffsetDateTime) {
            *self.0.lock().unwrap() = now;
        }
    }

    impl Clock for MutableClock {
        fn now(&self) -> OffsetDateTime {
            *self.0.lock().unwrap()
        }
    }

    fn task() -> GraphTaskRecord {
        const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
        const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let lock_id = LockId::from_hash(LockHash::from_bytes([0; 32]));
        GraphTaskRecord {
            task_id: TaskId::from_str("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10").unwrap(),
            snapshot: GraphTaskSnapshot::V1(
                crate::application::models::GraphTaskSnapshotV1::new(
                    creator,
                    BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
                    PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{lock_id}.json"))
                        .unwrap(),
                    [0; 32],
                    CreatorPubky::from_str(READER).unwrap(),
                    NOW,
                    300,
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

    #[tokio::test]
    async fn claim_ttl_starts_after_decisive_lock_is_acquired() {
        let clock = Arc::new(MutableClock(Mutex::new(NOW)));
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(
            clock.clone(),
        ));
        let repository = Arc::new(InMemoryGraphTaskRepository::with_deletion_fence(
            fence.clone(),
        ));
        repository.insert_graph_task(task()).await.unwrap();

        let records_guard = fence.graph_records.write().await;
        let blocked = {
            let repository = repository.clone();
            tokio::spawn(async move {
                repository
                    .claim_next_graph_task("worker-a", NOW, NOW + time::Duration::seconds(30))
                    .await
                    .unwrap()
                    .unwrap()
            })
        };
        tokio::task::yield_now().await;
        clock.set(NOW + time::Duration::minutes(1));
        drop(records_guard);
        let first = blocked.await.unwrap();

        assert!(
            repository
                .claim_next_graph_task(
                    "worker-b",
                    NOW + time::Duration::minutes(1),
                    NOW + time::Duration::minutes(2),
                )
                .await
                .unwrap()
                .is_none(),
            "post-lock claim must retain its full TTL instead of being born expired"
        );
        assert_eq!(first.task.status, VerificationTaskStatus::InProgress);
    }
}
