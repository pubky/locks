use locks_core::ids::TaskId;
use locks_core::verification::VerifiedProofBundleV2;
use time::{Duration, OffsetDateTime};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    GraphTaskRecord, GraphTaskSnapshot, MixedTaskRecord, MixedTaskSnapshot, PaymentTaskRecord,
    PaymentTaskSnapshot, VerificationTaskStatus,
};
use crate::application::ports::EvaluatorTaskKernel;

/// Mode adapter used by one in-memory claim/CAS/retry/publication kernel.
pub(crate) trait MemoryEvaluatorTask: Clone {
    type Snapshot;

    fn task_id(&self) -> TaskId;
    fn revision(&self) -> u64;
    fn set_revision(&mut self, revision: u64);
    fn status(&self) -> VerificationTaskStatus;
    fn set_status(&mut self, status: VerificationTaskStatus);
    fn root_pending(&self) -> bool;
    fn root_satisfied(&self) -> bool;
    fn set_snapshot(&mut self, snapshot: Self::Snapshot);
    fn set_publication(
        &mut self,
        publication: Option<VerifiedProofBundleV2>,
        token: Option<uuid::Uuid>,
    );
    fn publication_token(&self) -> Option<uuid::Uuid>;
    fn clear_publication_token(&mut self);
    fn has_publication(&self) -> bool;
    fn set_completed_at(&mut self, completed_at: Option<OffsetDateTime>);
}

impl MemoryEvaluatorTask for GraphTaskRecord {
    type Snapshot = GraphTaskSnapshot;

    fn task_id(&self) -> TaskId {
        self.task_id
    }
    fn revision(&self) -> u64 {
        self.evaluator_revision
    }
    fn set_revision(&mut self, revision: u64) {
        self.evaluator_revision = revision;
    }
    fn status(&self) -> VerificationTaskStatus {
        self.status
    }
    fn set_status(&mut self, status: VerificationTaskStatus) {
        self.status = status;
    }
    fn root_pending(&self) -> bool {
        self.snapshot.as_v1().root == crate::application::models::GraphRootState::Pending
    }
    fn root_satisfied(&self) -> bool {
        self.snapshot.as_v1().root == crate::application::models::GraphRootState::Satisfied
    }
    fn set_snapshot(&mut self, snapshot: Self::Snapshot) {
        self.snapshot = snapshot;
    }
    fn set_publication(
        &mut self,
        publication: Option<VerifiedProofBundleV2>,
        token: Option<uuid::Uuid>,
    ) {
        self.entitlement_to_publish = publication;
        self.entitlement_publication_token = token;
    }
    fn publication_token(&self) -> Option<uuid::Uuid> {
        self.entitlement_publication_token
    }
    fn clear_publication_token(&mut self) {
        self.entitlement_publication_token = None;
    }
    fn has_publication(&self) -> bool {
        self.entitlement_to_publish.is_some()
    }
    fn set_completed_at(&mut self, completed_at: Option<OffsetDateTime>) {
        self.completed_at = completed_at;
    }
}

impl MemoryEvaluatorTask for PaymentTaskRecord {
    type Snapshot = PaymentTaskSnapshot;

    fn task_id(&self) -> TaskId {
        self.task_id
    }
    fn revision(&self) -> u64 {
        self.evaluator_revision
    }
    fn set_revision(&mut self, revision: u64) {
        self.evaluator_revision = revision;
    }
    fn status(&self) -> VerificationTaskStatus {
        self.status
    }
    fn set_status(&mut self, status: VerificationTaskStatus) {
        self.status = status;
    }
    fn root_pending(&self) -> bool {
        self.snapshot.as_v1().root == crate::application::models::PaymentRootState::Pending
    }
    fn root_satisfied(&self) -> bool {
        self.snapshot.as_v1().root == crate::application::models::PaymentRootState::Satisfied
    }
    fn set_snapshot(&mut self, snapshot: Self::Snapshot) {
        self.snapshot = snapshot;
    }
    fn set_publication(
        &mut self,
        publication: Option<VerifiedProofBundleV2>,
        token: Option<uuid::Uuid>,
    ) {
        self.entitlement_to_publish = publication;
        self.entitlement_publication_token = token;
    }
    fn publication_token(&self) -> Option<uuid::Uuid> {
        self.entitlement_publication_token
    }
    fn clear_publication_token(&mut self) {
        self.entitlement_publication_token = None;
    }
    fn has_publication(&self) -> bool {
        self.entitlement_to_publish.is_some()
    }
    fn set_completed_at(&mut self, completed_at: Option<OffsetDateTime>) {
        self.completed_at = completed_at;
    }
}

impl MemoryEvaluatorTask for MixedTaskRecord {
    type Snapshot = MixedTaskSnapshot;

    fn task_id(&self) -> TaskId {
        self.task_id
    }
    fn revision(&self) -> u64 {
        self.evaluator_revision
    }
    fn set_revision(&mut self, revision: u64) {
        self.evaluator_revision = revision;
    }
    fn status(&self) -> VerificationTaskStatus {
        self.status
    }
    fn set_status(&mut self, status: VerificationTaskStatus) {
        self.status = status;
    }
    fn root_pending(&self) -> bool {
        self.snapshot.as_v1().root == crate::application::models::MixedRootState::Pending
    }
    fn root_satisfied(&self) -> bool {
        self.snapshot.as_v1().root == crate::application::models::MixedRootState::Satisfied
    }
    fn set_snapshot(&mut self, snapshot: Self::Snapshot) {
        self.snapshot = snapshot;
    }
    fn set_publication(
        &mut self,
        publication: Option<VerifiedProofBundleV2>,
        token: Option<uuid::Uuid>,
    ) {
        self.entitlement_to_publish = publication;
        self.entitlement_publication_token = token;
    }
    fn publication_token(&self) -> Option<uuid::Uuid> {
        self.entitlement_publication_token
    }
    fn clear_publication_token(&mut self) {
        self.entitlement_publication_token = None;
    }
    fn has_publication(&self) -> bool {
        self.entitlement_to_publish.is_some()
    }
    fn set_completed_at(&mut self, completed_at: Option<OffsetDateTime>) {
        self.completed_at = completed_at;
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StoredEvaluatorTask<T> {
    pub(crate) task: T,
    pub(crate) claimed_by: Option<String>,
    pub(crate) claim_token: Option<uuid::Uuid>,
    pub(crate) claim_expires_at: Option<OffsetDateTime>,
    pub(crate) next_attempt_at: Option<OffsetDateTime>,
}

impl<T> StoredEvaluatorTask<T> {
    pub(crate) fn new(task: T) -> Self {
        Self {
            task,
            claimed_by: None,
            claim_token: None,
            claim_expires_at: None,
            next_attempt_at: None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MemoryEvaluatorTransition {
    pub(crate) status: VerificationTaskStatus,
    pub(crate) publication: Option<VerifiedProofBundleV2>,
    pub(crate) publication_token: Option<uuid::Uuid>,
    pub(crate) completed_at: Option<OffsetDateTime>,
    pub(crate) retain_claim: bool,
}

pub(crate) fn claim_next<T, F>(
    records: &mut [StoredEvaluatorTask<T>],
    worker_id: &str,
    now: OffsetDateTime,
    claim_ttl: Duration,
    deletion_fenced: F,
) -> Option<StoredEvaluatorTask<T>>
where
    T: MemoryEvaluatorTask,
    F: Fn(TaskId) -> bool,
{
    let stored = records.iter_mut().find(|stored| {
        !deletion_fenced(stored.task.task_id())
            && EvaluatorTaskKernel::claim_is_available(
                stored.task.status(),
                stored.task.root_pending(),
                stored.task.root_satisfied(),
                stored.next_attempt_at,
                stored.claim_expires_at,
                now,
            )
    })?;
    if stored.task.status() != VerificationTaskStatus::PublishingEntitlement {
        stored.task.set_status(VerificationTaskStatus::InProgress);
    }
    stored.claimed_by = Some(worker_id.to_owned());
    stored.claim_token = Some(uuid::Uuid::new_v4());
    stored.claim_expires_at = Some(now + claim_ttl);
    stored.next_attempt_at = None;
    Some(stored.clone())
}

pub(crate) fn claimed_task_index<T, F>(
    records: &[StoredEvaluatorTask<T>],
    task_id: &TaskId,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    expected_revision: u64,
    now: OffsetDateTime,
    deletion_fenced: F,
) -> Option<usize>
where
    T: MemoryEvaluatorTask,
    F: Fn(TaskId) -> bool,
{
    records.iter().position(|stored| {
        stored.task.task_id() == *task_id
            && !deletion_fenced(*task_id)
            && EvaluatorTaskKernel::claim_binding_is_live(
                stored.task.revision(),
                expected_revision,
                stored.claimed_by.as_deref(),
                worker_id,
                stored.claim_token.as_ref(),
                claim_token,
                stored.claim_expires_at,
                now,
            )
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn persist_claimed<T, F>(
    records: &mut [StoredEvaluatorTask<T>],
    task_id: &TaskId,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    expected_revision: u64,
    now: OffsetDateTime,
    snapshot: T::Snapshot,
    transition: MemoryEvaluatorTransition,
    deletion_fenced: F,
) -> Result<Option<T>, ApplicationError>
where
    T: MemoryEvaluatorTask,
    F: Fn(TaskId) -> bool,
{
    let Some(index) = claimed_task_index(
        records,
        task_id,
        worker_id,
        claim_token,
        expected_revision,
        now,
        deletion_fenced,
    ) else {
        return Ok(None);
    };
    let stored = &mut records[index];
    if stored.task.status() != VerificationTaskStatus::InProgress {
        return Ok(None);
    }
    stored.task.set_snapshot(snapshot);
    stored
        .task
        .set_revision(EvaluatorTaskKernel::next_revision(expected_revision)?);
    stored.task.set_status(transition.status);
    stored
        .task
        .set_publication(transition.publication, transition.publication_token);
    stored.task.set_completed_at(transition.completed_at);
    stored.next_attempt_at = None;
    if !transition.retain_claim {
        clear_claim(stored);
    }
    Ok(Some(stored.task.clone()))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn retry_claimed<T, F>(
    records: &mut [StoredEvaluatorTask<T>],
    task_id: &TaskId,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    expected_revision: u64,
    now: OffsetDateTime,
    due: OffsetDateTime,
    snapshot: T::Snapshot,
    deletion_fenced: F,
) -> Result<Option<T>, ApplicationError>
where
    T: MemoryEvaluatorTask,
    F: Fn(TaskId) -> bool,
{
    let Some(index) = claimed_task_index(
        records,
        task_id,
        worker_id,
        claim_token,
        expected_revision,
        now,
        deletion_fenced,
    ) else {
        return Ok(None);
    };
    let stored = &mut records[index];
    if stored.task.status() != VerificationTaskStatus::InProgress {
        return Ok(None);
    }
    stored.task.set_snapshot(snapshot);
    stored
        .task
        .set_revision(EvaluatorTaskKernel::next_revision(expected_revision)?);
    stored.task.set_status(VerificationTaskStatus::Pending);
    clear_claim(stored);
    stored.next_attempt_at = Some(due);
    Ok(Some(stored.task.clone()))
}

pub(crate) fn release_claim<T, F>(
    records: &mut [StoredEvaluatorTask<T>],
    task_id: &TaskId,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    deletion_fenced: F,
) -> bool
where
    T: MemoryEvaluatorTask,
    F: Fn(TaskId) -> bool,
{
    let Some(stored) = records.iter_mut().find(|stored| {
        stored.task.task_id() == *task_id
            && stored.claimed_by.as_deref() == Some(worker_id)
            && stored.claim_token.as_ref() == Some(claim_token)
            && !deletion_fenced(*task_id)
    }) else {
        return false;
    };
    if stored.task.status() == VerificationTaskStatus::InProgress && stored.task.root_pending() {
        stored.task.set_status(VerificationTaskStatus::Pending);
    }
    clear_claim(stored);
    true
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn publication_is_live<T, F>(
    records: &[StoredEvaluatorTask<T>],
    task_id: &TaskId,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    expected_revision: u64,
    publication_token: uuid::Uuid,
    now: OffsetDateTime,
    deletion_fenced: F,
) -> bool
where
    T: MemoryEvaluatorTask,
    F: Fn(TaskId) -> bool,
{
    claimed_task_index(
        records,
        task_id,
        worker_id,
        claim_token,
        expected_revision,
        now,
        deletion_fenced,
    )
    .is_some_and(|index| {
        let task = &records[index].task;
        task.status() == VerificationTaskStatus::PublishingEntitlement
            && task.publication_token() == Some(publication_token)
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn complete_publication<T, F>(
    records: &mut [StoredEvaluatorTask<T>],
    task_id: &TaskId,
    worker_id: &str,
    claim_token: &uuid::Uuid,
    expected_revision: u64,
    publication_token: uuid::Uuid,
    now: OffsetDateTime,
    deletion_fenced: F,
) -> Option<T>
where
    T: MemoryEvaluatorTask,
    F: Fn(TaskId) -> bool,
{
    let index = claimed_task_index(
        records,
        task_id,
        worker_id,
        claim_token,
        expected_revision,
        now,
        deletion_fenced,
    )?;
    let stored = &mut records[index];
    if stored.task.status() != VerificationTaskStatus::PublishingEntitlement
        || !stored.task.has_publication()
        || stored.task.publication_token() != Some(publication_token)
    {
        return None;
    }
    stored.task.set_status(VerificationTaskStatus::Completed);
    stored.task.set_completed_at(Some(now));
    stored.task.clear_publication_token();
    clear_claim(stored);
    stored.next_attempt_at = None;
    Some(stored.task.clone())
}

fn clear_claim<T>(stored: &mut StoredEvaluatorTask<T>) {
    stored.claimed_by = None;
    stored.claim_token = None;
    stored.claim_expires_at = None;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::models::VerificationTaskStatus;
    use locks_core::ids::TaskId;
    use std::str::FromStr;
    use time::macros::datetime;

    #[derive(Debug, Clone)]
    struct FakeTask {
        task_id: TaskId,
        revision: u64,
        status: VerificationTaskStatus,
        pending: bool,
        satisfied: bool,
    }

    impl MemoryEvaluatorTask for FakeTask {
        type Snapshot = (bool, bool);

        fn task_id(&self) -> TaskId {
            self.task_id
        }
        fn revision(&self) -> u64 {
            self.revision
        }
        fn set_revision(&mut self, revision: u64) {
            self.revision = revision;
        }
        fn status(&self) -> VerificationTaskStatus {
            self.status
        }
        fn set_status(&mut self, status: VerificationTaskStatus) {
            self.status = status;
        }
        fn root_pending(&self) -> bool {
            self.pending
        }
        fn root_satisfied(&self) -> bool {
            self.satisfied
        }
        fn set_snapshot(&mut self, snapshot: Self::Snapshot) {
            self.pending = snapshot.0;
            self.satisfied = snapshot.1;
        }
        fn set_publication(
            &mut self,
            _publication: Option<VerifiedProofBundleV2>,
            _token: Option<uuid::Uuid>,
        ) {
        }
        fn publication_token(&self) -> Option<uuid::Uuid> {
            None
        }
        fn clear_publication_token(&mut self) {}
        fn has_publication(&self) -> bool {
            false
        }
        fn set_completed_at(&mut self, _completed_at: Option<OffsetDateTime>) {}
    }

    fn stored() -> StoredEvaluatorTask<FakeTask> {
        StoredEvaluatorTask::new(FakeTask {
            task_id: TaskId::from_str("123e4567-e89b-42d3-a456-426614174099").unwrap(),
            revision: 0,
            status: VerificationTaskStatus::Pending,
            pending: true,
            satisfied: false,
        })
    }

    #[test]
    fn both_modes_use_one_claim_cas_retry_and_deletion_fence_kernel() {
        let now = datetime!(2026-10-01 12:00:00 UTC);
        let mut graph = vec![stored()];
        let mut payment = vec![stored()];

        for records in [&mut graph, &mut payment] {
            let claimed =
                claim_next(records, "worker", now, Duration::seconds(30), |_| false).unwrap();
            let token = claimed.claim_token.unwrap();
            let task_id = claimed.task.task_id();
            assert!(
                claimed_task_index(records, &task_id, "worker", &token, 0, now, |_| false)
                    .is_some()
            );
            retry_claimed(
                records,
                &task_id,
                "worker",
                &token,
                0,
                now,
                now + Duration::seconds(5),
                (true, false),
                |_| false,
            )
            .unwrap();
            assert_eq!(records[0].task.status(), VerificationTaskStatus::Pending);
            assert_eq!(records[0].task.revision(), 1);

            records[0].claimed_by = Some("worker".to_owned());
            records[0].claim_token = Some(token);
            records[0].claim_expires_at = Some(now + Duration::seconds(30));
            records[0]
                .task
                .set_status(VerificationTaskStatus::InProgress);
            assert!(
                claimed_task_index(records, &task_id, "worker", &token, 1, now, |_| true).is_none()
            );
        }
    }
}
