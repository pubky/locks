use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use locks_core::ids::{BundleId, CreatorPubky, LockId, TaskId};
use time::OffsetDateTime;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use uuid::Uuid;

use crate::application::{
    models::{
        GraphTaskRecord, MixedTaskRecord, PaymentRootState, PaymentTaskRecord,
        VerificationTaskStatus, VerificationTerminalReason,
    },
    ports::Clock,
};

use super::evaluator_tasks::StoredEvaluatorTask;

type LockKey = (CreatorPubky, LockId);
type HandleKey = (CreatorPubky, BundleId);

#[derive(Debug)]
struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        let now = OffsetDateTime::now_utc();
        now.replace_nanosecond((now.nanosecond() / 1_000) * 1_000)
            .expect("truncated nanoseconds remain valid")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InMemoryVerificationTaskFenceRecord {
    pub(crate) creator: CreatorPubky,
    pub(crate) lock_id: LockId,
    pub(crate) bundle_id: BundleId,
    pub(crate) paykit_admission_required: bool,
    pub(crate) invoice_admission_ready: bool,
    pub(crate) status: VerificationTaskStatus,
    pub(crate) entitlement_publication_claim_token: Option<Uuid>,
    pub(crate) deletion_job_id: Option<Uuid>,
}

pub(crate) type InMemoryStoredGraphTask = StoredEvaluatorTask<GraphTaskRecord>;
pub(crate) type InMemoryStoredPaymentTask = StoredEvaluatorTask<PaymentTaskRecord>;
pub(crate) type InMemoryStoredMixedTask = StoredEvaluatorTask<MixedTaskRecord>;

/// Shared in-memory serialization state for all admission decisions for one content lock.
///
/// When several fields are needed, acquire them in this global order:
/// `folded_auth_reservations` -> `records` -> `payment_records` -> `mixed_records` ->
/// `current_statuses` -> `graph_records`. Per-lock admission does not serialize
/// operations for different locks, so reversing this order can deadlock globally.
pub struct InMemoryVerificationTaskDeletionFence {
    pub(crate) records: RwLock<HashMap<TaskId, InMemoryVerificationTaskFenceRecord>>,
    pub(crate) graph_records: RwLock<Vec<InMemoryStoredGraphTask>>,
    pub(crate) payment_records: RwLock<Vec<InMemoryStoredPaymentTask>>,
    pub(crate) mixed_records: RwLock<Vec<InMemoryStoredMixedTask>>,
    pub(crate) current_statuses: RwLock<HashMap<TaskId, VerificationTaskStatus>>,
    pub(crate) deletion_cutoffs: RwLock<HashSet<LockKey>>,
    pub(crate) folded_auth_reservations: RwLock<HashSet<HandleKey>>,
    lock_admissions: Mutex<HashMap<LockKey, Arc<Mutex<()>>>>,
    handle_admissions: Mutex<HashMap<HandleKey, Arc<Mutex<()>>>>,
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for InMemoryVerificationTaskDeletionFence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryVerificationTaskDeletionFence")
            .field("records", &self.records)
            .field("graph_records", &self.graph_records)
            .field("payment_records", &self.payment_records)
            .field("mixed_records", &self.mixed_records)
            .field("current_statuses", &self.current_statuses)
            .field("deletion_cutoffs", &self.deletion_cutoffs)
            .field("folded_auth_reservations", &self.folded_auth_reservations)
            .field("lock_admissions", &self.lock_admissions)
            .field("handle_admissions", &self.handle_admissions)
            .field("clock", &"<clock>")
            .finish()
    }
}

impl Default for InMemoryVerificationTaskDeletionFence {
    fn default() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }
}

impl InMemoryVerificationTaskDeletionFence {
    pub fn new() -> Self {
        Self::default()
    }
    /// Creates a canonical in-memory admission fence with an injected cutoff clock.
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            graph_records: RwLock::new(Vec::new()),
            payment_records: RwLock::new(Vec::new()),
            mixed_records: RwLock::new(Vec::new()),
            current_statuses: RwLock::new(HashMap::new()),
            deletion_cutoffs: RwLock::new(HashSet::new()),
            folded_auth_reservations: RwLock::new(HashSet::new()),
            lock_admissions: Mutex::new(HashMap::new()),
            handle_admissions: Mutex::new(HashMap::new()),
            clock,
        }
    }

    pub(crate) async fn acquire_lock_admission(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> OwnedMutexGuard<()> {
        let key = (creator.clone(), lock_id.clone());
        let admission = {
            let mut admissions = self.lock_admissions.lock().await;
            Arc::clone(
                admissions
                    .entry(key)
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        admission.lock_owned().await
    }

    pub(crate) async fn acquire_handle_admission(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> OwnedMutexGuard<()> {
        let key = (creator.clone(), bundle_id.clone());
        let admission = {
            let mut admissions = self.handle_admissions.lock().await;
            Arc::clone(
                admissions
                    .entry(key)
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        admission.lock_owned().await
    }

    pub(crate) fn authoritative_cutoff(&self) -> OffsetDateTime {
        self.clock.now()
    }

    pub(crate) async fn abandon_provider_v2_tasks(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        forced_at: OffsetDateTime,
        preserve_terminal: &HashSet<TaskId>,
    ) {
        let mut fence_records = self.records.write().await;
        let mut payment_records = self.payment_records.write().await;
        let mut mixed_records = self.mixed_records.write().await;
        let mut current_statuses = self.current_statuses.write().await;
        Self::abandon_provider_v2_tasks_under_locks(
            &mut fence_records,
            &mut payment_records,
            &mut mixed_records,
            &mut current_statuses,
            creator,
            lock_id,
            forced_at,
            preserve_terminal,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn abandon_provider_v2_tasks_under_locks(
        fence_records: &mut HashMap<TaskId, InMemoryVerificationTaskFenceRecord>,
        payment_records: &mut [InMemoryStoredPaymentTask],
        mixed_records: &mut [InMemoryStoredMixedTask],
        current_statuses: &mut HashMap<TaskId, VerificationTaskStatus>,
        creator: &CreatorPubky,
        lock_id: &LockId,
        forced_at: OffsetDateTime,
        preserve_terminal: &HashSet<TaskId>,
    ) {
        for stored in payment_records.iter_mut().filter(|stored| {
            let snapshot = stored.task.snapshot.as_v1();
            &snapshot.creator == creator
                && snapshot
                    .submitted_proof_bundle
                    .pubky_lock_resource
                    .lock_id()
                    == lock_id
                && stored.task.status != VerificationTaskStatus::Completed
                && !preserve_terminal.contains(&stored.task.task_id)
        }) {
            stored.task.status = VerificationTaskStatus::Expired;
            stored.task.snapshot.as_v1_mut().root =
                PaymentRootState::Expired(VerificationTerminalReason::ContentLockDeleted);
            stored.task.evaluator_revision = stored.task.evaluator_revision.saturating_add(1);
            stored.task.entitlement_to_publish = None;
            stored.task.entitlement_publication_token = None;
            stored.task.completed_at = Some(forced_at);
            stored.claimed_by = None;
            stored.claim_token = None;
            stored.claim_expires_at = None;
            stored.next_attempt_at = None;
            current_statuses.insert(stored.task.task_id, VerificationTaskStatus::Expired);
            if let Some(record) = fence_records.get_mut(&stored.task.task_id) {
                record.status = VerificationTaskStatus::Expired;
                record.entitlement_publication_claim_token = None;
                record.deletion_job_id = None;
            }
        }
        for stored in mixed_records.iter_mut().filter(|stored| {
            let snapshot = stored.task.snapshot.as_v1();
            &snapshot.creator == creator
                && snapshot.pubky_lock_resource.lock_id() == lock_id
                && stored.task.status != VerificationTaskStatus::Completed
                && !preserve_terminal.contains(&stored.task.task_id)
        }) {
            stored.task.status = VerificationTaskStatus::Expired;
            stored.task.terminal_reason = Some(VerificationTerminalReason::ContentLockDeleted);
            stored.task.evaluator_revision = stored.task.evaluator_revision.saturating_add(1);
            stored.task.entitlement_to_publish = None;
            stored.task.entitlement_publication_token = None;
            stored.task.completed_at = Some(forced_at);
            stored.claimed_by = None;
            stored.claim_token = None;
            stored.claim_expires_at = None;
            stored.next_attempt_at = None;
            current_statuses.insert(stored.task.task_id, VerificationTaskStatus::Expired);
            if let Some(record) = fence_records.get_mut(&stored.task.task_id) {
                record.status = VerificationTaskStatus::Expired;
                record.entitlement_publication_claim_token = None;
                record.deletion_job_id = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::{
        ids::{LockHash, PubkyLockResource},
        lock_policy::{GraphParticipantV2, GraphPolicyV2, GraphPredicateV2, PaymentPolicyV2},
        verification::{SUBMITTED_PROOF_BUNDLE_V2_VERSION, SubmittedProofBundleV2},
    };
    use time::macros::datetime;

    use crate::application::models::{MixedTaskSnapshot, MixedTaskSnapshotV1};

    use super::*;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
    const NOW: OffsetDateTime = datetime!(2026-11-10 12:00:00 UTC);

    #[test]
    fn force_abandonment_preserves_mixed_roots_and_branch_evidence() {
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let reader = CreatorPubky::from_str(READER).unwrap();
        let lock_id = LockId::from_hash(LockHash::from_bytes([0; 32]));
        let resource =
            PubkyLockResource::from_str(&format!("{creator}/pub/app.locks/{lock_id}.json"))
                .unwrap();
        let snapshot = MixedTaskSnapshotV1::new(
            SubmittedProofBundleV2 {
                version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
                bundle_id: BundleId::from_bytes([7; 16]),
                pubky_lock_resource: resource,
                reader_pubky: None,
            },
            [0; 32],
            reader,
            GraphPolicyV2 {
                operator: None,
                predicates: vec![GraphPredicateV2::Follows {
                    follower: GraphParticipantV2::Reader,
                    followed: GraphParticipantV2::Creator,
                }],
            },
            PaymentPolicyV2 {
                recipient_pubky: creator.clone(),
                amount: "1".to_owned(),
                asset: "BTC".to_owned(),
                minimum_confirmations: 0,
            },
            NOW,
            300,
        )
        .unwrap();
        let expected_snapshot = snapshot.clone();
        let task_id = TaskId::from_str(&Uuid::new_v4().to_string()).unwrap();
        let mut mixed_records = vec![StoredEvaluatorTask::new(MixedTaskRecord {
            task_id,
            snapshot: MixedTaskSnapshot::V1(snapshot),
            evaluator_revision: 0,
            status: VerificationTaskStatus::Pending,
            terminal_reason: None,
            entitlement_to_publish: None,
            entitlement_publication_token: None,
            completed_at: None,
        })];
        let mut current_statuses = HashMap::from([(task_id, VerificationTaskStatus::Pending)]);

        InMemoryVerificationTaskDeletionFence::abandon_provider_v2_tasks_under_locks(
            &mut HashMap::new(),
            &mut [],
            &mut mixed_records,
            &mut current_statuses,
            &creator,
            &lock_id,
            NOW,
            &HashSet::new(),
        );

        let abandoned = &mixed_records[0].task;
        assert_eq!(abandoned.status, VerificationTaskStatus::Expired);
        assert_eq!(abandoned.snapshot.as_v1(), &expected_snapshot);
    }
}
