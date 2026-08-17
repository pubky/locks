use std::{collections::HashMap, fmt, sync::Arc};

use locks_core::ids::{BundleId, CreatorPubky, LockId, TaskId};
use locks_core::lock_policy::VerifierType;
use time::OffsetDateTime;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use uuid::Uuid;

use crate::application::{
    models::{VerificationTaskRecord, VerificationTaskStatus},
    ports::Clock,
};

type LockKey = (CreatorPubky, LockId);

#[derive(Debug)]
struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InMemoryVerificationTaskFenceRecord {
    pub(crate) creator: CreatorPubky,
    pub(crate) lock_id: LockId,
    pub(crate) bundle_id: BundleId,
    pub(crate) paykit_admission_required: bool,
    pub(crate) status: VerificationTaskStatus,
    pub(crate) entitlement_publication_claim_token: Option<Uuid>,
    pub(crate) deletion_job_id: Option<Uuid>,
}

/// Shared in-memory serialization state for all admission decisions for one content lock.
pub struct InMemoryVerificationTaskDeletionFence {
    pub(crate) records: RwLock<HashMap<TaskId, InMemoryVerificationTaskFenceRecord>>,
    lock_admissions: Mutex<HashMap<LockKey, Arc<Mutex<()>>>>,
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for InMemoryVerificationTaskDeletionFence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryVerificationTaskDeletionFence")
            .field("records", &self.records)
            .field("lock_admissions", &self.lock_admissions)
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
            lock_admissions: Mutex::new(HashMap::new()),
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

    pub(crate) fn authoritative_cutoff(&self) -> OffsetDateTime {
        self.clock.now()
    }

    pub(crate) fn from_tasks(tasks: &[VerificationTaskRecord]) -> Self {
        Self {
            records: RwLock::new(
                tasks
                    .iter()
                    .map(|task| {
                        (
                            task.task_id,
                            InMemoryVerificationTaskFenceRecord {
                                creator: task.creator.clone(),
                                lock_id: task
                                    .submitted_proof_bundle
                                    .pubky_lock_resource
                                    .lock_id()
                                    .clone(),
                                bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
                                paykit_admission_required: task
                                    .submitted_proof_bundle
                                    .proofs
                                    .iter()
                                    .any(|proof| {
                                        proof.verifier_type == VerifierType::PaykitPayment
                                    }),
                                status: task.status,
                                entitlement_publication_claim_token: None,
                                deletion_job_id: None,
                            },
                        )
                    })
                    .collect(),
            ),
            lock_admissions: Mutex::new(HashMap::new()),
            clock: Arc::new(SystemClock),
        }
    }
}
