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
    models::{GraphTaskRecord, VerificationTaskStatus},
    ports::Clock,
};

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

#[derive(Debug, Clone)]
pub(crate) struct InMemoryStoredGraphTask {
    pub(crate) task: GraphTaskRecord,
    pub(crate) claimed_by: Option<String>,
    pub(crate) claim_token: Option<Uuid>,
    pub(crate) claim_expires_at: Option<OffsetDateTime>,
}

/// Shared in-memory serialization state for all admission decisions for one content lock.
pub struct InMemoryVerificationTaskDeletionFence {
    pub(crate) records: RwLock<HashMap<TaskId, InMemoryVerificationTaskFenceRecord>>,
    pub(crate) graph_records: RwLock<Vec<InMemoryStoredGraphTask>>,
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
}
