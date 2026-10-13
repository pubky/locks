use std::collections::HashMap;

use locks_core::ids::{CreatorPubky, LockId, TaskId};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct InMemoryVerificationTaskFenceRecord {
    pub(crate) creator: CreatorPubky,
    pub(crate) lock_id: LockId,
    pub(crate) entitlement_publication_claim_token: Option<Uuid>,
    pub(crate) deletion_job_id: Option<Uuid>,
}

/// Shared in-memory serialization state for verification publication and deletion admission.
#[derive(Debug, Default)]
pub struct InMemoryVerificationTaskDeletionFence {
    pub(crate) records: RwLock<HashMap<TaskId, InMemoryVerificationTaskFenceRecord>>,
}

impl InMemoryVerificationTaskDeletionFence {
    pub fn new() -> Self {
        Self::default()
    }
}
