use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};

use crate::application::errors::ApplicationError;
use crate::application::models::{ClaimedGraphTask, GraphTaskRecord, GraphTaskSnapshot};

/// Graph-only task persistence on the canonical verification-task lease lane.
#[async_trait]
pub trait GraphTaskRepository: Send + Sync {
    /// Inserts one graph-only task without creating invoice-admission state.
    /// Exact `(creator, Bundle ID)` replay returns the stored task; changed bytes conflict.
    async fn insert_graph_task(
        &self,
        task: GraphTaskRecord,
    ) -> Result<GraphTaskRecord, ApplicationError>;

    async fn get_graph_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError>;
}

/// Claim and revision fencing for whole graph-task snapshot replacement.
#[async_trait]
pub trait GraphTaskClaimer: Send + Sync {
    async fn claim_next_graph_task(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedGraphTask>, ApplicationError>;

    /// Replaces complete snapshot only for exact live claim and expected revision.
    async fn persist_claimed_graph_snapshot(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: GraphTaskSnapshot,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError>;

    /// Atomically persists one wave, schedules next due work, and releases the claim.
    #[allow(clippy::too_many_arguments)]
    async fn schedule_graph_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: GraphTaskSnapshot,
        next_attempt_at: time::OffsetDateTime,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError>;
}
