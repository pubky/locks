use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};

use crate::application::errors::ApplicationError;
use crate::application::models::{ClaimedMixedTask, MixedTaskRecord, MixedTaskSnapshot};

#[async_trait]
pub trait MixedPublicationActionGuard: Send {
    async fn complete(&mut self) -> Result<Option<MixedTaskRecord>, ApplicationError>;
    async fn release(self: Box<Self>) -> Result<(), ApplicationError>;
}

#[async_trait]
pub trait MixedTaskRepository: Send + Sync {
    async fn insert_mixed_task(
        &self,
        task: MixedTaskRecord,
    ) -> Result<MixedTaskRecord, ApplicationError>;

    async fn get_mixed_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError>;
}

#[async_trait]
pub trait MixedTaskClaimer: Send + Sync {
    async fn claim_next_mixed_task(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedMixedTask>, ApplicationError>;

    #[allow(clippy::too_many_arguments)]
    async fn persist_claimed_mixed_snapshot(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: MixedTaskSnapshot,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError>;

    #[allow(clippy::too_many_arguments)]
    async fn schedule_mixed_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: MixedTaskSnapshot,
        next_attempt_at: time::OffsetDateTime,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError>;

    async fn release_claimed_mixed_task(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError>;

    async fn acquire_mixed_publication_action(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<Box<dyn MixedPublicationActionGuard>>, ApplicationError>;

    #[allow(clippy::too_many_arguments)]
    async fn complete_claimed_mixed_publication(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError>;
}
