use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, LockId};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    models::{
        ClaimedContentLockDeletionJob, ContentLockDeletionFailureCode, ContentLockDeletionJob,
        ContentLockDeletionPhase, PrepareForceDeletionResult,
    },
};

/// Durable repository and fenced worker lease boundary for content-lock deletion jobs.
#[async_trait]
pub trait ContentLockDeletionRepository: Send + Sync {
    /// Reserves canonical publication under the same per-lock fence used by force deletion.
    async fn begin_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<(), ApplicationError>;

    /// Finalizes the exact publication reservation after external publication and ownership commit.
    async fn finish_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<bool, ApplicationError>;

    /// Removes the exact unfinalized reservation after a safely compensated publication failure.
    async fn abandon_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<bool, ApplicationError>;

    /// Checks publication admission under the canonical per-lock fence.
    async fn publication_in_progress(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<bool, ApplicationError>;

    async fn insert_job(&self, job: ContentLockDeletionJob) -> Result<(), ApplicationError>;

    async fn get_job(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError>;

    async fn claim_next(
        &self,
        worker_id: &str,
        now: OffsetDateTime,
        claim_expires_at: OffsetDateTime,
    ) -> Result<Option<ClaimedContentLockDeletionJob>, ApplicationError>;

    async fn schedule_retry(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        next_attempt_at: OffsetDateTime,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError>;

    async fn advance_phase(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        next_phase: ContentLockDeletionPhase,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError>;

    /// Persists terminal completion or a stable secret-free failure under the exact lease.
    async fn finish(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        failure_code: Option<ContentLockDeletionFailureCode>,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError>;

    /// Requeues a failed job with its frozen manifest unless a permanent force receipt exists.
    async fn resume_failed_job(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        resumed_at: OffsetDateTime,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError>;

    /// Atomically escalates an active job or establishes the permanent synchronous-force receipt.
    async fn prepare_force_deletion(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        forced_at: OffsetDateTime,
    ) -> Result<PrepareForceDeletionResult, ApplicationError>;

    async fn has_force_receipt(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<bool, ApplicationError>;
}
