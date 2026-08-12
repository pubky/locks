use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, LockId};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    models::{
        ClaimedContentLockDeletionJob, ContentLockDeletionFailureCode, ContentLockDeletionJob,
        ContentLockDeletionPhase,
    },
};

/// Durable repository and fenced worker lease boundary for content-lock deletion jobs.
#[async_trait]
pub trait ContentLockDeletionRepository: Send + Sync {
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

    /// Permanently records force escalation. Returns true only on the first request.
    async fn request_force(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        requested_at: OffsetDateTime,
    ) -> Result<bool, ApplicationError>;

    /// Idempotently records the permanent minimal force-deletion receipt.
    async fn record_force_receipt(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        forced_at: OffsetDateTime,
    ) -> Result<(), ApplicationError>;

    async fn has_force_receipt(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<bool, ApplicationError>;
}
