use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, LockId};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    models::{
        AdvanceContentLockDeletionPhaseResult, ClaimedContentLockDeletionJob,
        ContentLockDeletionFailureCode, ContentLockDeletionJob, ContentLockDeletionPhase,
        PrepareForceDeletionResult,
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
        claim_ttl: time::Duration,
    ) -> Result<Option<ClaimedContentLockDeletionJob>, ApplicationError>;

    async fn schedule_retry(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        retry_after: time::Duration,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError>;

    /// Releases a healthy-poll claim and schedules its next observation without
    /// charging the claim-acquisition increment to the transient failure budget.
    async fn defer(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        defer_for: time::Duration,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError>;

    async fn advance_phase(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        next_phase: ContentLockDeletionPhase,
    ) -> Result<AdvanceContentLockDeletionPhaseResult, ApplicationError>;

    /// Expires every unresolved frozen non-Paykit task under the exact live deletion claim.
    async fn expire_unresolved_non_paykit_tasks(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        let _ = (job_id, worker_id, claim_token);
        Err(ApplicationError::InvalidContentLockDeletionState {
            message: "non-Paykit deletion drain is not supported by this repository".to_owned(),
        })
    }

    /// Persists terminal completion or a stable secret-free failure under the exact lease.
    async fn finish(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
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
    ) -> Result<PrepareForceDeletionResult, ApplicationError>;

    /// Finalizes force deletion only for the exact live worker claim that observed the effects.
    async fn complete_force_deletion(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
    ) -> Result<bool, ApplicationError>;

    async fn has_force_receipt(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<bool, ApplicationError>;
}
