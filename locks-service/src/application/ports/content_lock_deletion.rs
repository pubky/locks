use async_trait::async_trait;
use std::collections::BTreeSet;

use locks_core::{
    ids::{CreatorPubky, LockId},
    lock_policy::ContentLock,
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    models::{
        ClaimedContentLockDeletionJob, ContentLockDeletionFailureCode, ContentLockDeletionJob,
        ContentLockDeletionPhase, ForceDeletionReceipt, PrepareForceDeletionResult,
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

    /// Clears a retained intent only from the early exact-published replay path, after exact
    /// canonical bytes and the complete published ownership set establish that no publisher is
    /// still before its external write.
    async fn reconcile_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        published_content_lock: &ContentLock,
        published_guarded_paths: &[String],
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
        frozen_content_lock: Option<&locks_core::lock_policy::ContentLock>,
        forced_at: OffsetDateTime,
    ) -> Result<PrepareForceDeletionResult, ApplicationError>;

    async fn get_force_receipt(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<Option<ForceDeletionReceipt>, ApplicationError>;

    /// Marks cleanup complete after public absence and all guarded-resource attempts.
    async fn complete_force_deletion(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        completed_at: OffsetDateTime,
    ) -> Result<bool, ApplicationError>;

    async fn has_force_receipt(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<bool, ApplicationError>;
}

pub(crate) fn publication_reconciliation_matches(
    creator: &CreatorPubky,
    lock_id: &LockId,
    published_content_lock: &ContentLock,
    published_guarded_paths: &[String],
) -> bool {
    if &published_content_lock.creator != creator
        || !published_content_lock
            .lock_id()
            .is_ok_and(|published_lock_id| &published_lock_id == lock_id)
    {
        return false;
    }

    let expected_paths = published_content_lock
        .primary_resource
        .iter()
        .map(|resource| resource.path.as_str())
        .chain(
            published_content_lock
                .secondary_resources
                .keys()
                .map(String::as_str),
        )
        .collect::<BTreeSet<_>>();
    let published_paths = published_guarded_paths
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    expected_paths.len() == published_guarded_paths.len() && expected_paths == published_paths
}
