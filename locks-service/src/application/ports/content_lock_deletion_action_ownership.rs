use async_trait::async_trait;
use uuid::Uuid;

use crate::application::{errors::ApplicationError, models::ContentLockDeletionPhase};

/// Exact expected live claim and lifecycle state for one external action lane.
#[derive(Debug, Clone, Copy)]
pub struct ContentLockDeletionActionClaim<'a> {
    pub job_id: Uuid,
    pub worker_id: &'a str,
    pub claim_token: Uuid,
    pub expected_phase: ContentLockDeletionPhase,
    pub force: bool,
}

/// Owns one job's external side-effect lane until explicitly released or dropped.
#[async_trait]
pub trait ContentLockDeletionActionGuard: Send {
    /// Releases ownership. Implementations must not return a still-locked
    /// connection or equivalent resource to shared storage.
    async fn release(self: Box<Self>) -> Result<(), ApplicationError>;
}

/// Closed result of post-lock live-claim validation.
pub enum ContentLockDeletionActionAcquireResult {
    Acquired(Box<dyn ContentLockDeletionActionGuard>),
    Busy,
    ClaimLost,
}

/// Nonblocking per-job ownership boundary for deletion external actions.
#[async_trait]
pub trait ContentLockDeletionActionOwnership: Send + Sync {
    /// Acquires the per-job lane, then validates the exact claim against
    /// storage-authoritative time and expected lifecycle state.
    async fn try_acquire(
        &self,
        claim: ContentLockDeletionActionClaim<'_>,
    ) -> Result<ContentLockDeletionActionAcquireResult, ApplicationError>;
}
