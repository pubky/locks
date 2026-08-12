use std::str::FromStr;

use locks_core::{
    ids::{CreatorPubky, LockId},
    lock_policy::ContentLock,
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::errors::ApplicationError;

/// Internal deletion workflow state. Public API status conversion is intentionally separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentLockDeletionState {
    Queued,
    Running,
    Completed,
    Failed,
}

/// Closed creator-visible failure vocabulary. Raw dependency errors never cross this boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentLockDeletionFailureCode {
    TombstoneMissing,
    TombstoneReplaced,
    RetryExhausted,
    StateCorrupt,
}

impl ContentLockDeletionFailureCode {
    /// Returns the exact stable public/database value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TombstoneMissing => "tombstone_missing",
            Self::TombstoneReplaced => "tombstone_replaced",
            Self::RetryExhausted => "retry_exhausted",
            Self::StateCorrupt => "state_corrupt",
        }
    }
}

impl FromStr for ContentLockDeletionFailureCode {
    type Err = ApplicationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "tombstone_missing" => Ok(Self::TombstoneMissing),
            "tombstone_replaced" => Ok(Self::TombstoneReplaced),
            "retry_exhausted" => Ok(Self::RetryExhausted),
            "state_corrupt" => Ok(Self::StateCorrupt),
            _ => Err(ApplicationError::InvalidContentLockDeletionState {
                message: "unknown content lock deletion failure code".to_owned(),
            }),
        }
    }
}

/// Internal orchestration phase. These values are not public API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentLockDeletionPhase {
    Withdraw,
    StartPaymentDrain,
    DrainPayments,
    DrainExistingCredentials,
    IssueFinalCredentials,
    DrainFinalReads,
    DeleteContent,
    DeleteTombstone,
    PurgeOperationalState,
}

impl ContentLockDeletionPhase {
    /// Returns true only for the immediate forward workflow transition.
    pub fn permits(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Withdraw, Self::StartPaymentDrain)
                | (Self::StartPaymentDrain, Self::DrainPayments)
                | (Self::DrainPayments, Self::DrainExistingCredentials)
                | (Self::DrainExistingCredentials, Self::IssueFinalCredentials)
                | (Self::IssueFinalCredentials, Self::DrainFinalReads)
                | (Self::DrainFinalReads, Self::DeleteContent)
                | (Self::DeleteContent, Self::DeleteTombstone)
                | (Self::DeleteTombstone, Self::PurgeOperationalState)
        )
    }
}

/// Durable graceful content-lock deletion job and immutable frozen manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentLockDeletionJob {
    pub job_id: Uuid,
    pub creator: CreatorPubky,
    pub lock_id: LockId,
    pub frozen_content_lock: ContentLock,
    pub deletion_started_at: OffsetDateTime,
    pub state: ContentLockDeletionState,
    pub phase: ContentLockDeletionPhase,
    pub attempt_count: u32,
    pub next_attempt_at: Option<OffsetDateTime>,
    pub force_requested_at: Option<OffsetDateTime>,
    pub failure_code: Option<ContentLockDeletionFailureCode>,
}

/// Claimed job plus the fresh lease-incarnation token required for fenced writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedContentLockDeletionJob {
    pub job: ContentLockDeletionJob,
    pub claim_token: Uuid,
}

/// Durable decision made while serializing force deletion against graceful lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareForceDeletionResult {
    /// A pre-existing publication intent must reconcile before force can begin.
    PublicationInProgress,
    /// An active graceful job was durably marked for asynchronous force processing.
    Active(ContentLockDeletionJob),
    /// A permanent force receipt was established. A terminal frozen job is returned when present.
    Synchronous(Option<ContentLockDeletionJob>),
}

impl ContentLockDeletionJob {
    /// Creates a queued deletion job from a canonical frozen content lock.
    pub fn new(
        job_id: Uuid,
        frozen_content_lock: ContentLock,
        deletion_started_at: OffsetDateTime,
    ) -> Result<Self, ApplicationError> {
        let lock_id = frozen_content_lock.lock_id().map_err(|error| {
            ApplicationError::ContentLockCanonicalization {
                message: error.to_string(),
            }
        })?;
        Ok(Self {
            job_id,
            creator: frozen_content_lock.creator.clone(),
            lock_id,
            frozen_content_lock,
            deletion_started_at,
            state: ContentLockDeletionState::Queued,
            phase: ContentLockDeletionPhase::Withdraw,
            attempt_count: 0,
            next_attempt_at: None,
            force_requested_at: None,
            failure_code: None,
        })
    }

    /// Recomputes the frozen lock identity and verifies the durable key fields.
    pub fn validate_frozen_identity(&self) -> Result<(), ApplicationError> {
        let actual = self.frozen_content_lock.lock_id().map_err(|error| {
            ApplicationError::ContentLockCanonicalization {
                message: error.to_string(),
            }
        })?;
        if actual != self.lock_id || self.frozen_content_lock.creator != self.creator {
            return Err(ApplicationError::InvalidContentLockDeletionState {
                message: "frozen content lock identity does not match deletion job".to_owned(),
            });
        }
        Ok(())
    }

    /// Validates lifecycle fields against whether persistence has a complete active lease.
    pub fn validate_state(&self, has_active_lease: bool) -> Result<(), ApplicationError> {
        let valid = match self.state {
            ContentLockDeletionState::Queued => !has_active_lease && self.failure_code.is_none(),
            ContentLockDeletionState::Running => {
                has_active_lease && self.next_attempt_at.is_none() && self.failure_code.is_none()
            }
            ContentLockDeletionState::Completed => {
                !has_active_lease && self.next_attempt_at.is_none() && self.failure_code.is_none()
            }
            ContentLockDeletionState::Failed => {
                !has_active_lease && self.next_attempt_at.is_none() && self.failure_code.is_some()
            }
        };
        if valid {
            Ok(())
        } else {
            Err(ApplicationError::InvalidContentLockDeletionState {
                message: "deletion lifecycle fields are inconsistent".to_owned(),
            })
        }
    }
}
