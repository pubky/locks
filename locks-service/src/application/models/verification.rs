use time::OffsetDateTime;

use locks_core::ids::{BundleId, CreatorPubky, LockId, LockServerPubky, TaskId};
use locks_core::lock_policy::Criterion;
use locks_core::verification::{Proof, SubmittedProofBundle};

use crate::application::errors::ApplicationError;

/// Verification task status used by the service layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationTaskStatus {
    /// Task exists but verification has not started.
    Pending,
    /// Verification is currently running.
    InProgress,
    /// Verification succeeded and entitlement storage can be read.
    Completed,
    /// Verification failed and no entitlement should be created.
    Failed,
    /// Task state aged out before completion.
    Expired,
}

/// Persisted service-layer verification task state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationTaskRecord {
    /// Server-generated operational task identifier.
    pub task_id: TaskId,
    /// Creator whose content lock is being verified.
    pub creator: CreatorPubky,
    /// Viewer-submitted proof material associated with the task.
    pub submitted_proof_bundle: SubmittedProofBundle,
    /// Current task status.
    pub status: VerificationTaskStatus,
    /// Timestamp when the task was created.
    pub submitted_at: OffsetDateTime,
    /// Timestamp when verification work started.
    pub started_at: Option<OffsetDateTime>,
    /// Timestamp when the task reached a terminal state.
    pub completed_at: Option<OffsetDateTime>,
    /// Non-empty failure detail for failed tasks only.
    pub failure_message: Option<String>,
}

/// Worker claim carrying the lease incarnation token required for fenced writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedVerificationTask {
    /// Task transitioned to or retained in `in_progress` state by the claim.
    pub task: VerificationTaskRecord,
    /// Fresh opaque token identifying this specific lease incarnation.
    pub claim_token: uuid::Uuid,
}

impl VerificationTaskRecord {
    /// Returns a new task record transitioned to the requested next status.
    ///
    /// The current record must satisfy lifecycle invariants before any transition
    /// is applied. Invalid transitions, malformed current state, and invalid
    /// failure-message usage are returned as application errors.
    pub fn transition_to(
        &self,
        next: VerificationTaskStatus,
        at: OffsetDateTime,
        failure_message: Option<String>,
    ) -> Result<Self, ApplicationError> {
        self.validate_state()?;
        self.validate_transition(next)?;

        let trimmed_failure_message = validate_failure_message(next, failure_message)?;
        let mut transitioned = self.clone();
        transitioned.status = next;

        match next {
            VerificationTaskStatus::Pending => {
                transitioned.started_at = None;
                transitioned.completed_at = None;
                transitioned.failure_message = None;
            }
            VerificationTaskStatus::InProgress => {
                transitioned.started_at = Some(at);
            }
            VerificationTaskStatus::Completed => {
                transitioned.completed_at = Some(at);
            }
            VerificationTaskStatus::Failed => {
                transitioned.completed_at = Some(at);
                transitioned.failure_message = trimmed_failure_message;
            }
            VerificationTaskStatus::Expired => {
                transitioned.completed_at = Some(at);
            }
        }

        Ok(transitioned)
    }

    fn validate_transition(&self, next: VerificationTaskStatus) -> Result<(), ApplicationError> {
        use VerificationTaskStatus::{Completed, Expired, Failed, InProgress, Pending};

        let allowed = matches!(
            (self.status, next),
            (Pending, InProgress)
                | (Pending, Expired)
                | (InProgress, Pending)
                | (InProgress, Completed)
                | (InProgress, Failed)
                | (InProgress, Expired)
        );

        if allowed {
            Ok(())
        } else {
            Err(ApplicationError::InvalidVerificationTaskTransition {
                from: self.status,
                to: next,
            })
        }
    }

    fn validate_state(&self) -> Result<(), ApplicationError> {
        use VerificationTaskStatus::{Completed, Expired, Failed, InProgress, Pending};

        let valid = match self.status {
            Pending => {
                self.started_at.is_none()
                    && self.completed_at.is_none()
                    && self.failure_message.is_none()
            }
            InProgress => {
                self.started_at.is_some()
                    && self.completed_at.is_none()
                    && self.failure_message.is_none()
            }
            Completed => {
                self.started_at.is_some()
                    && self.completed_at.is_some()
                    && self.failure_message.is_none()
            }
            Failed => {
                self.started_at.is_some()
                    && self.completed_at.is_some()
                    && self
                        .failure_message
                        .as_deref()
                        .is_some_and(|message| !message.trim().is_empty())
            }
            Expired => self.completed_at.is_some() && self.failure_message.is_none(),
        };

        if valid {
            Ok(())
        } else {
            Err(ApplicationError::InvalidVerificationTaskState {
                message: format!("record fields are inconsistent for {:?}", self.status),
            })
        }
    }
}

fn validate_failure_message(
    next: VerificationTaskStatus,
    failure_message: Option<String>,
) -> Result<Option<String>, ApplicationError> {
    match next {
        VerificationTaskStatus::Failed => {
            let message = failure_message
                .map(|message| message.trim().to_owned())
                .filter(|message| !message.is_empty())
                .ok_or(ApplicationError::InvalidVerificationTaskFailureMessage)?;
            Ok(Some(message))
        }
        _ if failure_message.is_none() => Ok(None),
        _ => Err(ApplicationError::InvalidVerificationTaskFailureMessage),
    }
}

/// Input passed to a criterion verifier adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CriterionVerificationRequest {
    /// Viewer-generated durable bundle identifier for status lookups.
    pub bundle_id: BundleId,
    /// Creator whose content lock contains the criterion.
    pub creator: CreatorPubky,
    /// Content lock identifier the criterion belongs to.
    pub lock_id: LockId,
    /// Criterion selected from the content lock.
    pub criterion: Criterion,
    /// Viewer-submitted proof for the criterion.
    pub proof: Proof,
    /// Lock Server identity producing the result.
    pub verified_by: LockServerPubky,
    /// Timestamp to place on successful criterion evidence.
    pub verified_at: OffsetDateTime,
}
