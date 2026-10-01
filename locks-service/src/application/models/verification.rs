use time::OffsetDateTime;

use locks_core::ids::{BundleId, CreatorPubky, LockId, LockServerPubky, TaskId};
use locks_core::lock_policy::Criterion;
use locks_core::verification::{Proof, SubmittedProofBundle, VerifiedProofBundle};

use crate::application::errors::ApplicationError;

/// Verification task status used by the service layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationTaskStatus {
    /// Task exists but verification has not started.
    Pending,
    /// Verification is currently running.
    InProgress,
    /// Exact entitlement payload won the lifecycle race and awaits verified publication.
    PublishingEntitlement,
    /// Verification succeeded and entitlement storage can be read.
    Completed,
    /// Verification failed and no entitlement should be created.
    Failed,
    /// Task state aged out before completion.
    Expired,
}

/// Canonical reason a Paykit-backed verification attempt ended without entitlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationTerminalReason {
    PaymentRequestRejected,
    PaymentRequestCanceled,
    ProposalExpired,
    PaymentDeadlineExpired,
}

impl VerificationTerminalReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PaymentRequestRejected => "payment_request_rejected",
            Self::PaymentRequestCanceled => "payment_request_canceled",
            Self::ProposalExpired => "proposal_expired",
            Self::PaymentDeadlineExpired => "payment_deadline_expired",
        }
    }

    pub fn from_storage_value(value: &str) -> Option<Self> {
        match value {
            "payment_request_rejected" => Some(Self::PaymentRequestRejected),
            "payment_request_canceled" => Some(Self::PaymentRequestCanceled),
            "proposal_expired" => Some(Self::ProposalExpired),
            "payment_deadline_expired" => Some(Self::PaymentDeadlineExpired),
            _ => None,
        }
    }
}

/// Closed criterion-verifier decision used by completion orchestration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CriterionVerificationOutcome {
    Pending,
    Satisfied(locks_core::verification::CriterionVerificationResult),
    TerminalUnsatisfied(VerificationTerminalReason),
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
    /// Typed no-entitlement reason for expired Paykit attempts only.
    pub terminal_reason: Option<VerificationTerminalReason>,
    /// Exact durable payload replayed while entitlement publication is pending.
    pub entitlement_to_publish: Option<VerifiedProofBundle>,
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
                transitioned.terminal_reason = None;
                transitioned.entitlement_to_publish = None;
            }
            VerificationTaskStatus::InProgress => {
                transitioned.started_at = Some(at);
            }
            VerificationTaskStatus::PublishingEntitlement => {
                return Err(ApplicationError::InvalidVerificationTaskState {
                    message: "publishing transitions require an entitlement payload".to_owned(),
                });
            }
            VerificationTaskStatus::Completed => {
                transitioned.completed_at = Some(at);
            }
            VerificationTaskStatus::Failed => {
                transitioned.completed_at = Some(at);
                transitioned.failure_message = trimmed_failure_message;
            }
            VerificationTaskStatus::Expired => {
                return Err(ApplicationError::InvalidVerificationTaskState {
                    message: "expired transitions require a terminal reason".to_owned(),
                });
            }
        }

        Ok(transitioned)
    }

    /// Persists exact entitlement payload before any external publication attempt.
    pub fn begin_entitlement_publication(
        &self,
        entitlement: VerifiedProofBundle,
    ) -> Result<Self, ApplicationError> {
        self.validate_state()?;
        self.validate_transition(VerificationTaskStatus::PublishingEntitlement)?;
        let mut transitioned = self.clone();
        transitioned.status = VerificationTaskStatus::PublishingEntitlement;
        transitioned.entitlement_to_publish = Some(entitlement);
        Ok(transitioned)
    }

    /// Marks publication complete only after equivalent entitlement read-back.
    pub fn complete_entitlement_publication(
        &self,
        at: OffsetDateTime,
    ) -> Result<Self, ApplicationError> {
        self.transition_to(VerificationTaskStatus::Completed, at, None)
    }

    /// Terminalizes an actively running Paykit attempt without entitlement or failure.
    pub fn expire(
        &self,
        reason: VerificationTerminalReason,
        at: OffsetDateTime,
    ) -> Result<Self, ApplicationError> {
        self.validate_state()?;
        self.validate_transition(VerificationTaskStatus::Expired)?;
        if self.status != VerificationTaskStatus::InProgress {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "only an in-progress task can expire with a terminal reason".to_owned(),
            });
        }
        let mut transitioned = self.clone();
        transitioned.status = VerificationTaskStatus::Expired;
        transitioned.completed_at = Some(at);
        transitioned.failure_message = None;
        transitioned.terminal_reason = Some(reason);
        Ok(transitioned)
    }

    fn validate_transition(&self, next: VerificationTaskStatus) -> Result<(), ApplicationError> {
        use VerificationTaskStatus::{
            Completed, Expired, Failed, InProgress, Pending, PublishingEntitlement,
        };

        let allowed = matches!(
            (self.status, next),
            (Pending, InProgress)
                | (Pending, Expired)
                | (InProgress, Pending)
                | (InProgress, PublishingEntitlement)
                | (InProgress, Failed)
                | (InProgress, Expired)
                | (PublishingEntitlement, Completed)
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
        use VerificationTaskStatus::{
            Completed, Expired, Failed, InProgress, Pending, PublishingEntitlement,
        };

        let valid = match self.status {
            Pending => {
                self.started_at.is_none()
                    && self.completed_at.is_none()
                    && self.failure_message.is_none()
                    && self.terminal_reason.is_none()
                    && self.entitlement_to_publish.is_none()
            }
            InProgress => {
                self.started_at.is_some()
                    && self.completed_at.is_none()
                    && self.failure_message.is_none()
                    && self.terminal_reason.is_none()
                    && self.entitlement_to_publish.is_none()
            }
            PublishingEntitlement => {
                self.started_at.is_some()
                    && self.completed_at.is_none()
                    && self.failure_message.is_none()
                    && self.terminal_reason.is_none()
                    && self.entitlement_to_publish.is_some()
            }
            Completed => {
                self.started_at.is_some()
                    && self.completed_at.is_some()
                    && self.failure_message.is_none()
                    && self.terminal_reason.is_none()
                    && self.entitlement_to_publish.is_some()
            }
            Failed => {
                self.started_at.is_some()
                    && self.completed_at.is_some()
                    && self
                        .failure_message
                        .as_deref()
                        .is_some_and(|message| !message.trim().is_empty())
                    && self.terminal_reason.is_none()
                    && self.entitlement_to_publish.is_none()
            }
            Expired => {
                self.started_at.is_some()
                    && self.completed_at.is_some()
                    && self.failure_message.is_none()
                    && self.terminal_reason.is_some()
                    && self.entitlement_to_publish.is_none()
            }
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
