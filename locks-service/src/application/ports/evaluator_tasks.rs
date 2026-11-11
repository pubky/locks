use locks_core::verification::{
    SatisfiedByV2, VERIFIED_PROOF_BUNDLE_V2_VERSION, VerifiedProofBundleV2,
};
use time::{Duration, OffsetDateTime};

use crate::application::errors::ApplicationError;
use crate::application::models::{EvaluatorTaskIdentityV1, VerificationTaskStatus};

/// Closed released evaluator modes. Mixed mode is introduced only by its own migration card.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvaluatorMode {
    Graph,
    Payment,
    Mixed,
}

impl EvaluatorMode {
    pub const fn as_storage_value(self) -> &'static str {
        match self {
            Self::Graph => "graph",
            Self::Payment => "payment",
            Self::Mixed => "mixed",
        }
    }

    pub const fn satisfied_by(self) -> Option<SatisfiedByV2> {
        match self {
            Self::Graph => Some(SatisfiedByV2::Graph),
            Self::Payment => Some(SatisfiedByV2::Payment),
            Self::Mixed => None,
        }
    }
}

/// Shared deterministic arithmetic used by memory and PostgreSQL evaluator lanes.
pub struct EvaluatorTaskKernel;

impl EvaluatorTaskKernel {
    pub fn next_revision(current: u64) -> Result<u64, ApplicationError> {
        current
            .checked_add(1)
            .ok_or_else(|| invalid_state("evaluator revision overflow"))
    }

    pub fn claim_ttl(
        caller_now: OffsetDateTime,
        requested_expiry: OffsetDateTime,
    ) -> Result<Duration, ApplicationError> {
        let ttl = requested_expiry - caller_now;
        if ttl.is_positive() {
            Ok(ttl)
        } else {
            Err(invalid_state(
                "evaluator claim expiry must follow claim time",
            ))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn claim_binding_is_live(
        task_revision: u64,
        expected_revision: u64,
        claimed_by: Option<&str>,
        worker_id: &str,
        claim_token: Option<&uuid::Uuid>,
        expected_claim_token: &uuid::Uuid,
        claim_expires_at: Option<OffsetDateTime>,
        now: OffsetDateTime,
    ) -> bool {
        task_revision == expected_revision
            && claimed_by == Some(worker_id)
            && claim_token == Some(expected_claim_token)
            && claim_expires_at.is_some_and(|expires_at| expires_at > now)
    }

    pub fn claim_is_available(
        task_status: VerificationTaskStatus,
        root_pending: bool,
        root_satisfied: bool,
        next_attempt_at: Option<OffsetDateTime>,
        claim_expires_at: Option<OffsetDateTime>,
        now: OffsetDateTime,
    ) -> bool {
        ((matches!(
            task_status,
            VerificationTaskStatus::Pending | VerificationTaskStatus::InProgress
        ) && root_pending)
            || (task_status == VerificationTaskStatus::PublishingEntitlement && root_satisfied))
            && next_attempt_at.is_none_or(|due_at| due_at <= now)
            && claim_expires_at.is_none_or(|expires_at| expires_at <= now)
    }

    pub fn retry_due(
        caller_now: OffsetDateTime,
        requested_due: OffsetDateTime,
        authoritative_now: OffsetDateTime,
        deadline: OffsetDateTime,
    ) -> Result<OffsetDateTime, ApplicationError> {
        let delay = requested_due - caller_now;
        if !delay.is_positive() {
            return Err(invalid_state("evaluator retry delay must be positive"));
        }
        Ok((authoritative_now + delay).min(deadline))
    }
}

pub fn build_evaluator_publication(
    identity: &EvaluatorTaskIdentityV1,
    mode: EvaluatorMode,
    verified_at: OffsetDateTime,
) -> Result<VerifiedProofBundleV2, ApplicationError> {
    let satisfied_by = mode
        .satisfied_by()
        .map(|value| vec![value])
        .ok_or_else(|| invalid_state("mixed publication requires exact satisfaction path"))?;
    build_evaluator_publication_with_path(identity, satisfied_by, verified_at)
}

pub fn build_evaluator_publication_with_path(
    identity: &EvaluatorTaskIdentityV1,
    satisfied_by: Vec<SatisfiedByV2>,
    verified_at: OffsetDateTime,
) -> Result<VerifiedProofBundleV2, ApplicationError> {
    let publication = VerifiedProofBundleV2 {
        version: VERIFIED_PROOF_BUNDLE_V2_VERSION,
        creator: identity.creator.clone(),
        reader: identity.reader.clone(),
        bundle_id: identity.bundle_id.clone(),
        pubky_lock_resource: identity.pubky_lock_resource.clone(),
        verified_at,
        satisfied_by,
    };
    publication
        .validate()
        .map_err(|error| invalid_state(format!("invalid evaluator publication: {error}")))?;
    Ok(publication)
}

fn invalid_state(message: impl Into<String>) -> ApplicationError {
    ApplicationError::InvalidVerificationTaskState {
        message: message.into(),
    }
}
