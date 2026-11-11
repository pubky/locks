use std::fmt;

use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource};
use locks_core::verification::SatisfiedByV2;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::graph::{
    CreatorGraphEvidenceResponse, GraphObservationOutcome, GraphPredicateEvidenceState,
};
use crate::viewer::{
    SECRET_BEARING_RESPONSE_FIELDS, VerificationTaskStatus, VerificationTerminalReason,
};
use crate::{LocksSdkError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MixedVerificationTaskLifecycleResponse {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub status: VerificationTaskStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub submitted_at: OffsetDateTime,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub completed_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub admission_deadline_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<VerificationTerminalReason>,
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum MixedSubmitResponse {
    AuthPending {
        auth_url: String,
        state: String,
        #[serde(with = "time::serde::rfc3339")]
        expires_at: OffsetDateTime,
    },
    Accepted {
        task: MixedVerificationTaskLifecycleResponse,
    },
}

impl fmt::Debug for MixedSubmitResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuthPending { expires_at, .. } => formatter
                .debug_struct("AuthPending")
                .field("auth_url", &"<redacted>")
                .field("state", &"<redacted>")
                .field("expires_at", expires_at)
                .finish(),
            Self::Accepted { task } => formatter
                .debug_struct("Accepted")
                .field("task", task)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum MixedFoldedAuthStatusResponse {
    Pending {
        #[serde(with = "time::serde::rfc3339")]
        expires_at: OffsetDateTime,
    },
    Accepted {
        task: MixedVerificationTaskLifecycleResponse,
    },
    RestartRequired,
    Expired {
        #[serde(with = "time::serde::rfc3339")]
        expired_at: OffsetDateTime,
    },
}

pub fn parse_mixed_lifecycle_response(
    value: Value,
) -> Result<MixedVerificationTaskLifecycleResponse> {
    crate::viewer::reject_fields(&value, SECRET_BEARING_RESPONSE_FIELDS)?;
    for field in ["completed_at", "terminal_reason"] {
        if value.get(field).is_some_and(Value::is_null) {
            return Err(invalid(format!("{field} must be omitted rather than null")));
        }
    }
    let response: MixedVerificationTaskLifecycleResponse =
        serde_json::from_value(value).map_err(|error| invalid(error.to_string()))?;
    let valid = match response.status {
        VerificationTaskStatus::Pending
        | VerificationTaskStatus::InProgress
        | VerificationTaskStatus::PublishingEntitlement => {
            response.completed_at.is_none() && response.terminal_reason.is_none()
        }
        VerificationTaskStatus::Completed => {
            response.completed_at.is_some() && response.terminal_reason.is_none()
        }
        VerificationTaskStatus::Expired => {
            response.completed_at.is_some()
                && matches!(
                    response.terminal_reason,
                    Some(
                        VerificationTerminalReason::PaymentRequestRejected
                            | VerificationTerminalReason::PaymentRequestCanceled
                            | VerificationTerminalReason::ProposalExpired
                            | VerificationTerminalReason::PaymentDeadlineExpired
                            | VerificationTerminalReason::ContentLockDeleted
                            | VerificationTerminalReason::GraphConditionNotObservedByDeadline
                    )
                )
        }
        VerificationTaskStatus::Failed => {
            response.completed_at.is_some()
                && matches!(
                    response.terminal_reason,
                    Some(
                        VerificationTerminalReason::VerificationUnavailableByDeadline
                            | VerificationTerminalReason::InvalidVerifierResponse
                            | VerificationTerminalReason::VerificationConflict
                    )
                )
        }
    };
    if !valid
        || response.admission_deadline_at < response.submitted_at
        || response
            .completed_at
            .is_some_and(|completed| completed < response.submitted_at)
    {
        return Err(invalid(
            "invalid mixed verification lifecycle field combination",
        ));
    }
    Ok(response)
}

pub fn parse_mixed_submit_response(value: Value) -> Result<MixedSubmitResponse> {
    crate::viewer::reject_fields(&value, SECRET_BEARING_RESPONSE_FIELDS)?;
    if value.get("status").and_then(Value::as_str) == Some("accepted") {
        let task = value
            .get("task")
            .cloned()
            .ok_or_else(|| invalid("accepted mixed submit response requires task"))?;
        parse_mixed_lifecycle_response(task)?;
    }
    serde_json::from_value(value).map_err(|error| invalid(error.to_string()))
}

pub fn parse_mixed_folded_auth_status_response(
    value: Value,
) -> Result<MixedFoldedAuthStatusResponse> {
    crate::viewer::reject_fields(&value, SECRET_BEARING_RESPONSE_FIELDS)?;
    if value.get("status").and_then(Value::as_str) == Some("accepted") {
        let task = value
            .get("task")
            .cloned()
            .ok_or_else(|| invalid("accepted mixed folded-auth response requires task"))?;
        parse_mixed_lifecycle_response(task)?;
    }
    serde_json::from_value(value).map_err(|error| invalid(error.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MixedEvidenceMode {
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status", deny_unknown_fields)]
pub enum MixedPaymentInvoiceEvidence {
    Pending,
    Created {
        #[serde(with = "time::serde::rfc3339")]
        invoice_created_at: OffsetDateTime,
        #[serde(with = "time::serde::rfc3339")]
        proposal_expires_at: OffsetDateTime,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MixedPaymentAsset {
    #[serde(rename = "BTC")]
    Btc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MixedPaymentRequestState {
    Proposed,
    ProposalExpired,
    Accepted,
    Rejected,
    Canceled,
    ProofSubmitted,
    ActiveRecurring,
    RecoveryRequired,
    InvalidConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MixedPaymentState {
    Undetected,
    Detected,
    Confirmed,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MixedPaymentSampleStatus {
    Undetected,
    Detected,
    Confirmed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MixedBitcoinOutpoint {
    pub txid: String,
    pub vout: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MixedPaymentSample {
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    pub status: MixedPaymentSampleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outpoint: Option<MixedBitcoinOutpoint>,
    pub confirmations: u8,
    pub amount_matched: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MixedPaymentProviderStatus {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub request_state: MixedPaymentRequestState,
    pub payment_state: MixedPaymentState,
    #[serde(with = "time::serde::rfc3339")]
    pub invoice_created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub proposal_expires_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub deadline: OffsetDateTime,
    pub asset: MixedPaymentAsset,
    pub minimum_confirmations: u8,
    pub current_sample: Option<MixedPaymentSample>,
    pub first_threshold_satisfied: Option<MixedPaymentSample>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaymentOperationalErrorCategory {
    RateLimited,
    NotFound,
    Server,
    Timeout,
    Transport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MixedPaymentEvidenceResponse {
    pub invoice: MixedPaymentInvoiceEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_status: Option<MixedPaymentProviderStatus>,
    pub operational_failure_count: u32,
    pub operational_failure_total: u32,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub operational_first_failure_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub operational_last_failure_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operational_last_category: Option<PaymentOperationalErrorCategory>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operational_last_http_status: Option<u16>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub next_due_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatorMixedVerificationEvidenceResponse {
    pub creator: CreatorPubky,
    pub reader: CreatorPubky,
    pub bundle_id: BundleId,
    pub pubky_lock_resource: PubkyLockResource,
    pub mode: MixedEvidenceMode,
    pub status: VerificationTaskStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub submitted_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub deadline_at: OffsetDateTime,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub completed_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<VerificationTerminalReason>,
    pub satisfied_by: Vec<SatisfiedByV2>,
    pub graph: CreatorGraphEvidenceResponse,
    pub payment: MixedPaymentEvidenceResponse,
}

pub fn parse_creator_mixed_verification_evidence_response(
    value: Value,
) -> Result<CreatorMixedVerificationEvidenceResponse> {
    crate::viewer::reject_fields(&value, SECRET_BEARING_RESPONSE_FIELDS)?;
    let response: CreatorMixedVerificationEvidenceResponse =
        serde_json::from_value(value).map_err(|error| invalid(error.to_string()))?;
    let canonical = matches!(
        response.satisfied_by.as_slice(),
        [] | [SatisfiedByV2::Payment]
            | [SatisfiedByV2::ZeroConfGraph]
            | [SatisfiedByV2::Payment, SatisfiedByV2::ZeroConfGraph]
    );
    let valid_status = match response.status {
        VerificationTaskStatus::Pending | VerificationTaskStatus::InProgress => {
            response.completed_at.is_none() && response.terminal_reason.is_none()
        }
        VerificationTaskStatus::PublishingEntitlement => {
            response.completed_at.is_none() && response.terminal_reason.is_none()
        }
        VerificationTaskStatus::Completed => {
            response.completed_at.is_some() && response.terminal_reason.is_none()
        }
        VerificationTaskStatus::Expired | VerificationTaskStatus::Failed => {
            response.completed_at.is_some() && response.terminal_reason.is_some()
        }
    };
    let graph_satisfied = match response.graph.operator {
        None => {
            response.graph.predicates.len() == 1
                && graph_predicate_satisfied(&response.graph.predicates[0])
        }
        Some(locks_core::lock_policy::GraphOperatorV2::And) => response
            .graph
            .predicates
            .iter()
            .all(graph_predicate_satisfied),
        Some(locks_core::lock_policy::GraphOperatorV2::Or) => response
            .graph
            .predicates
            .iter()
            .any(graph_predicate_satisfied),
    };
    let provider_valid = match (&response.payment.invoice, &response.payment.provider_status) {
        (MixedPaymentInvoiceEvidence::Pending, None) => true,
        (
            MixedPaymentInvoiceEvidence::Created {
                invoice_created_at,
                proposal_expires_at,
            },
            Some(provider),
        ) => {
            provider.creator == response.creator
                && provider.bundle_id == response.bundle_id
                && provider.invoice_created_at == *invoice_created_at
                && provider.proposal_expires_at == *proposal_expires_at
                && provider.deadline == response.deadline_at
                && timestamp_has_microsecond_precision(provider.invoice_created_at)
                && timestamp_has_microsecond_precision(provider.proposal_expires_at)
                && timestamp_has_microsecond_precision(provider.deadline)
                && provider.invoice_created_at >= response.submitted_at
                && provider.invoice_created_at < provider.proposal_expires_at
                && provider.proposal_expires_at < provider.deadline
                && provider.minimum_confirmations <= 6
                && provider.current_sample.as_ref().is_none_or(|sample| {
                    valid_payment_sample(sample)
                        && sample.observed_at >= provider.invoice_created_at
                })
                && provider
                    .first_threshold_satisfied
                    .as_ref()
                    .is_none_or(|sample| {
                        valid_payment_sample(sample)
                            && sample.observed_at >= provider.invoice_created_at
                            && sample.observed_at <= provider.deadline
                            && sample.outpoint.is_some()
                            && sample.amount_matched
                            && sample.confirmations >= provider.minimum_confirmations
                            && sample.status != MixedPaymentSampleStatus::Undetected
                    })
        }
        _ => false,
    };
    let path_has_payment = response.satisfied_by.contains(&SatisfiedByV2::Payment);
    let path_has_graph = response
        .satisfied_by
        .contains(&SatisfiedByV2::ZeroConfGraph);
    let provider_live = response
        .payment
        .provider_status
        .as_ref()
        .is_some_and(|provider| {
            matches!(
                provider.request_state,
                MixedPaymentRequestState::Accepted
                    | MixedPaymentRequestState::ProofSubmitted
                    | MixedPaymentRequestState::ActiveRecurring
            ) && provider.payment_state != MixedPaymentState::Expired
        });
    let payment_satisfied = response
        .payment
        .provider_status
        .as_ref()
        .and_then(|provider| provider.first_threshold_satisfied.as_ref())
        .is_some();
    let successful = matches!(
        response.status,
        VerificationTaskStatus::PublishingEntitlement | VerificationTaskStatus::Completed
    );
    let satisfaction_valid = if successful {
        provider_live
            && payment_satisfied == path_has_payment
            && graph_satisfied == path_has_graph
            && (path_has_payment || path_has_graph)
    } else {
        response.satisfied_by.is_empty()
    };
    if response.creator != *response.pubky_lock_resource.creator()
        || response.deadline_at < response.submitted_at
        || response
            .completed_at
            .is_some_and(|completed| completed < response.submitted_at)
        || response.graph.predicates.is_empty()
        || response.graph.predicates.len() > 8
        || response
            .graph
            .predicates
            .iter()
            .enumerate()
            .any(|(index, predicate)| predicate.index != index)
        || !canonical
        || !valid_status
        || !provider_valid
        || !satisfaction_valid
        || response.payment.operational_failure_count > response.payment.operational_failure_total
    {
        return Err(invalid("invalid creator mixed verification evidence"));
    }
    Ok(response)
}

fn valid_payment_sample(sample: &MixedPaymentSample) -> bool {
    if !timestamp_has_microsecond_precision(sample.observed_at) || sample.confirmations > 6 {
        return false;
    }
    if sample.outpoint.as_ref().is_some_and(|outpoint| {
        outpoint.txid.len() != 64
            || !outpoint
                .txid
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    }) {
        return false;
    }
    match sample.status {
        MixedPaymentSampleStatus::Undetected => {
            sample.outpoint.is_none() && sample.confirmations == 0 && !sample.amount_matched
        }
        MixedPaymentSampleStatus::Detected => {
            sample.outpoint.is_some() && sample.confirmations == 0
        }
        MixedPaymentSampleStatus::Confirmed => {
            sample.outpoint.is_some() && sample.confirmations > 0
        }
    }
}

fn timestamp_has_microsecond_precision(value: OffsetDateTime) -> bool {
    value.nanosecond().is_multiple_of(1_000)
}

fn graph_predicate_satisfied(
    predicate: &crate::graph::CreatorGraphPredicateEvidenceResponse,
) -> bool {
    predicate.state == GraphPredicateEvidenceState::Satisfied
        && predicate
            .first_true_observation
            .as_ref()
            .is_some_and(|observation| observation.outcome == GraphObservationOutcome::True)
}

fn invalid(message: impl Into<String>) -> LocksSdkError {
    LocksSdkError::InvalidResponse(message.into())
}
