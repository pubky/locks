use std::fmt;

use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource};
use locks_core::lock_policy::{GraphOperatorV2, GraphPredicateV2};
use locks_core::verification::SatisfiedByV2;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::viewer::{VerificationTaskStatus, VerificationTerminalReason};
use crate::{LocksSdkError, Result};

#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FoldedAuthStatusRequest {
    state: String,
}

impl FoldedAuthStatusRequest {
    pub fn new(state: impl Into<String>) -> Self {
        Self {
            state: state.into(),
        }
    }

    pub fn state(&self) -> &str {
        &self.state
    }
}

impl fmt::Debug for FoldedAuthStatusRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FoldedAuthStatusRequest")
            .field("state", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphSubmitResponse {
    AuthPending {
        auth_url: String,
        state: String,
        #[serde(with = "time::serde::rfc3339")]
        expires_at: OffsetDateTime,
    },
    Accepted {
        task: GraphVerificationTaskLifecycleResponse,
    },
}

impl fmt::Debug for GraphSubmitResponse {
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

impl GraphSubmitResponse {
    pub fn auth_pending_parts(&self) -> Option<(&str, &str, OffsetDateTime)> {
        match self {
            Self::AuthPending {
                auth_url,
                state,
                expires_at,
            } => Some((auth_url, state, *expires_at)),
            Self::Accepted { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum FoldedAuthStatusResponse {
    Pending {
        #[serde(with = "time::serde::rfc3339")]
        expires_at: OffsetDateTime,
    },
    Accepted {
        task: GraphVerificationTaskLifecycleResponse,
    },
    RestartRequired,
    Expired {
        #[serde(with = "time::serde::rfc3339")]
        expired_at: OffsetDateTime,
    },
}

impl FoldedAuthStatusResponse {
    pub const fn status_name(&self) -> &'static str {
        match self {
            Self::Pending { .. } => "pending",
            Self::Accepted { .. } => "accepted",
            Self::RestartRequired => "restart_required",
            Self::Expired { .. } => "expired",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphVerificationTaskLifecycleResponse {
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

pub fn parse_graph_lifecycle_response(
    value: Value,
) -> Result<GraphVerificationTaskLifecycleResponse> {
    reject_null_optional_fields(&value, &["completed_at", "terminal_reason"])?;
    let response: GraphVerificationTaskLifecycleResponse = parse(value)?;
    let valid = valid_graph_status_fields(
        &response.status,
        response.completed_at.is_some(),
        response.terminal_reason.as_ref(),
    ) && response.admission_deadline_at >= response.submitted_at
        && response
            .completed_at
            .is_none_or(|completed| completed >= response.submitted_at);
    if !valid {
        return Err(invalid(
            "invalid graph verification lifecycle field combination",
        ));
    }
    Ok(response)
}

pub fn parse_graph_submit_response(value: Value) -> Result<GraphSubmitResponse> {
    let status = tagged_status(&value)?;
    let allowed = match status {
        "auth_pending" => &["status", "auth_url", "state", "expires_at"][..],
        "accepted" => &["status", "task"][..],
        _ => return Err(invalid("unknown graph submit status")),
    };
    reject_unknown_fields(&value, allowed)?;
    if status == "accepted" {
        let task = value
            .get("task")
            .cloned()
            .ok_or_else(|| invalid("accepted graph submit response requires task"))?;
        parse_graph_lifecycle_response(task)?;
    }
    parse(value)
}

pub fn parse_folded_auth_status_response(value: Value) -> Result<FoldedAuthStatusResponse> {
    let status = tagged_status(&value)?;
    let allowed = match status {
        "pending" => &["status", "expires_at"][..],
        "accepted" => &["status", "task"][..],
        "restart_required" => &["status"][..],
        "expired" => &["status", "expired_at"][..],
        _ => return Err(invalid("unknown folded auth status")),
    };
    reject_unknown_fields(&value, allowed)?;
    if status == "accepted" {
        let task = value
            .get("task")
            .cloned()
            .ok_or_else(|| invalid("accepted folded auth response requires task"))?;
        parse_graph_lifecycle_response(task)?;
    }
    parse(value)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphEvidenceMode {
    Graph,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphPredicateEvidenceState {
    Pending,
    Satisfied,
    NotObserved,
    Unavailable,
    InvalidResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphObservationOutcome {
    True,
    False,
    DependencyUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphOperationalErrorCategory {
    Redirect,
    RateLimited,
    NonSuccess,
    Timeout,
    Transport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatorVerificationEvidenceResponse {
    pub creator: CreatorPubky,
    pub reader: CreatorPubky,
    pub bundle_id: BundleId,
    pub pubky_lock_resource: PubkyLockResource,
    pub mode: GraphEvidenceMode,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatorGraphEvidenceResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<GraphOperatorV2>,
    pub predicates: Vec<CreatorGraphPredicateEvidenceResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatorGraphPredicateEvidenceResponse {
    pub index: usize,
    pub predicate: GraphPredicateV2,
    pub state: GraphPredicateEvidenceState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_observation: Option<CreatorGraphObservationResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_true_observation: Option<CreatorGraphObservationResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operational_error: Option<CreatorGraphOperationalErrorResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatorGraphObservationResponse {
    pub outcome: GraphObservationOutcome,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatorGraphOperationalErrorResponse {
    pub consecutive_count: u32,
    pub total_count: u32,
    pub last_category: GraphOperationalErrorCategory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_http_status: Option<u16>,
    #[serde(with = "time::serde::rfc3339")]
    pub first_failure_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub last_failure_at: OffsetDateTime,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub next_due_at: Option<OffsetDateTime>,
}

pub fn parse_creator_verification_evidence_response(
    value: Value,
) -> Result<CreatorVerificationEvidenceResponse> {
    reject_any_null(&value)?;
    let response: CreatorVerificationEvidenceResponse = parse(value)?;
    let valid_status = valid_graph_status_fields(
        &response.status,
        response.completed_at.is_some(),
        response.terminal_reason.as_ref(),
    );
    let valid_satisfaction = match response.status {
        VerificationTaskStatus::PublishingEntitlement | VerificationTaskStatus::Completed => {
            response.satisfied_by.as_slice() == [SatisfiedByV2::Graph]
        }
        VerificationTaskStatus::Pending
        | VerificationTaskStatus::InProgress
        | VerificationTaskStatus::Failed
        | VerificationTaskStatus::Expired => response.satisfied_by.is_empty(),
    };
    if response.creator != *response.pubky_lock_resource.creator()
        || response.deadline_at < response.submitted_at
        || !valid_status
        || !valid_satisfaction
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
    {
        return Err(invalid("invalid creator graph verification evidence"));
    }
    Ok(response)
}

fn valid_graph_status_fields(
    status: &VerificationTaskStatus,
    has_completed_at: bool,
    terminal_reason: Option<&VerificationTerminalReason>,
) -> bool {
    match status {
        VerificationTaskStatus::Pending
        | VerificationTaskStatus::InProgress
        | VerificationTaskStatus::PublishingEntitlement => {
            !has_completed_at && terminal_reason.is_none()
        }
        VerificationTaskStatus::Completed => has_completed_at && terminal_reason.is_none(),
        VerificationTaskStatus::Expired => {
            has_completed_at
                && terminal_reason
                    == Some(&VerificationTerminalReason::GraphConditionNotObservedByDeadline)
        }
        VerificationTaskStatus::Failed => {
            has_completed_at
                && matches!(
                    terminal_reason,
                    Some(
                        VerificationTerminalReason::VerificationUnavailableByDeadline
                            | VerificationTerminalReason::InvalidVerifierResponse
                    )
                )
        }
    }
}

fn reject_any_null(value: &Value) -> Result<()> {
    match value {
        Value::Null => Err(invalid(
            "optional response fields must be omitted rather than null",
        )),
        Value::Array(values) => values.iter().try_for_each(reject_any_null),
        Value::Object(values) => values.values().try_for_each(reject_any_null),
        _ => Ok(()),
    }
}

fn reject_null_optional_fields(value: &Value, fields: &[&str]) -> Result<()> {
    for field in fields {
        if value.get(*field).is_some_and(Value::is_null) {
            return Err(invalid(format!("{field} must be omitted rather than null")));
        }
    }
    Ok(())
}

fn tagged_status(value: &Value) -> Result<&str> {
    value
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("response requires string status"))
}

fn reject_unknown_fields(value: &Value, allowed: &[&str]) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("response must be a JSON object"))?;
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(invalid(format!("unknown response field: {field}")));
    }
    Ok(())
}

fn parse<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|err| invalid(err.to_string()))
}

fn invalid(message: impl Into<String>) -> LocksSdkError {
    LocksSdkError::InvalidResponse(message.into())
}
