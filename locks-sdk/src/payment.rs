use locks_core::ids::{BundleId, CreatorPubky};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::viewer::{VerificationTaskStatus, VerificationTerminalReason};
use crate::{LocksSdkError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaymentVerificationTaskLifecycleResponse {
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

pub fn parse_payment_lifecycle_response(
    value: Value,
) -> Result<PaymentVerificationTaskLifecycleResponse> {
    crate::viewer::reject_fields(&value, crate::viewer::SECRET_BEARING_RESPONSE_FIELDS)?;
    for field in ["completed_at", "terminal_reason"] {
        if value.get(field).is_some_and(Value::is_null) {
            return Err(invalid(format!("{field} must be omitted rather than null")));
        }
    }
    let response: PaymentVerificationTaskLifecycleResponse =
        serde_json::from_value(value).map_err(|error| invalid(error.to_string()))?;
    let terminal = response.terminal_reason.as_ref();
    let valid_status = match response.status {
        VerificationTaskStatus::Pending
        | VerificationTaskStatus::InProgress
        | VerificationTaskStatus::PublishingEntitlement => {
            response.completed_at.is_none() && terminal.is_none()
        }
        VerificationTaskStatus::Completed => response.completed_at.is_some() && terminal.is_none(),
        VerificationTaskStatus::Expired => {
            response.completed_at.is_some()
                && matches!(
                    terminal,
                    Some(
                        VerificationTerminalReason::PaymentRequestRejected
                            | VerificationTerminalReason::PaymentRequestCanceled
                            | VerificationTerminalReason::ProposalExpired
                            | VerificationTerminalReason::PaymentDeadlineExpired
                            | VerificationTerminalReason::ContentLockDeleted
                    )
                )
        }
        VerificationTaskStatus::Failed => {
            response.completed_at.is_some()
                && matches!(
                    terminal,
                    Some(
                        VerificationTerminalReason::VerificationUnavailableByDeadline
                            | VerificationTerminalReason::InvalidVerifierResponse
                            | VerificationTerminalReason::VerificationConflict
                    )
                )
        }
    };
    if !valid_status
        || response.admission_deadline_at < response.submitted_at
        || response
            .completed_at
            .is_some_and(|completed| completed < response.submitted_at)
    {
        return Err(invalid(
            "invalid payment verification lifecycle field combination",
        ));
    }
    Ok(response)
}

fn invalid(message: impl Into<String>) -> LocksSdkError {
    LocksSdkError::InvalidResponse(message.into())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const BUNDLE: &str = "000G40R40M30E209185GR38E1W";

    fn pending() -> Value {
        json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE,
            "status": "pending",
            "submitted_at": "2026-11-05T12:00:00Z",
            "admission_deadline_at": "2026-11-05T12:05:00Z"
        })
    }

    #[test]
    fn payment_lifecycle_ignores_additive_non_secret_fields() {
        let mut value = pending();
        value["future_diagnostic"] = json!({"safe": true});

        let parsed = parse_payment_lifecycle_response(value).unwrap();
        let reserialized = serde_json::to_value(parsed).unwrap();
        assert!(reserialized.get("future_diagnostic").is_none());
    }

    #[test]
    fn payment_lifecycle_rejects_secret_bearing_additive_fields() {
        for field in [
            "task_id",
            "credential",
            "submitted_proof_bundle",
            "entitlement_to_publish",
        ] {
            let mut value = pending();
            value[field] = json!("must-not-cross-sdk-boundary");
            assert!(parse_payment_lifecycle_response(value).is_err(), "{field}");
        }
    }

    #[test]
    fn payment_lifecycle_accepts_only_payment_terminal_reasons() {
        for (status, reason, accepted) in [
            ("expired", "payment_request_rejected", true),
            ("expired", "payment_deadline_expired", true),
            ("failed", "verification_conflict", true),
            ("failed", "invalid_verifier_response", true),
            ("failed", "graph_condition_not_observed_by_deadline", false),
            ("expired", "verification_conflict", false),
        ] {
            let value = json!({
                "creator": CREATOR,
                "bundle_id": BUNDLE,
                "status": status,
                "submitted_at": "2026-11-05T12:00:00Z",
                "completed_at": "2026-11-05T12:01:00Z",
                "admission_deadline_at": "2026-11-05T12:05:00Z",
                "terminal_reason": reason
            });
            assert_eq!(
                parse_payment_lifecycle_response(value).is_ok(),
                accepted,
                "{status}/{reason}"
            );
        }
    }
}
