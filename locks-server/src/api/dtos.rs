pub use locks_core::creator_publishing::{
    CreateContentLockRequest as AuthenticatedCreateContentLockHttpRequest,
    SetLockServicePointerRequest as AuthenticatedSetLockServicePointerHttpRequest,
};
use locks_core::ids::{BundleId, ContentLockPath, CreatorPubky, LockId, LockServerPubky};
use locks_core::lock_policy::{ContentLock, GraphOperatorV2, GraphPredicateV2, GuardedResource};
use locks_core::lock_service_pointer::LockServicePointer;
use locks_core::verification::{SatisfiedByV2, SubmittedProofBundlePayload};
use locks_service::application::models::{
    AccessCredential, FrontendSessionCode, GraphObservationOutcome, GraphOperationalErrorCategory,
    GraphPredicateObservation, GraphTaskRecord, VerificationTaskStatus, VerificationTerminalReason,
};
use locks_service::application::use_cases::exchange_frontend_session_code::{
    ExchangeFrontendSessionCodeRequest, ExchangeFrontendSessionCodeResponse,
};
use locks_service::application::use_cases::get_creator_authority_status::CreatorAuthorityStatusView;
use locks_service::application::use_cases::get_verification_task::VerificationTaskLifecycleView;
use locks_service::application::use_cases::issue_access_credential::IssuedAccessCredential;

use std::fmt;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::paykit_http_client::{PaykitConnectionState, PaykitSetupStatusKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HealthHttpResponse {
    pub status: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadinessHttpResponse {
    pub status: &'static str,
    pub runtime_storage: &'static str,
    pub worker_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WellKnownLocksServerHttpResponse {
    pub service: &'static str,
    pub api_version: &'static str,
    pub lock_server: LockServerPubky,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PaykitSetupStatusHttpResponse {
    pub status: PaykitSetupStatusKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PaykitConnectionStateHttpResponse {
    pub state: PaykitConnectionState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegisterGuardedResourceHttpResponse {
    pub creator: CreatorPubky,
    pub guarded_resource: GuardedResource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreateContentLockHttpResponse {
    pub lock_id: LockId,
    pub content_lock_path: ContentLockPath,
    pub content_lock: ContentLock,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentLockDeletionStatusHttpResponse {
    pub lock_id: LockId,
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SetLockServicePointerHttpResponse {
    pub creator: CreatorPubky,
    pub path: &'static str,
    pub lock_service_pointer: LockServicePointer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitProofBundleHttpRequest {
    pub submitted_proof_bundle: SubmittedProofBundlePayload,
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct FoldedAuthPendingHttpResponse {
    pub status: &'static str,
    pub auth_url: String,
    pub state: String,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

impl fmt::Debug for FoldedAuthPendingHttpResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FoldedAuthPendingHttpResponse")
            .field("status", &self.status)
            .field("auth_url", &"<redacted>")
            .field("state", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FoldedAuthStatusHttpRequest {
    pub state: String,
}

impl fmt::Debug for FoldedAuthStatusHttpRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FoldedAuthStatusHttpRequest")
            .field("state", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum FoldedAuthStatusHttpResponse {
    Pending {
        #[serde(with = "time::serde::rfc3339")]
        expires_at: OffsetDateTime,
    },
    Accepted {
        task: GraphVerificationTaskLifecycleHttpResponse,
    },
    RestartRequired,
    Expired {
        #[serde(with = "time::serde::rfc3339")]
        expired_at: OffsetDateTime,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerificationTaskLifecycleHttpResponse {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    #[serde(serialize_with = "serialize_task_status")]
    pub status: VerificationTaskStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub submitted_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub completed_at: Option<OffsetDateTime>,
    pub failure_message: Option<String>,
    pub status_message: Option<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub admission_deadline_at: Option<OffsetDateTime>,
    #[serde(serialize_with = "serialize_optional_terminal_reason")]
    pub terminal_reason: Option<VerificationTerminalReason>,
}

impl From<VerificationTaskLifecycleView> for VerificationTaskLifecycleHttpResponse {
    fn from(view: VerificationTaskLifecycleView) -> Self {
        Self {
            creator: view.creator,
            bundle_id: view.bundle_id,
            status: view.status,
            submitted_at: view.submitted_at,
            started_at: view.started_at,
            completed_at: view.completed_at,
            failure_message: view.failure_message,
            status_message: None,
            admission_deadline_at: None,
            terminal_reason: view.terminal_reason,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GraphVerificationTaskLifecycleHttpResponse {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    #[serde(serialize_with = "serialize_task_status")]
    pub status: VerificationTaskStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub submitted_at: OffsetDateTime,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub completed_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub admission_deadline_at: OffsetDateTime,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_terminal_reason"
    )]
    pub terminal_reason: Option<VerificationTerminalReason>,
}

impl From<&GraphTaskRecord> for GraphVerificationTaskLifecycleHttpResponse {
    fn from(task: &GraphTaskRecord) -> Self {
        let snapshot = task.snapshot.as_v1();
        let terminal_reason = (snapshot.root.terminal_status() == Some(task.status))
            .then(|| snapshot.root.terminal_reason())
            .flatten();
        Self {
            creator: snapshot.creator.clone(),
            bundle_id: snapshot.bundle_id.clone(),
            status: task.status,
            submitted_at: snapshot.accepted_at,
            completed_at: task.completed_at,
            admission_deadline_at: snapshot.deadline_at,
            terminal_reason,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationTaskHandleHttpRequest {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatorVerificationEvidenceHttpRequest {
    pub bundle_id: BundleId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreatorVerificationEvidenceHttpResponse {
    pub creator: CreatorPubky,
    pub reader: CreatorPubky,
    pub bundle_id: BundleId,
    pub pubky_lock_resource: locks_core::ids::PubkyLockResource,
    pub mode: &'static str,
    #[serde(serialize_with = "serialize_task_status")]
    pub status: VerificationTaskStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub submitted_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub deadline_at: OffsetDateTime,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub completed_at: Option<OffsetDateTime>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_terminal_reason"
    )]
    pub terminal_reason: Option<VerificationTerminalReason>,
    pub satisfied_by: Vec<SatisfiedByV2>,
    pub graph: CreatorGraphEvidenceHttpResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreatorGraphEvidenceHttpResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<GraphOperatorV2>,
    pub predicates: Vec<CreatorGraphPredicateEvidenceHttpResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreatorGraphPredicateEvidenceHttpResponse {
    pub index: usize,
    pub predicate: GraphPredicateV2,
    pub state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_observation: Option<CreatorGraphObservationHttpResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_true_observation: Option<CreatorGraphObservationHttpResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operational_error: Option<CreatorGraphOperationalErrorHttpResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreatorGraphObservationHttpResponse {
    pub outcome: GraphObservationOutcome,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreatorGraphOperationalErrorHttpResponse {
    pub consecutive_count: u32,
    pub total_count: u32,
    pub last_category: GraphOperationalErrorCategory,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_http_status: Option<u16>,
    #[serde(with = "time::serde::rfc3339")]
    pub first_failure_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub last_failure_at: OffsetDateTime,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub next_due_at: Option<OffsetDateTime>,
}

impl From<GraphPredicateObservation> for CreatorGraphObservationHttpResponse {
    fn from(observation: GraphPredicateObservation) -> Self {
        Self {
            outcome: observation.outcome,
            observed_at: observation.observed_at,
            origin: observation.origin,
            http_status: observation.http_status,
        }
    }
}

impl From<GraphTaskRecord> for CreatorVerificationEvidenceHttpResponse {
    fn from(task: GraphTaskRecord) -> Self {
        let snapshot = task.snapshot.as_v1();
        let terminal_reason = (snapshot.root.terminal_status() == Some(task.status))
            .then(|| snapshot.root.terminal_reason())
            .flatten();
        let satisfied_by = task
            .entitlement_to_publish
            .as_ref()
            .map(|payload| payload.satisfied_by.clone())
            .unwrap_or_default();
        let predicates = snapshot
            .predicates
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, predicate)| {
                let state = if predicate.first_true_observation.is_some() {
                    "satisfied"
                } else if predicate.terminal_invalid {
                    "invalid_response"
                } else {
                    match predicate
                        .latest_observation
                        .as_ref()
                        .map(|value| value.outcome)
                    {
                        Some(GraphObservationOutcome::False) => "not_observed",
                        Some(GraphObservationOutcome::DependencyUnavailable) => "unavailable",
                        Some(GraphObservationOutcome::True) => "satisfied",
                        None => "pending",
                    }
                };
                CreatorGraphPredicateEvidenceHttpResponse {
                    index,
                    predicate: predicate.predicate,
                    state,
                    latest_observation: predicate.latest_observation.map(Into::into),
                    first_true_observation: predicate.first_true_observation.map(Into::into),
                    operational_error: (predicate.operational_failure_total > 0).then(|| {
                        CreatorGraphOperationalErrorHttpResponse {
                            consecutive_count: predicate.operational_failure_count,
                            total_count: predicate.operational_failure_total,
                            last_category: predicate
                                .operational_last_category
                                .expect("validated graph operational aggregate has category"),
                            last_http_status: predicate.operational_last_http_status,
                            first_failure_at: predicate
                                .operational_first_failure_at
                                .expect("validated graph operational aggregate has first time"),
                            last_failure_at: predicate
                                .operational_last_failure_at
                                .expect("validated graph operational aggregate has last time"),
                            next_due_at: predicate.next_due_at,
                        }
                    }),
                }
            })
            .collect();
        Self {
            creator: snapshot.creator.clone(),
            reader: snapshot.reader.clone(),
            bundle_id: snapshot.bundle_id.clone(),
            pubky_lock_resource: snapshot.pubky_lock_resource.clone(),
            mode: "graph",
            status: task.status,
            submitted_at: snapshot.accepted_at,
            deadline_at: snapshot.deadline_at,
            completed_at: task.completed_at,
            terminal_reason,
            satisfied_by,
            graph: CreatorGraphEvidenceHttpResponse {
                operator: snapshot.operator,
                predicates,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssueAccessCredentialHttpRequest {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IssueAccessCredentialHttpResponse {
    #[serde(serialize_with = "serialize_access_credential")]
    pub credential: AccessCredential,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

impl From<IssuedAccessCredential> for IssueAccessCredentialHttpResponse {
    fn from(issued: IssuedAccessCredential) -> Self {
        Self {
            credential: issued.credential,
            expires_at: issued.expires_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeFrontendSessionCodeHttpRequest {
    pub code: String,
    pub state: String,
}

impl From<ExchangeFrontendSessionCodeHttpRequest> for ExchangeFrontendSessionCodeRequest {
    fn from(request: ExchangeFrontendSessionCodeHttpRequest) -> Self {
        Self {
            code: FrontendSessionCode::new(request.code),
            state: request.state,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExchangeFrontendSessionCodeHttpResponse {
    pub session_token: String,
    pub creator: CreatorPubky,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

impl From<ExchangeFrontendSessionCodeResponse> for ExchangeFrontendSessionCodeHttpResponse {
    fn from(response: ExchangeFrontendSessionCodeResponse) -> Self {
        Self {
            session_token: response.session_token.expose_token().to_owned(),
            creator: response.creator,
            expires_at: response.expires_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreatorAuthorityStatusHttpResponse {
    pub creator: CreatorPubky,
    pub authorized: bool,
    pub auth_kind: Option<String>,
    pub granted_scopes: Vec<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub session_expires_at: Option<OffsetDateTime>,
}

impl From<CreatorAuthorityStatusView> for CreatorAuthorityStatusHttpResponse {
    fn from(view: CreatorAuthorityStatusView) -> Self {
        Self {
            creator: view.creator,
            authorized: view.authorized,
            auth_kind: view
                .auth_kind
                .map(|auth_kind| auth_kind.as_str().to_owned()),
            granted_scopes: view.granted_scopes,
            session_expires_at: view.session_expires_at,
        }
    }
}

fn serialize_task_status<S>(
    status: &VerificationTaskStatus,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(match status {
        VerificationTaskStatus::Pending => "pending",
        VerificationTaskStatus::InProgress | VerificationTaskStatus::PublishingEntitlement => {
            "in_progress"
        }
        VerificationTaskStatus::Completed => "completed",
        VerificationTaskStatus::Failed => "failed",
        VerificationTaskStatus::Expired => "expired",
    })
}

fn serialize_optional_terminal_reason<S>(
    reason: &Option<VerificationTerminalReason>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match reason {
        Some(reason) => serializer.serialize_some(reason.as_str()),
        None => serializer.serialize_none(),
    }
}

fn serialize_access_credential<S>(
    credential: &AccessCredential,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(credential.as_str())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
    use locks_core::lock_policy::{
        GraphParticipantV2, GraphPolicyV2, GraphPredicateV2, VerifierType,
    };
    use locks_core::verification::{
        Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle, SubmittedProofBundlePayload,
    };
    use locks_service::application::models::{
        AccessCredential, CreatorAuthorityAuthKind, GraphDeadlineResult, GraphRootState,
        GraphTaskRecord, GraphTaskSnapshot, GraphTaskSnapshotV1, VerificationTaskStatus,
    };
    use locks_service::application::use_cases::get_creator_authority_status::CreatorAuthorityStatusView;
    use locks_service::application::use_cases::get_verification_task::VerificationTaskLifecycleView;
    use locks_service::application::use_cases::issue_access_credential::IssuedAccessCredential;
    use serde_json::{Value, json};
    use time::macros::datetime;

    use super::{
        CreatorAuthorityStatusHttpResponse, CreatorVerificationEvidenceHttpResponse,
        ExchangeFrontendSessionCodeHttpRequest, ExchangeFrontendSessionCodeHttpResponse,
        FoldedAuthPendingHttpResponse, FoldedAuthStatusHttpRequest,
        GraphVerificationTaskLifecycleHttpResponse, IssueAccessCredentialHttpRequest,
        IssueAccessCredentialHttpResponse, SubmitProofBundleHttpRequest,
        VerificationTaskHandleHttpRequest, VerificationTaskLifecycleHttpResponse,
    };

    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    #[test]
    fn folded_auth_pending_debug_redacts_auth_url_and_state() {
        let response = FoldedAuthPendingHttpResponse {
            status: "auth_pending",
            auth_url: "pubkyauth://secret-bearing-url".to_owned(),
            state: "secret-bearing-state".to_owned(),
            expires_at: datetime!(2026-10-30 12:10:00 UTC),
        };

        let debug = format!("{response:?}");
        assert!(!debug.contains("secret-bearing-url"));
        assert!(!debug.contains("secret-bearing-state"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn folded_auth_status_request_debug_redacts_state() {
        let request = FoldedAuthStatusHttpRequest {
            state: "secret-bearing-state".to_owned(),
        };

        let debug = format!("{request:?}");
        assert!(!debug.contains("secret-bearing-state"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn submit_proof_request_uses_explicit_envelope() {
        let request = SubmitProofBundleHttpRequest {
            submitted_proof_bundle: SubmittedProofBundlePayload::V1(submitted_proof_bundle()),
        };

        let json = serde_json::to_value(request).unwrap();

        assert!(json.get("submitted_proof_bundle").is_some());
        assert!(json.get("bundle_id").is_none());
        assert!(json.get("proofs").is_none());
    }

    #[test]
    fn submit_proof_response_uses_public_lifecycle_handle_without_task_id() {
        let response = VerificationTaskLifecycleHttpResponse::from(VerificationTaskLifecycleView {
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            status: VerificationTaskStatus::Pending,
            submitted_at: datetime!(2026-05-29 12:00:00 UTC),
            started_at: None,
            completed_at: None,
            failure_message: None,
            terminal_reason: None,
        });

        let json = serde_json::to_value(response).unwrap();

        assert_eq!(
            json["creator"],
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
        );
        assert_eq!(json["bundle_id"], BUNDLE_ID);
        assert_eq!(json["status"], "pending");
        assert_eq!(json["submitted_at"], "2026-05-29T12:00:00Z");
        assert_no_keys(
            &json,
            &[
                "task_id",
                "pubky_lock_resource",
                "submitted_proof_bundle",
                "proofs",
            ],
        );
    }

    #[test]
    fn verification_task_response_is_lifecycle_only_and_secret_free() {
        let response = VerificationTaskLifecycleHttpResponse::from(VerificationTaskLifecycleView {
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            status: VerificationTaskStatus::Completed,
            submitted_at: datetime!(2026-05-29 12:00:00 UTC),
            started_at: Some(datetime!(2026-05-29 12:01:00 UTC)),
            completed_at: Some(datetime!(2026-05-29 12:02:00 UTC)),
            failure_message: None,
            terminal_reason: None,
        });

        let json = serde_json::to_value(response).unwrap();

        assert_eq!(
            json["creator"],
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
        );
        assert_eq!(json["bundle_id"], BUNDLE_ID);
        assert_eq!(json["status"], "completed");
        assert_eq!(json["submitted_at"], "2026-05-29T12:00:00Z");
        assert_eq!(json["started_at"], "2026-05-29T12:01:00Z");
        assert_eq!(json["completed_at"], "2026-05-29T12:02:00Z");
        assert_no_keys(&json, &["task_id", "credential", "credential_issuance"]);
    }

    #[test]
    fn expired_verification_task_response_serializes_typed_terminal_reason() {
        let response = VerificationTaskLifecycleHttpResponse::from(VerificationTaskLifecycleView {
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            status: VerificationTaskStatus::Expired,
            submitted_at: datetime!(2026-05-29 12:00:00 UTC),
            started_at: Some(datetime!(2026-05-29 12:01:00 UTC)),
            completed_at: Some(datetime!(2026-05-29 12:02:00 UTC)),
            failure_message: None,
            terminal_reason: Some(
                locks_service::application::models::VerificationTerminalReason::ProposalExpired,
            ),
        });

        let json = serde_json::to_value(response).unwrap();

        assert_eq!(json["status"], "expired");
        assert_eq!(json["terminal_reason"], "proposal_expired");
        assert_eq!(json["failure_message"], Value::Null);
    }

    #[test]
    fn creator_evidence_omits_snapshot_terminal_reason_for_legacy_in_progress_row() {
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let lock_hash = [0_u8; 32];
        let lock_id = LockId::from_hash(LockHash::from_bytes(lock_hash));
        let resource =
            PubkyLockResource::from_str(&format!("{creator}/pub/app.locks/{lock_id}.json"))
                .unwrap();
        let mut snapshot = GraphTaskSnapshotV1::new(
            creator,
            BundleId::from_str(BUNDLE_ID).unwrap(),
            resource,
            lock_hash,
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap(),
            datetime!(2026-05-29 12:00:00 UTC),
            60,
            GraphPolicyV2 {
                predicates: vec![GraphPredicateV2::Follows {
                    follower: GraphParticipantV2::Reader,
                    followed: GraphParticipantV2::Creator,
                }],
                operator: None,
            },
        )
        .unwrap();
        snapshot.root = GraphRootState::Expired(GraphDeadlineResult::False);
        let response = CreatorVerificationEvidenceHttpResponse::from(GraphTaskRecord {
            task_id: TaskId::from_str(TASK_ID).unwrap(),
            snapshot: GraphTaskSnapshot::V1(snapshot),
            evaluator_revision: 1,
            status: VerificationTaskStatus::InProgress,
            entitlement_to_publish: None,
            entitlement_publication_token: None,
            completed_at: None,
        });

        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json["status"], "in_progress");
        assert!(json.get("terminal_reason").is_none());
    }

    #[test]
    fn graph_lifecycle_omits_snapshot_terminal_reason_for_legacy_in_progress_row() {
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let lock_hash = [0_u8; 32];
        let lock_id = LockId::from_hash(LockHash::from_bytes(lock_hash));
        let resource =
            PubkyLockResource::from_str(&format!("{creator}/pub/app.locks/{lock_id}.json"))
                .unwrap();
        let mut snapshot = GraphTaskSnapshotV1::new(
            creator,
            BundleId::from_str(BUNDLE_ID).unwrap(),
            resource,
            lock_hash,
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap(),
            datetime!(2026-05-29 12:00:00 UTC),
            60,
            GraphPolicyV2 {
                predicates: vec![GraphPredicateV2::Follows {
                    follower: GraphParticipantV2::Reader,
                    followed: GraphParticipantV2::Creator,
                }],
                operator: None,
            },
        )
        .unwrap();
        snapshot.root = GraphRootState::Expired(GraphDeadlineResult::False);
        let task = GraphTaskRecord {
            task_id: TaskId::from_str(TASK_ID).unwrap(),
            snapshot: GraphTaskSnapshot::V1(snapshot),
            evaluator_revision: 1,
            status: VerificationTaskStatus::InProgress,
            entitlement_to_publish: None,
            entitlement_publication_token: None,
            completed_at: None,
        };

        let json =
            serde_json::to_value(GraphVerificationTaskLifecycleHttpResponse::from(&task)).unwrap();
        assert_eq!(json["status"], "in_progress");
        assert!(json.get("terminal_reason").is_none());
    }

    #[test]
    fn verification_task_handle_request_accepts_creator_and_bundle_id_only() {
        let request: VerificationTaskHandleHttpRequest = serde_json::from_value(json!({
            "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "bundle_id": BUNDLE_ID,
        }))
        .unwrap();

        assert_eq!(
            request.creator,
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap()
        );
        assert_eq!(request.bundle_id, BundleId::from_str(BUNDLE_ID).unwrap());
        assert!(
            serde_json::from_value::<VerificationTaskHandleHttpRequest>(json!({
                "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                "bundle_id": BUNDLE_ID,
                "task_id": "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10"
            }))
            .is_err()
        );
    }

    #[test]
    fn issue_access_credential_request_accepts_creator_and_bundle_id_only() {
        let json = json!({
            "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "bundle_id": BUNDLE_ID
        });

        let request: IssueAccessCredentialHttpRequest = serde_json::from_value(json).unwrap();

        assert_eq!(
            request.creator,
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap()
        );
        assert_eq!(request.bundle_id, BundleId::from_str(BUNDLE_ID).unwrap());
    }

    #[test]
    fn issue_access_credential_request_rejects_task_id_shortcut() {
        let json = json!({ "task_id": TASK_ID });

        let result = serde_json::from_value::<IssueAccessCredentialHttpRequest>(json);

        assert!(result.is_err());
    }

    #[test]
    fn issue_access_credential_response_is_only_success_dto_with_raw_credential() {
        let response = IssueAccessCredentialHttpResponse::from(IssuedAccessCredential {
            credential: AccessCredential::new("raw-secret-bearer"),
            expires_at: datetime!(2026-05-29 12:15:00 UTC),
        });

        let json = serde_json::to_value(response).unwrap();

        assert_eq!(json["credential"], "raw-secret-bearer");
        assert_eq!(json["expires_at"], "2026-05-29T12:15:00Z");
    }

    #[test]
    fn frontend_session_exchange_request_accepts_code_and_state_only() {
        let request: ExchangeFrontendSessionCodeHttpRequest = serde_json::from_value(json!({
            "code": "one-time-code",
            "state": "opaque-state"
        }))
        .unwrap();

        assert_eq!(request.code, "one-time-code");
        assert_eq!(request.state, "opaque-state");
        assert!(
            serde_json::from_value::<ExchangeFrontendSessionCodeHttpRequest>(json!({
                "code": "one-time-code",
                "state": "opaque-state",
                "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
            }))
            .is_err()
        );
    }

    #[test]
    fn frontend_session_exchange_response_is_only_success_dto_with_raw_token() {
        let response = ExchangeFrontendSessionCodeHttpResponse::from(
            locks_service::application::use_cases::exchange_frontend_session_code::ExchangeFrontendSessionCodeResponse {
                session_token: locks_service::application::models::FrontendSessionToken::new(
                    "frontend-session-token",
                ),
                creator: CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap(),
                expires_at: datetime!(2026-05-30 12:00:00 UTC),
            },
        );

        let json = serde_json::to_value(response).unwrap();

        assert_eq!(json["session_token"], "frontend-session-token");
        assert_eq!(
            json["creator"],
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
        );
        assert_eq!(json["expires_at"], "2026-05-30T12:00:00Z");
        assert_no_keys(&json, &["code", "state", "session_secret"]);
    }

    #[test]
    fn creator_authority_status_response_is_secret_free() {
        let response = CreatorAuthorityStatusHttpResponse::from(CreatorAuthorityStatusView {
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            authorized: true,
            auth_kind: Some(CreatorAuthorityAuthKind::LegacyCookie),
            granted_scopes: vec![
                "/pub/app.locks/:rw".to_owned(),
                "/priv/app.locks/:rw".to_owned(),
            ],
            session_expires_at: None,
        });

        let json = serde_json::to_value(response).unwrap();

        assert_eq!(
            json["creator"],
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
        );
        assert_eq!(json["authorized"], true);
        assert_eq!(json["auth_kind"], "legacy_cookie");
        assert_eq!(
            json["granted_scopes"],
            json!(["/pub/app.locks/:rw", "/priv/app.locks/:rw"])
        );
        assert_eq!(json["session_expires_at"], Value::Null);
        assert_no_keys(
            &json,
            &[
                "secret",
                "token",
                "frontend_session_token",
                "authorization_url",
                "code",
                "database_url",
            ],
        );
    }

    fn submitted_proof_bundle() -> SubmittedProofBundle {
        SubmittedProofBundle {
            version: SUBMITTED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(&format!(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/{LOCK_ID}.json"
            ))
            .unwrap(),
            reader_public_key: None,
            proofs: vec![Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                payload: json!({ "satisfied": true }),
            }],
        }
    }

    fn assert_no_keys(json: &Value, keys: &[&str]) {
        for key in keys {
            assert!(json.get(*key).is_none(), "unexpected key {key} in {json}");
        }
    }
}
