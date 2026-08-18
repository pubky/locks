pub use locks_core::creator_publishing::{
    CreateContentLockRequest as AuthenticatedCreateContentLockHttpRequest,
    SetLockServicePointerRequest as AuthenticatedSetLockServicePointerHttpRequest,
};
use locks_core::ids::{BundleId, ContentLockPath, CreatorPubky, LockId, LockServerPubky};
use locks_core::lock_policy::{ContentLock, GuardedResource};
use locks_core::lock_service_pointer::LockServicePointer;
use locks_core::verification::SubmittedProofBundle;
use locks_service::application::models::{
    AccessCredential, FrontendSessionCode, VerificationTaskStatus,
};
use locks_service::application::use_cases::exchange_frontend_session_code::{
    ExchangeFrontendSessionCodeRequest, ExchangeFrontendSessionCodeResponse,
};
use locks_service::application::use_cases::get_creator_authority_status::CreatorAuthorityStatusView;
use locks_service::application::use_cases::get_verification_task::VerificationTaskLifecycleView;
use locks_service::application::use_cases::issue_access_credential::IssuedAccessCredential;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

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
    pub submitted_proof_bundle: SubmittedProofBundle,
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
        VerificationTaskStatus::InProgress => "in_progress",
        VerificationTaskStatus::Completed => "completed",
        VerificationTaskStatus::Failed => "failed",
        VerificationTaskStatus::Expired => "expired",
    })
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

    use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource};
    use locks_core::lock_policy::VerifierType;
    use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};
    use locks_service::application::models::{
        AccessCredential, CreatorAuthorityAuthKind, VerificationTaskStatus,
    };
    use locks_service::application::use_cases::get_creator_authority_status::CreatorAuthorityStatusView;
    use locks_service::application::use_cases::get_verification_task::VerificationTaskLifecycleView;
    use locks_service::application::use_cases::issue_access_credential::IssuedAccessCredential;
    use serde_json::{Value, json};
    use time::macros::datetime;

    use super::{
        CreatorAuthorityStatusHttpResponse, ExchangeFrontendSessionCodeHttpRequest,
        ExchangeFrontendSessionCodeHttpResponse, IssueAccessCredentialHttpRequest,
        IssueAccessCredentialHttpResponse, SubmitProofBundleHttpRequest,
        VerificationTaskHandleHttpRequest, VerificationTaskLifecycleHttpResponse,
    };

    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    #[test]
    fn submit_proof_request_uses_explicit_envelope() {
        let request = SubmitProofBundleHttpRequest {
            submitted_proof_bundle: submitted_proof_bundle(),
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
                "/pub/locks.app/:rw".to_owned(),
                "/priv/locks.app/:rw".to_owned(),
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
            json!(["/pub/locks.app/:rw", "/priv/locks.app/:rw"])
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
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/{LOCK_ID}.json"
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
