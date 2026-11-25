use locks_core::ids::{BundleId, ClientReference, CreatorPubky, PubkyLockResource};
use locks_core::verification::{SubmittedProofBundle, SubmittedProofBundleV2};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

use crate::creator::encode_content_path;
use crate::graph::{
    FoldedAuthStatusRequest, FoldedAuthStatusResponse, GraphSubmitResponse,
    GraphVerificationTaskLifecycleResponse,
};
use crate::{LocksSdkError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewerLocks;

#[derive(Clone, PartialEq, Eq)]
pub struct SdkViewerRequest {
    pub method: &'static str,
    pub path: String,
    pub authorization: Option<String>,
    pub body: Value,
}

impl fmt::Debug for SdkViewerRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body = if self.path == "/reader-sessions" {
            Value::String("<redacted>".to_owned())
        } else {
            self.body.clone()
        };
        formatter
            .debug_struct("SdkViewerRequest")
            .field("method", &self.method)
            .field("path", &self.path)
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .field("body", &body)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct ReaderSessionCredentials {
    grant: String,
    pop: String,
}

impl ReaderSessionCredentials {
    pub fn new(grant: impl Into<String>, pop: impl Into<String>) -> Self {
        Self {
            grant: grant.into(),
            pop: pop.into(),
        }
    }
}

impl fmt::Debug for ReaderSessionCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReaderSessionCredentials(<redacted>)")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReaderSessionToken(String);

impl ReaderSessionToken {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn export_secret(&self) -> &str {
        &self.0
    }

    fn authorization_header_value(&self) -> String {
        format!("Bearer {}", self.0)
    }
}

impl fmt::Debug for ReaderSessionToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReaderSessionToken(<redacted>)")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct CreateReaderSessionRequest {
    pub challenge_id: String,
    pub credentials: ReaderSessionCredentials,
    pub existing_session: Option<ReaderSessionToken>,
}

impl fmt::Debug for CreateReaderSessionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateReaderSessionRequest")
            .field("challenge_id", &"<redacted>")
            .field("credentials", &self.credentials)
            .field("existing_session", &self.existing_session)
            .finish()
    }
}

#[derive(Serialize)]
struct CreateReaderSessionBody<'a> {
    challenge_id: &'a str,
    credentials: &'a ReaderSessionCredentials,
}

pub const READER_SESSION_PURPOSE: &str = "reader-session-v1";

#[derive(Clone, PartialEq, Eq)]
pub struct ReaderAuthChallengeResponse {
    pub challenge_id: String,
    pub challenge: String,
    pub lock_server_pubky: locks_core::ids::LockServerPubky,
    pub lock_server_origin: String,
    pub purpose: String,
    pub expires_at: time::OffsetDateTime,
}

impl fmt::Debug for ReaderAuthChallengeResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReaderAuthChallengeResponse")
            .field("challenge_id", &"<redacted>")
            .field("challenge", &"<redacted>")
            .field("lock_server_pubky", &self.lock_server_pubky)
            .field("lock_server_origin", &self.lock_server_origin)
            .field("purpose", &self.purpose)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReaderAuthChallengeWire {
    challenge_id: String,
    challenge: String,
    lock_server_pubky: locks_core::ids::LockServerPubky,
    lock_server_origin: String,
    purpose: String,
    #[serde(with = "time::serde::rfc3339")]
    expires_at: time::OffsetDateTime,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReaderSessionResponse {
    pub reader: CreatorPubky,
    pub session_token: ReaderSessionToken,
    pub idle_expires_at: time::OffsetDateTime,
    pub expires_at: time::OffsetDateTime,
}

impl fmt::Debug for ReaderSessionResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReaderSessionResponse")
            .field("reader", &self.reader)
            .field("session_token", &self.session_token)
            .field("idle_expires_at", &self.idle_expires_at)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReaderSessionWire {
    reader: CreatorPubky,
    session_token: String,
    #[serde(with = "time::serde::rfc3339")]
    idle_expires_at: time::OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    expires_at: time::OffsetDateTime,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReaderSessionLocks {
    token: ReaderSessionToken,
}

impl fmt::Debug for ReaderSessionLocks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReaderSessionLocks")
            .field("token", &self.token)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerificationTaskHandleRequest {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadLockedResourceRequest {
    pub credential: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SubmitProofBundleRequest {
    submitted_proof_bundle: SubmittedProofBundle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SubmitGraphProofBundleRequest {
    submitted_proof_bundle: SubmittedProofBundleV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum VerificationTaskStatus {
    Pending,
    InProgress,
    PublishingEntitlement,
    Completed,
    Failed,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationTerminalReason {
    PaymentRequestRejected,
    PaymentRequestCanceled,
    ProposalExpired,
    PaymentDeadlineExpired,
    ContentLockDeleted,
    GraphConditionNotObservedByDeadline,
    VerificationUnavailableByDeadline,
    InvalidVerifierResponse,
    VerificationConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaykitConnectionState {
    Connected,
    Handshake,
    None,
    RecoveryRequired,
    Blocked,
}

/// Response fields that carry internal identifiers, bearer secrets, or proof and entitlement
/// material. Lifecycle and connection-state responses never carry them, so their presence fails
/// the parse instead of being ignored like other unknown fields.
pub(crate) const SECRET_BEARING_RESPONSE_FIELDS: &[&str] = &[
    "task_id",
    "credential",
    "credential_issuance",
    "submitted_proof_bundle",
    "proofs",
    "entitlement",
    "verified_proof_bundle",
    "entitlement_to_publish",
];

/// Secret-bearing fields that an access-credential response must not carry. The raw
/// `credential` is its one documented secret.
const ACCESS_CREDENTIAL_FORBIDDEN_FIELDS: &[&str] = &[
    "task_id",
    "credential_issuance",
    "submitted_proof_bundle",
    "proofs",
    "entitlement",
    "verified_proof_bundle",
    "entitlement_to_publish",
];

/// Connection state moved to its own lookup; a lifecycle response that still carries it comes
/// from the superseded submit contract.
const LIFECYCLE_FOREIGN_FIELDS: &[&str] = &["connection_state"];

/// Verification task lifecycle returned by submission, lookup, and dev completion.
///
/// Unknown fields are ignored so a newer Lock Server can add fields without breaking this
/// release; secret-bearing fields are still rejected. Serializing a parsed response yields only
/// the fields below.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationTaskLifecycleResponse {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub status: VerificationTaskStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub submitted_at: time::OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<time::OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub completed_at: Option<time::OffsetDateTime>,
    pub failure_message: Option<String>,
    pub status_message: Option<String>,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub admission_deadline_at: Option<time::OffsetDateTime>,
    pub terminal_reason: Option<VerificationTerminalReason>,
    /// Present only for bundles submitted with a client reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_binding: Option<ClientBindingResponse>,
}

/// Relying-service binding echoed for a bundle submitted with a client reference.
///
/// A relying service accepts a completed task for one of its business objects only when
/// `client_reference` equals the value it minted for that object and `pubky_lock_resource` and
/// `criterion_ids` match the terms it expects. An absent binding never matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientBindingResponse {
    pub client_reference: ClientReference,
    pub pubky_lock_resource: PubkyLockResource,
    pub criterion_ids: Vec<String>,
}

/// Paykit connection state. Unknown fields are ignored; secret-bearing fields are rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaykitConnectionStateResponse {
    pub state: PaykitConnectionState,
}

/// Issued access credential. Unknown fields are ignored; proof and entitlement material is
/// rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessCredentialResponse {
    pub credential: String,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: time::OffsetDateTime,
}

impl ViewerLocks {
    pub fn new() -> Self {
        Self
    }

    pub fn create_reader_auth_challenge(&self) -> SdkViewerRequest {
        self.post_json(
            "/reader-auth/challenges",
            serde_json::Map::<String, Value>::new(),
        )
    }

    pub fn create_reader_session(&self, request: CreateReaderSessionRequest) -> SdkViewerRequest {
        SdkViewerRequest {
            method: "POST",
            path: "/reader-sessions".to_owned(),
            authorization: request
                .existing_session
                .as_ref()
                .map(ReaderSessionToken::authorization_header_value),
            body: serde_json::to_value(CreateReaderSessionBody {
                challenge_id: &request.challenge_id,
                credentials: &request.credentials,
            })
            .expect("reader session request serializes to JSON"),
        }
    }

    pub fn reader_session(&self, token: ReaderSessionToken) -> ReaderSessionLocks {
        ReaderSessionLocks { token }
    }

    pub fn parse_reader_auth_challenge_response(
        value: Value,
        expected_lock_server: &locks_core::ids::LockServerPubky,
    ) -> Result<ReaderAuthChallengeResponse> {
        let wire: ReaderAuthChallengeWire = serde_json::from_value(value)
            .map_err(|error| LocksSdkError::InvalidResponse(error.to_string()))?;
        let parsed_lock_server_origin = url::Url::parse(&wire.lock_server_origin)
            .map_err(|error| LocksSdkError::InvalidResponse(error.to_string()))?;
        if wire.challenge_id.is_empty()
            || wire.challenge.is_empty()
            || wire.purpose != READER_SESSION_PURPOSE
            || &wire.lock_server_pubky != expected_lock_server
            || parsed_lock_server_origin.cannot_be_a_base()
            || !matches!(parsed_lock_server_origin.scheme(), "http" | "https")
            || !parsed_lock_server_origin.username().is_empty()
            || parsed_lock_server_origin.password().is_some()
            || parsed_lock_server_origin.query().is_some()
            || parsed_lock_server_origin.fragment().is_some()
            || parsed_lock_server_origin.origin().ascii_serialization() != wire.lock_server_origin
        {
            return Err(LocksSdkError::InvalidResponse(
                "invalid reader authentication challenge".to_owned(),
            ));
        }
        Ok(ReaderAuthChallengeResponse {
            challenge_id: wire.challenge_id,
            challenge: wire.challenge,
            lock_server_pubky: wire.lock_server_pubky,
            lock_server_origin: wire.lock_server_origin,
            purpose: wire.purpose,
            expires_at: wire.expires_at,
        })
    }

    pub fn parse_reader_session_response(value: Value) -> Result<ReaderSessionResponse> {
        let wire: ReaderSessionWire = serde_json::from_value(value)
            .map_err(|error| LocksSdkError::InvalidResponse(error.to_string()))?;
        if wire.session_token.is_empty() || wire.idle_expires_at > wire.expires_at {
            return Err(LocksSdkError::InvalidResponse(
                "invalid reader session response".to_owned(),
            ));
        }
        Ok(ReaderSessionResponse {
            reader: wire.reader,
            session_token: ReaderSessionToken::new(wire.session_token),
            idle_expires_at: wire.idle_expires_at,
            expires_at: wire.expires_at,
        })
    }

    pub fn submit_proof_bundle(
        &self,
        submitted_proof_bundle: SubmittedProofBundle,
    ) -> SdkViewerRequest {
        self.post_json(
            "/proof-bundles",
            SubmitProofBundleRequest {
                submitted_proof_bundle,
            },
        )
    }

    pub fn submit_graph_proof_bundle(
        &self,
        submitted_proof_bundle: SubmittedProofBundleV2,
    ) -> SdkViewerRequest {
        self.post_json(
            "/proof-bundles",
            SubmitGraphProofBundleRequest {
                submitted_proof_bundle,
            },
        )
    }

    pub fn submit_payment_proof_bundle(
        &self,
        submitted_proof_bundle: SubmittedProofBundleV2,
    ) -> SdkViewerRequest {
        self.post_json(
            "/proof-bundles",
            SubmitGraphProofBundleRequest {
                submitted_proof_bundle,
            },
        )
    }

    pub fn submit_mixed_proof_bundle(
        &self,
        submitted_proof_bundle: SubmittedProofBundleV2,
    ) -> SdkViewerRequest {
        self.post_json(
            "/proof-bundles",
            SubmitGraphProofBundleRequest {
                submitted_proof_bundle,
            },
        )
    }

    pub fn poll_folded_auth(&self, request: FoldedAuthStatusRequest) -> SdkViewerRequest {
        self.post_json("/proof-bundles/auth-status", request)
    }

    pub fn lookup_graph_verification_task(
        &self,
        request: VerificationTaskHandleRequest,
    ) -> SdkViewerRequest {
        self.lookup_verification_task(request)
    }

    pub fn lookup_payment_verification_task(
        &self,
        request: VerificationTaskHandleRequest,
    ) -> SdkViewerRequest {
        self.lookup_verification_task(request)
    }

    pub fn lookup_mixed_verification_task(
        &self,
        request: VerificationTaskHandleRequest,
    ) -> SdkViewerRequest {
        self.lookup_verification_task(request)
    }

    pub fn lookup_verification_task(
        &self,
        request: VerificationTaskHandleRequest,
    ) -> SdkViewerRequest {
        self.post_json("/verification-task-lookups", request)
    }

    pub fn lookup_paykit_connection_state(
        &self,
        request: VerificationTaskHandleRequest,
    ) -> SdkViewerRequest {
        self.post_json("/paykit-connection-state-lookups", request)
    }

    pub fn issue_access_credential(
        &self,
        request: VerificationTaskHandleRequest,
    ) -> SdkViewerRequest {
        self.post_json("/access-credentials", request)
    }

    /// Issues through the compatibility folded-auth flow using its opaque state capability.
    pub fn issue_access_credential_with_folded_auth(
        &self,
        request: VerificationTaskHandleRequest,
        folded_auth: &crate::FoldedAuthStatusRequest,
    ) -> SdkViewerRequest {
        let mut request = self.issue_access_credential(request);
        request.authorization = Some(format!("Bearer {}", folded_auth.state()));
        request
    }

    pub fn complete_verification_task(
        &self,
        request: VerificationTaskHandleRequest,
    ) -> SdkViewerRequest {
        self.post_json("/verification-task-completions", request)
    }

    pub fn read_locked_resource(&self, request: ReadLockedResourceRequest) -> SdkViewerRequest {
        SdkViewerRequest {
            method: "GET",
            path: format!(
                "/priv-resources/content/{}",
                encode_content_path(&request.path)
            ),
            authorization: Some(format!("Bearer {}", request.credential)),
            body: Value::Null,
        }
    }

    pub fn proxy_read_guarded_resource(
        &self,
        access_credential: impl AsRef<str>,
        path: impl Into<String>,
    ) -> SdkViewerRequest {
        self.read_locked_resource(ReadLockedResourceRequest {
            credential: access_credential.as_ref().to_owned(),
            path: path.into(),
        })
    }

    pub fn parse_lifecycle_response(value: Value) -> Result<VerificationTaskLifecycleResponse> {
        reject_fields(&value, SECRET_BEARING_RESPONSE_FIELDS)?;
        reject_fields(&value, LIFECYCLE_FOREIGN_FIELDS)?;
        let response: VerificationTaskLifecycleResponse = serde_json::from_value(value)
            .map_err(|err| LocksSdkError::InvalidResponse(err.to_string()))?;
        validate_lifecycle_response(&response)?;
        Ok(response)
    }

    pub fn parse_submit_proof_bundle_response(
        value: Value,
    ) -> Result<VerificationTaskLifecycleResponse> {
        Self::parse_lifecycle_response(value)
    }

    pub fn parse_graph_submit_response(value: Value) -> Result<GraphSubmitResponse> {
        crate::graph::parse_graph_submit_response(value)
    }

    pub fn parse_folded_auth_status_response(value: Value) -> Result<FoldedAuthStatusResponse> {
        crate::graph::parse_folded_auth_status_response(value)
    }

    pub fn parse_mixed_submit_response(value: Value) -> Result<crate::mixed::MixedSubmitResponse> {
        crate::mixed::parse_mixed_submit_response(value)
    }

    pub fn parse_mixed_folded_auth_status_response(
        value: Value,
    ) -> Result<crate::mixed::MixedFoldedAuthStatusResponse> {
        crate::mixed::parse_mixed_folded_auth_status_response(value)
    }

    pub fn parse_graph_lifecycle_response(
        value: Value,
    ) -> Result<GraphVerificationTaskLifecycleResponse> {
        crate::graph::parse_graph_lifecycle_response(value)
    }

    pub fn parse_payment_lifecycle_response(
        value: Value,
    ) -> Result<crate::payment::PaymentVerificationTaskLifecycleResponse> {
        crate::payment::parse_payment_lifecycle_response(value)
    }

    pub fn parse_mixed_lifecycle_response(
        value: Value,
    ) -> Result<crate::mixed::MixedVerificationTaskLifecycleResponse> {
        crate::mixed::parse_mixed_lifecycle_response(value)
    }

    pub fn parse_paykit_connection_state_response(
        value: Value,
    ) -> Result<PaykitConnectionStateResponse> {
        reject_fields(&value, SECRET_BEARING_RESPONSE_FIELDS)?;
        serde_json::from_value(value).map_err(|err| LocksSdkError::InvalidResponse(err.to_string()))
    }

    pub fn parse_access_credential_response(value: Value) -> Result<AccessCredentialResponse> {
        reject_fields(&value, ACCESS_CREDENTIAL_FORBIDDEN_FIELDS)?;
        serde_json::from_value(value).map_err(|err| LocksSdkError::InvalidResponse(err.to_string()))
    }

    fn post_json(&self, path: &'static str, body: impl Serialize) -> SdkViewerRequest {
        SdkViewerRequest {
            method: "POST",
            path: path.to_owned(),
            authorization: None,
            body: serde_json::to_value(body).expect("SDK viewer request bodies serialize to JSON"),
        }
    }
}

impl ReaderSessionLocks {
    fn authorize(&self, mut request: SdkViewerRequest) -> SdkViewerRequest {
        request.authorization = Some(self.token.authorization_header_value());
        request
    }

    pub fn submit_graph_proof_bundle(
        &self,
        submitted_proof_bundle: SubmittedProofBundleV2,
    ) -> SdkViewerRequest {
        self.authorize(ViewerLocks::new().submit_graph_proof_bundle(submitted_proof_bundle))
    }

    pub fn submit_mixed_proof_bundle(
        &self,
        submitted_proof_bundle: SubmittedProofBundleV2,
    ) -> SdkViewerRequest {
        self.authorize(ViewerLocks::new().submit_mixed_proof_bundle(submitted_proof_bundle))
    }

    pub fn issue_access_credential(
        &self,
        request: VerificationTaskHandleRequest,
    ) -> SdkViewerRequest {
        self.authorize(ViewerLocks::new().issue_access_credential(request))
    }

    pub fn delete_current(&self) -> SdkViewerRequest {
        SdkViewerRequest {
            method: "DELETE",
            path: "/reader-sessions/current".to_owned(),
            authorization: Some(self.token.authorization_header_value()),
            body: Value::Null,
        }
    }
}

impl Default for ViewerLocks {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn reject_fields(value: &Value, fields: &[&str]) -> Result<()> {
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    match fields.iter().find(|field| object.contains_key(**field)) {
        Some(field) => Err(LocksSdkError::InvalidResponse(format!(
            "response carries forbidden field `{field}`"
        ))),
        None => Ok(()),
    }
}

fn validate_lifecycle_response(response: &VerificationTaskLifecycleResponse) -> Result<()> {
    if let Some(binding) = &response.client_binding
        && binding.pubky_lock_resource.creator() != &response.creator
    {
        return Err(LocksSdkError::InvalidResponse(
            "client binding names a lock of another creator".to_owned(),
        ));
    }
    let valid = match response.status {
        VerificationTaskStatus::Pending => {
            response.started_at.is_none()
                && response.completed_at.is_none()
                && response.failure_message.is_none()
                && matches!(
                    response.status_message.as_deref(),
                    None | Some("Reader wallet setup needed")
                )
                && response.terminal_reason.is_none()
        }
        VerificationTaskStatus::InProgress => {
            response.started_at.is_some()
                && response.completed_at.is_none()
                && response.failure_message.is_none()
                && response.status_message.is_none()
                && response.admission_deadline_at.is_none()
                && response.terminal_reason.is_none()
        }
        VerificationTaskStatus::PublishingEntitlement => false,
        VerificationTaskStatus::Completed => {
            response.started_at.is_some()
                && response.completed_at.is_some()
                && response.failure_message.is_none()
                && response.status_message.is_none()
                && response.admission_deadline_at.is_none()
                && response.terminal_reason.is_none()
        }
        VerificationTaskStatus::Failed => {
            response.started_at.is_some()
                && response.completed_at.is_some()
                && response
                    .failure_message
                    .as_deref()
                    .is_some_and(|message| !message.trim().is_empty())
                && response.status_message.is_none()
                && response.admission_deadline_at.is_none()
                && response.terminal_reason.is_none()
        }
        VerificationTaskStatus::Expired => {
            response.started_at.is_some()
                && response.completed_at.is_some()
                && response.failure_message.is_none()
                && response.status_message.is_none()
                && response.admission_deadline_at.is_none()
                && response.terminal_reason.is_some()
        }
    };
    if valid {
        Ok(())
    } else {
        Err(LocksSdkError::InvalidResponse(
            "invalid verification task lifecycle field combination".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::{BundleId, PubkyLockResource};
    use locks_core::lock_policy::VerifierType;
    use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};
    use serde_json::json;

    use super::*;

    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    #[test]
    fn reader_session_responses_and_protected_request_plans_are_closed_and_redacted() {
        let viewer = ViewerLocks::new();
        let lock_server = locks_core::ids::LockServerPubky::from_str(
            "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
        )
        .unwrap();
        let challenge = ViewerLocks::parse_reader_auth_challenge_response(
            json!({
                "challenge_id": "challenge-1",
                "challenge": "challenge-secret",
                "lock_server_pubky": lock_server,
                "lock_server_origin": "https://locks.example",
                "purpose": "reader-session-v1",
                "expires_at": "2026-11-23T12:05:00Z"
            }),
            &lock_server,
        )
        .unwrap();
        assert_eq!(challenge.purpose, "reader-session-v1");
        assert_eq!(challenge.lock_server_origin, "https://locks.example");
        assert!(!format!("{challenge:?}").contains("challenge-secret"));

        let response = ViewerLocks::parse_reader_session_response(json!({
            "reader": CREATOR,
            "session_token": "reader-session-secret",
            "idle_expires_at": "2026-11-23T12:10:00Z",
            "expires_at": "2026-11-23T13:00:00Z"
        }))
        .unwrap();
        assert_eq!(
            response.session_token.export_secret(),
            "reader-session-secret"
        );
        assert!(!format!("{response:?}").contains("reader-session-secret"));

        let session = viewer.reader_session(response.session_token);
        let handle = VerificationTaskHandleRequest {
            creator: CREATOR.parse().unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        };
        let issued = session.issue_access_credential(handle);
        assert_eq!(issued.path, "/access-credentials");
        assert_eq!(
            issued.authorization.as_deref(),
            Some("Bearer reader-session-secret")
        );
        let deleted = session.delete_current();
        assert_eq!(deleted.method, "DELETE");
        assert_eq!(deleted.path, "/reader-sessions/current");
        assert_eq!(deleted.body, Value::Null);
        assert!(!format!("{deleted:?}").contains("reader-session-secret"));
    }

    #[test]
    fn reader_session_challenge_and_creation_requests_match_the_closed_contract() {
        let viewer = ViewerLocks::new();
        let challenge = viewer.create_reader_auth_challenge();
        assert_eq!(challenge.method, "POST");
        assert_eq!(challenge.path, "/reader-auth/challenges");
        assert_eq!(challenge.authorization, None);
        assert_eq!(challenge.body, json!({}));

        let credentials = ReaderSessionCredentials::new("grant-secret", "pop-secret");
        let create = viewer.create_reader_session(CreateReaderSessionRequest {
            challenge_id: "challenge-1".to_owned(),
            credentials: credentials.clone(),
            existing_session: None,
        });
        assert_eq!(create.path, "/reader-sessions");
        assert_eq!(create.authorization, None);
        assert_eq!(
            create.body,
            json!({
                "challenge_id": "challenge-1",
                "credentials": {
                    "grant": "grant-secret",
                    "pop": "pop-secret"
                }
            })
        );

        let rotate = viewer.create_reader_session(CreateReaderSessionRequest {
            challenge_id: "challenge-2".to_owned(),
            credentials,
            existing_session: Some(ReaderSessionToken::new("old-reader-token")),
        });
        assert_eq!(
            rotate.authorization.as_deref(),
            Some("Bearer old-reader-token")
        );
        let debug = format!("{rotate:?}");
        assert!(!debug.contains("old-reader-token"));
        assert!(!debug.contains("grant-secret"));
        assert!(!debug.contains("pop-secret"));
    }

    #[test]
    fn submit_proof_bundle_request_uses_public_envelope_without_auth_or_task_id() {
        let viewer = ViewerLocks::new();
        let submitted = submitted_proof_bundle();

        let request = viewer.submit_proof_bundle(submitted.clone());

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/proof-bundles");
        assert_eq!(request.authorization, None);
        assert_eq!(request.body, json!({ "submitted_proof_bundle": submitted }));
        assert!(request.body.get("task_id").is_none());
    }

    #[test]
    fn verification_task_lookup_request_keeps_bundle_id_in_json_body() {
        let viewer = ViewerLocks::new();
        let request = viewer.lookup_verification_task(VerificationTaskHandleRequest {
            creator: CREATOR.parse().unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        });

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/verification-task-lookups");
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "creator": CREATOR, "bundle_id": BUNDLE_ID })
        );
        assert!(!request.path.contains(BUNDLE_ID));
    }

    #[test]
    fn paykit_connection_state_lookup_uses_task_handle_body() {
        let viewer = ViewerLocks::new();
        let request = viewer.lookup_paykit_connection_state(VerificationTaskHandleRequest {
            creator: CREATOR.parse().unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        });

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/paykit-connection-state-lookups");
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "creator": CREATOR, "bundle_id": BUNDLE_ID })
        );
    }

    #[test]
    fn issue_access_credential_request_uses_handle_body_without_task_id() {
        let viewer = ViewerLocks::new();
        let request = viewer.issue_access_credential(VerificationTaskHandleRequest {
            creator: CREATOR.parse().unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        });

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/access-credentials");
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "creator": CREATOR, "bundle_id": BUNDLE_ID })
        );
        assert!(request.body.get("task_id").is_none());
    }

    #[test]
    fn complete_verification_task_request_uses_handle_body_without_task_id() {
        let viewer = ViewerLocks::new();
        let request = viewer.complete_verification_task(VerificationTaskHandleRequest {
            creator: CREATOR.parse().unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        });

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/verification-task-completions");
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "creator": CREATOR, "bundle_id": BUNDLE_ID })
        );
        assert!(request.body.get("task_id").is_none());
    }

    #[test]
    fn proxy_read_guarded_resource_request_puts_credential_only_in_authorization_header() {
        let viewer = ViewerLocks::new();
        let request = viewer.read_locked_resource(ReadLockedResourceRequest {
            credential: "raw-access-credential".to_owned(),
            path: "nested/example file.txt".to_owned(),
        });

        assert_eq!(request.method, "GET");
        assert_eq!(
            request.path,
            "/priv-resources/content/nested/example%20file.txt"
        );
        assert_eq!(
            request.authorization,
            Some("Bearer raw-access-credential".to_owned())
        );
        assert_eq!(request.body, serde_json::Value::Null);
        assert!(!request.path.contains("raw-access-credential"));
        assert!(!request.path.contains("credential"));
    }

    #[test]
    fn submit_proof_bundle_response_is_lifecycle_only() {
        let response = ViewerLocks::parse_submit_proof_bundle_response(json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "completed",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": "2026-06-01T12:00:01Z",
            "completed_at": "2026-06-01T12:00:02Z",
            "failure_message": null,
            "terminal_reason": null
        }))
        .unwrap();

        assert_eq!(response.creator.to_string(), CREATOR);
        assert_eq!(response.bundle_id.to_string(), BUNDLE_ID);
        assert_eq!(response.status, VerificationTaskStatus::Completed);
        assert!(response.failure_message.is_none());
    }

    #[test]
    fn lifecycle_response_parses_terminal_reason_and_rejects_invalid_status_tuple() {
        let response = ViewerLocks::parse_lifecycle_response(json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "expired",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": "2026-06-01T12:00:01Z",
            "completed_at": "2026-06-01T12:00:02Z",
            "failure_message": null,
            "terminal_reason": "payment_request_rejected"
        }))
        .unwrap();

        assert_eq!(
            response.terminal_reason,
            Some(VerificationTerminalReason::PaymentRequestRejected)
        );

        for invalid in [
            json!({
                "creator": CREATOR,
                "bundle_id": BUNDLE_ID,
                "status": "expired",
                "submitted_at": "2026-06-01T12:00:00Z",
                "started_at": "2026-06-01T12:00:01Z",
                "completed_at": "2026-06-01T12:00:02Z",
                "failure_message": null,
                "terminal_reason": null
            }),
            json!({
                "creator": CREATOR,
                "bundle_id": BUNDLE_ID,
                "status": "failed",
                "submitted_at": "2026-06-01T12:00:00Z",
                "started_at": "2026-06-01T12:00:01Z",
                "completed_at": "2026-06-01T12:00:02Z",
                "failure_message": "verification failed",
                "terminal_reason": "payment_request_rejected"
            }),
        ] {
            assert!(ViewerLocks::parse_lifecycle_response(invalid).is_err());
        }
    }

    #[test]
    fn lifecycle_response_rejects_unknown_or_non_pending_status_message() {
        let base = json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "pending",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": null,
            "completed_at": null,
            "failure_message": null,
            "status_message": "future progress text",
            "terminal_reason": null
        });
        assert!(ViewerLocks::parse_lifecycle_response(base).is_err());

        let completed = json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "completed",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": "2026-06-01T12:00:01Z",
            "completed_at": "2026-06-01T12:00:02Z",
            "failure_message": null,
            "status_message": "Reader wallet setup needed",
            "terminal_reason": null
        });
        assert!(ViewerLocks::parse_lifecycle_response(completed).is_err());
    }

    #[test]
    fn pending_lifecycle_response_exposes_optional_invoice_admission_deadline() {
        let response = ViewerLocks::parse_lifecycle_response(json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "pending",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": null,
            "completed_at": null,
            "failure_message": null,
            "status_message": "Reader wallet setup needed",
            "admission_deadline_at": "2026-06-01T12:10:00Z",
            "terminal_reason": null
        }))
        .unwrap();

        assert_eq!(
            response.admission_deadline_at,
            Some(
                time::OffsetDateTime::parse(
                    "2026-06-01T12:10:00Z",
                    &time::format_description::well_known::Rfc3339,
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn paykit_connection_state_response_parses_closed_vocabulary() {
        for (wire, expected) in [
            ("none", PaykitConnectionState::None),
            ("handshake", PaykitConnectionState::Handshake),
            ("connected", PaykitConnectionState::Connected),
            ("recovery_required", PaykitConnectionState::RecoveryRequired),
            ("blocked", PaykitConnectionState::Blocked),
        ] {
            let response = ViewerLocks::parse_paykit_connection_state_response(json!({
                "state": wire
            }))
            .unwrap();
            assert_eq!(response.state, expected);
        }
    }

    #[test]
    fn connection_state_is_rejected_from_submit_and_unknown_state_is_rejected_from_lookup() {
        assert!(
            ViewerLocks::parse_submit_proof_bundle_response(json!({
                "creator": CREATOR,
                "bundle_id": BUNDLE_ID,
                "status": "pending",
                "submitted_at": "2026-06-01T12:00:00Z",
                "started_at": null,
                "completed_at": null,
                "failure_message": null,
                "terminal_reason": null,
                "connection_state": "connected"
            }))
            .is_err()
        );
        for response in [json!({}), json!({"state": "future"})] {
            assert!(ViewerLocks::parse_paykit_connection_state_response(response).is_err());
        }
    }

    #[test]
    fn lifecycle_response_rejects_internal_task_id_and_credentials() {
        let result = ViewerLocks::parse_lifecycle_response(json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "pending",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": null,
            "completed_at": null,
            "failure_message": null,
            "task_id": "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            "credential": "raw-secret"
        }));

        assert!(result.is_err());
    }

    #[test]
    fn access_credential_response_parses_raw_credential_once() {
        let response = ViewerLocks::parse_access_credential_response(json!({
            "credential": "raw-access-credential",
            "expires_at": "2026-06-01T12:15:00Z"
        }))
        .unwrap();

        assert_eq!(response.credential, "raw-access-credential");
        assert_eq!(
            response.expires_at,
            time::OffsetDateTime::parse(
                "2026-06-01T12:15:00Z",
                &time::format_description::well_known::Rfc3339
            )
            .unwrap()
        );
    }

    #[test]
    fn lifecycle_response_ignores_unknown_fields_from_newer_servers() {
        let response = ViewerLocks::parse_lifecycle_response(json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "completed",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": "2026-06-01T12:00:01Z",
            "completed_at": "2026-06-01T12:00:02Z",
            "failure_message": null,
            "status_message": null,
            "admission_deadline_at": null,
            "terminal_reason": null,
            "future_object": { "nested": true },
            "future_scalar": 7
        }))
        .unwrap();

        assert_eq!(response.status, VerificationTaskStatus::Completed);
        let reserialized = serde_json::to_value(&response).unwrap();
        assert_eq!(
            reserialized,
            json!({
                "creator": CREATOR,
                "bundle_id": BUNDLE_ID,
                "status": "completed",
                "submitted_at": "2026-06-01T12:00:00Z",
                "started_at": "2026-06-01T12:00:01Z",
                "completed_at": "2026-06-01T12:00:02Z",
                "failure_message": null,
                "status_message": null,
                "admission_deadline_at": null,
                "terminal_reason": null
            })
        );
    }

    #[test]
    fn lifecycle_response_exposes_client_binding_only_when_present() {
        let resource = format!("{CREATOR}/pub/app.locks/{LOCK_ID}.json");
        let mut body = json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "completed",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": "2026-06-01T12:00:01Z",
            "completed_at": "2026-06-01T12:00:02Z",
            "failure_message": null,
            "status_message": null,
            "admission_deadline_at": null,
            "terminal_reason": null
        });
        let without = ViewerLocks::parse_lifecycle_response(body.clone()).unwrap();
        assert_eq!(without.client_binding, None);
        assert_eq!(serde_json::to_value(&without).unwrap(), body);

        body["client_binding"] = json!({
            "client_reference": "order-1",
            "pubky_lock_resource": resource,
            "criterion_ids": ["criterion-1"]
        });
        let with = ViewerLocks::parse_lifecycle_response(body.clone()).unwrap();
        let binding = with.client_binding.clone().unwrap();
        assert_eq!(binding.client_reference.as_str(), "order-1");
        assert_eq!(binding.pubky_lock_resource.to_string(), resource);
        assert_eq!(binding.criterion_ids, vec!["criterion-1".to_owned()]);
        assert_eq!(serde_json::to_value(&with).unwrap(), body);
    }

    #[test]
    fn lifecycle_response_rejects_malformed_or_foreign_client_binding() {
        let other_creator = "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo";
        for binding in [
            json!({
                "client_reference": "order-1",
                "pubky_lock_resource": format!("{other_creator}/pub/app.locks/{LOCK_ID}.json"),
                "criterion_ids": ["criterion-1"]
            }),
            json!({
                "client_reference": "",
                "pubky_lock_resource": format!("{CREATOR}/pub/app.locks/{LOCK_ID}.json"),
                "criterion_ids": ["criterion-1"]
            }),
            json!({
                "pubky_lock_resource": format!("{CREATOR}/pub/app.locks/{LOCK_ID}.json"),
                "criterion_ids": ["criterion-1"]
            }),
        ] {
            let body = json!({
                "creator": CREATOR,
                "bundle_id": BUNDLE_ID,
                "status": "pending",
                "submitted_at": "2026-06-01T12:00:00Z",
                "started_at": null,
                "completed_at": null,
                "failure_message": null,
                "terminal_reason": null,
                "client_binding": binding
            });
            assert!(ViewerLocks::parse_lifecycle_response(body).is_err());
        }
    }

    #[test]
    fn connection_state_and_credential_responses_ignore_unknown_fields() {
        let connection = ViewerLocks::parse_paykit_connection_state_response(json!({
            "state": "connected",
            "observed_generation": 3
        }))
        .unwrap();
        assert_eq!(connection.state, PaykitConnectionState::Connected);
        assert_eq!(
            serde_json::to_value(&connection).unwrap(),
            json!({ "state": "connected" })
        );

        let issued = ViewerLocks::parse_access_credential_response(json!({
            "credential": "raw-access-credential",
            "expires_at": "2026-06-01T12:15:00Z",
            "credential_version": 2
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(&issued).unwrap(),
            json!({
                "credential": "raw-access-credential",
                "expires_at": "2026-06-01T12:15:00Z"
            })
        );
    }

    #[test]
    fn every_secret_bearing_field_still_fails_lifecycle_and_connection_parses() {
        for field in SECRET_BEARING_RESPONSE_FIELDS {
            let mut lifecycle = json!({
                "creator": CREATOR,
                "bundle_id": BUNDLE_ID,
                "status": "pending",
                "submitted_at": "2026-06-01T12:00:00Z",
                "started_at": null,
                "completed_at": null,
                "failure_message": null,
                "terminal_reason": null
            });
            lifecycle[*field] = json!("leaked");
            assert!(
                ViewerLocks::parse_lifecycle_response(lifecycle).is_err(),
                "{field} must fail the lifecycle parse"
            );
            let mut connection = json!({ "state": "connected" });
            connection[*field] = json!("leaked");
            assert!(
                ViewerLocks::parse_paykit_connection_state_response(connection).is_err(),
                "{field} must fail the connection-state parse"
            );
        }
        for field in ACCESS_CREDENTIAL_FORBIDDEN_FIELDS {
            let mut issued = json!({
                "credential": "raw-access-credential",
                "expires_at": "2026-06-01T12:15:00Z"
            });
            issued[*field] = json!("leaked");
            assert!(
                ViewerLocks::parse_access_credential_response(issued).is_err(),
                "{field} must fail the access-credential parse"
            );
        }
    }

    #[test]
    fn access_credential_response_rejects_extra_entitlement_or_proof_material() {
        let result = ViewerLocks::parse_access_credential_response(json!({
            "credential": "raw-access-credential",
            "expires_at": "2026-06-01T12:15:00Z",
            "submitted_proof_bundle": { "not": "viewer safe" }
        }));

        assert!(result.is_err());
    }

    fn submitted_proof_bundle() -> SubmittedProofBundle {
        SubmittedProofBundle {
            version: SUBMITTED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(&format!(
                "{CREATOR}/pub/app.locks/{LOCK_ID}.json"
            ))
            .unwrap(),
            reader_public_key: None,
            client_reference: None,
            proofs: vec![Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                payload: json!({ "satisfied": true }),
            }],
        }
    }
}
