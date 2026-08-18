use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use locks_service::application::errors::ApplicationError;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiErrorCode {
    InvalidRequest,
    InvalidIdentifier,
    InvalidAccessCredential,
    ExpiredAccessCredential,
    EntitlementNotAuthorized,
    VerificationTaskNotFound,
    GuardedResourceNotFound,
    ContentLockNotFound,
    CreatorAuthorityUnavailable,
    CreatorConnectFlowUnavailable,
    CreatorConnectFlowExpired,
    FrontendSessionCodeUnavailable,
    FrontendSessionCodeExpired,
    FrontendSessionCodeAlreadyConsumed,
    FrontendSessionUnavailable,
    FrontendSessionExpired,
    FrontendSessionStateMismatch,
    ContentLockPathConflict,
    ContentLockDeletionInProgress,
    ContentLockDeletionNotFound,
    TaskStateConflict,
    UnsupportedVerifierType,
    PaykitNotConfigured,
    ReaderPubkyUnresolvable,
    PaykitInvoiceCreationFailed,
    RateLimited,
    PayloadTooLarge,
    InternalError,
}

impl ApiErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidIdentifier => "invalid_identifier",
            Self::InvalidAccessCredential => "invalid_access_credential",
            Self::ExpiredAccessCredential => "expired_access_credential",
            Self::EntitlementNotAuthorized => "entitlement_not_authorized",
            Self::VerificationTaskNotFound => "verification_task_not_found",
            Self::GuardedResourceNotFound => "guarded_resource_not_found",
            Self::ContentLockNotFound => "content_lock_not_found",
            Self::CreatorAuthorityUnavailable => "creator_authority_unavailable",
            Self::CreatorConnectFlowUnavailable => "creator_connect_flow_unavailable",
            Self::CreatorConnectFlowExpired => "creator_connect_flow_expired",
            Self::FrontendSessionCodeUnavailable => "frontend_session_code_unavailable",
            Self::FrontendSessionCodeExpired => "frontend_session_code_expired",
            Self::FrontendSessionCodeAlreadyConsumed => "frontend_session_code_already_consumed",
            Self::FrontendSessionUnavailable => "frontend_session_unavailable",
            Self::FrontendSessionExpired => "frontend_session_expired",
            Self::FrontendSessionStateMismatch => "frontend_session_state_mismatch",
            Self::ContentLockPathConflict => "content_lock_path_conflict",
            Self::ContentLockDeletionInProgress => "content_lock_deletion_in_progress",
            Self::ContentLockDeletionNotFound => "content_lock_deletion_not_found",
            Self::TaskStateConflict => "task_state_conflict",
            Self::UnsupportedVerifierType => "unsupported_verifier_type",
            Self::PaykitNotConfigured => "paykit_not_configured",
            Self::ReaderPubkyUnresolvable => "reader_pubky_unresolvable",
            Self::PaykitInvoiceCreationFailed => "paykit_invoice_creation_failed",
            Self::RateLimited => "rate_limited",
            Self::PayloadTooLarge => "payload_too_large",
            Self::InternalError => "internal_error",
        }
    }

    pub fn status_code(self) -> StatusCode {
        match self {
            Self::InvalidRequest | Self::InvalidIdentifier => StatusCode::BAD_REQUEST,
            Self::InvalidAccessCredential | Self::ExpiredAccessCredential => {
                StatusCode::UNAUTHORIZED
            }
            Self::EntitlementNotAuthorized => StatusCode::FORBIDDEN,
            Self::VerificationTaskNotFound
            | Self::GuardedResourceNotFound
            | Self::ContentLockNotFound
            | Self::ContentLockDeletionNotFound
            | Self::CreatorConnectFlowUnavailable
            | Self::FrontendSessionCodeUnavailable => StatusCode::NOT_FOUND,
            Self::CreatorAuthorityUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::CreatorConnectFlowExpired
            | Self::FrontendSessionCodeExpired
            | Self::FrontendSessionCodeAlreadyConsumed => StatusCode::GONE,
            Self::FrontendSessionUnavailable | Self::FrontendSessionExpired => {
                StatusCode::UNAUTHORIZED
            }
            Self::FrontendSessionStateMismatch => StatusCode::BAD_REQUEST,
            Self::ContentLockPathConflict
            | Self::ContentLockDeletionInProgress
            | Self::TaskStateConflict => StatusCode::CONFLICT,
            Self::UnsupportedVerifierType
            | Self::PaykitNotConfigured
            | Self::ReaderPubkyUnresolvable => StatusCode::UNPROCESSABLE_ENTITY,
            Self::PaykitInvoiceCreationFailed => StatusCode::BAD_GATEWAY,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    code: ApiErrorCode,
    message: String,
}

impl ApiError {
    pub fn new(code: ApiErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn status_code(&self) -> StatusCode {
        self.code.status_code()
    }

    pub fn error_response(&self) -> ErrorResponse {
        ErrorResponse::new(self.code, self.message.clone())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status_code(), Json(self.error_response())).into_response()
    }
}

impl From<ApplicationError> for ApiError {
    fn from(error: ApplicationError) -> Self {
        match error {
            ApplicationError::MissingRecord {
                record: "verification_task",
            } => Self::new(
                ApiErrorCode::VerificationTaskNotFound,
                "verification task not found",
            ),
            ApplicationError::MissingRecord {
                record: "guarded_resource",
            }
            | ApplicationError::GuardedResourceUnavailable => Self::new(
                ApiErrorCode::GuardedResourceNotFound,
                "guarded resource not found",
            ),
            ApplicationError::MissingRecord {
                record: "content_lock",
            }
            | ApplicationError::ContentLockUnavailable => {
                Self::new(ApiErrorCode::ContentLockNotFound, "content lock not found")
            }
            ApplicationError::InvalidAccessCredential => Self::new(
                ApiErrorCode::InvalidAccessCredential,
                "invalid access credential",
            ),
            ApplicationError::ExpiredAccessCredential => Self::new(
                ApiErrorCode::ExpiredAccessCredential,
                "expired access credential",
            ),
            ApplicationError::CreatorAuthorityUnavailable
            | ApplicationError::CreatorAuthoritySecret { .. }
            | ApplicationError::InvalidCreatorAuthorityAuthKind { .. } => Self::new(
                ApiErrorCode::CreatorAuthorityUnavailable,
                "creator authority unavailable",
            ),
            ApplicationError::CreatorConnectFlowUnavailable => Self::new(
                ApiErrorCode::CreatorConnectFlowUnavailable,
                "creator connect flow unavailable",
            ),
            ApplicationError::CreatorConnectFlowExpired => Self::new(
                ApiErrorCode::CreatorConnectFlowExpired,
                "creator connect flow expired",
            ),
            ApplicationError::FrontendSessionCodeUnavailable => Self::new(
                ApiErrorCode::FrontendSessionCodeUnavailable,
                "frontend session code unavailable",
            ),
            ApplicationError::FrontendSessionCodeExpired => Self::new(
                ApiErrorCode::FrontendSessionCodeExpired,
                "frontend session code expired",
            ),
            ApplicationError::FrontendSessionCodeAlreadyConsumed => Self::new(
                ApiErrorCode::FrontendSessionCodeAlreadyConsumed,
                "frontend session code already consumed",
            ),
            ApplicationError::FrontendSessionUnavailable => Self::new(
                ApiErrorCode::FrontendSessionUnavailable,
                "frontend session unavailable",
            ),
            ApplicationError::FrontendSessionExpired => Self::new(
                ApiErrorCode::FrontendSessionExpired,
                "frontend session expired",
            ),
            ApplicationError::FrontendSessionStateMismatch => Self::new(
                ApiErrorCode::FrontendSessionStateMismatch,
                "frontend session state mismatch",
            ),
            ApplicationError::ContentLockPathConflict { .. } => Self::new(
                ApiErrorCode::ContentLockPathConflict,
                "content lock path is already owned",
            ),
            ApplicationError::ContentLockDeletionInProgress => Self::new(
                ApiErrorCode::ContentLockDeletionInProgress,
                "content lock deletion is in progress",
            ),
            ApplicationError::InvalidGuardedResource { .. } => {
                Self::new(ApiErrorCode::InvalidRequest, "invalid guarded resource")
            }
            ApplicationError::InvalidPaykitPaymentSubmission => Self::new(
                ApiErrorCode::InvalidRequest,
                "invalid paykit payment submission",
            ),
            ApplicationError::EntitlementNotFound
            | ApplicationError::EntitlementNotSatisfied
            | ApplicationError::ContentLockHashMismatch { .. } => Self::new(
                ApiErrorCode::EntitlementNotAuthorized,
                "entitlement not authorized",
            ),
            ApplicationError::InvalidVerificationTaskTransition { .. }
            | ApplicationError::InvalidVerificationTaskState { .. }
            | ApplicationError::InvalidVerificationTaskFailureMessage
            | ApplicationError::VerificationPending
            | ApplicationError::VerificationTaskClaimLost
            | ApplicationError::VerificationTaskConflict => Self::new(
                ApiErrorCode::TaskStateConflict,
                "verification task state conflict",
            ),
            ApplicationError::UnsupportedVerifierType { .. } => Self::new(
                ApiErrorCode::UnsupportedVerifierType,
                "unsupported verifier type",
            ),
            ApplicationError::RateLimited => {
                Self::new(ApiErrorCode::RateLimited, "rate limit exceeded")
            }
            ApplicationError::Storage { .. }
            | ApplicationError::InvalidContentLockDeletionState { .. }
            | ApplicationError::Verifier { .. }
            | ApplicationError::CredentialGeneration { .. }
            | ApplicationError::ContentLockCanonicalization { .. }
            | ApplicationError::DuplicateRecord { .. }
            | ApplicationError::UnsupportedCredentialTtl { .. }
            | ApplicationError::EmptyContentLockCriteria
            | ApplicationError::DuplicateContentLockCriterion { .. }
            | ApplicationError::DuplicateVerificationResultCriterion { .. }
            | ApplicationError::UnknownVerificationResultCriterion { .. }
            | ApplicationError::MissingRecord { .. } => {
                Self::new(ApiErrorCode::InternalError, "internal server error")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

impl ErrorResponse {
    pub fn new(code: ApiErrorCode, message: impl Into<String>) -> Self {
        Self {
            error: ErrorBody {
                code: code.as_str(),
                message: message.into(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use locks_service::application::errors::ApplicationError;
    use locks_service::application::models::VerificationTaskStatus;
    use serde_json::json;

    use super::{ApiError, ApiErrorCode, ErrorResponse};

    #[test]
    fn error_response_uses_stable_envelope() {
        let response = ErrorResponse::new(
            ApiErrorCode::VerificationTaskNotFound,
            "verification task not found",
        );

        let json = serde_json::to_value(response).unwrap();

        assert_eq!(
            json,
            json!({
                "error": {
                    "code": "verification_task_not_found",
                    "message": "verification task not found"
                }
            })
        );
    }

    #[test]
    fn error_fixtures_match_error_response_serialization() {
        let cases = [
            (
                include_str!("../../tests/fixtures/errors/invalid_request.json"),
                ErrorResponse::new(ApiErrorCode::InvalidRequest, "invalid request"),
            ),
            (
                include_str!("../../tests/fixtures/errors/priv_resource_not_found.json"),
                ErrorResponse::new(
                    ApiErrorCode::GuardedResourceNotFound,
                    "guarded resource not found",
                ),
            ),
            (
                include_str!("../../tests/fixtures/errors/rate_limited.json"),
                ErrorResponse::new(ApiErrorCode::RateLimited, "rate limit exceeded"),
            ),
        ];

        for (fixture, response) in cases {
            let expected: serde_json::Value = serde_json::from_str(fixture).unwrap();
            let actual = serde_json::to_value(response).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn status_and_code_mapping_matches_api_contract() {
        let cases = [
            (
                ApiErrorCode::InvalidRequest,
                StatusCode::BAD_REQUEST,
                "invalid_request",
            ),
            (
                ApiErrorCode::InvalidIdentifier,
                StatusCode::BAD_REQUEST,
                "invalid_identifier",
            ),
            (
                ApiErrorCode::InvalidAccessCredential,
                StatusCode::UNAUTHORIZED,
                "invalid_access_credential",
            ),
            (
                ApiErrorCode::ExpiredAccessCredential,
                StatusCode::UNAUTHORIZED,
                "expired_access_credential",
            ),
            (
                ApiErrorCode::EntitlementNotAuthorized,
                StatusCode::FORBIDDEN,
                "entitlement_not_authorized",
            ),
            (
                ApiErrorCode::VerificationTaskNotFound,
                StatusCode::NOT_FOUND,
                "verification_task_not_found",
            ),
            (
                ApiErrorCode::GuardedResourceNotFound,
                StatusCode::NOT_FOUND,
                "guarded_resource_not_found",
            ),
            (
                ApiErrorCode::ContentLockNotFound,
                StatusCode::NOT_FOUND,
                "content_lock_not_found",
            ),
            (
                ApiErrorCode::CreatorAuthorityUnavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                "creator_authority_unavailable",
            ),
            (
                ApiErrorCode::CreatorConnectFlowUnavailable,
                StatusCode::NOT_FOUND,
                "creator_connect_flow_unavailable",
            ),
            (
                ApiErrorCode::CreatorConnectFlowExpired,
                StatusCode::GONE,
                "creator_connect_flow_expired",
            ),
            (
                ApiErrorCode::FrontendSessionCodeUnavailable,
                StatusCode::NOT_FOUND,
                "frontend_session_code_unavailable",
            ),
            (
                ApiErrorCode::FrontendSessionCodeExpired,
                StatusCode::GONE,
                "frontend_session_code_expired",
            ),
            (
                ApiErrorCode::FrontendSessionCodeAlreadyConsumed,
                StatusCode::GONE,
                "frontend_session_code_already_consumed",
            ),
            (
                ApiErrorCode::FrontendSessionUnavailable,
                StatusCode::UNAUTHORIZED,
                "frontend_session_unavailable",
            ),
            (
                ApiErrorCode::FrontendSessionExpired,
                StatusCode::UNAUTHORIZED,
                "frontend_session_expired",
            ),
            (
                ApiErrorCode::FrontendSessionStateMismatch,
                StatusCode::BAD_REQUEST,
                "frontend_session_state_mismatch",
            ),
            (
                ApiErrorCode::TaskStateConflict,
                StatusCode::CONFLICT,
                "task_state_conflict",
            ),
            (
                ApiErrorCode::ContentLockPathConflict,
                StatusCode::CONFLICT,
                "content_lock_path_conflict",
            ),
            (
                ApiErrorCode::UnsupportedVerifierType,
                StatusCode::UNPROCESSABLE_ENTITY,
                "unsupported_verifier_type",
            ),
            (
                ApiErrorCode::PaykitNotConfigured,
                StatusCode::UNPROCESSABLE_ENTITY,
                "paykit_not_configured",
            ),
            (
                ApiErrorCode::ReaderPubkyUnresolvable,
                StatusCode::UNPROCESSABLE_ENTITY,
                "reader_pubky_unresolvable",
            ),
            (
                ApiErrorCode::PaykitInvoiceCreationFailed,
                StatusCode::BAD_GATEWAY,
                "paykit_invoice_creation_failed",
            ),
            (
                ApiErrorCode::RateLimited,
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
            ),
            (
                ApiErrorCode::InternalError,
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ),
        ];

        for (code, status, string_code) in cases {
            assert_eq!(code.status_code(), status);
            assert_eq!(code.as_str(), string_code);
        }
    }

    #[test]
    fn application_errors_map_without_leaking_internal_enum_formatting() {
        let api_error = ApiError::from(ApplicationError::InvalidAccessCredential);

        assert_eq!(api_error.status_code(), StatusCode::UNAUTHORIZED);
        let response = api_error.error_response();
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json["error"]["code"], "invalid_access_credential");
        assert_eq!(json["error"]["message"], "invalid access credential");
        assert!(!json.to_string().contains("InvalidAccessCredential"));
    }

    #[test]
    fn rate_limit_error_maps_to_429_stable_envelope() {
        let api_error = ApiError::from(ApplicationError::RateLimited);

        assert_eq!(api_error.status_code(), StatusCode::TOO_MANY_REQUESTS);
        let response = api_error.error_response();
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(
            json,
            json!({
                "error": {
                    "code": "rate_limited",
                    "message": "rate limit exceeded"
                }
            })
        );
    }

    #[test]
    fn content_lock_path_conflict_maps_to_409_stable_envelope() {
        let api_error = ApiError::from(ApplicationError::ContentLockPathConflict {
            guarded_path: "/priv/locks.app/content/already-owned.txt".to_owned(),
        });

        assert_eq!(api_error.status_code(), StatusCode::CONFLICT);
        let json = serde_json::to_value(api_error.error_response()).unwrap();
        assert_eq!(
            json,
            json!({
                "error": {
                    "code": "content_lock_path_conflict",
                    "message": "content lock path is already owned"
                }
            })
        );
        assert!(!json.to_string().contains("already-owned.txt"));
    }

    #[test]
    fn content_lock_deletion_cutoff_maps_to_409_stable_envelope() {
        let api_error = ApiError::from(ApplicationError::ContentLockDeletionInProgress);

        assert_eq!(api_error.status_code(), StatusCode::CONFLICT);
        assert_eq!(
            serde_json::to_value(api_error.error_response()).unwrap(),
            json!({
                "error": {
                    "code": "content_lock_deletion_in_progress",
                    "message": "content lock deletion is in progress"
                }
            })
        );
    }

    #[test]
    fn invalid_guarded_resource_maps_to_400_stable_envelope() {
        let api_error = ApiError::from(ApplicationError::InvalidGuardedResource {
            message: "bad mime".to_owned(),
        });

        assert_eq!(api_error.status_code(), StatusCode::BAD_REQUEST);
        let json = serde_json::to_value(api_error.error_response()).unwrap();
        assert_eq!(
            json,
            json!({
                "error": {
                    "code": "invalid_request",
                    "message": "invalid guarded resource"
                }
            })
        );
    }

    #[test]
    fn invalid_paykit_payment_submission_maps_to_400_stable_envelope() {
        let api_error = ApiError::from(ApplicationError::InvalidPaykitPaymentSubmission);

        assert_eq!(api_error.status_code(), StatusCode::BAD_REQUEST);
        let json = serde_json::to_value(api_error.error_response()).unwrap();
        assert_eq!(
            json,
            json!({
                "error": {
                    "code": "invalid_request",
                    "message": "invalid paykit payment submission"
                }
            })
        );
    }

    #[test]
    fn creator_authority_errors_map_to_503_without_leaking_secret_details() {
        let cases = [
            ApplicationError::CreatorAuthorityUnavailable,
            ApplicationError::CreatorAuthoritySecret {
                message: "raw legacy-cookie-session-secret failed to restore".to_owned(),
            },
            ApplicationError::InvalidCreatorAuthorityAuthKind {
                auth_kind: "legacy-cookie-session-secret".to_owned(),
            },
        ];

        for error in cases {
            let api_error = ApiError::from(error);

            assert_eq!(api_error.status_code(), StatusCode::SERVICE_UNAVAILABLE);
            let json = serde_json::to_value(api_error.error_response()).unwrap();
            assert_eq!(json["error"]["code"], "creator_authority_unavailable");
            assert_eq!(json["error"]["message"], "creator authority unavailable");
            assert!(!json.to_string().contains("legacy-cookie-session-secret"));
        }
    }

    #[test]
    fn terminal_task_transition_maps_to_conflict() {
        let api_error = ApiError::from(ApplicationError::InvalidVerificationTaskTransition {
            from: VerificationTaskStatus::Completed,
            to: VerificationTaskStatus::Completed,
        });

        assert_eq!(api_error.status_code(), StatusCode::CONFLICT);
        assert_eq!(api_error.error_response().error.code, "task_state_conflict");
    }
}
