use axum::http::{HeaderMap, header};
use locks_core::ids::CreatorPubky;
use locks_service::application::models::FrontendSessionToken;
use locks_service::application::use_cases::validate_frontend_session::{
    ValidateFrontendSessionRequest, validate_frontend_session,
};

use crate::api::errors::{ApiError, ApiErrorCode};
use crate::app_state::AppState;

pub(super) async fn authenticated_creator_from_headers(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<CreatorPubky, ApiError> {
    let session_token = parse_frontend_session_token(headers)?;
    let validated = validate_frontend_session(
        state.frontend_sessions().as_ref(),
        state.clock().as_ref(),
        ValidateFrontendSessionRequest { session_token },
    )
    .await?;
    Ok(validated.creator)
}

pub(crate) fn parse_frontend_session_token(
    headers: &HeaderMap,
) -> Result<FrontendSessionToken, ApiError> {
    let invalid = || {
        ApiError::new(
            ApiErrorCode::FrontendSessionUnavailable,
            "frontend session unavailable",
        )
    };
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next().ok_or_else(invalid)?;
    if values.next().is_some() {
        return Err(invalid());
    }
    let value = value.to_str().map_err(|_| invalid())?;
    let mut parts = value.split(' ');
    let scheme = parts.next().ok_or_else(invalid)?;
    let token = parts.next().ok_or_else(invalid)?;
    if parts.next().is_some() || !scheme.eq_ignore_ascii_case("Bearer") || token.is_empty() {
        return Err(invalid());
    }
    Ok(FrontendSessionToken::new(token.to_owned()))
}
