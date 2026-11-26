use axum::http::{HeaderMap, header};
use locks_core::ids::CreatorPubky;
use locks_service::application::models::FrontendSessionToken;
use locks_service::application::models::ReaderSessionToken;
use locks_service::application::use_cases::validate_frontend_session::{
    ValidateFrontendSessionRequest, validate_frontend_session,
};

use crate::api::errors::{ApiError, ApiErrorCode};
use crate::app_state::AppState;

pub(crate) enum ViewerAccessBearer {
    Reader(ReaderSessionToken),
    Folded(String),
}

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
    parse_bearer_token(headers, invalid).map(FrontendSessionToken::new)
}

pub(crate) fn parse_bearer_token(
    headers: &HeaderMap,
    invalid: impl Fn() -> ApiError,
) -> Result<String, ApiError> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next().ok_or_else(&invalid)?;
    if values.next().is_some() {
        return Err(invalid());
    }
    let value = value.to_str().map_err(|_| invalid())?;
    let mut parts = value.split(' ');
    let scheme = parts.next().ok_or_else(&invalid)?;
    let token = parts.next().ok_or_else(&invalid)?;
    if parts.next().is_some() || !scheme.eq_ignore_ascii_case("Bearer") || token.is_empty() {
        return Err(invalid());
    }
    Ok(token.to_owned())
}

pub(crate) fn parse_viewer_access_bearer(
    headers: &HeaderMap,
    invalid: impl Fn() -> ApiError,
) -> Result<Option<ViewerAccessBearer>, ApiError> {
    if !headers.contains_key(header::AUTHORIZATION) {
        return Ok(None);
    }
    let token = parse_bearer_token(headers, &invalid)?;
    classify_viewer_access_bearer(&token)
        .map(Some)
        .ok_or_else(invalid)
}

pub(crate) fn classify_viewer_access_bearer(token: &str) -> Option<ViewerAccessBearer> {
    if uuid::Uuid::parse_str(token).is_ok_and(|state| state.to_string() == token) {
        return Some(ViewerAccessBearer::Folded(token.to_owned()));
    }

    use base64::Engine;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .ok()?;
    (decoded.len() == 32
        && base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(decoded) == token)
        .then(|| ViewerAccessBearer::Reader(ReaderSessionToken::new(token)))
}
