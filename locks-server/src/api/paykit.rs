use axum::Json;
use axum::body::to_bytes;
use axum::extract::{Request, State};

use crate::api::auth::authenticated_creator_from_headers;
use crate::api::dtos::PaykitSetupStatusHttpResponse;
use crate::api::errors::{ApiError, ApiErrorCode};
use crate::app_state::AppState;
use crate::paykit_http_client::PaykitSetupStatusKind;

pub(super) async fn paykit_setup_status_for_authenticated_creator(
    State(state): State<AppState>,
    request: Request,
) -> Result<Json<PaykitSetupStatusHttpResponse>, ApiError> {
    let creator = authenticated_creator_from_headers(&state, request.headers()).await?;
    if request.uri().query().is_some() {
        return Err(ApiError::new(
            ApiErrorCode::InvalidRequest,
            "invalid request",
        ));
    }
    let body = to_bytes(request.into_body(), 1)
        .await
        .map_err(|_| ApiError::new(ApiErrorCode::InvalidRequest, "invalid request"))?;
    if !body.is_empty() {
        return Err(ApiError::new(
            ApiErrorCode::InvalidRequest,
            "invalid request",
        ));
    }

    let status = match state.paykit_setup_status_provider() {
        Some(provider) => provider
            .setup_status(&creator)
            .await
            .unwrap_or(PaykitSetupStatusKind::Unavailable),
        None => PaykitSetupStatusKind::Unavailable,
    };

    Ok(Json(PaykitSetupStatusHttpResponse { status }))
}
