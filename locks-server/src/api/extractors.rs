use axum::Json;
use percent_encoding::percent_decode_str;

use axum::extract::rejection::JsonRejection;

use crate::api::errors::{ApiError, ApiErrorCode};

pub(super) fn parse_json<T>(request: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    let Json(request) =
        request.map_err(|_| ApiError::new(ApiErrorCode::InvalidRequest, "invalid request"))?;
    Ok(request)
}

pub(super) fn guarded_resource_path_from_tail(raw_tail: &str) -> Result<String, ApiError> {
    let tail = percent_decode_str(raw_tail).decode_utf8().map_err(|_| {
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            "invalid guarded resource path",
        )
    })?;
    let path = format!("/priv/locks.app/content/{tail}");
    if path == "/priv/locks.app/content/"
        || path.contains("..")
        || path.contains("//")
        || path.contains("://")
    {
        return Err(ApiError::new(
            ApiErrorCode::InvalidRequest,
            "invalid guarded resource path",
        ));
    }
    Ok(path)
}
