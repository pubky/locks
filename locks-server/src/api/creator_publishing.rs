use axum::Json;
use axum::body::{Body, to_bytes};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use locks_service::application::use_cases::create_content_lock::{
    CreateContentLockRequest, CreateContentLockUseCase,
};
use locks_service::application::use_cases::delete_guarded_resource::{
    DeleteGuardedResourceRequest, DeleteGuardedResourceUseCase,
};
use locks_service::application::use_cases::register_guarded_resource::{
    RegisterGuardedResourceRequest, RegisterGuardedResourceUseCase,
};
use locks_service::application::use_cases::set_lock_service_pointer::{
    SetLockServicePointerRequest, SetLockServicePointerUseCase,
};

use crate::api::auth::authenticated_creator_from_headers;
use crate::api::dtos::{
    AuthenticatedCreateContentLockHttpRequest, AuthenticatedSetLockServicePointerHttpRequest,
    CreateContentLockHttpResponse, RegisterGuardedResourceHttpResponse,
    SetLockServicePointerHttpResponse,
};
use crate::api::errors::{ApiError, ApiErrorCode};
use crate::api::extractors::{guarded_resource_path_from_tail, parse_json};
use crate::app_state::AppState;

pub(super) async fn register_guarded_resource_empty_tail_for_authenticated_creator(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<RegisterGuardedResourceHttpResponse>, ApiError> {
    let _ = authenticated_creator_from_headers(&state, &headers).await?;
    Err(ApiError::new(
        ApiErrorCode::InvalidRequest,
        "guarded resource content tail must not be empty",
    ))
}

pub(super) async fn register_guarded_resource_for_authenticated_creator(
    State(state): State<AppState>,
    Path(tail): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<RegisterGuardedResourceHttpResponse>, ApiError> {
    let authenticated_creator = authenticated_creator_from_headers(&state, &headers).await?;
    let path = guarded_resource_path_from_tail(&tail)?;
    let content_type = parse_upload_content_type(&headers)?;
    let bytes = to_bytes(body, state.config().content_locks.max_resource_bytes)
        .await
        .map_err(|_| {
            ApiError::new(
                ApiErrorCode::PayloadTooLarge,
                "guarded resource exceeds max resource size",
            )
        })?
        .to_vec();
    let use_case = RegisterGuardedResourceUseCase::new(state.guarded_resources().as_ref());
    let registered = use_case
        .execute(RegisterGuardedResourceRequest {
            creator: authenticated_creator,
            path,
            content_type,
            bytes,
        })
        .await?;

    Ok(Json(RegisterGuardedResourceHttpResponse {
        creator: registered.creator,
        guarded_resource: registered.guarded_resource,
    }))
}

pub(super) async fn delete_guarded_resource_for_authenticated_creator(
    State(state): State<AppState>,
    Path(tail): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let authenticated_creator = authenticated_creator_from_headers(&state, &headers).await?;
    let path = guarded_resource_path_from_tail(&tail)?;
    let use_case = DeleteGuardedResourceUseCase::new(state.guarded_resources().as_ref());
    use_case
        .execute(DeleteGuardedResourceRequest {
            creator: authenticated_creator,
            path,
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn create_content_lock_for_authenticated_creator(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Result<Json<AuthenticatedCreateContentLockHttpRequest>, JsonRejection>,
) -> Result<Json<CreateContentLockHttpResponse>, ApiError> {
    let authenticated_creator = authenticated_creator_from_headers(&state, &headers).await?;
    let request = parse_json(request)?;
    validate_content_lock_limits(&request, state.config().content_locks.clone())?;
    let use_case = CreateContentLockUseCase::new(
        state.content_locks().as_ref(),
        state.guarded_resources().as_ref(),
        state.clock().as_ref(),
    );
    let created = use_case
        .execute(CreateContentLockRequest {
            creator: authenticated_creator,
            primary_resource: request.primary_resource,
            secondary_resources: request.secondary_resources,
            criteria: request.criteria,
            lock_logic: request.lock_logic,
            access_policy: request.access_policy,
            lock_server: request.lock_server,
        })
        .await?;

    Ok(Json(CreateContentLockHttpResponse {
        lock_id: created.lock_id,
        content_lock_path: created.content_lock_path,
        content_lock: created.content_lock,
    }))
}

pub(super) async fn set_lock_service_pointer_for_authenticated_creator(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Result<Json<AuthenticatedSetLockServicePointerHttpRequest>, JsonRejection>,
) -> Result<Json<SetLockServicePointerHttpResponse>, ApiError> {
    let authenticated_creator = authenticated_creator_from_headers(&state, &headers).await?;
    let request = parse_json(request)?;
    let use_case = SetLockServicePointerUseCase::new(
        state.lock_service_pointers().as_ref(),
        state.clock().as_ref(),
    );
    let response = use_case
        .execute(SetLockServicePointerRequest {
            creator: authenticated_creator,
            default_lock_server: request.default_lock_server,
        })
        .await?;

    Ok(Json(SetLockServicePointerHttpResponse {
        creator: response.creator,
        path: response.path,
        lock_service_pointer: response.lock_service_pointer,
    }))
}

fn parse_upload_content_type(headers: &HeaderMap) -> Result<String, ApiError> {
    let value = headers
        .get(header::CONTENT_TYPE)
        .ok_or_else(|| ApiError::new(ApiErrorCode::InvalidRequest, "missing content-type"))?
        .to_str()
        .map_err(|_| ApiError::new(ApiErrorCode::InvalidRequest, "invalid content-type"))?;
    value
        .parse::<mime::Mime>()
        .map_err(|_| ApiError::new(ApiErrorCode::InvalidRequest, "invalid content-type"))?;
    Ok(value.to_owned())
}

fn validate_content_lock_limits(
    request: &AuthenticatedCreateContentLockHttpRequest,
    limits: crate::config::ContentLocksConfig,
) -> Result<(), ApiError> {
    let primary_count = usize::from(request.primary_resource.is_some());
    let resource_count = primary_count + request.secondary_resources.len();
    if resource_count > limits.max_resources {
        return Err(ApiError::new(
            ApiErrorCode::InvalidRequest,
            format!(
                "content lock must include at most {} resources",
                limits.max_resources
            ),
        ));
    }

    let mut total_size = 0_u64;
    if let Some(primary_resource) = &request.primary_resource {
        validate_descriptor_size(primary_resource.size, &limits)?;
        total_size = total_size.saturating_add(primary_resource.size);
    }
    for secondary_resource in request.secondary_resources.values() {
        validate_descriptor_size(secondary_resource.size, &limits)?;
        total_size = total_size.saturating_add(secondary_resource.size);
    }
    if total_size > limits.max_total_resource_bytes {
        return Err(ApiError::new(
            ApiErrorCode::InvalidRequest,
            format!(
                "content lock total resource size exceeds {} bytes",
                limits.max_total_resource_bytes
            ),
        ));
    }

    Ok(())
}

fn validate_descriptor_size(
    size: u64,
    limits: &crate::config::ContentLocksConfig,
) -> Result<(), ApiError> {
    if size > limits.max_resource_bytes as u64 {
        return Err(ApiError::new(
            ApiErrorCode::InvalidRequest,
            "guarded resource size exceeds max_resource_bytes",
        ));
    }
    Ok(())
}
