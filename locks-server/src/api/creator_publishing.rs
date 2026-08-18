use axum::Json;
use axum::body::{Body, to_bytes};
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use locks_core::ids::{ContentLockPath, CreatorPubky, LockId};
use locks_service::application::models::{
    ContentLockDeletionFailureCode, ContentLockDeletionJob, ContentLockDeletionState,
    PrepareForceDeletionResult,
};
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
use serde::Deserialize;
use serde_json::{Value, json};
use std::str::FromStr;
use uuid::Uuid;

use crate::api::auth::authenticated_creator_from_headers;
use crate::api::dtos::{
    AuthenticatedCreateContentLockHttpRequest, AuthenticatedSetLockServicePointerHttpRequest,
    ContentLockDeletionStatusHttpResponse, CreateContentLockHttpResponse,
    RegisterGuardedResourceHttpResponse, SetLockServicePointerHttpResponse,
};
use crate::api::errors::{ApiError, ApiErrorCode};
use crate::api::extractors::{guarded_resource_path_from_tail, parse_json};
use crate::app_state::AppState;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeleteContentLockQuery {
    force: Option<bool>,
    graceful: Option<bool>,
}

pub(super) async fn delete_content_lock_for_authenticated_creator(
    State(state): State<AppState>,
    Path(lock_id): Path<String>,
    headers: HeaderMap,
    query: Result<Query<DeleteContentLockQuery>, QueryRejection>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let creator = authenticated_creator_from_headers(&state, &headers).await?;
    let Query(query) =
        query.map_err(|_| ApiError::new(ApiErrorCode::InvalidRequest, "invalid request"))?;
    let force = match (query.force, query.graceful) {
        (None, None | Some(true)) => false,
        (Some(true), None) => true,
        _ => {
            return Err(ApiError::new(
                ApiErrorCode::InvalidRequest,
                "invalid request",
            ));
        }
    };
    let lock_id = LockId::from_str(&lock_id)
        .map_err(|_| ApiError::new(ApiErrorCode::InvalidIdentifier, "invalid lock id"))?;

    if force {
        return force_delete_content_lock(&state, creator, lock_id).await;
    }

    if state
        .content_lock_deletions()
        .has_force_receipt(&creator, &lock_id)
        .await?
    {
        return Ok((
            StatusCode::OK,
            Json(json!({ "lock_id": lock_id, "status": "completed" })),
        ));
    }

    if let Some(job) = state
        .content_lock_deletions()
        .get_job(&creator, &lock_id)
        .await?
    {
        let job = if job.state == ContentLockDeletionState::Failed {
            match state
                .content_lock_deletions()
                .resume_failed_job(&creator, &lock_id, state.clock().now())
                .await?
            {
                Some(resumed) => resumed,
                None if state
                    .content_lock_deletions()
                    .has_force_receipt(&creator, &lock_id)
                    .await? =>
                {
                    return Ok((
                        StatusCode::OK,
                        Json(json!({ "lock_id": lock_id, "status": "completed" })),
                    ));
                }
                None => job,
            }
        } else {
            job
        };
        let status = match job.state {
            ContentLockDeletionState::Queued | ContentLockDeletionState::Running => {
                StatusCode::ACCEPTED
            }
            ContentLockDeletionState::Completed | ContentLockDeletionState::Failed => {
                StatusCode::OK
            }
        };
        return Ok((
            status,
            Json(deletion_status_json(lock_id, job.state, job.failure_code)),
        ));
    }

    let path = ContentLockPath::from_lock_id(lock_id.clone());
    let Some(content_lock) = state
        .content_locks()
        .get_content_lock(&creator, &path)
        .await?
    else {
        if state
            .content_lock_deletions()
            .publication_in_progress(&creator, &lock_id)
            .await?
        {
            return Err(ApiError::new(
                ApiErrorCode::ContentLockPathConflict,
                "content lock publication is in progress",
            ));
        }
        return Ok((
            StatusCode::OK,
            Json(json!({ "lock_id": lock_id, "status": "completed" })),
        ));
    };

    validate_content_lock_identity(&content_lock, &creator, &lock_id)?;

    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock, state.clock().now())?;
    match state.content_lock_deletions().insert_job(job.clone()).await {
        Ok(()) => Ok((
            StatusCode::ACCEPTED,
            Json(deletion_status_json(lock_id, job.state, job.failure_code)),
        )),
        Err(locks_service::application::errors::ApplicationError::DuplicateRecord {
            record: "content_lock_deletion_job",
        }) => {
            let persisted = state
                .content_lock_deletions()
                .get_job(&creator, &lock_id)
                .await?
                .ok_or_else(|| {
                    ApiError::new(
                        ApiErrorCode::InternalError,
                        "content lock deletion unavailable",
                    )
                })?;
            let status = match persisted.state {
                ContentLockDeletionState::Queued | ContentLockDeletionState::Running => {
                    StatusCode::ACCEPTED
                }
                ContentLockDeletionState::Completed | ContentLockDeletionState::Failed => {
                    StatusCode::OK
                }
            };
            Ok((
                status,
                Json(deletion_status_json(
                    lock_id,
                    persisted.state,
                    persisted.failure_code,
                )),
            ))
        }
        Err(
            locks_service::application::errors::ApplicationError::ContentLockDeletionInProgress,
        ) if state
            .content_lock_deletions()
            .has_force_receipt(&creator, &lock_id)
            .await? =>
        {
            Ok((
                StatusCode::OK,
                Json(json!({ "lock_id": lock_id, "status": "completed" })),
            ))
        }
        Err(error) => Err(error.into()),
    }
}

async fn force_delete_content_lock(
    state: &AppState,
    creator: CreatorPubky,
    lock_id: LockId,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let path = ContentLockPath::from_lock_id(lock_id.clone());
    let existing_job = state
        .content_lock_deletions()
        .get_job(&creator, &lock_id)
        .await?;
    let published_content_lock = if existing_job.is_none() {
        state
            .content_locks()
            .get_content_lock(&creator, &path)
            .await?
    } else {
        None
    };
    if let Some(content_lock) = published_content_lock.as_ref() {
        validate_content_lock_identity(content_lock, &creator, &lock_id)?;
    }
    let content_lock = match state
        .content_lock_deletions()
        .prepare_force_deletion(&creator, &lock_id, state.clock().now())
        .await?
    {
        PrepareForceDeletionResult::PublicationInProgress => {
            return Err(ApiError::new(
                ApiErrorCode::ContentLockPathConflict,
                "content lock publication is in progress",
            ));
        }
        PrepareForceDeletionResult::Active(job) => {
            return Ok((
                StatusCode::ACCEPTED,
                Json(deletion_status_json(lock_id, job.state, job.failure_code)),
            ));
        }
        PrepareForceDeletionResult::Synchronous(Some(job)) => Some(job.frozen_content_lock),
        PrepareForceDeletionResult::Synchronous(None) => published_content_lock,
    };

    if let Some(content_lock) = content_lock.as_ref() {
        validate_content_lock_identity(content_lock, &creator, &lock_id)?;
    }

    state
        .content_locks()
        .delete_content_lock(&creator, &path)
        .await?;
    if state
        .content_locks()
        .get_content_lock(&creator, &path)
        .await?
        .is_some()
    {
        return Err(ApiError::new(
            ApiErrorCode::InternalError,
            "content lock deletion postcondition failed",
        ));
    }

    let mut failed_resource_paths = Vec::new();
    if let Some(content_lock) = content_lock {
        let mut resource_paths = content_lock
            .secondary_resources
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        if let Some(primary) = content_lock.primary_resource {
            resource_paths.push(primary.path);
        }
        resource_paths.sort();
        resource_paths.dedup();
        for resource_path in resource_paths {
            if state
                .guarded_resources()
                .delete_guarded_resource(&creator, &resource_path)
                .await
                .is_err()
            {
                failed_resource_paths.push(resource_path);
            }
        }
    }

    Ok((
        StatusCode::OK,
        Json(json!({
            "lock_id": lock_id,
            "lock_deleted": true,
            "failed_resource_paths": failed_resource_paths
        })),
    ))
}

fn validate_content_lock_identity(
    content_lock: &locks_core::lock_policy::ContentLock,
    creator: &CreatorPubky,
    expected_lock_id: &LockId,
) -> Result<(), ApiError> {
    let actual_lock_id = content_lock
        .lock_id()
        .map_err(|_| ApiError::new(ApiErrorCode::InternalError, "content lock unavailable"))?;
    if &content_lock.creator != creator || &actual_lock_id != expected_lock_id {
        return Err(ApiError::new(
            ApiErrorCode::InternalError,
            "content lock unavailable",
        ));
    }
    Ok(())
}

fn deletion_status_json(
    lock_id: LockId,
    state: ContentLockDeletionState,
    failure_code: Option<ContentLockDeletionFailureCode>,
) -> Value {
    serde_json::to_value(deletion_status_response(lock_id, state, failure_code))
        .expect("deletion status response must serialize")
}

pub(super) async fn get_content_lock_deletion_status_for_authenticated_creator(
    State(state): State<AppState>,
    Path(lock_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ContentLockDeletionStatusHttpResponse>, ApiError> {
    let creator = authenticated_creator_from_headers(&state, &headers).await?;
    let lock_id = LockId::from_str(&lock_id)
        .map_err(|_| ApiError::new(ApiErrorCode::InvalidIdentifier, "invalid lock id"))?;
    if state
        .content_lock_deletions()
        .has_force_receipt(&creator, &lock_id)
        .await?
    {
        return Ok(Json(ContentLockDeletionStatusHttpResponse {
            lock_id,
            status: "completed",
            failure_code: None,
        }));
    }
    if let Some(job) = state
        .content_lock_deletions()
        .get_job(&creator, &lock_id)
        .await?
    {
        return Ok(Json(deletion_status_response(
            lock_id,
            job.state,
            job.failure_code,
        )));
    }
    Err(ApiError::new(
        ApiErrorCode::ContentLockDeletionNotFound,
        "content lock deletion not found",
    ))
}

fn deletion_status_response(
    lock_id: LockId,
    state: ContentLockDeletionState,
    failure_code: Option<ContentLockDeletionFailureCode>,
) -> ContentLockDeletionStatusHttpResponse {
    let status = match state {
        ContentLockDeletionState::Queued => "queued",
        ContentLockDeletionState::Running => "running",
        ContentLockDeletionState::Completed => "completed",
        ContentLockDeletionState::Failed => "failed",
    };
    ContentLockDeletionStatusHttpResponse {
        lock_id,
        status,
        failure_code: failure_code.map(|code| code.as_str().to_owned()),
    }
}

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
        state.content_lock_deletions().as_ref(),
        state.content_lock_ownership().as_ref(),
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
