use axum::Json;
use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use locks_core::ids::{BundleId, CreatorPubky};
use locks_service::application::models::{
    AccessCredential, FoldedAuthStateHash, READER_SESSION_IDLE_TTL_SECONDS, ReaderSessionTokenHash,
};
use locks_service::application::use_cases::issue_access_credential::{
    IssueAccessCredentialRequest, IssueAccessCredentialUseCase,
};
use locks_service::application::use_cases::proxy_read_guarded_resource::{
    ProxiedGuardedResource, ProxyReadGuardedResourceRequest, ProxyReadGuardedResourceUseCase,
};

use crate::api::dtos::{IssueAccessCredentialHttpRequest, IssueAccessCredentialHttpResponse};
use crate::api::errors::{ApiError, ApiErrorCode};
use crate::api::extractors::{guarded_resource_path_from_tail, parse_json};
use crate::app_state::AppState;

pub(super) async fn issue_access_credential(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
    request: Result<Json<IssueAccessCredentialHttpRequest>, JsonRejection>,
) -> Result<Json<IssueAccessCredentialHttpResponse>, ApiError> {
    let request = parse_json(request)?;
    let authenticated_reader =
        authenticate_access_reader(&state, &headers, &request.creator, &request.bundle_id).await?;
    let use_case = IssueAccessCredentialUseCase::new(
        state.entitlements().as_ref(),
        state.content_locks().as_ref(),
        state.access_credentials().as_ref(),
        state.credential_generator().as_ref(),
        state.clock().as_ref(),
        state.access_credential_policy(),
    );
    let issued = use_case
        .execute_authenticated(
            IssueAccessCredentialRequest {
                creator: request.creator,
                bundle_id: request.bundle_id,
            },
            authenticated_reader,
        )
        .await?;

    Ok(Json(IssueAccessCredentialHttpResponse::from(issued)))
}

async fn authenticate_access_reader(
    state: &AppState,
    headers: &HeaderMap,
    creator: &CreatorPubky,
    bundle_id: &BundleId,
) -> Result<Option<CreatorPubky>, ApiError> {
    let invalid = || {
        ApiError::new(
            ApiErrorCode::ReaderSessionUnavailable,
            "reader session unavailable",
        )
    };
    let bearer = crate::api::auth::parse_viewer_access_bearer(headers, invalid)?;
    let Some(bearer) = bearer else {
        return Ok(None);
    };
    if let crate::api::auth::ViewerAccessBearer::Reader(token) = bearer {
        let reader_origin = crate::api::reader_auth::extract_allowed_reader_origin(state, headers)?;
        let session = state
            .reader_auth()
            .ok_or_else(&invalid)?
            .validate_and_touch(
                &ReaderSessionTokenHash::derive(&token),
                &reader_origin,
                READER_SESSION_IDLE_TTL_SECONDS,
            )
            .await?
            .ok_or_else(invalid)?;
        return Ok(Some(session.reader));
    }

    let crate::api::auth::ViewerAccessBearer::Folded(bearer) = bearer else {
        unreachable!("Reader bearer returned above")
    };

    let attempt = state
        .folded_auth()
        .ok_or_else(invalid)?
        .get_live_accepted_by_state_hash_and_handle(
            &FoldedAuthStateHash::derive(bearer.as_bytes()),
            creator,
            bundle_id,
        )
        .await?
        .ok_or_else(invalid)?;
    debug_assert_eq!(&attempt.creator, creator);

    if let Some(task) = state
        .mixed_tasks()
        .get_mixed_task_by_handle(creator, bundle_id)
        .await?
    {
        return Ok(Some(task.snapshot.as_v1().reader.clone()));
    }
    if let Some(task) = state
        .graph_tasks()
        .get_graph_task_by_handle(creator, bundle_id)
        .await?
    {
        return Ok(Some(task.snapshot.as_v1().reader.clone()));
    }
    Err(invalid())
}

pub(super) async fn proxy_read_guarded_resource(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(tail): axum::extract::Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    reject_query_credential(&uri)?;
    let path = guarded_resource_path_from_tail(&tail)?;
    let credential = parse_bearer_credential(&headers)?;
    let use_case = ProxyReadGuardedResourceUseCase::new(
        state.access_credentials().as_ref(),
        state.entitlements().as_ref(),
        state.content_locks().as_ref(),
        state.guarded_resources().as_ref(),
        state.clock().as_ref(),
    );
    let proxied = use_case
        .execute(ProxyReadGuardedResourceRequest { credential, path })
        .await?;
    let response = match build_proxy_read_response(&proxied) {
        Ok(response) => response,
        Err(error) => {
            use_case.release_prepared_deletion_read(&proxied).await?;
            return Err(error);
        }
    };
    if method == Method::HEAD {
        use_case.release_prepared_deletion_read(&proxied).await?;
    } else {
        use_case.consume_prepared_deletion_read(&proxied).await?;
    }
    Ok(response)
}

fn build_proxy_read_response(proxied: &ProxiedGuardedResource) -> Result<Response, ApiError> {
    let content_type = HeaderValue::from_str(&proxied.content_type).map_err(|_| {
        ApiError::new(
            ApiErrorCode::InternalError,
            "stored guarded resource content type is invalid",
        )
    })?;

    let content_length = proxied.bytes.len().to_string();
    let hash = serde_json::to_value(proxied.hash)
        .map_err(|_| ApiError::new(ApiErrorCode::InternalError, "invalid etag"))?
        .as_str()
        .ok_or_else(|| ApiError::new(ApiErrorCode::InternalError, "invalid etag"))?
        .to_owned();
    let etag = format!("\"{hash}\"");
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&content_length).map_err(|_| {
                    ApiError::new(ApiErrorCode::InternalError, "invalid content length")
                })?,
            ),
            (
                header::ETAG,
                HeaderValue::from_str(&etag)
                    .map_err(|_| ApiError::new(ApiErrorCode::InternalError, "invalid etag"))?,
            ),
        ],
        Body::from(proxied.bytes.clone()),
    )
        .into_response())
}

fn reject_query_credential(uri: &Uri) -> Result<(), ApiError> {
    let Some(query) = uri.query() else {
        return Ok(());
    };
    if query.split('&').any(|part| {
        part.split_once('=')
            .map_or(part, |(key, _)| key)
            .eq_ignore_ascii_case("credential")
    }) {
        return Err(ApiError::new(
            ApiErrorCode::InvalidAccessCredential,
            "invalid access credential",
        ));
    }
    Ok(())
}

fn parse_bearer_credential(headers: &HeaderMap) -> Result<AccessCredential, ApiError> {
    let invalid = || {
        ApiError::new(
            ApiErrorCode::InvalidAccessCredential,
            "invalid access credential",
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
    Ok(AccessCredential::new(token.to_owned()))
}
