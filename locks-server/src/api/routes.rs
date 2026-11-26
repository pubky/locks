use axum::Router;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::{delete, get, post, put};
use tower_http::cors::CorsLayer;

use crate::api::access::{issue_access_credential, proxy_read_guarded_resource};
use crate::api::creator_authority::{
    connect_shell_complete, connect_shell_start, creator_authority_status_route,
    exchange_frontend_session_code_route, frontend_session_signout_route,
};
use crate::api::creator_publishing::{
    create_content_lock_for_authenticated_creator, delete_content_lock_for_authenticated_creator,
    delete_guarded_resource_for_authenticated_creator,
    get_content_lock_deletion_status_for_authenticated_creator,
    register_guarded_resource_empty_tail_for_authenticated_creator,
    register_guarded_resource_for_authenticated_creator,
    set_lock_service_pointer_for_authenticated_creator,
};
use crate::api::paykit::paykit_setup_status_for_authenticated_creator;
use crate::api::reader_auth::{
    create_reader_auth_challenge, create_reader_session, delete_reader_session,
};
use crate::api::runtime::{healthz, readyz, well_known_locks_server};
use crate::api::verification::{
    complete_verification_task, creator_verification_evidence, lookup_folded_auth_status,
    lookup_paykit_connection_state, lookup_verification_task, submit_proof_bundle,
};
use crate::app_state::AppState;

pub fn router(state: AppState) -> Router {
    let expose_dev_completion_route = state.config().runtime.environment.is_development();
    let expose_hosted_creator_connect_routes = state.config().creator_authority_acquisition.enabled;
    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/.well-known/locks-server", get(well_known_locks_server))
        .route("/verification-task-lookups", post(lookup_verification_task))
        .route(
            "/paykit-connection-state-lookups",
            post(lookup_paykit_connection_state),
        )
        .route(
            "/creator/authority-status",
            get(creator_authority_status_route),
        )
        .route(
            "/creator/paykit/setup-status",
            get(paykit_setup_status_for_authenticated_creator),
        )
        .route(
            "/creator/verification-evidence",
            post(creator_verification_evidence),
        )
        .route(
            "/priv-resources/content/{*tail}",
            get(proxy_read_guarded_resource),
        );

    let router = if expose_dev_completion_route {
        router.route(
            "/verification-task-completions",
            post(complete_verification_task),
        )
    } else {
        router
    };

    let router = router
        .route(
            "/creator/priv-resources/content",
            put(register_guarded_resource_empty_tail_for_authenticated_creator),
        )
        .route(
            "/creator/priv-resources/content/{*tail}",
            put(register_guarded_resource_for_authenticated_creator),
        )
        .route(
            "/creator/priv-resources/content/{*tail}",
            delete(delete_guarded_resource_for_authenticated_creator),
        )
        .route(
            "/creator/content-locks",
            post(create_content_lock_for_authenticated_creator),
        )
        .route(
            "/creator/content-locks/{lock_id}",
            delete(delete_content_lock_for_authenticated_creator),
        )
        .route(
            "/creator/content-locks/{lock_id}/deletion",
            get(get_content_lock_deletion_status_for_authenticated_creator),
        )
        .route(
            "/creator/lock-service-config",
            post(set_lock_service_pointer_for_authenticated_creator),
        );

    let router = if expose_hosted_creator_connect_routes {
        router
            .route("/connect", get(connect_shell_start))
            .route("/connect/{flow_id}/complete", post(connect_shell_complete))
            .route(
                "/frontend-sessions",
                post(exchange_frontend_session_code_route),
            )
            .route(
                "/frontend-sessions/current",
                delete(frontend_session_signout_route),
            )
    } else {
        router
    };

    // PROD REVIEW: `very_permissive` reflects any Origin and allows any method/header, with no
    // environment gating — every caller is trusted. Fine for local/dev cross-origin (pubky-app →
    // Lock Server), but before a production deploy decide whether to restrict this to the known
    // frontend origins (reuse `allowed_return_origins`, or a dedicated CORS allowlist).
    let public = router
        .with_state(state.clone())
        .layer(CorsLayer::very_permissive());
    let auth = Router::new()
        .route(
            "/access-credentials",
            post(issue_access_credential).options(auth_preflight),
        )
        .route(
            "/proof-bundles",
            post(submit_proof_bundle).options(auth_preflight),
        )
        .route(
            "/proof-bundles/auth-status",
            post(lookup_folded_auth_status).options(auth_preflight),
        )
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_cors))
        .with_state(state.clone());
    let reader = Router::new()
        .route(
            "/reader-auth/challenges",
            post(create_reader_auth_challenge).options(reader_preflight),
        )
        .route(
            "/reader-sessions",
            post(create_reader_session).options(reader_preflight),
        )
        .route(
            "/reader-sessions/current",
            delete(delete_reader_session).options(reader_preflight),
        )
        .layer(DefaultBodyLimit::max(16 * 1024))
        .route_layer(middleware::from_fn_with_state(state.clone(), reader_cors))
        .with_state(state);
    public.merge(auth).merge(reader)
}

async fn reader_preflight() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn reader_cors(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let mut values = request.headers().get_all(header::ORIGIN).iter();
    let origin = values.next().cloned();
    let allowed = values.next().is_none()
        && origin.as_ref().is_some_and(|value| {
            value.to_str().ok().is_some_and(|value| {
                value != "null"
                    && crate::config::canonical_grant_origin(value).as_deref() == Some(value)
                    && state
                        .config()
                        .reader_auth
                        .allowed_origins
                        .iter()
                        .any(|allowed| allowed == value)
            })
        });
    if !allowed {
        let mut response = StatusCode::FORBIDDEN.into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return response;
    }
    let is_preflight = request.method() == Method::OPTIONS;
    let mut response = next.run(request).await;
    let response_headers = response.headers_mut();
    response_headers.remove(header::SET_COOKIE);
    response_headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response_headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        origin.expect("allowed reader origin exists"),
    );
    response_headers.insert(header::VARY, HeaderValue::from_static("Origin"));
    if is_preflight {
        response_headers.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("POST, DELETE"),
        );
        response_headers.insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("authorization, content-type, pubky-host"),
        );
    }
    response
}

async fn auth_preflight() -> StatusCode {
    StatusCode::NO_CONTENT
}

pub(crate) fn auth_origin_allowed(allowed_origins: &[String], origin: &str) -> bool {
    let Some(origin) = crate::config::canonical_grant_origin(origin) else {
        return false;
    };
    if allowed_origins.iter().any(|allowed| allowed == "*") {
        return allowed_origins == ["*"];
    }
    allowed_origins.iter().any(|allowed| allowed == &origin)
}

fn public_submission_origin_allowed(origin: &str) -> bool {
    if origin == "null" {
        return false;
    }
    let Ok(parsed) = url::Url::parse(origin) else {
        return false;
    };
    let scheme_allowed = match parsed.scheme() {
        "https" => true,
        "http" => parsed.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        }),
        _ => false,
    };
    scheme_allowed
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.query().is_none()
        && parsed.fragment().is_none()
        && parsed.origin().ascii_serialization() == origin
}

fn preflight_requests_authorization(request: &Request) -> bool {
    request
        .headers()
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|name| name.trim().eq_ignore_ascii_case("authorization"))
        })
}

async fn auth_cors(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let is_submission = request.uri().path() == "/proof-bundles";
    let is_access_credential = request.uri().path() == "/access-credentials";
    let access_bearer = is_access_credential.then(|| {
        let invalid = || {
            crate::api::errors::ApiError::new(
                crate::api::errors::ApiErrorCode::ReaderSessionUnavailable,
                "reader session unavailable",
            )
        };
        crate::api::auth::parse_viewer_access_bearer(request.headers(), invalid)
    });
    let invalid_access_bearer = matches!(access_bearer.as_ref(), Some(Err(_)));
    let is_preflight = request.method() == Method::OPTIONS;
    let requests_authorization = request.headers().contains_key(header::AUTHORIZATION)
        || is_preflight && preflight_requests_authorization(&request);
    let is_reader_submission = is_submission && requests_authorization
        || matches!(
            access_bearer.as_ref(),
            Some(Ok(Some(crate::api::auth::ViewerAccessBearer::Reader(_))))
        );
    let mut origins = request.headers().get_all(header::ORIGIN).iter();
    let origin = origins.next().cloned();
    let duplicate_origin = origins.next().is_some();
    let anonymous_access_credential = is_access_credential
        && if is_preflight {
            !requests_authorization
        } else {
            matches!(access_bearer.as_ref(), Some(Ok(None)))
        };
    let public_origin_allowed = || {
        origin.as_ref().is_none_or(|origin| {
            origin
                .to_str()
                .ok()
                .is_some_and(public_submission_origin_allowed)
        })
    };
    let reader_origin_allowed = || {
        origin.as_ref().is_some_and(|origin| {
            let Ok(origin) = origin.to_str() else {
                return false;
            };
            origin != "null"
                && crate::config::canonical_grant_origin(origin).as_deref() == Some(origin)
                && state
                    .config()
                    .reader_auth
                    .allowed_origins
                    .iter()
                    .any(|allowed| allowed == origin)
        })
    };
    let legacy_origin_allowed = || {
        origin.as_ref().is_none_or(|origin| {
            let Ok(origin) = origin.to_str() else {
                return false;
            };
            state
                .config()
                .creator_authority_acquisition
                .grant_connect
                .as_ref()
                .is_some_and(|grant| auth_origin_allowed(&grant.allowed_return_origins, origin))
        })
    };
    let allowed = !duplicate_origin
        && !invalid_access_bearer
        && if is_reader_submission {
            reader_origin_allowed()
        } else if anonymous_access_credential {
            public_origin_allowed()
        } else if is_access_credential && is_preflight {
            reader_origin_allowed() || legacy_origin_allowed()
        } else {
            is_submission || legacy_origin_allowed()
        };
    if !allowed {
        let mut response = StatusCode::FORBIDDEN.into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return response;
    }
    let mut response = next.run(request).await;
    let origin_denied = response
        .headers_mut()
        .remove("x-locks-folded-auth-origin-denied")
        .is_some();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some(origin) = origin.filter(|_| !origin_denied) {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        response.headers_mut().insert(
            header::VARY,
            HeaderValue::from_static(
                "Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
            ),
        );
        if is_preflight {
            response.headers_mut().insert(
                header::ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("POST"),
            );
            response.headers_mut().insert(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static("authorization, content-type, pubky-host"),
            );
        }
    }
    response
}

#[cfg(test)]
mod tests;
