use axum::Router;
use axum::extract::{Request, State};
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
        .route("/access-credentials", post(issue_access_credential))
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
            "/proof-bundles",
            post(submit_proof_bundle).options(auth_preflight),
        )
        .route(
            "/proof-bundles/auth-status",
            post(lookup_folded_auth_status).options(auth_preflight),
        )
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_cors))
        .with_state(state);
    public.merge(auth)
}

async fn auth_preflight() -> StatusCode {
    StatusCode::NO_CONTENT
}

pub(crate) fn auth_origin_allowed(allowed_origins: &[String], origin: &str) -> bool {
    let Ok(parsed) = url::Url::parse(origin) else {
        return false;
    };
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.origin().ascii_serialization() != origin
    {
        return false;
    }
    allowed_origins.iter().any(|allowed| allowed == origin)
}

async fn auth_cors(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let is_submission = request.uri().path() == "/proof-bundles";
    let mut origins = request.headers().get_all(header::ORIGIN).iter();
    let origin = origins.next().cloned();
    let duplicate_origin = origins.next().is_some();
    let allowed = !duplicate_origin
        && (is_submission
            || origin.as_ref().is_none_or(|origin| {
                let Ok(origin) = origin.to_str() else {
                    return false;
                };
                state
                    .config()
                    .creator_authority_acquisition
                    .grant_connect
                    .as_ref()
                    .is_some_and(|grant| auth_origin_allowed(&grant.allowed_return_origins, origin))
            }));
    if !allowed {
        let mut response = StatusCode::FORBIDDEN.into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return response;
    }
    let is_preflight = request.method() == Method::OPTIONS;
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
                HeaderValue::from_static("authorization, content-type"),
            );
        }
    }
    response
}

#[cfg(test)]
mod tests;
