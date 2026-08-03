use axum::Router;
use axum::routing::{delete, get, post, put};
use tower_http::cors::CorsLayer;

use crate::api::access::{issue_access_credential, proxy_read_guarded_resource};
use crate::api::creator_authority::{
    connect_shell_complete, connect_shell_start, creator_authority_status_route,
    exchange_frontend_session_code_route, frontend_session_signout_route,
};
use crate::api::creator_publishing::{
    create_content_lock_for_authenticated_creator,
    delete_guarded_resource_for_authenticated_creator,
    register_guarded_resource_empty_tail_for_authenticated_creator,
    register_guarded_resource_for_authenticated_creator,
    set_lock_service_pointer_for_authenticated_creator,
};
use crate::api::runtime::{healthz, readyz, well_known_locks_server};
use crate::api::verification::{
    complete_verification_task, lookup_verification_task, submit_proof_bundle,
};
use crate::app_state::AppState;

pub fn router(state: AppState) -> Router {
    let expose_dev_completion_route = state.config().runtime.environment.is_development();
    let expose_hosted_creator_connect_routes = state.config().creator_authority_acquisition.enabled;
    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/.well-known/locks-server", get(well_known_locks_server))
        .route("/proof-bundles", post(submit_proof_bundle))
        .route("/verification-task-lookups", post(lookup_verification_task))
        .route("/access-credentials", post(issue_access_credential))
        .route(
            "/creator/authority-status",
            get(creator_authority_status_route),
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
    router.with_state(state).layer(CorsLayer::very_permissive())
}

#[cfg(test)]
mod tests;
